/**
 * Core job manager implementation.
 * 
 * Manages the lifecycle of a single running workflow job: progress reporting,
 * pushback timers, error handling, retry/reschedule logic, and outcome logging.
 * 
 * All database access goes through the injected WorkflowJobStorage adapter.
 */
import type {
    BasicJobPayload,
    WorkflowJobHeader,
    WorkflowJobKey,
    WorkflowJobValue,
    WorkflowJobOutcome,
    WorkflowJobError,
    WorkflowJobLogCustomEntry,
    BaseJobManager,
    WorkflowConfig,
} from "./workflowTypes"
import { durationMinutes } from "./workflowTypes"
import type { WorkflowJobStorage } from "./workflowStorageAdapter"
import { JobError } from "./jobErrors"
import { computeNextSchedule } from "./schedule"
import { WorkflowCounter } from "./counter"
import crypto from "crypto"

export interface JobManagerOptions<PAYLOAD_T extends BasicJobPayload> {
    jobKey: Readonly<WorkflowJobKey>
    execution_id: string
    workflow_supress_job_outcome_logs: WorkflowConfig["workflow_supress_job_outcome_logs"]
    storage: WorkflowJobStorage<PAYLOAD_T>
    suppress_error_emails?: boolean
    /**
     * Called on fatal/vanished/rescheduled-error outcomes so the consumer
     * can trigger notifications (e.g. error emails). Replaces the monorepo's
     * WorkflowDbModule.ScheduleForwardEmail.
     */
    onFatalError?: (args: {
        job: WorkflowJobValue<PAYLOAD_T>
        jobKey: WorkflowJobKey
        outcome: WorkflowJobOutcome
    }) => Promise<void>
}

type JobRunnerFn<PAYLOAD_T extends BasicJobPayload> =
    (mgr: JobManager<PAYLOAD_T>, oopE: Promise<any>) => Promise<void | number>

export class JobManager<PAYLOAD_T extends BasicJobPayload> implements BaseJobManager<PAYLOAD_T> {
    readonly outofProcessError = (() => {
        let callback: any = undefined
        const promise = new Promise<void>((_, E) => {
            callback = E
        })
        return { promise, raise: callback as (reason: any) => void }
    })()

    private completed = false
    private pushbackTimer: any = undefined
    private progressTimer: any = undefined
    private startTime = Date.now()
    private static readonly runningJobs = new Map<string, number>()
    private static totalRunningJobs = 0

    private readonly storage: WorkflowJobStorage<PAYLOAD_T>
    readonly workflow_supress_job_outcome_logs: WorkflowConfig["workflow_supress_job_outcome_logs"]
    private readonly suppress_error_emails: boolean
    private readonly onFatalError?: JobManagerOptions<PAYLOAD_T>["onFatalError"]

    private _job: Promise<{
        header: Readonly<WorkflowJobHeader>
        payload: PAYLOAD_T
        for_userspace_id: string | null
    }>
    private readonly execution_id: string
    readonly jobKey: Readonly<WorkflowJobKey>

    constructor(args: JobManagerOptions<PAYLOAD_T>) {
        this.execution_id = args.execution_id
        this.workflow_supress_job_outcome_logs = args.workflow_supress_job_outcome_logs
        this.suppress_error_emails = args.suppress_error_emails ?? false
        this.storage = args.storage
        this.jobKey = args.jobKey
        this.onFatalError = args.onFatalError
        this._job = this.GetUpdatedJob()
    }

    static get JobRunningCount() {
        return { total: this.totalRunningJobs, types: Array.from(this.runningJobs) }
    }

    async Wait(duration: number) {
        if (duration > durationMinutes(2))
            return Date.now() + duration
        const deadline = (await this.GetJob()).header.progress_deadline_ms / 2
        const progress = duration > deadline / 2
            ? setInterval(() => { this.Progress() }, deadline / 2)
            : undefined
        await new Promise(r => setTimeout(r, duration))
        if (progress) clearInterval(progress)
        return undefined
    }

    private async GetUpdatedJob(): Promise<{
        header: Readonly<WorkflowJobHeader>
        payload: PAYLOAD_T
        for_userspace_id: string | null
    }> {
        const job = await this.storage.getJob(this.jobKey)
        if (!job) {
            throw new JobError({ type: "vanished" })
        } else if (job.header.execution_id !== this.execution_id) {
            throw new JobError({ type: "readopted", by: job.header.execution_id || "undefined" })
        }
        return { ...job }
    }

    async GetJob() {
        if (this.completed) {
            throw new Error("Job not running")
        }
        if (!this.progressTimer) {
            this.ResetProgressTimer((await this._job).header.progress_deadline_ms)
        }
        const job = await this._job
        return job
    }

    private async ResetJob(options: { progress: boolean, payload?: Partial<PAYLOAD_T> }) {
        this._job = (async () => {
            const updated = await this.storage.updateJob(
                this.jobKey,
                this.execution_id,
                (job) => {
                    const amended = { ...job, header: { ...job.header } }
                    if (options.progress) amended.header.at = Date.now() + job.header.lost_deadline_ms
                    if (options.payload) amended.payload = { ...amended.payload, ...options.payload }
                    if (options.progress || !amended.header.last_progress) {
                        amended.header.last_progress = Date.now()
                    }
                    return amended
                }
            )
            if (!updated) throw new JobError({ type: "vanished" })
            return updated
        })()
        const job = await this.GetJob()
        if (options.progress) this.ResetProgressTimer(job.header.progress_deadline_ms)
        this.ResetPushBackTimer(job.header.lost_deadline_ms / 2.5)
        return job
    }

    private ResetPushBackTimer(duration: number) {
        if (this.pushbackTimer) clearTimeout(this.pushbackTimer)
        this.pushbackTimer = setTimeout(async () => {
            try {
                await this.ResetJob({ progress: false })
            } catch (e) {
                this.outofProcessError.raise(e)
            }
        }, duration)
    }

    private ResetProgressTimer(duration: number) {
        if (this.progressTimer) clearTimeout(this.progressTimer)
        this.progressTimer = setTimeout(async () => {
            this.outofProcessError.raise(new JobError({ type: "progress-deadline" }))
        }, duration)
    }

    private clearTimers() {
        if (this.progressTimer) clearTimeout(this.progressTimer)
        if (this.pushbackTimer) clearTimeout(this.pushbackTimer)
    }

    async Progress(payload?: Partial<PAYLOAD_T>) {
        return this.ResetJob({ progress: true, payload })
    }

    async Log(entry: WorkflowJobLogCustomEntry) {
        const at = Date.now()
        await this.storage.writeLog(
            { job_id: this.jobKey.job_id, timestamp: Date.now(), random: crypto.randomUUID() },
            { at, execution_id: this.execution_id, outcome: entry }
        )
    }

    async Run(
        runner: JobRunnerFn<PAYLOAD_T>,
    ): Promise<WorkflowJobOutcome> {
        const job = await this.GetJob()
        const outcome = await (async () => {
            try {
                console.debug("Workflow starting job", this.jobKey, job.header, job.payload)
                const running = (JobManager.runningJobs.get(job.payload.type) || 0) + 1
                JobManager.totalRunningJobs++
                JobManager.runningJobs.set(job.payload.type, running)

                const result = await Promise.race([
                    runner(this, this.outofProcessError.promise),
                    this.outofProcessError.promise,
                ])
                this.clearTimers()

                // Success path: clear/reschedule + post-processing in one transaction
                return await this.storage.runInTransaction(async (txn) => {
                    let outcome: WorkflowJobOutcome = { type: "success" }

                    // Post-processing hook (storage adapter's case statement)
                    if (txn.processJobOutcome) {
                        outcome = await txn.processJobOutcome({
                            outcome, payload: job.payload, jobKey: this.jobKey,
                        })
                    }

                    const currentJob = await txn.getJob(this.jobKey)
                    if (!currentJob || currentJob.header.execution_id !== this.execution_id) {
                        return outcome
                    }

                    if (result) {
                        // Runner returned a future timestamp — reschedule
                        txn.setJob(this.jobKey, {
                            ...currentJob,
                            header: { ...currentJob.header, at: result, execution_id: undefined },
                        })
                    } else {
                        // Clear the job (complete or reschedule if repeating)
                        const next_schedule = computeNextSchedule(currentJob.header)
                        if (next_schedule) {
                            console.debug("Rescheduling job for", next_schedule)
                            txn.setJob(this.jobKey, {
                                ...currentJob,
                                header: { ...currentJob.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, execution_id: undefined },
                            })
                        } else {
                            txn.setJob(this.jobKey, {
                                ...currentJob,
                                header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined },
                            })
                        }
                    }
                    return outcome
                })

            } catch (e: any) {
                this.clearTimers()

                const err: WorkflowJobError = e instanceof JobError
                    ? e.outcome
                    : e instanceof Error
                        ? { type: "custom-recoverable", message: e.message }
                        : { type: "custom-recoverable", message: String(e) }

                const config = this.workflow_supress_job_outcome_logs || ["success"]

                // Handle error within a transactional context
                return await this.storage.runInTransaction(async (txn) => {
                    const currentJob = await txn.getJobByJobId(this.jobKey.job_id)
                    if (!currentJob) return { type: "vanished" } as WorkflowJobOutcome

                    let outcome = await (async (): Promise<WorkflowJobOutcome> => {
                        switch (err.type) {
                            case "vanished":
                                // Clear the job
                                txn.setJob(this.jobKey, {
                                    ...currentJob,
                                    header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined }
                                })
                                return err
                            case "custom-recoverable":
                            case "progress-deadline":
                            case "payloadMismatch":
                                if (currentJob.header.retries.max > 0) {
                                    currentJob.header.retries.max--
                                    currentJob.header.at = Date.now() + currentJob.header.retries.initial_backoff_ms
                                    currentJob.header.retries.initial_backoff_ms *= currentJob.header.retries.exponent
                                    currentJob.header.execution_id = undefined
                                    txn.setJob(this.jobKey, currentJob)
                                    return { type: "rescheduled-error", cause: err as any }
                                } else {
                                    // Clear via schedule or negate at
                                    const next_schedule = computeNextSchedule(currentJob.header)
                                    if (next_schedule) {
                                        txn.setJob(this.jobKey, {
                                            ...currentJob,
                                            header: { ...currentJob.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, execution_id: undefined }
                                        })
                                    } else {
                                        txn.setJob(this.jobKey, {
                                            ...currentJob,
                                            header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined }
                                        })
                                    }
                                    return { type: "fatal-error", cause: err as any }
                                }
                            case "readopted":
                                return err
                            case "fatal-error":
                                // Clear via schedule or negate at
                                const next_schedule = computeNextSchedule(currentJob.header)
                                if (next_schedule) {
                                    txn.setJob(this.jobKey, {
                                        ...currentJob,
                                        header: { ...currentJob.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, execution_id: undefined }
                                    })
                                } else {
                                    txn.setJob(this.jobKey, {
                                        ...currentJob,
                                        header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined }
                                    })
                                }
                                return err
                        }
                    })()

                    // Write log if outcome is not suppressed
                    if (!config.find(v => v === outcome.type)) {
                        txn.writeLog(
                            { job_id: this.jobKey.job_id, timestamp: Date.now(), random: crypto.randomUUID() },
                            { at: Date.now(), execution_id: this.execution_id, outcome }
                        )
                    }

                    // Post-processing hook (storage adapter's case statement)
                    if (txn.processJobOutcome) {
                        outcome = await txn.processJobOutcome({
                            outcome, payload: currentJob.payload, jobKey: this.jobKey,
                        })
                    }

                    // Notify on fatal errors via callback
                    switch (outcome.type) {
                        case "fatal-error":
                        case "vanished":
                        case "rescheduled-error":
                            if (
                                outcome.type === "rescheduled-error"
                                && (
                                    !currentJob.header.repeatSchedule
                                    || currentJob.header.repeatSchedule.type !== "periodic"
                                    || currentJob.header.repeatSchedule.until.format !== "forever"
                                )
                            ) break
                            if (currentJob.payload.type !== "scheduled_email" && !this.suppress_error_emails && this.onFatalError) {
                                // Fire and forget — don't let email failures affect the outcome
                                this.onFatalError({
                                    job: currentJob,
                                    jobKey: this.jobKey,
                                    outcome,
                                }).catch(e => console.error("Error in onFatalError callback", e))
                            }
                            break
                        default:
                            outcome.type satisfies "readopted" | "rescheduled-error" | "success"
                    }
                    return outcome
                })
            }
        })()

        // Update running count & metrics
        const running_at_end = (JobManager.runningJobs.get(job.payload.type) || 0) - 1
        if (running_at_end > 0)
            JobManager.runningJobs.set(job.payload.type, running_at_end)
        else
            JobManager.runningJobs.delete(job.payload.type)
        JobManager.totalRunningJobs--

        WorkflowCounter.pegValue({
            jobs: {
                [job.payload.type]: {
                    totals: { executed: 1, duration: Date.now() - this.startTime },
                    outcomes: { [outcome.type]: 1 }
                }
            }
        })
        this.completed = true
        return outcome
    }
}
