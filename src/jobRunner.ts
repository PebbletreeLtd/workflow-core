/**
 * Core job runner implementation.
 * 
 * Manages the lifecycle of a single running workflow job: progress reporting,
 * pushback timers, error handling, retry/reschedule logic, and outcome logging.
 * 
 * Subclass this and implement `runJob()` to define the actual work for a
 * specific job type. The runner has access to all manager methods via `this`.
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
    WorkflowRetryPolicy,
} from "./workflowTypes"
import { JobError } from "./jobErrors"
import { computeNextSchedule } from "./schedule"
import { iWorkflowCounter } from "./counter"
import { WorkflowStorageTransaction, WorkflowJobStorage } from "./workflowStorageAdapter"
import { v4 } from "uuid"
import { WorkflowClock, WorkflowClockTimerCancel } from "./workflowClock"

/** If a snapshot of the initial retry policy exists, return a fresh policy
 * with the original `max` and `initial_backoff_ms` restored. */
function restoreRetriesOnSuccess(retries: WorkflowRetryPolicy): WorkflowRetryPolicy {
    if (!retries._initial) return retries
    const { _initial, ...rest } = retries
    return { ...rest, max: _initial.max, initial_backoff_ms: _initial.initial_backoff_ms }
}

export interface JobRunnerOptions<PAYLOAD_T extends BasicJobPayload, T extends PAYLOAD_T["type"], TXN extends WorkflowStorageTransaction<PAYLOAD_T> = WorkflowStorageTransaction<PAYLOAD_T>> {
    jobKey: Readonly<WorkflowJobKey>
    execution_id: string,
    store: WorkflowJobStorage<PAYLOAD_T, TXN>
    /**
     * Optional promise that must resolve before the first store read.
     * Used by SimulatedJobRunner to ensure the seed transaction commits first.
     */
    ready?: Promise<unknown>,
    type: T,
    clock: WorkflowClock,
    pegCounterValue: (value: Partial<iWorkflowCounter>) => void
}

/** Constructor type for a concrete JobRunner subclass. */
export type JobRunnerConstructor<PAYLOAD_T extends BasicJobPayload, T extends PAYLOAD_T["type"], TXN extends WorkflowStorageTransaction<PAYLOAD_T>> =
    { new(args: JobRunnerOptions<PAYLOAD_T, T, TXN>): JobRunner<PAYLOAD_T, T, TXN> }



export abstract class JobRunner<PAYLOAD_T extends BasicJobPayload, T extends PAYLOAD_T["type"], TXN extends WorkflowStorageTransaction<PAYLOAD_T>> {
    readonly outofProcessError = (() => {
        let callback: any = undefined
        const promise = new Promise<void>((_, E) => {
            callback = E
        })
        return { promise, raise: callback as (reason: any) => void }
    })()

    private completed = false
    private pushbackTimer: WorkflowClockTimerCancel | undefined = undefined
    private progressTimer: WorkflowClockTimerCancel | undefined = undefined
    private startTime;
    private static readonly runningJobs = new Map<string, number>()
    private static totalRunningJobs = 0
    private _store: WorkflowJobStorage<PAYLOAD_T, TXN>
    private _job: Promise<{
        header: Readonly<WorkflowJobHeader>
        payload: PAYLOAD_T & { type: T }
    }> | undefined
    private readonly execution_id: string
    private readonly _ready: Promise<unknown> | undefined
    protected pegCounterValue: (value: Partial<iWorkflowCounter>) => void
    readonly clock: WorkflowClock
    readonly jobType: T
    readonly jobKey: Readonly<WorkflowJobKey>

    /** The storage adapter backing this runner. */
    get store(): WorkflowJobStorage<PAYLOAD_T, TXN> { return this._store }

    /**
     * Resolves once any setup work (e.g. seed transactions) is complete.
     * Useful when you need to access the store directly before calling Run().
     */
    async whenReady(): Promise<void> { if (this._ready) await this._ready }

    constructor(args: JobRunnerOptions<PAYLOAD_T, T, TXN>) {
        this.execution_id = args.execution_id
        this.jobKey = args.jobKey
        this._store = args.store
        this._ready = args.ready
        this.jobType = args.type
        this.clock = args.clock
        this.startTime = this.clock.now()
        this.pegCounterValue = args.pegCounterValue
    }
    /**
     * Implement this method to define the actual work for this job type.
     * Has full access to the runner via `this` — call `this.GetJob()`,
     * `this.Progress()`, `this.Wait()`, etc.
     * 
     * Return `void` for normal completion, or a future timestamp (number)
     * to reschedule the job.
     */
    abstract runJob(): Promise<void | number>

    static get JobRunningCount() {
        return { total: this.totalRunningJobs, types: Array.from(this.runningJobs) }
    }

    async Wait(duration: number): Promise<void> {
        const chunk = (await this.GetJob()).header.progress_deadline_ms / 2
        const numWaits = Math.ceil(duration / chunk)
        for (let i = 0; i < numWaits; i++) {
            const waitedSoFar = i * chunk
            const thisWait = Math.min(chunk, duration - waitedSoFar)
            await this.clock.sleep(thisWait);
            await this.Progress();
        }
    }

    private async GetUpdatedJob(
        txn?: TXN
    ): Promise<{
        header: Readonly<WorkflowJobHeader>
        payload: PAYLOAD_T & { type: T }
    }> {
        if (!txn) {
            if (this._ready) await this._ready
            return this._store.doTn(txn => this.GetUpdatedJob(txn))
        }
        const job = await txn.job.get(this.jobKey)
        if (!job) {
            throw new JobError({ type: "vanished" })
        } else if (job.header.execution_id !== this.execution_id) {
            throw new JobError({ type: "readopted", by: job.header.execution_id || "undefined" })
        } else if (job.payload.type !== this.jobType) {
            throw new Error(`Job payload type mismatch: expected ${this.jobType} but got ${job.payload.type}`)
        }
        return { ...job, payload: { ...job.payload, type: this.jobType } }
    }

    async GetJob() {
        if (this.completed) {
            throw new Error("Job not running")
        }
        if (!this._job) {
            this._job = this.GetUpdatedJob()
        }
        if (!this.progressTimer) {
            this.ResetProgressTimer((await this._job).header.progress_deadline_ms)
        }
        const job = await this._job
        return job
    }
    private async ResetJob(options: { progress: boolean, payload?: Partial<PAYLOAD_T> }) {
        this._job = this._store.doTn(async txn => {
            const job = await this.GetUpdatedJob(txn)
            const amended = { ...job, header: { ...job.header } }
            if (options.progress) amended.header.at = this.clock.now() + job.header.lost_deadline_ms
            if (options.payload) amended.payload = { ...amended.payload, ...options.payload }
            if (options.progress || !amended.header.last_progress) {
                amended.header.last_progress = this.clock.now()
            }
            txn.job.set(this.jobKey, amended)
            return amended
        })
        const job = await this.GetJob()
        if (options.progress) this.ResetProgressTimer(job.header.progress_deadline_ms)
        this.ResetPushBackTimer(job.header.lost_deadline_ms / 2.5)
        return job
    }

    private ResetPushBackTimer(duration: number) {
        this.pushbackTimer?.cancel();
        this.pushbackTimer = this.clock.setTimer(async () => {
            try {
                await this.ResetJob({ progress: false })
            } catch (e) {
                this.outofProcessError.raise(e)
            }
        }, duration)
    }

    private ResetProgressTimer(duration: number) {
        this.progressTimer?.cancel();
        this.progressTimer = this.clock.setTimer(async () => {
            this.outofProcessError.raise(new JobError({ type: "progress-deadline" }))
        }, duration)
    }

    private clearTimers() {
        this.progressTimer?.cancel()
        this.pushbackTimer?.cancel();
        this.progressTimer = undefined
        this.pushbackTimer = undefined
    }

    async Progress(payload?: Partial<PAYLOAD_T & { type: T }>) {
        return this.ResetJob({ progress: true, payload })
    }


    async Run(): Promise<WorkflowJobOutcome> {
        let job: Awaited<ReturnType<typeof this.GetJob>>
        try {
            job = await this.GetJob()
        } catch (e) {
            if (e instanceof JobError) return e.outcome as WorkflowJobOutcome
            throw e
        }
        const outcome = await (async () => {
            try {
                console.debug("Workflow starting job", this.jobKey, job.header, job.payload)
                const running = (JobRunner.runningJobs.get(job.payload.type) || 0) + 1
                JobRunner.totalRunningJobs++
                JobRunner.runningJobs.set(job.payload.type, running)

                const result = await Promise.race([
                    this.runJob(),
                    this.outofProcessError.promise,
                ])
                this.clearTimers()

                // Success path: clear/reschedule + post-processing in one transaction
                return await this._store.doTn(async (txn) => {
                    let outcome: WorkflowJobOutcome = { type: "success" }
                    const job = await txn.job.get(this.jobKey)
                    if (!job || job.header.execution_id !== this.execution_id) {
                        return outcome
                    }
                    // Post-processing hook (storage adapter's case statement)
                    if (job.payload.type === this.jobType)
                        outcome = await this.onJobOutcome({
                            outcome,
                            payload: {
                                ...job.payload, type: this.jobType
                            },
                            jobKey: this.jobKey,
                            txn
                        })

                    if (result) {
                        // Runner returned a future timestamp — reschedule
                        txn.job.set(this.jobKey, {
                            ...job,
                            header: { ...job.header, at: result, execution_id: undefined },
                        })
                    } else {
                        // Clear the job (complete or reschedule if repeating)
                        const next_schedule = computeNextSchedule(job.header, this.clock)
                        if (next_schedule) {
                            console.debug("Rescheduling job for", next_schedule)
                            const retries = restoreRetriesOnSuccess(job.header.retries)
                            txn.job.set(this.jobKey, {
                                ...job,
                                header: { ...job.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, retries, execution_id: undefined },
                            })
                        } else {
                            txn.job.set(this.jobKey, {
                                ...job,
                                header: { ...job.header, at: 0 - Math.abs(job.header.at), execution_id: undefined },
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


                // Handle error within a transactional context
                let onFatalErrorCallback: (() => void) | undefined = undefined as (() => void) | undefined
                const ret = await this._store.doTn(async (txn) => {
                    const currentJob = await txn.job.get(this.jobKey)
                    if (!currentJob) return { type: "vanished" } as WorkflowJobOutcome

                    let outcome = await (async (): Promise<WorkflowJobOutcome> => {
                        switch (err.type) {
                            case "vanished":
                                // Clear the job
                                txn.job.set(this.jobKey, {
                                    ...currentJob,
                                    header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined }
                                })
                                return err
                            case "custom-recoverable":
                            case "progress-deadline":
                            case "payloadMismatch":
                                if (currentJob.header.retries.max > 0) {
                                    if (!currentJob.header.retries._initial) {
                                        currentJob.header.retries._initial = {
                                            max: currentJob.header.retries.max,
                                            initial_backoff_ms: currentJob.header.retries.initial_backoff_ms,
                                        }
                                    }
                                    currentJob.header.retries.max--
                                    currentJob.header.at = this.clock.now() + currentJob.header.retries.initial_backoff_ms
                                    currentJob.header.retries.initial_backoff_ms *= currentJob.header.retries.exponent
                                    currentJob.header.execution_id = undefined
                                    txn.job.set(this.jobKey, currentJob)
                                    return { type: "rescheduled-error", cause: err as any }
                                } else {
                                    // Clear via schedule or negate at
                                    const next_schedule = computeNextSchedule(currentJob.header, this.clock)
                                    if (next_schedule) {
                                        txn.job.set(this.jobKey, {
                                            ...currentJob,
                                            header: { ...currentJob.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, execution_id: undefined }
                                        })
                                    } else {
                                        txn.job.set(this.jobKey, {
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
                                const next_schedule = computeNextSchedule(currentJob.header, this.clock)
                                if (next_schedule) {
                                    txn.job.set(this.jobKey, {
                                        ...currentJob,
                                        header: { ...currentJob.header, at: next_schedule.nextDate, repeatSchedule: next_schedule, execution_id: undefined }
                                    })
                                } else {
                                    txn.job.set(this.jobKey, {
                                        ...currentJob,
                                        header: { ...currentJob.header, at: 0 - Math.abs(currentJob.header.at), execution_id: undefined }
                                    })
                                }
                                return err
                        }
                    })()

                    // Write log if outcome is not suppressed
                    await this.WriteJobLog(txn, outcome)

                    // Post-processing hook (storage adapter's case statement)
                    if (currentJob.payload.type === this.jobType)
                        outcome = await this.onJobOutcome({
                            outcome, payload: { ...currentJob.payload, type: this.jobType }, jobKey: this.jobKey, txn
                        })

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
                            if (currentJob.payload.type !== "scheduled_email") {
                                // Fire and forget — don't let email failures affect the outcome
                                onFatalErrorCallback = () => this.onFatalError({
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
                });
                if (onFatalErrorCallback) {
                    onFatalErrorCallback();
                }
                return ret;
            }
        })()

        // Update running count & metrics
        const running_at_end = (JobRunner.runningJobs.get(job.payload.type) || 0) - 1
        if (running_at_end > 0)
            JobRunner.runningJobs.set(job.payload.type, running_at_end)
        else
            JobRunner.runningJobs.delete(job.payload.type)
        JobRunner.totalRunningJobs--

        this.pegCounterValue({
            jobs: {
                [job.payload.type]: {
                    totals: { executed: 1, duration: this.clock.now() - this.startTime },
                    outcomes: { [outcome.type]: 1 }
                }
            }
        })
        this.completed = true
        return outcome
    }

    /**Override if to implement customize job log writing */
    async WriteJobLog(
        txn: TXN,
        outcome: WorkflowJobOutcome
    ) {

        txn.jobLogKey.set(
            { timestamp: this.clock.now(), job_id: this.jobKey.job_id, random: v4() },
            { at: this.clock.now(), execution_id: this.execution_id, outcome }
        )
    }    /**
     * Called on fatal / vanished / rescheduled-error outcomes so the implementer
     * can trigger notifications (e.g. error emails).
     * No-op by default.
     */
    async onFatalError(_args: {
        job: WorkflowJobValue<PAYLOAD_T>
        jobKey: WorkflowJobKey
        outcome: WorkflowJobOutcome
    }): Promise<void> {
        console.warn("JobManager onFatalError called with no implementation")
    }
    /** Called on job outcome so the implementer can trigger notifications or other actions.
     * Returns the outcome unchanged by default. */
    async onJobOutcome(args: {
        outcome: WorkflowJobOutcome
        payload: PAYLOAD_T & { type: T }
        jobKey: WorkflowJobKey,
        txn: TXN
    }): Promise<WorkflowJobOutcome> { return args.outcome }
}

