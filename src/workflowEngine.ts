/**
 * Workflow engine — high-level API that ties pickers, capabilities, and
 * job managers together.
 *
 * The consumer creates an engine, wires its methods to a scheduling mechanism
 * (e.g. token ring's onToken / onServerUnresponsive), and the engine handles
 * the pick → resolve-runner → create-manager → run lifecycle.
 */
import type {
    BasicJobPayload,
    WorkflowConfig,
    WorkflowJobKey,
    WorkflowJobOutcome,
    WorkflowJobValue,
    JobRunnerFunction,
} from "./workflowTypes"
import type { WorkflowJobStorage } from "./workflowStorageAdapter"
import { WorkflowPicker, type PickContext } from "./workflowPicker"
import { WorkflowCapabilities } from "./workflowCapabilities"
import { JobManager, type JobManagerOptions } from "./jobManager"
import { JobError } from "./jobErrors"
import { WorkflowCounter } from "./counter"

// =========================================================================
// Types
// =========================================================================

export interface WorkflowEngineOptions<PAYLOAD_T extends BasicJobPayload> {
    pickers: WorkflowPicker<PAYLOAD_T>[]
    capabilities: WorkflowCapabilities<PAYLOAD_T>
    config: WorkflowConfig
    /** Maps string payload type names → numeric enum values */
    enumLookup: Record<string, number>
    suppress_error_emails?: boolean
    /**
     * Called on fatal / vanished / rescheduled-error outcomes so the consumer
     * can trigger notifications (e.g. error emails).
     */
    onFatalError?: JobManagerOptions<PAYLOAD_T>["onFatalError"]
    /**
     * Wraps each job's execution. The consumer can add contextual logging,
     * tracing, or other cross-cutting concerns.
     *
     * The `fn` callback must be called exactly once; its return value is the
     * job outcome.
     *
     * Default behaviour: calls `fn()` directly.
     */
    runJobWrapper?: (args: {
        jobKey: WorkflowJobKey
        payload: PAYLOAD_T
        fn: () => Promise<WorkflowJobOutcome>
    }) => Promise<void>
    /** If set, prints a periodic summary at this interval (ms). */
    summaryIntervalMs?: number
}

// =========================================================================
// Engine
// =========================================================================

export class WorkflowEngine<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    private readonly pickers: WorkflowPicker<PAYLOAD_T>[]
    private readonly capabilities: WorkflowCapabilities<PAYLOAD_T>
    private readonly config: WorkflowConfig
    private readonly enumLookup: Record<string, number>
    private readonly suppress_error_emails: boolean
    private readonly onFatalError?: WorkflowEngineOptions<PAYLOAD_T>["onFatalError"]
    private readonly runJobWrapper?: WorkflowEngineOptions<PAYLOAD_T>["runJobWrapper"]
    private summaryInterval?: ReturnType<typeof setInterval>

    constructor(options: WorkflowEngineOptions<PAYLOAD_T>) {
        this.pickers = options.pickers
        this.capabilities = options.capabilities
        this.config = options.config
        this.enumLookup = options.enumLookup
        this.suppress_error_emails = options.suppress_error_emails ?? false
        this.onFatalError = options.onFatalError
        this.runJobWrapper = options.runJobWrapper
        if (options.summaryIntervalMs) {
            this.startSummaryInterval(options.summaryIntervalMs)
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Run a pick cycle across all pickers, then start every adopted job.
     *
     * Returns the total number of jobs picked. Job execution is fire-and-forget
     * — errors are logged but do not propagate.
     */
    async pick(ctx: PickContext): Promise<{ pickedJobs: number }> {
        const allPicked = await Promise.all(
            this.pickers.map(async (picker) => ({
                picker,
                picked: await picker.pick(ctx),
            })),
        )

        for (const { picker, picked } of allPicked) {
            for (const { jobKey, job } of picked) {
                this.startJob({
                    jobKey,
                    job,
                    storage: picker.storage,
                    executorId: job.header.execution_id!,
                }).catch((e) => {
                    console.error("Fatal error starting job", jobKey, e)
                })
            }
        }

        return {
            pickedJobs: allPicked.reduce((sum, { picked }) => sum + picked.length, 0),
        }
    }

    /**
     * Start a single job: resolve runner, create a JobManager, and run it.
     *
     * Typically called by `pick()`, but can also be invoked directly when
     * the caller already has a specific job to execute.
     */
    async startJob(args: {
        jobKey: WorkflowJobKey
        job: WorkflowJobValue<PAYLOAD_T>
        storage: WorkflowJobStorage<PAYLOAD_T>
        executorId: string
    }): Promise<WorkflowJobOutcome> {
        const runner: JobRunnerFunction<PAYLOAD_T> = (() => {
            try {
                const r = this.capabilities.getRunner(args.job.payload, this.enumLookup)
                if (!r) throw new Error("No runner found for capability " + args.job.payload.type)
                return r
            } catch (e: any) {
                // Return an error-throwing runner so the job goes through the
                // normal retry / fatal-error lifecycle.
                return ((_mgr, _oopE) =>
                    Promise.reject(
                        new JobError({
                            type: "custom-recoverable",
                            message: e?.message ?? String(e),
                        }),
                    ))
            }
        })()

        const mgr = new JobManager<PAYLOAD_T>({
            jobKey: args.jobKey,
            execution_id: args.executorId,
            storage: args.storage,
            workflow_supress_job_outcome_logs: this.config.workflow_supress_job_outcome_logs,
            suppress_error_emails: this.suppress_error_emails,
            onFatalError: this.onFatalError,
        })

        if (this.runJobWrapper) {
            let outcome: WorkflowJobOutcome = { type: "success" }
            await this.runJobWrapper({
                jobKey: args.jobKey,
                payload: args.job.payload,
                fn: async () => {
                    outcome = await mgr.Run(runner)
                    return outcome
                },
            })
            return outcome
        }

        return mgr.Run(runner)
    }

    /**
     * Reset all orphaned jobs for the given executor across every picker's
     * storage. Called when the ring detects an unresponsive server.
     */
    async resetOrphanedJobs(executorId: string): Promise<number> {
        let total = 0
        for (const picker of this.pickers) {
            total += await picker.storage.resetOrphanedJobs(executorId)
        }
        return total
    }

    /** Stop the summary interval and clean up. */
    destroy(): void {
        if (this.summaryInterval) clearInterval(this.summaryInterval)
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private startSummaryInterval(ms: number) {
        this.summaryInterval = setInterval(() => {
            try {
                console.log(
                    "Work summary",
                    JSON.stringify({
                        current: JobManager.JobRunningCount,
                        historic: WorkflowCounter.getDefaultCounter().getCurrentValue(),
                    }),
                )
            } catch { /* swallow — summary is best-effort */ }
        }, ms)
    }
}
