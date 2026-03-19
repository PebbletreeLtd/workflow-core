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
    WorkflowJobKey,
    WorkflowJobOutcome,
    WorkflowJobValue,
} from "./workflowTypes"
import { WorkflowPicker, type PickContext } from "./workflowPicker"
import { WorkflowCapabilities } from "./workflowCapabilities"
import { JobRunner } from "./jobRunner"
import { JobError } from "./jobErrors"
import { WorkflowCounter } from "./counter"

// =========================================================================
// Types
// =========================================================================

export interface WorkflowEngineOptions<PAYLOAD_T extends BasicJobPayload> {
    pickers: WorkflowPicker<PAYLOAD_T>[]
    capabilities: WorkflowCapabilities<PAYLOAD_T>
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


    private summaryInterval?: ReturnType<typeof setInterval>

    constructor(private options: WorkflowEngineOptions<PAYLOAD_T>) {
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
            this.options.pickers.map(async (picker) => ({
                picker,
                picked: await picker.pick(ctx),
            })),
        )

        for (const { picker, picked } of allPicked) {
            for (const { jobKey, job } of picked) {
                this.startJob({
                    jobKey,
                    job,
                    picker,
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
     * Start a single job: resolve runner, instantiate it, and run it.
     *
     * Typically called by `pick()`, but can also be invoked directly when
     * the caller already has a specific job to execute.
     */
    async startJob(args: {
        jobKey: WorkflowJobKey
        job: WorkflowJobValue<PAYLOAD_T>
        picker: WorkflowPicker<PAYLOAD_T>
        executorId: string
    }): Promise<WorkflowJobOutcome> {
        const RunnerClass = (() => {
            const Cls = this.options.capabilities.getRunner(args.job.payload)
            if (Cls) return Cls
            // Return an error runner so the job goes through the
            // normal retry / fatal-error lifecycle.
            return class ErrorRunner extends JobRunner<PAYLOAD_T> {
                async runJob(): Promise<void | number> {
                    throw new JobError({
                        type: "custom-recoverable",
                        message: `No runner found for capability ${args.job.payload.type}`,
                    })
                }
            }
        })()

        const runner = new RunnerClass({
            jobKey: args.jobKey,
            execution_id: args.executorId,
            store: args.picker.storage,
        })

        if (this.options.runJobWrapper) {
            let outcome: WorkflowJobOutcome = { type: "success" }
            await this.options.runJobWrapper({
                jobKey: args.jobKey,
                payload: args.job.payload,
                fn: async () => {
                    outcome = await runner.Run()
                    return outcome
                },
            })
            return outcome
        }

        return runner.Run()
    }

    /**
     * Reset all lost jobs for the given executor across every picker's
     * storage. Called when the ring detects an unresponsive server.
     */
    async resetLostJobs(executorId: string): Promise<number> {
        let total = 0
        for (const picker of this.options.pickers) {
            total += await picker.resetLostJobs(executorId)
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
                        current: JobRunner.JobRunningCount,
                        historic: WorkflowCounter.getDefaultCounter().getCurrentValue(),
                    }),
                )
            } catch { /* swallow — summary is best-effort */ }
        }, ms)
    }
}
