/**
 * Workflow engine — extends TokenRingWorkDistributor to form a complete
 * distributed job processing system.
 *
 * On each token receipt the engine picks ready jobs from all configured
 * pickers, resolves the appropriate runner for each, and executes them.
 * When the ring detects an unresponsive server, lost jobs are reset
 * automatically.
 */
import type {
    BasicJobPayload,
    WorkflowJobKey,
    WorkflowJobOutcome,
    WorkflowJobValue,
} from "./workflowTypes"
import { WorkflowPicker, type PickContext } from "./workflowPicker"
import { capabilitiesToBuffer, bufferToCapabilities } from "./workflowCapabilities"
import { JobRunner, type JobRunnerConstructor } from "./jobRunner"
import { JobError } from "./jobErrors"
import { iWorkflowCounter, WorkflowCounter } from "./counter"
import {
    TokenRingWorkDistributor,
    TokenFlags,
    type TokenRingConfig,
    type TokenRingWorkDistributorInterface,
    type Token,
    type TokenRingRegistrationKey,
    type TokenRingRegistrationValue,
    TokenRingOptions,
} from "@pebbletree/tokenring"
import { WorkflowStorageTransaction } from "./workflowStorageAdapter"
import { defaultWorkflowClock, WorkflowClock, WorkflowClockTimerCancel } from "./workflowClock"

// =========================================================================
// Types
// =========================================================================

export interface WorkflowEngineOptions<PAYLOAD_T extends BasicJobPayload, TXN extends WorkflowStorageTransaction<PAYLOAD_T>, CTX = never> {
    /** One or more pickers, each backed by a different storage table */
    pickers: WorkflowPicker<PAYLOAD_T, TXN>[]
    /** All possible job type values, sorted ascending (for bitmap encode/decode) */
    allSortedCapabilities: PAYLOAD_T["type"][]

    // --- Token ring ---
    /** Ring segment / namespace */
    segment_name: string
    /** Unique identifier for this server */
    issuer_id: string
    /** Token ring configuration */
    ringConfig: TokenRingConfig
    /** Transaction factory for the ring membership table */
    ringStorage: TokenRingOptions["storage"]
    /** Optional context for this engine instance. */
    ctx?: CTX

    // --- Optional ---

    /** If set, prints a periodic summary at this interval (ms). */
    summaryIntervalMs?: number
    /** Optional job clock for this engine instance. */
    clock?: WorkflowClock
}

// =========================================================================
// Engine
// =========================================================================

export abstract class WorkflowEngine<PAYLOAD_T extends BasicJobPayload, TXN extends WorkflowStorageTransaction<PAYLOAD_T>, CTX = never>
    extends TokenRingWorkDistributor {
    private namedCounters = new Map<string, WorkflowCounter>()
    private readonly pickers: WorkflowPicker<PAYLOAD_T, TXN>[]
    private readonly allSortedCapabilities: PAYLOAD_T["type"][]
    private summaryInterval?: WorkflowClockTimerCancel
    protected readonly runners: { [T in PAYLOAD_T["type"]]?: null | undefined | JobRunnerConstructor<PAYLOAD_T, T, TXN> } = {};
    readonly clock: WorkflowClock
    readonly ctx: CTX
    constructor(options: WorkflowEngineOptions<PAYLOAD_T, TXN, CTX>) {
        super({
            segment_name: options.segment_name,
            issuer_id: options.issuer_id,
            config: options.ringConfig,
            capabilities: Buffer.alloc(0),
            storage: options.ringStorage,
        });
        this.clock = options.clock ?? defaultWorkflowClock
        this.InitialiseRunners();
        this.pickers = options.pickers
        this.allSortedCapabilities = options.allSortedCapabilities
        // Derive supported capabilities by probing runners (available now that super() has returned)
        this.args.capabilities = capabilitiesToBuffer(
            options.allSortedCapabilities.filter(t => !!this.runners[t]),
            options.allSortedCapabilities,
        )
        if (options.summaryIntervalMs) {
            this.startSummaryInterval(options.summaryIntervalMs)
        }
        this.ctx = options.ctx || {} as CTX
    }
    abstract InitialiseRunners(): void;
    AddRunner<T extends PAYLOAD_T["type"]>(type: T) {
        return (runner: JobRunnerConstructor<PAYLOAD_T, T, TXN, CTX> | null): void => {
            this.runners[type] = runner as any
        }
    }
    // ------------------------------------------------------------------
    // Token ring overrides
    // ------------------------------------------------------------------

    onToken(ctx: {
        ring: TokenRingWorkDistributorInterface
        token: Readonly<Token>
        done: (workload: { running: number }) => void
        error: (e: any) => void
    }): void {
        const supportedTypes = new Set(
            bufferToCapabilities(
                ctx.token.capabilities,
                this.allSortedCapabilities,
            ),
        )

        const pickCtx: Omit<PickContext<PAYLOAD_T>, "canRunType"> = {
            executorId: this.issuer_id,
            averageWorkload: ctx.token.averageWorkload,
            currentRunning: JobRunner.JobRunningCount.total,
            ringState: {
                isProvisional: !!(ctx.token.flags & TokenFlags.provisional),
                supportedTypes,
            },
            isTerminating: () => this.destroyed,
            clock: this.clock,
            pegCounterValue: (value) => this.pegCounterValue(value),
        }

        this.pick(pickCtx)
            .then(() => {
                ctx.done({ running: JobRunner.JobRunningCount.total })
            })
            .catch((e) => {
                ctx.error(e)
            })
    }

    override async onServerUnresponsive(registration: {
        key: TokenRingRegistrationKey
        value: TokenRingRegistrationValue
    }): Promise<void> {
        const count = await this.resetLostJobs(registration.value.executor_id)
        if (count > 0) {
            console.log(
                `Reset ${count} lost jobs for unresponsive server`,
                registration.value.executor_id,
            )
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
    async pick(_ctx: Omit<PickContext<PAYLOAD_T>, "canRunType">): Promise<{ pickedJobs: number }> {
        const ctx: PickContext<PAYLOAD_T> = {
            ..._ctx,
            canRunType: (type) => !!this.runners[type],
        }
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
        picker: WorkflowPicker<PAYLOAD_T, TXN>
        executorId: string
    }): Promise<WorkflowJobOutcome> {
        const RunnerClass = (() => {
            const Cls = this.runners[args.job.payload.type as PAYLOAD_T["type"]]
            if (Cls) return Cls
            // Return an error runner so the job goes through the
            // normal retry / fatal-error lifecycle.
            return class ErrorRunner extends JobRunner<PAYLOAD_T, any, TXN> {
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
            type: args.job.payload.type,
            store: args.picker.storage,
            clock: this.clock,
            pegCounterValue: (value) => this.pegCounterValue(value),
            context: this.ctx as never,
        })
        const outcome = await this.runJob({
            jobKey: args.jobKey,
            payload: args.job.payload,
            fn: async () => {
                return await runner.Run()
            },
        })
        try {
            await this.OnAfterWorkflowJob({ jobKey: args.jobKey, initialJobValue: args.job, outcome })
        } catch (e) {
            console.error("Error in OnAfterWorkflowJob callback", args.jobKey, e)
        }
        return outcome
    }

    /**
     * Called after a job has run and its outcome has been persisted to storage,
     * outside of any transaction. Override to react to completed jobs
     * (metrics, notifications, downstream side effects, etc.).
     *
     * `initialJobValue` is the snapshot the runner started with (payload +
     * header). It does not reflect any mutations the runner made mid-run.
     *
     * Errors thrown here are logged and swallowed so they do not affect the
     * job's returned outcome.
     */
    OnAfterWorkflowJob(_args: {
        jobKey: WorkflowJobKey
        initialJobValue: WorkflowJobValue<PAYLOAD_T>
        outcome: WorkflowJobOutcome
    }): Promise<void> | void { }

    /**
         * Wraps each job's execution. The consumer can add contextual logging,
         * tracing, or other cross-cutting concerns.
         *
         * The `fn` callback must be called exactly once; its return value is the
         * job outcome.
         *
         * Default behaviour: calls `fn()` directly.
         */
    runJob(args: {
        jobKey: WorkflowJobKey
        payload: PAYLOAD_T
        fn: () => Promise<WorkflowJobOutcome>
    }): Promise<WorkflowJobOutcome> {
        return args.fn()
    }

    /**
     * Reset all lost jobs for the given executor across every picker's
     * storage. Called when the ring detects an unresponsive server.
     */
    async resetLostJobs(executorId: string): Promise<number> {
        let total = 0
        for (const picker of this.pickers) {
            total += await picker.resetLostJobs(executorId, this)
        }
        return total
    }

    /** Stop the summary interval and destroy the ring. */
    override Destroy(cause?: any): void {
        this.summaryInterval?.cancel()
        this.summaryInterval = undefined;
        super.Destroy(cause)
    }
    //----------------------------------------------------
    // Counters
    //----------------------------------------------------

    CreateCounter(options: { name: string, counter_duration_ms: number }) {
        console.log(`Creating workflow counter ${JSON.stringify(options)}`)
        const ret = new WorkflowCounter({
            counter_duration_ms: options.counter_duration_ms,
            clock: this.clock,
        });
        this.namedCounters.set(options.name, ret);
        return ret;
    }
    private pegCounterValue(value: Partial<iWorkflowCounter>) {
        for (const counter of Array.from(this.namedCounters.values()))
            counter.pegValue(value)
    }

    getCounterValue(name: string) {
        const counter = this.namedCounters.get(name)
        if (!counter) throw new Error("Counter not found: " + name)
        return counter.getCurrentValue()
    }

    getDefaultCounter() {
        const counter = Array.from(this.namedCounters.values())[0]
        if (!counter) throw new Error("Default counter not found")
        return counter
    }
    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private startSummaryInterval(ms: number) {
        this.summaryInterval = this.clock.setTimerInterval(() => {
            try {
                console.log(
                    "Work summary",
                    JSON.stringify({
                        current: JobRunner.JobRunningCount,
                        historic: this.getDefaultCounter().getCurrentValue(),
                    }),
                )
            } catch { /* swallow — summary is best-effort */ }
        }, ms)
    }

}
