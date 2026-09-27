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
    WorkflowDebugLog,
    WorkflowJobKey,
    WorkflowJobOutcome,
    WorkflowJobValue,
} from "./workflowTypes"
import { noopDebugLog } from "./workflowTypes"
import { WorkflowPicker, type PickContext } from "./workflowPicker"
import { capabilitiesToBuffer, bufferToCapabilities } from "./workflowCapabilities"
import { JobRunner, JobRunnerClosure, JobRunnerOptions, type JobRunnerConstructor } from "./jobRunner"
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
    /**
     * Optional debug logger. Called at every meaningful transition in the
     * engine → picker → runner pipeline. Wire it to your log framework
     * when you need to trace a stall.
     */
    debugLog?: WorkflowDebugLog
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
    protected readonly runners: { [T in PAYLOAD_T["type"]]?: null | undefined | JobRunnerClosure<PAYLOAD_T, T, TXN, CTX> } = {};
    readonly clock: WorkflowClock
    readonly ctx: CTX
    readonly debugLog: WorkflowDebugLog
    constructor(options: WorkflowEngineOptions<PAYLOAD_T, TXN, CTX>) {
        super({
            segment_name: options.segment_name,
            issuer_id: options.issuer_id,
            config: options.ringConfig,
            capabilities: Buffer.alloc(0),
            storage: options.ringStorage,
        });
        this.clock = options.clock ?? defaultWorkflowClock
        this.pickers = options.pickers
        this.allSortedCapabilities = options.allSortedCapabilities
        this.debugLog = options.debugLog ?? noopDebugLog
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
    AddRunner<T extends PAYLOAD_T["type"]>(type: T) {
        return (runner: JobRunnerConstructor<PAYLOAD_T, T, TXN, CTX> | null): void => {
            return this.AddRunnerClosure(type)(
                runner
                    ? (args: JobRunnerOptions<PAYLOAD_T, T, TXN, CTX>) => new runner(args)
                    : null
            )
        }
    }
    AddRunnerClosure<T extends PAYLOAD_T["type"]>(type: T) {
        return (runner: JobRunnerClosure<PAYLOAD_T, T, TXN, CTX> | null): void => {
            this.runners[type] = runner
            // Rebuild the advertised capability bitmap so late-added runners
            // are reflected in the token this engine emits.
            this.args.capabilities = capabilitiesToBuffer(
                this.allSortedCapabilities.filter(t => !!this.runners[t]),
                this.allSortedCapabilities,
            )
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
        this.debugLog("engine.onToken.enter", {
            executorId: this.issuer_id,
            issuedBy: ctx.token.issued_by,
            issuerVersion: ctx.token.issuer_version,
            provisional: !!(ctx.token.flags & TokenFlags.provisional),
            averageWorkload: ctx.token.averageWorkload,
            running: JobRunner.JobRunningCount.total,
        })
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
            debugLog: this.debugLog,
        }

        this.pick(pickCtx)
            .then((result) => {
                this.debugLog("engine.onToken.done", {
                    executorId: this.issuer_id,
                    pickedJobs: result.pickedJobs,
                    running: JobRunner.JobRunningCount.total,
                })
                ctx.done({ running: JobRunner.JobRunningCount.total })
            })
            .catch((e) => {
                this.debugLog("engine.onToken.error", {
                    executorId: this.issuer_id,
                    error: (e as any)?.message ?? String(e),
                })
                ctx.error(e)
            })
    }

    override async onServerUnresponsive(registration: {
        key: TokenRingRegistrationKey
        value: TokenRingRegistrationValue
    }): Promise<void> {
        this.debugLog("engine.ring.onServerUnresponsive.enter", {
            executorId: this.issuer_id,
            deadExecutor: registration.value.executor_id,
            server_ip: registration.key.server_ip,
            server_port: registration.key.server_port,
        })
        const count = await this.resetLostJobs(registration.value.executor_id)
        if (count > 0) {
            console.log(
                `Reset ${count} lost jobs for unresponsive server`,
                registration.value.executor_id,
            )
        }
        this.debugLog("engine.ring.onServerUnresponsive.exit", {
            executorId: this.issuer_id,
            deadExecutor: registration.value.executor_id,
            resetCount: count,
        })
    }

    override onError(e: any): void {
        this.debugLog("engine.ring.onError", {
            executorId: this.issuer_id,
            error: (e as any)?.message ?? String(e),
            stack: (e as any)?.stack,
        })
        super.onError(e)
    }

    override onDestroy(): void {
        this.debugLog("engine.ring.onDestroy", { executorId: this.issuer_id })
        super.onDestroy()
    }

    protected override async MarkServerAsUnresponsive(key: TokenRingRegistrationKey): Promise<void> {
        this.debugLog("engine.ring.MarkServerAsUnresponsive.enter", {
            executorId: this.issuer_id,
            deadServer: key,
        })
        try {
            await super.MarkServerAsUnresponsive(key)
            this.debugLog("engine.ring.MarkServerAsUnresponsive.exit", {
                executorId: this.issuer_id,
                deadServer: key,
            })
        } catch (e: any) {
            this.debugLog("engine.ring.MarkServerAsUnresponsive.error", {
                executorId: this.issuer_id,
                error: e?.message ?? String(e),
            })
            throw e
        }
    }

    protected override async Register(): Promise<void> {
        this.debugLog("engine.ring.Register.enter", { executorId: this.issuer_id })
        try {
            await super.Register()
            this.debugLog("engine.ring.Register.exit", { executorId: this.issuer_id })
        } catch (e: any) {
            this.debugLog("engine.ring.Register.error", {
                executorId: this.issuer_id,
                error: e?.message ?? String(e),
            })
            throw e
        }
    }

    override async Start(): Promise<this> {
        this.debugLog("engine.Start.enter", { executorId: this.issuer_id, segment: this.segmentName })
        try {
            const ret = await super.Start()
            this.debugLog("engine.Start.exit", {
                executorId: this.issuer_id,
                boundAddress: this.boundAddress,
            })
            return ret as this
        } catch (e: any) {
            this.debugLog("engine.Start.error", {
                executorId: this.issuer_id,
                error: e?.message ?? String(e),
            })
            throw e
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
        this.debugLog("engine.pick.enter", { executorId: this.issuer_id, pickerCount: this.pickers.length })
        const ctx: PickContext<PAYLOAD_T> = {
            ..._ctx,
            canRunType: (type) => !!this.runners[type],
            debugLog: _ctx.debugLog ?? this.debugLog,
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
                    this.debugLog("engine.startJob.fatal", {
                        job_id: jobKey.job_id,
                        error: (e as any)?.message ?? String(e),
                    })
                })
            }
        }

        const total = allPicked.reduce((sum, { picked }) => sum + picked.length, 0)
        this.debugLog("engine.pick.exit", { executorId: this.issuer_id, pickedJobs: total })
        return { pickedJobs: total }
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
        this.debugLog("engine.startJob.enter", {
            job_id: args.jobKey.job_id,
            type: args.job.payload.type,
            executorId: args.executorId,
        })
        const RunnerClass = (() => {
            const Cls = this.runners[args.job.payload.type as PAYLOAD_T["type"]]
            if (Cls) return Cls
            this.debugLog("engine.startJob.noRunner", {
                job_id: args.jobKey.job_id,
                type: args.job.payload.type,
            })
            // Return an error runner so the job goes through the
            // normal retry / fatal-error lifecycle.
            return (args: JobRunnerOptions<any, any, any>) => new ErrorRunner(args)
        })()

        const runner = RunnerClass({
            jobKey: args.jobKey,
            execution_id: args.executorId,
            type: args.job.payload.type,
            store: args.picker.storage,
            clock: this.clock,
            pegCounterValue: (value) => this.pegCounterValue(value),
            context: this.ctx as never,
            debugLog: this.debugLog,
        })
        const outcome = await this.runJob({
            jobKey: args.jobKey,
            payload: args.job.payload,
            fn: async () => {
                return await runner.Run()
            },
        })
        this.debugLog("engine.startJob.outcome", {
            job_id: args.jobKey.job_id,
            outcome: outcome.type,
        })
        try {
            await this.OnAfterWorkflowJob({ jobKey: args.jobKey, initialJobValue: args.job, outcome })
        } catch (e) {
            console.error("Error in OnAfterWorkflowJob callback", args.jobKey, e)
            this.debugLog("engine.startJob.afterHookError", {
                job_id: args.jobKey.job_id,
                error: (e as any)?.message ?? String(e),
            })
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
        this.debugLog("engine.Destroy.enter", {
            executorId: this.issuer_id,
            destroyed: this.destroyed,
            cause: cause ? ((cause as any)?.message ?? String(cause)) : undefined,
        })
        this.summaryInterval?.cancel()
        this.summaryInterval = undefined;
        super.Destroy(cause)
        this.debugLog("engine.Destroy.exit", { executorId: this.issuer_id })
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

class ErrorRunner extends JobRunner<any, any, any> {
    async runJob(): Promise<void | number> {
        throw new JobError({
            type: "custom-recoverable",
            message: `No runner found for capability ${this.jobType}`,
        })
    }
}
