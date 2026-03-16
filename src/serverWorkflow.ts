/**
 * High-level workflow server orchestrator.
 *
 * Combines a {@link WorkflowEngine} with a token ring to form a complete,
 * self-managing workflow server that picks, executes, and monitors jobs.
 *
 * All application-specific concerns (database handles, config resolution,
 * logging, error notifications) are injected via the {@link ServerWorkflowCreateOptions}
 * interface, keeping this class fully generic.
 */
import type {
    BasicJobPayload,
    WorkflowConfig,
    WorkflowJobKey,
    WorkflowJobOutcome,
    WorkflowJobValue,
} from "./workflowTypes"
import type { WorkflowJobStorage } from "./workflowStorageAdapter"
import { WorkflowPicker } from "./workflowPicker"
import { WorkflowEngine, type WorkflowEngineOptions } from "./workflowEngine"
import type { WorkflowCapabilities } from "./workflowCapabilities"
import { JobManager } from "./jobManager"

// =========================================================================
// Token ring integration types
// =========================================================================

/**
 * Minimal token ring interface that ServerWorkflow depends on.
 *
 * This matches the public API of `TokenRingWorkDistributor` from
 * `@pebbletree/tokenring` without importing it, so consumers can
 * provide any compatible implementation.
 */
export interface TokenRingInstance {
    readonly issuer_id: string
    Destroy(cause?: any): void
}

/**
 * Token ring factory function.
 *
 * Called by {@link ServerWorkflow.Create} to construct and start the ring.
 * The consumer provides this so they can inject their own storage adapter,
 * config flags, and any other environment-specific options.
 *
 * The ring MUST be started before being returned (i.e. call `.Start()` inside the factory).
 */
export type TokenRingFactory<PAYLOAD_T extends BasicJobPayload> = (args: {
    /** The workflow engine — use `engine.pick()` in `onToken` and `engine.resetOrphanedJobs()` in `onServerUnresponsive` */
    engine: WorkflowEngine<PAYLOAD_T>
    /** The capabilities buffer — pass to the token ring constructor */
    capabilitiesBuffer: Buffer
    /** The workflow config (reregister_time_ms, token_ack_timeout_ms, etc.) */
    config: WorkflowConfig
    /** Call this from `onToken.done()` — returns total running jobs */
    getRunningCount: () => number
    /** Signal the ServerWorkflow about each pick result  */
    onPickComplete?: (result: { pickedJobs: number }) => void
    /** Lifecycle error callback — forward to the ring's `onError` */
    onError: (e: any) => void
    /** Lifecycle complete callback — forward to the ring's `onDestroy` */
    onComplete: () => void
}) => Promise<TokenRingInstance>

// =========================================================================
// Create options
// =========================================================================

export interface ServerWorkflowCreateOptions<PAYLOAD_T extends BasicJobPayload> {
    /**
     * Pre-built storage adapters — one per workflow table.
     * The first adapter is used for direct `startJob()` calls.
     */
    adapters: WorkflowJobStorage<PAYLOAD_T>[]

    /** Capabilities (runner map + bitmap) for this server */
    capabilities: WorkflowCapabilities<PAYLOAD_T>

    /** Workflow engine configuration */
    config: WorkflowConfig

    /** Maps string payload type names → numeric enum values */
    enumLookup: Record<string, number>

    /**
     * Factory function that constructs and starts the token ring.
     * See {@link TokenRingFactory} for details.
     */
    tokenRingFactory: TokenRingFactory<PAYLOAD_T>

    // --- Optional engine callbacks (forwarded to WorkflowEngine) ---

    /** Whether to suppress error notification emails */
    suppressErrorEmails?: boolean

    /**
     * Called on fatal / vanished / rescheduled-error outcomes so the consumer
     * can trigger notifications (e.g. error emails).
     */
    onFatalError?: WorkflowEngineOptions<PAYLOAD_T>["onFatalError"]

    /**
     * Wraps each job's execution for contextual logging, tracing, etc.
     */
    runJobWrapper?: WorkflowEngineOptions<PAYLOAD_T>["runJobWrapper"]

    /** Summary log interval in ms. Defaults to 30 seconds. */
    summaryIntervalMs?: number

    /**
     * Called after each pick cycle with the number of picked jobs.
     * Use for metrics, observability, or test synchronisation.
     */
    onPickComplete?: (result: { pickedJobs: number }) => void

    // --- Lifecycle ---

    onError: (e: any) => void
    onComplete: () => void
}

// =========================================================================
// ServerWorkflow
// =========================================================================

export class ServerWorkflow<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    readonly tokenRing: TokenRingInstance
    private readonly engine: WorkflowEngine<PAYLOAD_T>
    /** The first adapter is always the primary job table */
    private readonly adapters: WorkflowJobStorage<PAYLOAD_T>[]

    private constructor(args: {
        engine: WorkflowEngine<PAYLOAD_T>
        adapters: WorkflowJobStorage<PAYLOAD_T>[]
        tokenRing: TokenRingInstance
    }) {
        this.engine = args.engine
        this.adapters = args.adapters
        this.tokenRing = args.tokenRing
    }

    /** The unique executor ID for this server (from the token ring). */
    exec_id(): string {
        return this.tokenRing.issuer_id
    }

    /**
     * Start a specific job immediately on this server (bypasses normal pick cycle).
     * Fetches the job from the first (primary) storage adapter.
     */
    async startJob(args: {
        payload: { type: string } & Record<string, any>
        jobKey: WorkflowJobKey
        execution_id: string
    }): Promise<WorkflowJobOutcome | void> {
        const storage = this.adapters[0]!
        const job = await storage.getJob(args.jobKey)
        if (!job) {
            console.error("startJob: job not found", args.jobKey)
            return
        }
        return this.engine.startJob({
            jobKey: args.jobKey,
            job: job as WorkflowJobValue<PAYLOAD_T>,
            storage,
            executorId: args.execution_id,
        }).catch(e => {
            console.error("Fatal error starting job", args.jobKey, e)
        })
    }

    /** Stop the engine and token ring. */
    Destroy(): void {
        this.engine.destroy()
        this.tokenRing.Destroy()
    }

    // ------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------

    /**
     * Create and start a fully-wired workflow server.
     *
     * Returns `null` if no capabilities are registered (nothing to run).
     */
    static async Create<PAYLOAD_T extends BasicJobPayload = BasicJobPayload>(
        options: ServerWorkflowCreateOptions<PAYLOAD_T>,
    ): Promise<ServerWorkflow<PAYLOAD_T> | null> {
        if (options.capabilities.runnerMap.size === 0) {
            console.warn("No workflow capabilities registered, not starting workflow")
            return null
        }

        const { adapters, capabilities, config, enumLookup } = options

        const pickers = adapters.map(adapter => new WorkflowPicker<PAYLOAD_T>({
            storage: adapter,
            capabilities,
            config,
            enumLookup,
        }))

        const engine = new WorkflowEngine<PAYLOAD_T>({
            pickers,
            capabilities,
            config,
            enumLookup,
            suppress_error_emails: options.suppressErrorEmails,
            onFatalError: options.onFatalError,
            runJobWrapper: options.runJobWrapper,
            summaryIntervalMs: options.summaryIntervalMs,
        })

        const tokenRing = await options.tokenRingFactory({
            engine,
            capabilitiesBuffer: capabilities.capabilitiesBuffer,
            config,
            getRunningCount: () => JobManager.JobRunningCount.total,
            onPickComplete: options.onPickComplete,
            onError: options.onError,
            onComplete: options.onComplete,
        })

        return new ServerWorkflow<PAYLOAD_T>({
            engine,
            adapters,
            tokenRing,
        })
    }
}
