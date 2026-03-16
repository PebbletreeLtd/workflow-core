/**
 * Workflow job picker.
 *
 * Scans for ready jobs, filters by capability, and atomically adopts them
 * for execution. One picker instance corresponds to one storage table.
 *
 * The pick loop retries on transactional conflicts (code 1020) and adapts
 * the batch size based on the server's load relative to the ring average.
 */
import type {
    BasicJobPayload,
    WorkflowConfig,
    WorkflowJobKey,
    WorkflowJobValue,
} from "./workflowTypes"
import type { WorkflowJobStorage } from "./workflowStorageAdapter"
import type { WorkflowCapabilities } from "./workflowCapabilities"
import { WorkflowCounter } from "./counter"

// =========================================================================
// Types
// =========================================================================

export interface PickContext {
    /** The unique ID of this picking server / executor */
    executorId: string
    /** Average workload across all servers in the ring */
    averageWorkload: number
    /** Current number of running jobs on this server */
    currentRunning: number
    /**
     * Ring-level capability state for "unsupported job" detection.
     * Omit to only pick jobs this server can run locally.
     */
    ringState?: {
        /** Whether the ring capabilities are still being established (provisional) */
        isProvisional: boolean
        /**
         * String type names that the entire ring can handle.
         *
         * If a job type is NOT in this set and the ring is established,
         * the picker adopts it anyway (it will fail-and-reschedule, clearing
         * the queue for when a capable server joins).
         */
        supportedTypes: Set<string>
    }
    /**
     * Optional short-circuit: if this returns true the pick is aborted
     * immediately (e.g. graceful shutdown).
     */
    isTerminating?: () => boolean
}

export interface PickedJob<PAYLOAD_T extends BasicJobPayload> {
    jobKey: WorkflowJobKey
    job: WorkflowJobValue<PAYLOAD_T>
}

// =========================================================================
// Picker
// =========================================================================

export class WorkflowPicker<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    /** Exposed so the engine / orchestrator can create JobManagers against the same storage */
    readonly storage: WorkflowJobStorage<PAYLOAD_T>
    private readonly capabilities: WorkflowCapabilities<PAYLOAD_T>
    private readonly config: WorkflowConfig
    private readonly enumLookup: Record<string, number>

    constructor(args: {
        storage: WorkflowJobStorage<PAYLOAD_T>
        capabilities: WorkflowCapabilities<PAYLOAD_T>
        config: WorkflowConfig
        /** Maps string payload type names → numeric enum values (used for canRunType) */
        enumLookup: Record<string, number>
    }) {
        this.storage = args.storage
        this.capabilities = args.capabilities
        this.config = args.config
        this.enumLookup = args.enumLookup
    }

    /**
     * Run one pick cycle: scan → filter → adopt.
     *
     * Returns the list of jobs that were successfully adopted. The caller is
     * responsible for actually running them (via JobManager / WorkflowEngine).
     */
    async pick(ctx: PickContext): Promise<PickedJob<PAYLOAD_T>[]> {
        try {
            if (ctx.isTerminating?.()) return []

            const runningRatioToAverage =
                (0.0001 + ctx.currentRunning) / (0.0001 + ctx.averageWorkload)

            // Adapt what we pick based on how busy we are compared with others.
            // Cap so we pick at most 6× less, or at least 2× more.
            const targetJobs = Math.ceil(
                this.config.workflow_batch_size /
                Math.min(6, Math.max(runningRatioToAverage, 0.5)),
            )

            let max_cycles = 10
            while (max_cycles-- >= 0) {
                const jobs = await (async (): Promise<PickedJob<PAYLOAD_T>[] | undefined> => {
                    try {
                        // Phase I — snapshot scan for candidate job keys.
                        // The storage adapter uses a snapshot / non-conflicting read
                        // so concurrent inserts don't cause transaction conflicts.
                        const candidates = await this.storage.scanReadyJobs({
                            fromAt: 1,
                            toAt: Date.now(),
                            limit: targetJobs * 3, // overscan to account for capability filtering
                        })

                        // Filter by capability (local + ring)
                        const filtered: typeof candidates = []
                        for (const candidate of candidates) {
                            const jobType = candidate.indexMeta.type
                            if (jobType && this.capabilities.canRunType(jobType, this.enumLookup)) {
                                // This server can handle the type — pick it
                                filtered.push(candidate)
                            } else if (ctx.ringState && !ctx.ringState.isProvisional) {
                                // Ring is established — check if *any* server can handle it
                                if (jobType === undefined || !ctx.ringState.supportedTypes.has(jobType)) {
                                    // Unsupported by the entire ring — pick anyway
                                    // (will fail and reschedule, which clears the queue)
                                    filtered.push(candidate)
                                }
                            }
                            if (filtered.length >= targetJobs) break
                        }

                        // Phase II — adopt each candidate in its own transaction
                        // to minimise conflict surface.
                        const results = await Promise.all(
                            filtered.map(async ({ jobKey, indexMeta }) => {
                                try {
                                    const adopted = await this.storage.adoptJob(
                                        jobKey,
                                        indexMeta.at,
                                        ctx.executorId,
                                    )
                                    if (!adopted) return undefined
                                    return { jobKey, job: adopted } as PickedJob<PAYLOAD_T>
                                } catch (e: any) {
                                    if (e?.code === 1020) {
                                        console.warn("Got unexpected conflicts on workflow selection phase II")
                                        return undefined
                                    }
                                    throw e
                                }
                            }),
                        )

                        return results.filter((x): x is PickedJob<PAYLOAD_T> => x !== undefined)
                    } catch (e: any) {
                        if (e?.code === 1020) {
                            console.warn("Got unexpected conflicts on workflow selection snapshot")
                            return undefined // signal conflict → retry cycle
                        }
                        throw e
                    }
                })()

                WorkflowCounter.pegValue({
                    picks: {
                        conflicts: !jobs ? 1 : 0,
                        cycles: 1,
                        got: jobs?.length || 0,
                        requested: targetJobs,
                        cycleUtilisationTotal:
                            ctx.currentRunning / this.config.workflow_ideal_maximum_jobs_running,
                    },
                })

                if (jobs) return jobs
            }

            console.error("Ran out of conflict cycles")
        } catch (e) {
            console.error("Unexpected workflow pick error", e)
        }
        return []
    }
}
