/**
 * Workflow job picker.
 *
 * Scans for ready jobs, filters by capability, and atomically adopts them
 * for execution. One picker instance corresponds to one storage table.
 *
 * The pick loop retries on transactional conflicts (code 1020) and adapts
 * the batch size based on the server's load relative to the ring average.
 */
import {
    durationSeconds,
    type BasicJobPayload,
    type WorkflowJobKey,
    type WorkflowJobValue,
} from "./workflowTypes"
import type { atSubspaceKey, WorkflowStorageTransaction, WorkflowJobStorage } from "./workflowStorageAdapter"
import { iWorkflowCounter } from "./counter"
import { WorkflowClock } from "./workflowClock"

// =========================================================================
// Types
// =========================================================================

export interface PickContext<PAYLOAD_T extends BasicJobPayload> {
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
    isTerminating?: () => boolean,
    canRunType: (type: PAYLOAD_T["type"]) => boolean,
    clock: WorkflowClock
    pegCounterValue: (value: Partial<iWorkflowCounter>) => void

}

export interface PickedJob<PAYLOAD_T extends BasicJobPayload> {
    jobKey: WorkflowJobKey
    job: WorkflowJobValue<PAYLOAD_T>
}

// =========================================================================
// Picker
// =========================================================================

export interface WorkflowPickerArgs<PAYLOAD_T extends BasicJobPayload, TXN extends WorkflowStorageTransaction<PAYLOAD_T>> {
    storage: WorkflowJobStorage<PAYLOAD_T, TXN>
    batchSize: number
    idealMaxRunning: number
}

export class WorkflowPicker<PAYLOAD_T extends BasicJobPayload, TXN extends WorkflowStorageTransaction<PAYLOAD_T>> {
    /** Exposed so the engine / orchestrator can create JobRunners against the same storage */
    readonly storage: WorkflowJobStorage<PAYLOAD_T, TXN>
    private readonly batchSize: number
    private readonly idealMaxRunning: number

    constructor(args: WorkflowPickerArgs<PAYLOAD_T, TXN>) {
        this.storage = args.storage
        this.batchSize = args.batchSize
        this.idealMaxRunning = args.idealMaxRunning
    }

    /**
     * Run one pick cycle: scan → filter → adopt.
     *
     * Returns the list of jobs that were successfully adopted. The caller is
     * responsible for actually running them (via JobRunner / WorkflowEngine).
     */
    async pick(ctx: PickContext<PAYLOAD_T>): Promise<PickedJob<PAYLOAD_T>[]> {
        try {
            if (ctx.isTerminating?.()) return []

            const runningRatioToAverage =
                (0.0001 + ctx.currentRunning) / (0.0001 + ctx.averageWorkload)

            // Adapt what we pick based on how busy we are compared with others.
            // Cap so we pick at most 6× less, or at least 2× more.
            const targetJobs = Math.ceil(
                this.batchSize /
                Math.min(6, Math.max(runningRatioToAverage, 0.5)),
            )

            let max_cycles = 10
            while (max_cycles-- >= 0) {
                const jobs = await (async (): Promise<PickedJob<PAYLOAD_T>[] | undefined> => {
                    try {
                        // Phase I — snapshot scan for candidate job keys.
                        // The storage adapter uses a snapshot / non-conflicting read
                        // so concurrent inserts don't cause transaction conflicts.

                        const filtered = await this.storage.doTn(async txn => {

                            const filtered: Array<atSubspaceKey<PAYLOAD_T>> = [];
                            const candidates = txn.at.getRangeSnapshot({ at: 1 }, { at: ctx.clock.now() });
                            for await (const [candidate] of candidates) {
                                let jobType = candidate.type

                                // If the at-index doesn't carry the type (non-type-indexed storage),
                                // look up the actual job to determine its type.
                                if (jobType === undefined) {
                                    const job = await txn.job.snapshotGet({ job_id: candidate.job_id })
                                    jobType = job?.payload.type
                                }

                                if (jobType && ctx.canRunType(jobType)) {
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
                            return filtered
                        });


                        // Phase II — adopt each candidate in its own transaction
                        // to minimise conflict surface.
                        const results = await Promise.all(
                            filtered.map(async (jobKey) => {
                                try {
                                    const adopted = await this.storage.doTn(async txn => {
                                        const job = await txn.job.get(jobKey)

                                        if (!job) return null
                                        if (job.header.at !== jobKey.at) return null // stale — job was modified since scan

                                        // Fix bad configuration: lost_deadline must be at least 2× progress_deadline
                                        let progress_deadline_ms = job.header.progress_deadline_ms
                                        if (job.header.lost_deadline_ms < job.header.progress_deadline_ms * 2) {
                                            console.warn("Resetting bad job configuration (lost<progress*2)")
                                            progress_deadline_ms = job.header.lost_deadline_ms * 2
                                        }

                                        if (job.header.execution_id) {
                                            if (job.header.execution_id === ctx.executorId) {
                                                // Self-adopt: don't take it, but push into the future hoping
                                                // the issue clears itself up.
                                                const pushed = {
                                                    ...job,
                                                    header: { ...job.header, at: ctx.clock.now() + durationSeconds(5) },
                                                }
                                                txn.job.set(jobKey, pushed)
                                                console.log("Refusing to re-adopt own job. Job configuration issue?", jobKey, "Pushed into the future:", pushed)
                                                return null
                                            }
                                            console.log("Adopted lost job", jobKey, "from executor", job.header.execution_id)
                                        }

                                        const adopted = {
                                            ...job,
                                            header: {
                                                ...job.header,
                                                at: ctx.clock.now() + job.header.lost_deadline_ms,
                                                progress_deadline_ms,
                                                execution_id: ctx.executorId,
                                            },
                                        }
                                        txn.job.set({ job_id: jobKey.job_id }, adopted)
                                        return adopted
                                    });

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

                ctx.pegCounterValue({
                    picks: {
                        conflicts: !jobs ? 1 : 0,
                        cycles: 1,
                        got: jobs?.length || 0,
                        requested: targetJobs,
                        cycleUtilisationTotal:
                            ctx.currentRunning / this.idealMaxRunning,
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

    /**
     * Reset all lost jobs for the given executor in this picker's storage.
     * Called when the ring detects an unresponsive server.
     */
    async resetLostJobs(executorId: string, ctx: { clock: WorkflowClock }): Promise<number> {
        let total = 0
        let iterEscapeCounter = 1000 // safeguard: at most 50,000 jobs (50 per batch)

        while (iterEscapeCounter-- > 0) {
            const length = await this.storage.doTn(async (txn) => {

                const jobs: [any, any][] = await txn.executor.getRangeAllStartsWith(
                    { execution_id: executorId },
                    { limit: 50 },
                )

                await Promise.all(
                    jobs.map(async ([key]: any) => {
                        console.debug(
                            "Resetting job",
                            key,
                            "which was running on unresponsive server",
                            executorId,
                        )

                        const job = await txn.job.get({ job_id: key.job_id })
                        if (job) {
                            txn.job.set(
                                { job_id: key.job_id },
                                {
                                    ...job,
                                    header: {
                                        ...job.header,
                                        at: ctx.clock.now(),
                                        execution_id: undefined,
                                    },
                                },
                            )
                        }
                    }),
                )
                return jobs.length
            })

            total += length
            if (length <= 0) break
        }

        if (iterEscapeCounter <= 0) {
            console.error(
                "Escape counter hit while resetting jobs for unresponsive server",
                executorId,
                " something is wrong with the index",
            )
        }

        return total
    }
}
