/**
 * Simulated (in-memory) job runner for testing workflows without a database.
 *
 * Extends JobRunner with an InMemoryJobStorage so subclasses only need to
 * implement `runJob()`. Has the full lifecycle (timers, retry, outcome logging)
 * of a real JobRunner but backed entirely by in-memory storage.
 */
import type { BasicJobPayload, WorkflowJobKey, WorkflowJobValue } from "./workflowTypes"
import { JobRunner, type JobRunnerOptions } from "./jobRunner"
import { InMemoryJobStorage } from "./inMemoryStorage"
import { v4 } from "uuid"
import { WorkflowStorageTransaction } from "./workflowStorageAdapter"
import { defaultWorkflowClock, type WorkflowClock } from "./workflowClock"

export interface SimulatedJobRunnerOptions<PAYLOAD_T extends BasicJobPayload, CTX = never> {
    jobKey?: WorkflowJobKey
    job: WorkflowJobValue<PAYLOAD_T>
    typeIndex?: boolean
    /** Provide a shared store instead of creating a fresh one per runner.
     * When set, an existing job at `jobKey` is preserved (only the payload is
     * refreshed and a new `execution_id` is stamped) so retry state, backoff,
     * and snapshots carry across successive runs. */
    store?: InMemoryJobStorage<PAYLOAD_T>
    /** Optional clock (defaults to `defaultWorkflowClock`). Pass a
     * `SimulatedWorkflowClock` to drive time deterministically in tests. */
    clock?: WorkflowClock,
    ctx?: CTX
}

export abstract class SimulatedJobRunner<
    PAYLOAD_T extends BasicJobPayload,
    T extends PAYLOAD_T["type"],
    CTX = never
> extends JobRunner<PAYLOAD_T, T, WorkflowStorageTransaction<PAYLOAD_T>, CTX> {
    readonly memoryStore: InMemoryJobStorage<PAYLOAD_T>
    // Supports both test-driven instantiation (with `job` for auto-seeding)
    // and engine-driven instantiation as a JobRunnerConstructor (job already
    // in the shared store).
    constructor(options: SimulatedJobRunnerOptions<PAYLOAD_T, CTX> | JobRunnerOptions<PAYLOAD_T, T, WorkflowStorageTransaction<PAYLOAD_T>, CTX>) {
        if (!("job" in options)) {
            super(options as any)
            this.memoryStore = options.store as InMemoryJobStorage<PAYLOAD_T>
            return
        }
        const storage = options.store ?? new InMemoryJobStorage<PAYLOAD_T>({
            typeIndex: options.typeIndex ?? false,
        })
        const jobKey = options.jobKey ?? { job_id: v4() }
        const execution_id = v4()

        const seedPromise = storage.doTn(async txn => {
            const existing = await txn.job.get(jobKey)
            const base = existing ?? options.job
            txn.job.set(jobKey, {
                ...base,
                payload: options.job.payload,
                header: { ...base.header, execution_id },
            })
        })
        super({
            pegCounterValue(_) {

            }, jobKey, execution_id, store: storage, ready: seedPromise, type: options.job.payload.type as T, clock: options.clock ?? defaultWorkflowClock,
            context: options.ctx as never,
        })
        this.memoryStore = storage
    }
}
