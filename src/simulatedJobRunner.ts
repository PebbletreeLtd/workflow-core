/**
 * Simulated (in-memory) job runner for testing workflows without a database.
 *
 * Extends JobRunner with an InMemoryJobStorage so subclasses only need to
 * implement `runJob()`. Has the full lifecycle (timers, retry, outcome logging)
 * of a real JobRunner but backed entirely by in-memory storage.
 */
import type { BasicJobPayload, WorkflowJobKey, WorkflowJobValue } from "./workflowTypes"
import { JobRunner } from "./jobRunner"
import { InMemoryJobStorage } from "./inMemoryStorage"
import { v4 } from "uuid"
import { WorkflowStorageTransaction } from "./workflowStorageAdapter"

export interface SimulatedJobRunnerOptions<PAYLOAD_T extends BasicJobPayload> {
    jobKey?: WorkflowJobKey
    job: WorkflowJobValue<PAYLOAD_T>
    typeIndex?: boolean
    /** Provide a shared store instead of creating a fresh one per runner.
     * When set, an existing job at `jobKey` is preserved (only the payload is
     * refreshed and a new `execution_id` is stamped) so retry state, backoff,
     * and snapshots carry across successive runs. */
    store?: InMemoryJobStorage<PAYLOAD_T>
}

export abstract class SimulatedJobRunner<PAYLOAD_T extends BasicJobPayload, T extends PAYLOAD_T["type"]> extends JobRunner<PAYLOAD_T, T, WorkflowStorageTransaction<PAYLOAD_T>> {
    readonly memoryStore;
    constructor(options: SimulatedJobRunnerOptions<PAYLOAD_T>) {
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
        super({ jobKey, execution_id, store: storage, ready: seedPromise, type: options.job.payload.type as T })
        this.memoryStore = storage
    }
}
