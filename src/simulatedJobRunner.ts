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
    /** Provide a shared store instead of creating a fresh one per runner. */
}

export abstract class SimulatedJobRunner<PAYLOAD_T extends BasicJobPayload, T extends PAYLOAD_T["type"]> extends JobRunner<PAYLOAD_T, T, WorkflowStorageTransaction<PAYLOAD_T>> {
    readonly memoryStore;
    constructor(options: SimulatedJobRunnerOptions<PAYLOAD_T>) {
        const storage = new InMemoryJobStorage<PAYLOAD_T>({
            typeIndex: options.typeIndex ?? false,
        })
        const jobKey = options.jobKey ?? { job_id: v4() }
        const execution_id = v4()

        // Seed the job into the in-memory store synchronously via a transaction
        // that resolves before super() needs it.
        const job = {
            ...options.job,
            header: {
                ...options.job.header,
                execution_id,
            },
        }
        // We need to insert the job before the base class constructor reads it.
        // MVCCCore.Store.doTn is synchronous-start so we kick it off and let
        // the base class's GetUpdatedJob() await it naturally.
        const seedPromise = storage.doTn(async txn => { txn.job.set(jobKey, job) })
        super({ jobKey, execution_id, store: storage, ready: seedPromise, type: job.payload.type as T })
        this.memoryStore = storage
    }
}
