/**
 * Workflow job storage adapter interface.
 * 
 * Abstracts all database operations behind self-transactional methods.
 * Each method manages its own transaction internally — no leaked transaction handles.
 * 
 * Consumers implement this interface for their chosen database (FDB, Postgres, etc.)
 * and pass instances to the workflow engine.
 * 
 * One adapter instance corresponds to one "job table" — for systems with
 * multiple job tables, create one adapter per table.
 */
import type {
    WorkflowJobKey,
    BasicJobPayload,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
    WorkflowJobValue,
} from "./workflowTypes"

// =========================================================================
// Storage adapter interface
// =========================================================================


export type atSubspaceKey<PAYLOAD_T extends BasicJobPayload> = { at: number, type?: PAYLOAD_T["type"] } & WorkflowJobKey


export interface WorkflowStorageTransaction<PAYLOAD_T extends BasicJobPayload> {
    job: {
        get: (key: WorkflowJobKey) => Promise<WorkflowJobValue<PAYLOAD_T> | undefined>
        snapshotGet: (key: WorkflowJobKey) => Promise<WorkflowJobValue<PAYLOAD_T> | undefined>
        set: (key: WorkflowJobKey, value: WorkflowJobValue<PAYLOAD_T>) => void
        clear: (key: WorkflowJobKey) => void
    },
    jobLogKey: {
        set: (key: WorkflowJobLogKey, value: WorkflowJobLogValue) => void
    },
    at: {
        getRangeSnapshot: (startKey: { at: number }, endKey: { at: number }, options?: { limit?: number; reverse?: boolean }) => AsyncGenerator<[atSubspaceKey<PAYLOAD_T>, unknown]> // snapshot read of the "at" index
    },
    executor: {
        getRangeAllStartsWith: (startKey: { execution_id: string }, options?: { limit?: number; reverse?: boolean }) => Promise<Array<[{ execution_id: string }, unknown]>>,
    },
}


export interface WorkflowJobStorage<PAYLOAD_T extends BasicJobPayload, TXN extends WorkflowStorageTransaction<PAYLOAD_T>> {
    /**
     * Used to retrieve ready jobs for picking. The engine scans by the `at` index
     */
    doTn: <R>(callback: (txn: TXN) => Promise<R>) => Promise<R>
}
