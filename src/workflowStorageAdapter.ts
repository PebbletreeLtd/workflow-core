import { MVCCCore } from "@pebbletree/mvcc-testing"
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
    WorkflowJobValue,
    BasicJobPayload,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
} from "./workflowTypes"

// =========================================================================
// Storage adapter interface
// =========================================================================


export type atSubspaceKey<PAYLOAD_T extends BasicJobPayload> = { at: number, type?: PAYLOAD_T["type"] } & WorkflowJobKey
export interface WorkflowJobStorage<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    doTn: MVCCCore.TransactionFactory<WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>>
    /**
     * Used to retrieve ready jobs for picking. The engine scans by the `at` index
     */
    subspaces: {
        at: MVCCCore.ISubspace<{ at: number }, atSubspaceKey<PAYLOAD_T>, never, unknown>,
        jobLogKey: MVCCCore.ISubspace<WorkflowJobLogKey, WorkflowJobLogKey, unknown, WorkflowJobLogValue>,
        executor: MVCCCore.ISubspace<{ execution_id: string }, WorkflowJobKey, never, unknown>,
    }
}
