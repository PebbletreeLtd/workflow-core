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
    WorkflowJobOutcome,
} from "./workflowTypes"

// =========================================================================
// Storage adapter interface
// =========================================================================

export interface WorkflowJobStorage<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {

    /**
     * Read a single job by key.
     * Returns null if the job does not exist.
     */
    getJob(key: WorkflowJobKey): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /**
     * Scan jobs whose execution time (header.at) falls within [fromAt, toAt].
     * Returns up to `limit` jobs, ordered by execution time ascending.
     * 
     * This should use a **snapshot read** (or equivalent non-conflicting read)
     * to avoid contention with adoption writes.
     * 
     * The returned `indexMeta` contains the `at` value from the index (used to
     * verify freshness during adoption) and an optional `type` string if the
     * index carries it (avoids a full job read for type filtering).
     */
    scanReadyJobs(options: {
        fromAt: number
        toAt: number
        limit: number
    }): Promise<Array<{
        jobKey: WorkflowJobKey
        indexMeta: { at: number, type?: string }
    }>>

    /**
     * Attempt to atomically adopt a job for execution.
     * 
     * Reads the job, validates:
     *  - Job exists
     *  - header.at matches expectedAt (ensures freshness — job hasn't been modified since scan)
     *  - header.execution_id is unclaimed or from a different executor
     *  - If execution_id === executorId (self-adopt), pushes job into the future and returns null
     * 
     * If valid, sets execution_id, fixes bad config (lost_deadline < progress_deadline * 2),
     * and bumps header.at to `now + job.header.lost_deadline_ms` to create a "lease".
     * Returns the adopted job value, or null if adoption fails (conflict/stale/missing/self-adopt).
     * 
     * @param key The job key
     * @param expectedAt The `at` value observed during the scan — used as an optimistic lock
     * @param executorId The adopting server's unique ID
     */
    adoptJob(
        key: WorkflowJobKey,
        expectedAt: number,
        executorId: string,
    ): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /**
     * Atomically read-then-write a job.
     * 
     * Reads the job, validates execution_id matches the given executorId,
     * applies the mutator function to produce a new value, and writes it back.
     * 
     * Returns the updated job value, or null if the job is missing/stolen.
     * 
     * This covers: progress pushback, timer resets, clearing (negate at),
     * rescheduling, retry backoff, etc.
     */
    updateJob(
        key: WorkflowJobKey,
        executorId: string,
        mutator: (job: WorkflowJobValue<PAYLOAD_T>) => WorkflowJobValue<PAYLOAD_T>,
    ): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /**
     * Read a job for the purpose of validation (e.g. checking execution_id).
     * Unlike getJob, this may be called within a broader operation context.
     * 
     * Default implementations can simply delegate to getJob().
     */
    getJobForValidation?(key: WorkflowJobKey, executorId: string): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /**
     * Reset all orphaned jobs belonging to the given executor.
     * 
     * Scans by executor index, clears execution_id and resets header.at to now
     * on all matching jobs. Returns the number of jobs reset.
     * 
     * Called when the token ring detects an unresponsive server.
     */
    resetOrphanedJobs(executorId: string): Promise<number>

    /**
     * Write a structured log entry.
     */
    writeLog(key: WorkflowJobLogKey, value: WorkflowJobLogValue): Promise<void>

    /**
     * Run a compound operation atomically.
     * 
     * The callback receives a "transactional" view of this adapter where
     * multiple reads/writes happen in one transaction.
     * 
     * Implementations that don't support transactions can simply execute
     * the callback against `this`.
     */
    runInTransaction<T>(fn: (adapter: WorkflowJobStorageTransaction<PAYLOAD_T>) => Promise<T>): Promise<T>
}

/**
 * Transactional view of the storage adapter.
 * 
 * All methods on this interface execute within a single database transaction.
 * Used by `runInTransaction` for operations that need atomicity across
 * multiple reads/writes (e.g., read job + validate + write + write log).
 */
export interface WorkflowJobStorageTransaction<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    /** Read a job by key within the transaction. */
    getJob(key: WorkflowJobKey): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /** Write/update a job within the transaction. */
    setJob(key: WorkflowJobKey, value: WorkflowJobValue<PAYLOAD_T>): void

    /** Read the full job value for a job key within this transaction. */
    getJobByJobId(jobId: string): Promise<WorkflowJobValue<PAYLOAD_T> | null>

    /** Write a log entry within the transaction. */
    writeLog(key: WorkflowJobLogKey, value: WorkflowJobLogValue): void

    /**
     * Optional post-processing hook called within the transaction after a
     * job outcome has been determined.
     *
     * This replaces the old "postRunner" concept: the storage adapter
     * implementation dispatches to type-specific post-processors via a
     * case statement, with full transactional database access.
     *
     * May return a modified outcome (e.g. escalating to fatal-error).
     * If not implemented, the outcome passes through unchanged.
     */
    processJobOutcome?(args: {
        outcome: WorkflowJobOutcome
        payload: PAYLOAD_T
        jobKey: WorkflowJobKey
    }): Promise<WorkflowJobOutcome>
}
