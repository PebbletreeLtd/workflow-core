/**
 * In-memory WorkflowJobStorage adapter for tests.
 *
 * Stores everything in plain Maps — no external deps, no atomicity
 * guarantees beyond what JS single-threaded execution gives us.
 */
import type {
    WorkflowJobKey,
    WorkflowJobValue,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
    BasicJobPayload,
} from "../workflowTypes"
import { JobError } from "../jobErrors"
import type {
    WorkflowJobStorage,
    WorkflowJobStorageTransaction,
} from "../workflowStorageAdapter"

function cloneJob<T>(job: T): T {
    return JSON.parse(JSON.stringify(job))
}

export class InMemoryJobStorage<PAYLOAD_T extends BasicJobPayload = BasicJobPayload>
    implements WorkflowJobStorage<PAYLOAD_T> {

    readonly jobs = new Map<string, WorkflowJobValue<PAYLOAD_T>>()
    readonly logs = new Map<string, { key: WorkflowJobLogKey; value: WorkflowJobLogValue }>()

    // ------------------------------------------------------------------
    // WorkflowJobStorage
    // ------------------------------------------------------------------

    async getJob(key: WorkflowJobKey): Promise<WorkflowJobValue<PAYLOAD_T> | null> {
        const job = this.jobs.get(key.job_id)
        return job ? cloneJob(job) : null
    }

    async scanReadyJobs(options: {
        fromAt: number
        toAt: number
        limit: number
    }): Promise<Array<{ jobKey: WorkflowJobKey; indexMeta: { at: number; type?: string } }>> {
        const results: Array<{ jobKey: WorkflowJobKey; indexMeta: { at: number; type?: string } }> = []
        for (const [job_id, job] of this.jobs) {
            if (job.header.at >= options.fromAt && job.header.at <= options.toAt) {
                results.push({
                    jobKey: { job_id },
                    indexMeta: { at: job.header.at, type: job.payload.type },
                })
            }
            if (results.length >= options.limit) break
        }
        // Sort by at ascending
        results.sort((a, b) => a.indexMeta.at - b.indexMeta.at)
        return results.slice(0, options.limit)
    }

    async adoptJob(
        key: WorkflowJobKey,
        expectedAt: number,
        executorId: string,
    ): Promise<WorkflowJobValue<PAYLOAD_T> | null> {
        const job = this.jobs.get(key.job_id)
        if (!job) return null
        if (job.header.at !== expectedAt) return null

        // Self-adopt: push into the future and return null
        if (job.header.execution_id === executorId) {
            job.header.at = Date.now() + job.header.lost_deadline_ms
            return null
        }

        // Fix bad config: lost_deadline must be at least 2× progress_deadline
        if (job.header.lost_deadline_ms < job.header.progress_deadline_ms * 2) {
            job.header.lost_deadline_ms = job.header.progress_deadline_ms * 2
        }

        job.header.execution_id = executorId
        job.header.at = Date.now() + job.header.lost_deadline_ms
        return cloneJob(job)
    }

    async updateJob(
        key: WorkflowJobKey,
        executorId: string,
        mutator: (job: WorkflowJobValue<PAYLOAD_T>) => WorkflowJobValue<PAYLOAD_T>,
    ): Promise<WorkflowJobValue<PAYLOAD_T> | null> {
        const job = this.jobs.get(key.job_id)
        if (!job) return null
        if (job.header.execution_id !== executorId) {
            throw new JobError({ type: "readopted", by: job.header.execution_id || "undefined" })
        }
        const updated = mutator(cloneJob(job))
        this.jobs.set(key.job_id, updated)
        return cloneJob(updated)
    }

    async resetOrphanedJobs(executorId: string): Promise<number> {
        let count = 0
        for (const [, job] of this.jobs) {
            if (job.header.execution_id === executorId) {
                job.header.execution_id = undefined
                job.header.at = Date.now()
                count++
            }
        }
        return count
    }

    async writeLog(key: WorkflowJobLogKey, value: WorkflowJobLogValue): Promise<void> {
        this.logs.set(`${key.job_id}:${key.timestamp}:${key.random}`, { key, value })
    }

    async runInTransaction<T>(
        fn: (adapter: WorkflowJobStorageTransaction<PAYLOAD_T>) => Promise<T>,
    ): Promise<T> {
        // In-memory: transactions are just direct calls against the same data.
        const self = this
        const txn: WorkflowJobStorageTransaction<PAYLOAD_T> = {
            async getJob(key: WorkflowJobKey) {
                return self.getJob(key)
            },
            setJob(key: WorkflowJobKey, value: WorkflowJobValue<PAYLOAD_T>) {
                self.jobs.set(key.job_id, cloneJob(value))
            },
            async getJobByJobId(jobId: string) {
                return self.getJob({ job_id: jobId })
            },
            writeLog(key: WorkflowJobLogKey, value: WorkflowJobLogValue) {
                self.logs.set(`${key.job_id}:${key.timestamp}:${key.random}`, { key, value })
            },
        }
        return fn(txn)
    }

    // ------------------------------------------------------------------
    // Test helpers
    // ------------------------------------------------------------------

    /** Remove all jobs and logs from the store. */
    clear(): void {
        this.jobs.clear()
        this.logs.clear()
    }

    /** Insert a job directly (bypasses adoption). */
    setJob(key: WorkflowJobKey, value: WorkflowJobValue<PAYLOAD_T>): void {
        this.jobs.set(key.job_id, cloneJob(value))
    }
}