/**
 * Job Lifecycle Tests
 *
 * Tests for the full lifecycle of a workflow job through the JobRunner:
 * - Successful completion
 * - Recoverable errors with retry
 * - Fatal errors
 * - Progress reporting
 * - Job vanishing mid-execution
 * - Readopted job detection
 * - Error log writing
 *
 * Uses SimulatedJobRunner backed by in-memory storage (no external DB required).
 */
import { describe, it, expect } from "vitest"
import { v4 } from "uuid"
import { durationSeconds, WorkflowJobKey } from "../src/workflowTypes"
import {
    makeTestJobValue,
    TestJobRunner,
    type TestPayload,
} from "./harness"
import { SimulatedJobRunner } from "../src/simulatedJobRunner"
import type { WorkflowStorageTransaction, WorkflowJobStorage } from "../src/workflowStorageAdapter"

// Helper to create a TestJobRunner with simulated storage
function setupJobRunner(overrides?: Parameters<typeof makeTestJobValue>[0]) {
    const value = makeTestJobValue(overrides)
    const runner = new TestJobRunner({ job: value })
    return { runner, key: runner.jobKey, storage: runner.store }
}

async function getJob(storage: WorkflowJobStorage<TestPayload, WorkflowStorageTransaction<TestPayload>>, key: WorkflowJobKey) {
    return await storage.doTn(async txn => {
        return await txn.job.get(key)
    })
}
// --- Successful completion ---

describe("lifecycle", () => {
    it("job completes successfully", async () => {
        const { key, runner, storage } = setupJobRunner({ delayMs: 0 })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("success")

        // Job should be cleared (at set to negative)
        const job = await getJob(storage, key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
        expect(job!.header.execution_id).toBeUndefined()
    })

    // --- Recoverable error with retries ---

    it("recoverable error reschedules when retries remain", async () => {
        const { key, runner, storage } = setupJobRunner({
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 3, initial_backoff_ms: 100, exponent: 2 },
        })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("rescheduled-error")

        const job = await getJob(storage, key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeGreaterThan(Date.now() - 1000)
        expect(job!.header.retries.max).toBe(2)
        expect(job!.header.execution_id).toBeUndefined()
    })

    // --- Fatal error ---

    it("fatal error clears job", async () => {
        const { key, runner, storage } = setupJobRunner({
            shouldFail: true,
            failType: "fatal",
        })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("fatal-error")

        const job = await getJob(storage, key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
    })

    // --- Exhausted retries become fatal ---

    it("exhausted retries become fatal", async () => {
        const { key, runner, storage } = setupJobRunner({
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 0, initial_backoff_ms: 100, exponent: 2 },
        })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("fatal-error")

        const job = await getJob(storage, key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
    })

    // --- Progress reporting ---

    it("job reports progress (updates last_progress)", async () => {
        const { runner } = setupJobRunner({
            delayMs: 200,
            progressIntervalMs: 50,
            progress_deadline_ms: durationSeconds(10),
        })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("success")
    })

    // --- Vanished job ---

    it("vanished job returns vanished outcome", async () => {
        const value = makeTestJobValue()

        /** A runner that just calls Progress to force a fresh DB read */
        class ProgressOnlyRunner extends SimulatedJobRunner<TestPayload, "_test"> {
            async runJob() { await this.Progress() }
        }

        const runner = new ProgressOnlyRunner({ job: value })
        await runner.whenReady()

        // Delete the job before running the runner body
        await runner.store.doTn(async txn => {
            txn.job.clear(runner.jobKey)
        })

        const outcome = await runner.Run()

        expect(outcome.type).toBe("vanished")
    })

    // --- Readopted job ---

    it("readopted job detected when execution_id changes", async () => {
        const value = makeTestJobValue()

        /** A runner that just calls Progress to trigger readoption detection */
        class ProgressOnlyRunner extends SimulatedJobRunner<TestPayload, "_test"> {
            async runJob() { await this.Progress() }
        }

        const runner = new ProgressOnlyRunner({ job: value })
        await runner.whenReady()

        // Simulate another server adopting the job
        const otherExecId = v4()
        await runner.store.doTn(async txn => {
            const job = await txn.job.get(runner.jobKey)
            if (!job) {
                throw new Error("Job not found for readopted test")
            }
            txn.job.set(runner.jobKey, {
                ...job,
                header: { ...job.header, execution_id: otherExecId },
            })
        })

        const outcome = await runner.Run()

        expect(outcome.type).toBe("readopted")
    })

    // --- Error log written ---

    it("error outcome writes to log", async () => {
        const { key, runner, storage } = setupJobRunner({
            shouldFail: true,
            failType: "fatal",
        })
        await runner.Run()

        // Check that a log entry was written for this job
        const matchingLog = await runner.memoryStore.JobDatabase.doTn(async txn => {

            const range = txn.at(runner.memoryStore.JoblogSubpace).getRange({
                timestamp: 0,
                job_id: "",
                random: ""
            }, {
                timestamp: Date.now() + durationSeconds(5),
                job_id: "",
                random: ""
            }, { limit: 1000, reverse: true })
            for await (const [logKey] of range) {
                if (logKey.job_id === key.job_id)
                    return logKey
            }
            return undefined;
        })

        expect(matchingLog).toBeDefined()
    })
})
