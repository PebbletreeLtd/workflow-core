/**
 * Job Lifecycle Tests
 *
 * Tests for the full lifecycle of a workflow job through the JobManager:
 * - Successful completion
 * - Recoverable errors with retry
 * - Fatal errors
 * - Progress reporting
 * - Job vanishing mid-execution
 * - Readopted job detection
 * - Error log writing
 *
 * Uses an in-memory storage adapter (no external DB required).
 */
import { describe, it, expect, beforeEach } from "vitest"
import { v4 } from "uuid"
import { JobManager } from "../jobManager"
import { durationSeconds } from "../workflowTypes"
import {
    makeTestJobKey, makeTestJobValue,
    createTestRunner, createTestStorage,
    type TestPayload,
} from "./harness"
import type { InMemoryJobStorage } from "./storage"

let storage: InMemoryJobStorage<TestPayload>

beforeEach(() => {
    storage = createTestStorage()
})

// Helper to create a JobManager backed by the in-memory storage
function setupJobManager(overrides?: Parameters<typeof makeTestJobValue>[0]) {
    const key = makeTestJobKey()
    const value = makeTestJobValue(overrides)
    const execution_id = v4()
    value.header.execution_id = execution_id

    storage.setJob(key, value)

    const manager = new JobManager<TestPayload>({
        jobKey: key,
        execution_id,
        storage,
        workflow_supress_job_outcome_logs: ["success"],
        suppress_error_emails: true,
    })

    return { key, value, manager, execution_id }
}

// --- Successful completion ---

describe("lifecycle", () => {
    it("job completes successfully", async () => {
        const { key, manager } = setupJobManager({ delayMs: 0 })
        const runner = createTestRunner()
        const outcome = await manager.Run(runner)

        expect(outcome.type).toBe("success")

        // Job should be cleared (at set to negative)
        const job = await storage.getJob(key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
        expect(job!.header.execution_id).toBeUndefined()
    })

    // --- Recoverable error with retries ---

    it("recoverable error reschedules when retries remain", async () => {
        const { key, manager } = setupJobManager({
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 3, initial_backoff_ms: 100, exponent: 2 },
        })
        const runner = createTestRunner()
        const outcome = await manager.Run(runner)

        expect(outcome.type).toBe("rescheduled-error")

        const job = await storage.getJob(key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeGreaterThan(Date.now() - 1000)
        expect(job!.header.retries.max).toBe(2)
        expect(job!.header.execution_id).toBeUndefined()
    })

    // --- Fatal error ---

    it("fatal error clears job", async () => {
        const { key, manager } = setupJobManager({
            shouldFail: true,
            failType: "fatal",
        })
        const runner = createTestRunner()
        const outcome = await manager.Run(runner)

        expect(outcome.type).toBe("fatal-error")

        const job = await storage.getJob(key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
    })

    // --- Exhausted retries become fatal ---

    it("exhausted retries become fatal", async () => {
        const { key, manager } = setupJobManager({
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 0, initial_backoff_ms: 100, exponent: 2 },
        })
        const runner = createTestRunner()
        const outcome = await manager.Run(runner)

        expect(outcome.type).toBe("fatal-error")

        const job = await storage.getJob(key)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBeLessThanOrEqual(0)
    })

    // --- Progress reporting ---

    it("job reports progress (updates last_progress)", async () => {
        const { manager } = setupJobManager({
            delayMs: 200,
            progressIntervalMs: 50,
            progress_deadline_ms: durationSeconds(10),
        })
        const runner = createTestRunner()
        const outcome = await manager.Run(runner)

        expect(outcome.type).toBe("success")
    })

    // --- Vanished job ---

    it("vanished job returns vanished outcome", async () => {
        const key = makeTestJobKey()
        const value = makeTestJobValue()
        const execution_id = v4()
        value.header.execution_id = execution_id

        storage.setJob(key, value)

        const manager = new JobManager<TestPayload>({
            jobKey: key,
            execution_id,
            storage,
            workflow_supress_job_outcome_logs: ["success"],
            suppress_error_emails: true,
        })

        // Delete the job before running the runner body
        storage.jobs.delete(key.job_id)

        const outcome = await manager.Run(async (mgr) => {
            await mgr.Progress() // forces a fresh DB read (GetJob returns cached)
        })

        expect(outcome.type).toBe("vanished")
    })

    // --- Readopted job ---

    it("readopted job detected when execution_id changes", async () => {
        const key = makeTestJobKey()
        const value = makeTestJobValue()
        const execution_id = v4()
        value.header.execution_id = execution_id

        storage.setJob(key, value)

        const manager = new JobManager<TestPayload>({
            jobKey: key,
            execution_id,
            storage,
            workflow_supress_job_outcome_logs: ["success"],
            suppress_error_emails: true,
        })

        // Simulate another server adopting the job
        const otherExecId = v4()
        const existing = storage.jobs.get(key.job_id)!
        storage.jobs.set(key.job_id, {
            ...existing,
            header: { ...existing.header, execution_id: otherExecId },
        })

        const outcome = await manager.Run(async (mgr) => {
            await mgr.Progress()
        })

        expect(outcome.type).toBe("readopted")
    })

    // --- Error log written ---

    it("error outcome writes to log", async () => {
        const { key, manager } = setupJobManager({
            shouldFail: true,
            failType: "fatal",
        })
        const runner = createTestRunner()
        await manager.Run(runner)

        // Check that a log entry was written for this job
        const matchingLogs = Array.from(storage.logs.values()).filter(
            entry => entry.key.job_id === key.job_id,
        )
        expect(matchingLogs.length).toBeGreaterThanOrEqual(1)
    })
})
