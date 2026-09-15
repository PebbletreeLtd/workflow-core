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
import { describe, it } from "node:test"
import { expect } from "./expect"
import { v4 } from "uuid"
import { durationSeconds, WorkflowJobKey } from "../workflowTypes"
import {
    makeTestJobValue,
    TestJobRunner,
    type TestPayload,
} from "./harness"
import { SimulatedJobRunner } from "../simulatedJobRunner"
import { SimulatedWorkflowClock } from "../simulatedWorkflowClock"
import { defaultWorkflowClock } from "../workflowClock"
import type { WorkflowStorageTransaction, WorkflowJobStorage } from "../workflowStorageAdapter"

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
        const clock = new SimulatedWorkflowClock(10_000)
        const value = makeTestJobValue({
            clock,
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 3, initial_backoff_ms: 100, exponent: 2 },
        })
        const runner = new TestJobRunner({ job: value, clock })
        const outcome = await runner.Run()

        expect(outcome.type).toBe("rescheduled-error")

        const job = await getJob(runner.store, runner.jobKey)
        expect(job).not.toBeNull()
        expect(job!.header.at).toBe(10_100)
        expect(job!.header.retries.max).toBe(2)
        expect(job!.header.retries.initial_backoff_ms).toBe(200)
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
        const { key, runner } = setupJobRunner({
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
                timestamp: defaultWorkflowClock.now() + durationSeconds(5),
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

    // --- Retry policy reset on success ---

    it("restores retry policy on next scheduled run after a successful attempt", async () => {
        // Run 1: fail once, consuming a retry.
        const first = setupJobRunner({
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 3, initial_backoff_ms: 100, exponent: 2 },
            repeatSchedule: {
                type: "periodic",
                unit: "minutes",
                period: 1,
                nextDate: defaultWorkflowClock.now(),
                until: { format: "forever" },
            },
        })
        const outcome1 = await first.runner.Run()
        expect(outcome1.type).toBe("rescheduled-error")

        const afterFail = await getJob(first.storage, first.key)
        expect(afterFail!.header.retries.max).toBe(2)
        expect(afterFail!.header.retries.initial_backoff_ms).toBe(200)

        // Run 2: same job in the same store, this time succeeds.
        const succeedingPayload = makeTestJobValue({ shouldFail: false })
        const runner2 = new TestJobRunner({
            store: first.runner.memoryStore,
            jobKey: first.key,
            job: succeedingPayload,
        })
        const outcome2 = await runner2.Run()
        expect(outcome2.type).toBe("success")

        const afterSuccess = await getJob(first.storage, first.key)
        expect(afterSuccess).not.toBeNull()
        // Rescheduled to next occurrence…
        expect(afterSuccess!.header.at).toBeGreaterThan(defaultWorkflowClock.now())
        // …with the retry budget fully restored.
        expect(afterSuccess!.header.retries.max).toBe(3)
        expect(afterSuccess!.header.retries.initial_backoff_ms).toBe(100)
    })

    // --- Exponential backoff sequence across multiple retries ---

    it("exponential backoff advances at and initial_backoff_ms across successive retries", async () => {
        const clock = new SimulatedWorkflowClock(10_000)
        const value = makeTestJobValue({
            clock,
            shouldFail: true,
            failType: "recoverable",
            retries: { max: 3, initial_backoff_ms: 100, exponent: 2 },
        })
        const runner1 = new TestJobRunner({ job: value, clock })
        const key = runner1.jobKey
        const store = runner1.memoryStore

        const attempts: Array<{ at: number; max: number; backoff: number }> = []

        // Attempt 1 at t=10_000 → at becomes 10_100, backoff doubles to 200
        expect((await runner1.Run()).type).toBe("rescheduled-error")
        let job = (await getJob(store, key))!
        attempts.push({ at: job.header.at, max: job.header.retries.max, backoff: job.header.retries.initial_backoff_ms })

        // Advance to attempt 2's scheduled time
        await clock.advance(500)
        const runner2 = new TestJobRunner({ job: value, store, jobKey: key, clock })
        expect((await runner2.Run()).type).toBe("rescheduled-error")
        job = (await getJob(store, key))!
        attempts.push({ at: job.header.at, max: job.header.retries.max, backoff: job.header.retries.initial_backoff_ms })

        // Advance further and try attempt 3
        await clock.advance(500)
        const runner3 = new TestJobRunner({ job: value, store, jobKey: key, clock })
        expect((await runner3.Run()).type).toBe("rescheduled-error")
        job = (await getJob(store, key))!
        attempts.push({ at: job.header.at, max: job.header.retries.max, backoff: job.header.retries.initial_backoff_ms })

        expect(attempts).toEqual([
            { at: 10_100, max: 2, backoff: 200 },  // 10_000 + 100
            { at: 10_700, max: 1, backoff: 400 },  // 10_500 + 200
            { at: 11_400, max: 0, backoff: 800 },  // 11_000 + 400
        ])

        // One more attempt: retries exhausted → fatal-error
        await clock.advance(500)
        const runner4 = new TestJobRunner({ job: value, store, jobKey: key, clock })
        expect((await runner4.Run()).type).toBe("fatal-error")
    })
})
