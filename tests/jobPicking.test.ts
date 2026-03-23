/**
 * Job Picking Tests
 *
 * Tests for:
 * - Job picking via WorkflowEngine.pick()
 * - Future-dated job exclusion
 *
 * Uses an in-memory storage adapter and creates its own WorkflowEngine
 * (no external DB or server wrapper required).
 */
import { describe, it, expect, beforeEach } from "vitest"
import { v4 } from "uuid"
import type { WorkflowJobKey } from "../src/workflowTypes"
import {
    makeTestJobKey, makeTestJobValue,
    createTestEngine, createTestStorage,
    type TestPayload,
} from "./harness"
import type { InMemoryJobStorage } from "../src/inMemoryStorage"

let storage: InMemoryJobStorage<TestPayload>

beforeEach(() => {
    storage = createTestStorage({ typeIndex: true })
})

/** Insert a ready-to-pick test job into the in-memory storage */
async function insertPickableTestJob(overrides?: {
    at?: number
    shouldFail?: boolean
    failType?: "recoverable" | "fatal" | "progress-timeout"
    delayMs?: number
}): Promise<{ key: WorkflowJobKey }> {
    const key = makeTestJobKey()
    const value = makeTestJobValue({
        at: overrides?.at ?? Date.now() - 1000,
        shouldFail: overrides?.shouldFail,
        failType: overrides?.failType,
        delayMs: overrides?.delayMs ?? 0,
    })
    await storage.doTn(async txn => {
        txn.job.set(key, value)
    })
    return { key }
}
async function getJob(key: WorkflowJobKey) {
    return await storage.doTn(async txn => {
        return await txn.job.get(key)
    })
}

describe("picking", async () => {
    it("test job gets picked and executed", async () => {
        const { key } = await insertPickableTestJob({ delayMs: 0 })
        const executorId = `test-pick-${v4().slice(0, 8)}`
        const { engine } = createTestEngine(storage)

        try {
            const { pickedJobs } = await engine.pick({
                executorId,
                averageWorkload: 0,
                currentRunning: 0,
            })

            expect(pickedJobs).toBeGreaterThanOrEqual(1)

            // Give the fire-and-forget job execution a moment to complete
            await new Promise(r => setTimeout(r, 200))

            const job = await getJob(key)

            // Job should either be picked (execution_id set) or already completed (at <= 0)
            expect(job).not.toBeNull()
            expect(
                job!.header.execution_id !== undefined || job!.header.at <= 0,
            ).toBe(true)
        } finally {
            engine.Destroy()
        }
    })

    it("future-dated job is NOT picked", async () => {
        const key = makeTestJobKey()
        const value = makeTestJobValue({ at: Date.now() + 60_000 })
        await storage.doTn(async txn => {
            txn.job.set(key, value)
        })

        const executorId = `test-future-${v4().slice(0, 8)}`
        const { engine } = createTestEngine(storage)

        try {
            const { pickedJobs } = await engine.pick({
                executorId,
                averageWorkload: 0,
                currentRunning: 0,
            })

            expect(pickedJobs).toBe(0)

            const job = await getJob(key)
            expect(job).not.toBeNull()
            expect(job!.header.execution_id).toBeUndefined()
            expect(job!.header.at).toBeGreaterThan(Date.now())
        } finally {
            engine.Destroy()
        }
    })
})
