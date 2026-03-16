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
import type { WorkflowJobKey } from "../workflowTypes"
import {
    makeTestJobKey, makeTestJobValue,
    createTestEngine, createTestStorage,
    type TestPayload,
} from "./harness"
import type { InMemoryJobStorage } from "./storage"

let storage: InMemoryJobStorage<TestPayload>

beforeEach(() => {
    storage = createTestStorage()
})

/** Insert a ready-to-pick test job into the in-memory storage */
function insertPickableTestJob(overrides?: {
    at?: number
    shouldFail?: boolean
    failType?: "recoverable" | "fatal" | "progress-timeout"
    delayMs?: number
}): { key: WorkflowJobKey } {
    const key = makeTestJobKey()
    const value = makeTestJobValue({
        at: overrides?.at ?? Date.now() - 1000,
        shouldFail: overrides?.shouldFail,
        failType: overrides?.failType,
        delayMs: overrides?.delayMs ?? 0,
    })
    storage.setJob(key, value)
    return { key }
}

describe("picking", () => {
    it("test job gets picked and executed", async () => {
        const { key } = insertPickableTestJob({ delayMs: 0 })
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

            const job = await storage.getJob(key)

            // Job should either be picked (execution_id set) or already completed (at <= 0)
            expect(job).not.toBeNull()
            expect(
                job!.header.execution_id !== undefined || job!.header.at <= 0,
            ).toBe(true)
        } finally {
            engine.destroy()
        }
    })

    it("future-dated job is NOT picked", async () => {
        const key = makeTestJobKey()
        const value = makeTestJobValue({ at: Date.now() + 60_000 })
        storage.setJob(key, value)

        const executorId = `test-future-${v4().slice(0, 8)}`
        const { engine } = createTestEngine(storage)

        try {
            const { pickedJobs } = await engine.pick({
                executorId,
                averageWorkload: 0,
                currentRunning: 0,
            })

            expect(pickedJobs).toBe(0)

            const job = await storage.getJob(key)
            expect(job).not.toBeNull()
            expect(job!.header.execution_id).toBeUndefined()
            expect(job!.header.at).toBeGreaterThan(Date.now())
        } finally {
            engine.destroy()
        }
    })
})
