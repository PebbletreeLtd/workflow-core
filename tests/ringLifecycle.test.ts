/**
 * Ring lifecycle integration tests.
 *
 * Exercises the full token ring → engine → picker → runner pipeline using
 * InMemoryTransport (no UDP sockets) and InMemoryJobStorage.
 *
 * Inspired by the token ring repo's integration tests but focuses on the
 * workflow-specific behaviour: job picking, execution, capability routing,
 * and lost-job recovery triggered by the ring.
 */
import { describe, it, expect, afterEach, beforeEach } from "vitest"
import { v4 } from "uuid"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import * as tuple from "fdb-tuple"
import { InMemoryTransport, type TokenRingRegistrationKey, type TokenRingRegistrationValue, TokenFlags } from "@pebbletree/tokenring"
import { WorkflowEngine, type WorkflowEngineOptions } from "../src/workflowEngine"
import { WorkflowPicker } from "../src/workflowPicker"
import { JobRunner, type JobRunnerConstructor } from "../src/jobRunner"
import { InMemoryJobStorage } from "../src/inMemoryStorage"
import { durationSeconds } from "../src/workflowTypes"
import type {
    BasicJobPayload,
    WorkflowJobKey,
    WorkflowJobValue,
} from "../src/workflowTypes"
import { WorkflowStorageTransaction } from "../src/workflowStorageAdapter"

// =========================================================================
// Test payload type
// =========================================================================

interface RingTestPayload extends BasicJobPayload {
    type: "_ringtest"
    marker?: string
}

const ALL_SORTED: RingTestPayload["type"][] = ["_ringtest"]

// =========================================================================
// Simple runner that records completed jobs
// =========================================================================

const completedJobs: string[] = []

class RingTestRunner extends JobRunner<RingTestPayload, RingTestPayload["type"], WorkflowStorageTransaction<RingTestPayload>> {
    async runJob(): Promise<void> {
        const { payload } = await this.GetJob()
        if (payload.marker) completedJobs.push(payload.marker)
    }
}

// =========================================================================
// Ring membership store (same pattern as tokenRing tests)
// =========================================================================

const ringStore = new MVCCCore.Store<TokenRingRegistrationKey, TokenRingRegistrationKey, TokenRingRegistrationValue, TokenRingRegistrationValue>({
    keyTransformer: {
        pack: (k) => tuple.pack([k.segment_name, k.server_ip, k.server_port]),
        unpack: (buf) => {
            const [segment_name, server_ip, server_port] = tuple.unpack(buf)
            if (typeof segment_name !== "string" || typeof server_ip !== "string" || typeof server_port !== "number") {
                throw new Error("Invalid ring key")
            }
            return { segment_name, server_ip, server_port }
        },
    },
})

// =========================================================================
// Helpers
// =========================================================================

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

async function waitFor(condition: () => boolean, timeoutMs: number, label?: string): Promise<void> {
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
        if (condition()) return
        await sleep(50)
    }
    throw new Error(`waitFor timed out after ${timeoutMs}ms: ${label ?? "condition not met"}`)
}

function createJobStorage() {
    return new InMemoryJobStorage<RingTestPayload>({ typeIndex: true })
}

function seedJob(storage: InMemoryJobStorage<RingTestPayload>, overrides?: {
    marker?: string
    at?: number
}): WorkflowJobKey {
    const key: WorkflowJobKey = { job_id: v4() }
    const value: WorkflowJobValue<RingTestPayload> = {
        payload: { type: "_ringtest", marker: overrides?.marker },
        header: {
            at: overrides?.at ?? Date.now() - 1000,
            lost_deadline_ms: durationSeconds(30),
            progress_deadline_ms: durationSeconds(10),
            retries: { max: 0, initial_backoff_ms: 100, exponent: 2 },
        },
    }
    // Synchronous seed via doTn — InMemoryJobStorage resolves immediately
    storage.doTn(async txn => { txn.job.set(key, value) })
    return key
}

function createEngine(storage: InMemoryJobStorage<RingTestPayload>, options?: {
    segment_name?: string
}): WorkflowEngine<RingTestPayload, WorkflowStorageTransaction<RingTestPayload>> {
    const runners = new Map<RingTestPayload["type"], JobRunnerConstructor<RingTestPayload, RingTestPayload["type"], WorkflowStorageTransaction<RingTestPayload>>>()
    runners.set("_ringtest", RingTestRunner)

    const picker = new WorkflowPicker<RingTestPayload, WorkflowStorageTransaction<RingTestPayload>>({
        storage,
        batchSize: 10,
        idealMaxRunning: 50,
    })

    class TestEngine extends WorkflowEngine<RingTestPayload, WorkflowStorageTransaction<RingTestPayload>> {
        InitialiseRunners(): void {
            for (const [type, runner] of runners.entries()) {
                this.AddRunner(type)(runner)
            }
        }
        protected override createTransport() { return new InMemoryTransport() }
        override getLocalAddress() { return { address: "127.0.0.1" } }
    }

    return new TestEngine({
        pickers: [picker],
        allSortedCapabilities: ALL_SORTED,
        segment_name: options?.segment_name ?? `ring-test-${v4().slice(0, 8)}`,
        issuer_id: v4(),
        ringConfig: {
            reregister_time_ms: 60_000,
            token_ack_timeout_ms: 500,
            skipInitialTokenTimeout: true,
        },
        ringStorage: {
            doTn: (callback) => ringStore.doTn(txn => callback({
                tokenRingRegistration: {
                    get: async (key) => txn.get(key),
                    set: (key, value) => txn.set(key, value),
                    clear: (key) => txn.clear(key),
                    getRangeAll: async (startKey, endKey, options) => txn.getRangeAll(startKey, endKey, options),
                }
            }))
        },
    })
}

// =========================================================================
// Tests
// =========================================================================

describe("ring lifecycle", () => {
    beforeEach(() => {
        completedJobs.length = 0
    })

    afterEach(() => {
        InMemoryTransport.clearRegistry()
    })

    // -----------------------------------------------------------------
    // Basic ring formation
    // -----------------------------------------------------------------

    it("single engine starts, receives token, and picks a job", async () => {
        const storage = createJobStorage()
        const key = seedJob(storage, { marker: "ring-pick-1" })

        const engine = createEngine(storage)
        try {
            await engine.Start()

            // Wait for the job to be completed by the runner
            await waitFor(
                () => completedJobs.includes("ring-pick-1"),
                5000,
                "job should be picked and completed via ring token",
            )

            expect(completedJobs).toContain("ring-pick-1")
        } finally {
            engine.Destroy()
        }
    })

    it("provisional flag clears after a full token loop", async () => {
        const storage = createJobStorage()
        const engine = createEngine(storage)
        try {
            await engine.Start()

            // Wait for at least 2 rounds — first is provisional, second clears it
            await waitFor(
                () => (engine.Token.flags & TokenFlags.provisional) === 0,
                5000,
                "provisional flag should clear after full token loop",
            )

            expect(engine.Token.flags & TokenFlags.provisional).toBe(0)
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Multi-engine ring
    // -----------------------------------------------------------------

    it("two engines in the same segment both receive tokens and pick jobs", async () => {
        const segment = `multi-${v4().slice(0, 8)}`
        const storage = createJobStorage()

        seedJob(storage, { marker: "multi-a" })
        seedJob(storage, { marker: "multi-b" })

        const engine1 = createEngine(storage, { segment_name: segment })
        const engine2 = createEngine(storage, { segment_name: segment })

        try {
            await engine1.Start()
            await engine2.Start()

            // Both jobs should get picked (by whichever engine gets the token)
            await waitFor(
                () => completedJobs.length >= 2,
                10000,
                "both jobs should be picked across the ring",
            )

            expect(completedJobs).toContain("multi-a")
            expect(completedJobs).toContain("multi-b")
        } finally {
            engine1.Destroy()
            engine2.Destroy()
        }
    })

    it("capabilities merge across engines in the ring", async () => {
        const segment = `caps-${v4().slice(0, 8)}`
        const storage = createJobStorage()

        const engine1 = createEngine(storage, { segment_name: segment })
        const engine2 = createEngine(storage, { segment_name: segment })

        try {
            await engine1.Start()
            await engine2.Start()

            // Wait for both to have received at least one token and
            // the provisional flag to clear (meaning a full loop occurred)
            await waitFor(
                () =>
                    (engine1.Token.flags & TokenFlags.provisional) === 0 &&
                    (engine2.Token.flags & TokenFlags.provisional) === 0,
                10000,
                "ring should establish with merged capabilities",
            )

            // Both engines register the same capability ("_ringtest" at index 0)
            // so after merging, both tokens should have bit 0 set
            expect(engine1.Token.capabilities[0]! & 1).toBe(1)
            expect(engine2.Token.capabilities[0]! & 1).toBe(1)
        } finally {
            engine1.Destroy()
            engine2.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Future-dated jobs not picked
    // -----------------------------------------------------------------

    it("future-dated job is not picked during ring lifecycle", async () => {
        const storage = createJobStorage()
        seedJob(storage, { marker: "future-skip", at: Date.now() + 60_000 })

        const engine = createEngine(storage)
        try {
            await engine.Start()

            // Give a few token rounds to confirm nothing gets picked
            await sleep(2000)

            expect(completedJobs).not.toContain("future-skip")
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Multiple jobs in one pick cycle
    // -----------------------------------------------------------------

    it("picks multiple jobs in a single token round", async () => {
        const storage = createJobStorage()
        for (let i = 0; i < 5; i++) {
            seedJob(storage, { marker: `batch-${i}` })
        }

        const engine = createEngine(storage)
        try {
            await engine.Start()

            await waitFor(
                () => completedJobs.length >= 5,
                10000,
                "all 5 jobs should be completed",
            )

            for (let i = 0; i < 5; i++) {
                expect(completedJobs).toContain(`batch-${i}`)
            }
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Destroy stops picking
    // -----------------------------------------------------------------

    it("engine stops picking after Destroy()", async () => {
        const storage = createJobStorage()
        const engine = createEngine(storage)

        try {
            await engine.Start()

            // Let at least one round proceed
            await waitFor(
                () => (engine.Token.flags & TokenFlags.provisional) === 0,
                5000,
                "ring should establish",
            )
        } finally {
            engine.Destroy()
        }

        // Seed a job after destroy — it should never be picked
        seedJob(storage, { marker: "after-destroy" })
        await sleep(1500)

        expect(completedJobs).not.toContain("after-destroy")
    })
})
