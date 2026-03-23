/**
 * Multi-picker tests.
 *
 * Verifies that the engine correctly fans out across multiple pickers, and
 * that picking works correctly with and without type-indexed storage:
 *
 *  - With typeIndex: candidate.type is populated → canRunType filters by type
 *  - Without typeIndex: candidate.type is undefined → falls through to ringState
 *    logic (or picks everything when no ringState is provided)
 */
import { describe, it, expect, beforeEach } from "vitest"
import { v4 } from "uuid"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import { InMemoryJobStorage } from "../src/inMemoryStorage"
import { WorkflowPicker, type PickContext } from "../src/workflowPicker"
import { WorkflowEngine } from "../src/workflowEngine"
import { JobRunner, type JobRunnerConstructor } from "../src/jobRunner"
import { InMemoryTransport, type TokenRingRegistrationKey, type TokenRingRegistrationValue } from "@pebbletree/tokenring"
import { durationSeconds, type BasicJobPayload, type WorkflowJobKey, type WorkflowJobValue } from "../src/workflowTypes"
import * as tuple from "fdb-tuple"
import { WorkflowStorageTransaction } from "../src/workflowStorageAdapter"

// =========================================================================
// Payload types — two distinct job types
// =========================================================================

interface MultiPayload extends BasicJobPayload {
    type: "_alpha" | "_beta"
    marker?: string
}

const ALL_SORTED: MultiPayload["type"][] = ["_alpha", "_beta"]

// =========================================================================
// Runners — track completed jobs per type
// =========================================================================

const completed: { alpha: string[]; beta: string[] } = { alpha: [], beta: [] }

class AlphaRunner extends JobRunner<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>> {
    async runJob(): Promise<void> {
        const { payload } = await this.GetJob()
        if (payload.marker) completed.alpha.push(payload.marker)
    }
}

class BetaRunner extends JobRunner<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>> {
    async runJob(): Promise<void> {
        const { payload } = await this.GetJob()
        if (payload.marker) completed.beta.push(payload.marker)
    }
}

// =========================================================================
// Ring membership store (shared across tests)
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

function createStorage(typeIndex: boolean) {
    return new InMemoryJobStorage<MultiPayload>({ typeIndex })
}

function seedJob(
    storage: InMemoryJobStorage<MultiPayload>,
    type: MultiPayload["type"],
    marker: string,
    overrides?: { at?: number },
): Promise<WorkflowJobKey> {
    const key: WorkflowJobKey = { job_id: v4() }
    const value: WorkflowJobValue<MultiPayload> = {
        payload: { type, marker },
        header: {
            at: overrides?.at ?? Date.now() - 1000,
            lost_deadline_ms: durationSeconds(30),
            progress_deadline_ms: durationSeconds(10),
            retries: { max: 0, initial_backoff_ms: 100, exponent: 2 },
        },
    }
    return storage.doTn(async txn => { txn.job.set(key, value) }).then(() => key)
}

function makeRunners(): Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>> {
    const runners = new Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>()
    runners.set("_alpha", AlphaRunner)
    runners.set("_beta", BetaRunner)
    return runners
}

/**
 * Create an engine with one or more pickers backed by separate storage instances.
 */
function createEngine(pickers: WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>[], runners?: Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>): WorkflowEngine<MultiPayload, WorkflowStorageTransaction<MultiPayload>> {
    const r = runners ?? makeRunners()

    class ConcreteEngine extends WorkflowEngine<MultiPayload, WorkflowStorageTransaction<MultiPayload>> {
        InitialiseRunners(): void {
            for (const [type, runner] of r.entries()) {
                this.AddRunner(type)(runner)
            }
        }

        protected override createTransport() { return new InMemoryTransport() }
        override getLocalAddress() { return { address: "127.0.0.1" } }
    }

    return new ConcreteEngine({
        pickers,
        allSortedCapabilities: ALL_SORTED,
        segment_name: `multi-${v4().slice(0, 8)}`,
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

function makePickCtx<PAYLOAD_T extends BasicJobPayload>(overrides?: Partial<PickContext<PAYLOAD_T>>): Omit<PickContext<PAYLOAD_T>, "canRunType"> {
    return {
        executorId: v4(),
        averageWorkload: 0,
        currentRunning: 0,
        ...overrides,
    }
}

// =========================================================================
// Tests
// =========================================================================

describe("multi-picker", () => {
    beforeEach(() => {
        completed.alpha.length = 0
        completed.beta.length = 0
        InMemoryTransport.clearRegistry()
    })

    // -----------------------------------------------------------------
    // Two separate storages, each with its own picker (both type-indexed)
    // -----------------------------------------------------------------

    it("engine picks from multiple pickers backed by different storage tables", async () => {
        const storageA = createStorage(true)
        const storageB = createStorage(true)

        await seedJob(storageA, "_alpha", "a1")
        await seedJob(storageB, "_beta", "b1")

        const pickerA = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: storageA,
            batchSize: 10,
            idealMaxRunning: 50,
        })
        const pickerB = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: storageB,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const engine = createEngine([pickerA, pickerB])
        try {
            const { pickedJobs } = await engine.pick(makePickCtx())
            expect(pickedJobs).toBe(2)

            await sleep(200)
            expect(completed.alpha).toContain("a1")
            expect(completed.beta).toContain("b1")
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Type-indexed storage: canRunType filters correctly
    // -----------------------------------------------------------------

    it("type-indexed picker only picks jobs matching canRunType", async () => {
        const storage = createStorage(true)

        await seedJob(storage, "_alpha", "alpha-yes")
        await seedJob(storage, "_beta", "beta-skip")

        // This picker only knows about _alpha
        const picker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        // Only register _alpha runner so the engine's canRunType filters out _beta
        const alphaOnly = new Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>()
        alphaOnly.set("_alpha", AlphaRunner)
        const engine = createEngine([picker], alphaOnly)
        try {
            const { pickedJobs } = await engine.pick(makePickCtx())
            expect(pickedJobs).toBe(1)

            await sleep(200)
            expect(completed.alpha).toContain("alpha-yes")
            expect(completed.beta).not.toContain("beta-skip")
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Non-type-indexed storage: candidate.type is undefined
    // -----------------------------------------------------------------

    it("non-type-indexed picker resolves type from database and filters correctly", async () => {
        const storage = createStorage(false)

        await seedJob(storage, "_alpha", "noindex-a")
        await seedJob(storage, "_beta", "noindex-b")

        const picker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        // Even without a type index, the picker reads the job to determine
        // its type, so canRunType filtering still works.
        const alphaOnly = new Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>()
        alphaOnly.set("_alpha", AlphaRunner)
        const engine = createEngine([picker], alphaOnly)
        try {
            const { pickedJobs } = await engine.pick(makePickCtx())
            expect(pickedJobs).toBe(1)

            await sleep(200)
            expect(completed.alpha).toContain("noindex-a")
            expect(completed.beta).not.toContain("noindex-b")
        } finally {
            engine.Destroy()
        }
    })

    it("non-type-indexed picker with established ringState picks jobs unsupported by ring", async () => {
        const storage = createStorage(false)

        await seedJob(storage, "_alpha", "noindex-ring-a")
        await seedJob(storage, "_beta", "noindex-ring-b")

        const picker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const engine = createEngine([picker])
        try {
            // Ring is established (not provisional) but supportedTypes is empty
            // → picker adopts all jobs since no server supports them
            const { pickedJobs } = await engine.pick(makePickCtx({
                ringState: {
                    isProvisional: false,
                    supportedTypes: new Set(),
                },
            }))
            expect(pickedJobs).toBe(2)
        } finally {
            engine.Destroy()
        }
    })

    it("non-type-indexed picker with provisional ring skips unknown jobs", async () => {
        const storage = createStorage(false)

        await seedJob(storage, "_alpha", "prov-skip")

        const picker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        // No runners → canRunType returns false for everything
        const noRunners = new Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>()
        const engine = createEngine([picker], noRunners)
        try {
            // Ring is provisional → don't pick jobs we can't run, ring might
            // not be fully established yet
            const { pickedJobs } = await engine.pick(makePickCtx({
                ringState: {
                    isProvisional: true,
                    supportedTypes: new Set(),
                },
            }))
            expect(pickedJobs).toBe(0)
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Mixed: type-indexed + non-type-indexed pickers on same engine
    // -----------------------------------------------------------------

    it("engine with mixed indexed and non-indexed pickers picks from both", async () => {
        const indexedStorage = createStorage(true)
        const plainStorage = createStorage(false)

        await seedJob(indexedStorage, "_alpha", "mixed-indexed")
        await seedJob(plainStorage, "_beta", "mixed-plain")

        const indexedPicker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: indexedStorage,
            batchSize: 10,
            idealMaxRunning: 50,
        })
        const plainPicker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: plainStorage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const engine = createEngine([indexedPicker, plainPicker])
        try {
            // The indexed picker picks alpha (type known from index).
            // The plain picker resolves the type from the database, then
            // canRunType → true, so it picks beta.
            const { pickedJobs } = await engine.pick(makePickCtx())
            expect(pickedJobs).toBe(2)

            await sleep(200)
            expect(completed.alpha).toContain("mixed-indexed")
            expect(completed.beta).toContain("mixed-plain")
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Full ring lifecycle with multiple pickers
    // -----------------------------------------------------------------

    it("ring token drives picks across multiple pickers", async () => {
        const storageA = createStorage(true)
        const storageB = createStorage(true)

        await seedJob(storageA, "_alpha", "ring-multi-a")
        await seedJob(storageB, "_beta", "ring-multi-b")

        const pickerA = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: storageA,
            batchSize: 10,
            idealMaxRunning: 50,
        })
        const pickerB = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage: storageB,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const engine = createEngine([pickerA, pickerB])
        try {
            await engine.Start()

            await waitFor(
                () => completed.alpha.includes("ring-multi-a") && completed.beta.includes("ring-multi-b"),
                5000,
                "both jobs should be picked via ring from separate pickers",
            )

            expect(completed.alpha).toContain("ring-multi-a")
            expect(completed.beta).toContain("ring-multi-b")
        } finally {
            engine.Destroy()
        }
    })

    // -----------------------------------------------------------------
    // Partial capability: engine only runs one type, skips the other
    // -----------------------------------------------------------------

    it("engine with partial runners picks only matching types from indexed storage", async () => {
        const storage = createStorage(true)

        await seedJob(storage, "_alpha", "partial-alpha")
        await seedJob(storage, "_beta", "partial-beta")

        // Only register _alpha runner
        const runners = new Map<MultiPayload["type"], JobRunnerConstructor<MultiPayload, MultiPayload["type"], WorkflowStorageTransaction<MultiPayload>>>()
        runners.set("_alpha", AlphaRunner)

        const picker = new WorkflowPicker<MultiPayload, WorkflowStorageTransaction<MultiPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const engine = createEngine([picker], runners)
        try {
            const { pickedJobs } = await engine.pick(makePickCtx())
            expect(pickedJobs).toBe(1)

            await sleep(200)
            expect(completed.alpha).toContain("partial-alpha")
            expect(completed.beta).not.toContain("partial-beta")
        } finally {
            engine.Destroy()
        }
    })
})
