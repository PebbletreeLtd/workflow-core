/**
 * Engine Context Tests
 *
 * Verifies that the `ctx` object provided to `WorkflowEngine` is threaded
 * through to job runners with reference-identity preserved.
 */
import { describe, it } from "node:test"
import { expect } from "./expect"
import { v4 } from "uuid"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import type { TokenRingRegistrationKey, TokenRingRegistrationValue } from "@pebbletree/tokenring"
import {
    ALL_SORTED_CAPABILITIES,
    createTestStorage,
    makeTestJobKey,
    makeTestJobValue,
    type TestPayload,
} from "./harness"
import { WorkflowEngine } from "../workflowEngine"
import { WorkflowPicker } from "../workflowPicker"
import { JobRunner, type JobRunnerConstructor } from "../jobRunner"
import { WorkflowStorageTransaction } from "../workflowStorageAdapter"
import { defaultWorkflowClock } from "../workflowClock"

interface TestCtx {
    tag: string
    marker: object
}

describe("engine context", () => {
    it("passes the exact ctx object to job runners", async () => {
        const ctx: TestCtx = { tag: "ctx-under-test", marker: {} }
        const received: TestCtx[] = []
        let resolveDone!: () => void
        const done = new Promise<void>(resolve => { resolveDone = resolve })

        class CtxCapturingRunner extends JobRunner<TestPayload, "_test", WorkflowStorageTransaction<TestPayload>, TestCtx> {
            async runJob(): Promise<void> {
                received.push(this.context)
                resolveDone()
            }
        }

        const storage = createTestStorage({ typeIndex: true })
        const key = makeTestJobKey()
        await storage.doTn(async txn => {
            txn.job.set(key, makeTestJobValue({ delayMs: 0 }))
        })

        const picker = new WorkflowPicker<TestPayload, WorkflowStorageTransaction<TestPayload>>({
            storage,
            batchSize: 10,
            idealMaxRunning: 50,
        })

        const ringMembershipStore = new MVCCCore.Store<TokenRingRegistrationKey, TokenRingRegistrationKey, TokenRingRegistrationValue, TokenRingRegistrationValue>({
            keyTransformer: {
                pack: () => { throw new Error("not implemented") },
                unpack: () => { throw new Error("not implemented") },
            },
        })

        class TestEngine extends WorkflowEngine<TestPayload, WorkflowStorageTransaction<TestPayload>, TestCtx> {
            InitialiseRunners(): void {
                this.AddRunner("_test")(
                    CtxCapturingRunner as unknown as JobRunnerConstructor<TestPayload, "_test", WorkflowStorageTransaction<TestPayload>, TestCtx>,
                )
            }
        }

        const engine = new TestEngine({
            pickers: [picker],
            allSortedCapabilities: ALL_SORTED_CAPABILITIES,
            segment_name: "ctx-test",
            issuer_id: v4(),
            ctx,
            ringConfig: {
                reregister_time_ms: 60_000,
                token_ack_timeout_ms: 500,
                skipInitialTokenTimeout: true,
            },
            ringStorage: {
                doTn(callback) {
                    return ringMembershipStore.doTn(txn => callback({
                        tokenRingRegistration: {
                            get: async (k) => txn.get(k),
                            set: (k, v) => txn.set(k, v),
                            clear: (k) => txn.clear(k),
                            getRangeAll: async (s, e, o) => txn.getRangeAll(s, e, o),
                        },
                    }))
                },
            },
        })

        try {
            expect(engine.ctx).toBe(ctx)

            const { pickedJobs } = await engine.pick({
                executorId: `ctx-test-${v4().slice(0, 8)}`,
                averageWorkload: 0,
                currentRunning: 0,
                clock: defaultWorkflowClock,
                pegCounterValue() { },
            })
            expect(pickedJobs).toBeGreaterThanOrEqual(1)

            await done

            expect(received.length).toBe(1)
            expect(received[0]).toBe(ctx)
            expect(received[0]!.marker).toBe(ctx.marker)
        } finally {
            engine.Destroy()
        }
    })
})
