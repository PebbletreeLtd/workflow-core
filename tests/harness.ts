/**
 * Vitest test harness for workflow-core.
 *
 * Provides:
 *  - Job key / value factory helpers
 *  - A shared InMemoryJobStorage instance
 *  - A pre-configured WorkflowEngine + helpers to start it
 *  - Assertions re-exported from vitest for convenience
 */
import { v4 } from "uuid"
import type {
    BasicJobPayload,
    WorkflowJobKey,
    WorkflowJobValue,
    WorkflowJobHeader,
    WorkflowJobOutcome,
} from "../src/workflowTypes"
import { durationSeconds } from "../src/workflowTypes"
import { WorkflowEngine } from "../src/workflowEngine"
import { WorkflowPicker } from "../src/workflowPicker"
import { WorkflowCapabilities } from "../src/workflowCapabilities"
import { JobRunner, type JobRunnerConstructor } from "../src/jobRunner"
import { SimulatedJobRunner, type SimulatedJobRunnerOptions } from "../src/simulatedJobRunner"
import { JobError } from "../src/jobErrors"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import { InMemoryJobStorage } from "../src/inMemoryStorage"

// =========================================================================
// Test payload type
// =========================================================================

export interface TestPayload extends BasicJobPayload {
    type: "_test"
    shouldFail?: boolean
    failType?: "recoverable" | "fatal" | "progress-timeout"
    delayMs?: number
    progressIntervalMs?: number
}

// =========================================================================
// Enum lookup – maps the string type "_test" → numeric value 1
// =========================================================================

export const ALL_SORTED_CAPABILITIES: TestPayload["type"][] = ["_test"]

// =========================================================================
// Shared test storage (reset between tests via beforeEach)
// =========================================================================

export function createTestStorage(args: {
    typeIndex: boolean
}): InMemoryJobStorage<TestPayload> {
    return new InMemoryJobStorage<TestPayload>({
        typeIndex: args.typeIndex
    })
}



// =========================================================================
// Job factory helpers
// =========================================================================

export function makeTestJobKey(): WorkflowJobKey {
    return { job_id: v4() }
}

export function makeTestJobValue(overrides?: {
    at?: number
    shouldFail?: boolean
    failType?: "recoverable" | "fatal" | "progress-timeout"
    delayMs?: number
    progressIntervalMs?: number
    progress_deadline_ms?: number
    retries?: { max: number; initial_backoff_ms: number; exponent: number }
    repeatSchedule?: WorkflowJobHeader["repeatSchedule"]
}): WorkflowJobValue<TestPayload> {
    return {
        payload: {
            type: "_test",
            shouldFail: overrides?.shouldFail,
            failType: overrides?.failType,
            delayMs: overrides?.delayMs ?? 0,
            progressIntervalMs: overrides?.progressIntervalMs,
        },
        header: {
            at: overrides?.at ?? Date.now() - 1000,
            lost_deadline_ms: durationSeconds(30),
            progress_deadline_ms: overrides?.progress_deadline_ms ?? durationSeconds(10),
            retries: overrides?.retries ?? { max: 0, initial_backoff_ms: 100, exponent: 2 },
            repeatSchedule: overrides?.repeatSchedule,
        },
        for_userspace_id: null,
    }
}

// =========================================================================
// Test runner — concrete SimulatedJobRunner for the _test payload type
// =========================================================================

/**
 * A concrete SimulatedJobRunner that executes the _test payload, honouring
 * shouldFail / failType / delayMs / progressIntervalMs fields.
 */
export class TestJobRunner extends SimulatedJobRunner<TestPayload> {
    async runJob(): Promise<void | number> {
        const { payload } = await this.GetJob()
        const delayMs = payload.delayMs ?? 0

        if (delayMs > 0) {
            const progressInterval = payload.progressIntervalMs
            if (progressInterval && progressInterval > 0) {
                const steps = Math.ceil(delayMs / progressInterval)
                for (let i = 0; i < steps; i++) {
                    await new Promise(r => setTimeout(r, progressInterval))
                    await this.Progress()
                }
            } else {
                await new Promise(r => setTimeout(r, delayMs))
            }
        }

        if (payload.shouldFail) {
            switch (payload.failType) {
                case "fatal":
                    throw new JobError({ type: "fatal-error", cause: { type: "custom", message: "test fatal" } })
                case "progress-timeout":
                    throw new JobError({ type: "progress-deadline" })
                case "recoverable":
                default:
                    throw new JobError({ type: "custom-recoverable", message: "test recoverable" })
            }
        }
    }
}


// =========================================================================
// Engine factory — wires picker + capabilities + storage into an engine
// =========================================================================

export function createTestEngine(
    storage: InMemoryJobStorage<TestPayload>,
) {
    const runners = new Map<TestPayload["type"], JobRunnerConstructor<TestPayload>>()
    runners.set("_test", TestJobRunner)

    const capabilities = WorkflowCapabilities.Create(ALL_SORTED_CAPABILITIES)({
        runners,
    })

    const picker = new WorkflowPicker<TestPayload>({
        storage,
        capabilities,
        batchSize: 10,
        idealMaxRunning: 50,
    })

    const engine = new WorkflowEngine<TestPayload>({
        pickers: [picker],
        capabilities,
    })

    return { engine, picker, capabilities, storage }
}
