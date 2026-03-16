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
    WorkflowConfig,
    JobRunnerFunction,
    WorkflowJobHeader,
} from "../workflowTypes"
import { durationSeconds } from "../workflowTypes"
import { WorkflowEngine } from "../workflowEngine"
import { WorkflowPicker } from "../workflowPicker"
import { WorkflowCapabilities } from "../workflowCapabilities"
import { InMemoryJobStorage } from "./storage"

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

export const TEST_ENUM_LOOKUP: Record<string, number> = { _test: 1 }
export const ALL_SORTED_CAPABILITIES = [1]

// =========================================================================
// Shared test storage (reset between tests via beforeEach)
// =========================================================================

export function createTestStorage(): InMemoryJobStorage<TestPayload> {
    return new InMemoryJobStorage<TestPayload>()
}

// =========================================================================
// Default workflow config
// =========================================================================

export function testWorkflowConfig(overrides?: Partial<WorkflowConfig>): WorkflowConfig {
    return {
        workflow_server_reregister_time_ms: 60_000,
        workflow_token_ack_timeout_ms: 500,
        workflow_batch_size: 10,
        workflow_ideal_maximum_jobs_running: 50,
        workflow_max_job_retention_age_days: undefined,
        workflow_supress_job_outcome_logs: ["success"],
        ...overrides,
    }
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
// Test runner — executes the _test payload
// =========================================================================

/**
 * Creates a runner function that honours the test payload's shouldFail /
 * failType / delayMs / progressIntervalMs fields.
 */
export function createTestRunner(): JobRunnerFunction<TestPayload> {
    return async (mgr, _oopE) => {
        const { payload } = await mgr.GetJob()
        const delayMs = payload.delayMs ?? 0

        if (delayMs > 0) {
            const progressInterval = payload.progressIntervalMs
            if (progressInterval && progressInterval > 0) {
                const steps = Math.ceil(delayMs / progressInterval)
                for (let i = 0; i < steps; i++) {
                    await new Promise(r => setTimeout(r, progressInterval))
                    await mgr.Progress()
                }
            } else {
                await new Promise(r => setTimeout(r, delayMs))
            }
        }

        if (payload.shouldFail) {
            const { JobError } = await import("../jobErrors")
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
    config?: Partial<WorkflowConfig>,
) {
    const cfg = testWorkflowConfig(config)

    const runners = new Map<number, JobRunnerFunction<TestPayload>>()
    runners.set(1, createTestRunner())

    const capabilities = new WorkflowCapabilities<TestPayload>({
        allSortedCapabilities: ALL_SORTED_CAPABILITIES,
        runners,
    })

    const picker = new WorkflowPicker<TestPayload>({
        storage,
        capabilities,
        config: cfg,
        enumLookup: TEST_ENUM_LOOKUP,
    })

    const engine = new WorkflowEngine<TestPayload>({
        pickers: [picker],
        capabilities,
        config: cfg,
        enumLookup: TEST_ENUM_LOOKUP,
        suppress_error_emails: true,
    })

    return { engine, picker, capabilities, config: cfg, storage }
}
