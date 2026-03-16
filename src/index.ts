/**
 * @pebbletree/workflow — core workflow engine package.
 *
 * Public API surface. Re-exports everything consumers need to create
 * storage adapters, configure capabilities, and run the engine.
 */

// Types
export type {
    RepeatSchedule,
    RepeatScheduleNone,
    RepeatScheduleDaily,
    RepeatScheduleWeekly,
    RepeatScheduleMonthlyDay,
    RepeatScheduleMonthlyDate,
    RepeatScheduleAnnual,
    RepeatSchedulePeriodic,
    RepeatUntil,
    WorkflowRetryPolicy,
    WorkflowJobKey,
    WorkflowJobHeader,
    BasicJobPayload,
    WorkflowJobValue,
    WorkflowJobError,
    WorkflowJobErrorCause,
    WorkflowJobOutcome,
    WorkflowJobLogCustomEntry,
    WorkflowJobLogOutcome,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
    WorkflowConfig,
    JobRunnerFunction,
    JobRunner,
    BaseJobManager,
} from "./workflowTypes"

export {
    durationSeconds,
    durationMinutes,
    durationHours,
    durationDays,
    namedDuration,
} from "./workflowTypes"

// Storage adapter
export type {
    WorkflowJobStorage,
    WorkflowJobStorageTransaction,
} from "./workflowStorageAdapter"

// Capabilities
export { WorkflowCapabilities } from "./workflowCapabilities"

// Capability buffer (low-level bitmap encode/decode)
export {
    capabilitiesToBuffer,
    bufferToCapabilities,
    mergeCapabilityBuffers,
} from "./capabilityBuffer"

// Counter / metrics
export { WorkflowCounter } from "./counter"
export type { iWorkflowCounter, iJobSummary } from "./counter"

// Schedule computation
export { computeNextSchedule } from "./schedule"

// Job errors
export { JobError, JobFatalError } from "./jobErrors"

// Job manager
export { JobManager } from "./jobManager"
export type { JobManagerOptions } from "./jobManager"

// Picker
export { WorkflowPicker } from "./workflowPicker"
export type { PickContext, PickedJob } from "./workflowPicker"

// Engine (orchestrator)
export { WorkflowEngine } from "./workflowEngine"
export type { WorkflowEngineOptions } from "./workflowEngine"

// Simulated (in-memory) job manager for testing
export { SimulatedJobManager } from "./simulatedJobManager"
