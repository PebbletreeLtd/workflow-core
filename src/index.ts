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
} from "./workflowStorageAdapter"

// Capabilities
export { WorkflowCapabilities } from "./workflowCapabilities"

// Counter / metrics
export { WorkflowCounter } from "./counter"
export type { iWorkflowCounter, iJobSummary } from "./counter"

// Schedule computation
export { computeNextSchedule } from "./schedule"

// Job errors
export { JobError, JobFatalError } from "./jobErrors"

// Job runner
export { JobRunner } from "./jobRunner"
export type { JobRunnerOptions, JobRunnerConstructor } from "./jobRunner"

// Picker
export { WorkflowPicker } from "./workflowPicker"
export type { PickContext, PickedJob, WorkflowPickerArgs } from "./workflowPicker"

// Engine (orchestrator)
export { WorkflowEngine } from "./workflowEngine"
export type { WorkflowEngineOptions } from "./workflowEngine"


// Simulated (in-memory) job runner for testing
export { SimulatedJobRunner } from "./simulatedJobRunner"
export type { SimulatedJobRunnerOptions } from "./simulatedJobRunner"

// In-memory storage
export { InMemoryJobStorage } from "./inMemoryStorage"
