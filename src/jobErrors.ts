/**
 * Job error types used by runners to signal failure modes.
 */
import type { WorkflowJobError } from "./workflowTypes"

export class JobError extends Error {
    constructor(readonly outcome: WorkflowJobError) {
        super()
    }
}

export class JobFatalError extends JobError {
    constructor(message: string) {
        super({ type: "fatal-error", cause: { type: "custom", message: message } })
    }
}
