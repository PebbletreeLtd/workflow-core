/**
 * Core workflow type definitions.
 * 
 * These are the generic types used by the workflow engine. They have no
 * dependencies on any database or application-specific code.
 */

// =========================================================================
// Duration utilities (inlined to avoid external deps)
// =========================================================================

const SECOND_MS = 1000
const MINUTE_MS = SECOND_MS * 60
const HOUR_MS = MINUTE_MS * 60
const DAY_MS = HOUR_MS * 24

export function durationSeconds(seconds: number): number { return seconds * SECOND_MS }
export function durationMinutes(minutes: number): number { return minutes * MINUTE_MS }
export function durationHours(hours: number): number { return hours * HOUR_MS }
export function durationDays(days: number): number { return days * DAY_MS }

export function namedDuration(name: "hours" | "minutes" | "days", value: number): number {
    switch (name) {
        case "days": return durationDays(value)
        case "hours": return durationHours(value)
        case "minutes": return durationMinutes(value)
    }
}

// =========================================================================
// Repeat scheduling types
// =========================================================================

export type RepeatUntil =
    | { format: "date", value: number }
    | { format: "count", value: number }
    | { format: "forever" }

export type RepeatSchedulePeriodic = {
    nextDate: number
    type: "periodic"
    unit: "minutes" | "hours" | "days"
    period: number
    until: RepeatUntil
}

export type RepeatScheduleDaily = {
    type: "daily"
    nextDate: number
    until: RepeatUntil
}

export type RepeatScheduleWeekly = {
    type: "weekly"
    weekDay: [boolean, boolean, boolean, boolean, boolean, boolean, boolean]
    nextDate: number
    until: RepeatUntil
}

export type RepeatScheduleMonthlyDay = {
    type: "monthlyDay"
    weekDay: number   // 0–6
    week: number      // 1–5
    nextDate: number
    until: RepeatUntil
}

export type RepeatScheduleMonthlyDate = {
    type: "monthlyDate"
    date: number      // 1–31
    nextDate: number
    until: RepeatUntil
}

export type RepeatScheduleAnnual = {
    type: "annually"
    date: number      // 1–31
    month: number     // 1–12
    nextDate: number
    until: RepeatUntil
}

export type RepeatScheduleNone = {
    type: "none"
    nextDate: number
}

export type RepeatSchedule =
    | RepeatScheduleNone
    | RepeatScheduleDaily
    | RepeatScheduleWeekly
    | RepeatScheduleMonthlyDay
    | RepeatScheduleMonthlyDate
    | RepeatScheduleAnnual
    | RepeatSchedulePeriodic

// =========================================================================
// Retry policy
// =========================================================================

export interface WorkflowRetryPolicy {
    max: number
    initial_backoff_ms: number
    exponent: number
}

// =========================================================================
// Job key, header, value
// =========================================================================

export interface WorkflowJobKey {
    job_id: string
}

export interface WorkflowJobHeader {
    at: number
    last_progress?: number
    execution_id?: string
    lost_deadline_ms: number
    progress_deadline_ms: number
    retries: WorkflowRetryPolicy
    repeatSchedule?: RepeatSchedule
}

export interface BasicJobPayload {
    type: string
}

export type WorkflowJobValue<PAYLOAD_T extends BasicJobPayload> = {
    payload: PAYLOAD_T
    header: WorkflowJobHeader
}

// =========================================================================
// Job outcome & error types
// =========================================================================

export type WorkflowJobError =
    | { type: "vanished" }
    | { type: "fatal-error", cause: WorkflowJobErrorCause }
    | { type: "readopted", by: string }
    | { type: "payloadMismatch" }
    | { type: "progress-deadline" }
    | { type: "custom-recoverable", message: string }

export type WorkflowJobErrorCause =
    | { type: "payloadMismatch" }
    | { type: "progress-deadline" }
    | { type: "custom", message: string }
    | { type: "custom-recoverable", message: string }

export type WorkflowJobOutcome =
    | { type: "vanished" }
    | { type: "fatal-error", cause: WorkflowJobErrorCause }
    | { type: "readopted", by: string }
    | { type: "success" }
    | { type: "rescheduled-error", cause: { type: "payloadMismatch" } | { type: "progress-deadline" } | { type: "custom-recoverable", message: string } }

// =========================================================================
// Job log types
// =========================================================================

export type WorkflowJobLogCustomEntry = {
    type: "custom-log"
    message: string
}

export type WorkflowJobLogOutcome = WorkflowJobOutcome | WorkflowJobLogCustomEntry

export interface WorkflowJobLogKey {
    job_id: string
    timestamp: number
    random: string
}

export interface WorkflowJobLogValue {
    outcome: WorkflowJobLogOutcome
    at: number
    execution_id: string
}


