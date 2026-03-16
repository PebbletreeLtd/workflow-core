/**
 * Simulated (in-memory) job manager for testing workflows without a database.
 */
import type { BaseJobManager, BasicJobPayload, WorkflowJobHeader, WorkflowJobKey } from "./workflowTypes"

export class SimulatedJobManagerProgressException extends Error {}

type SimulatedJobManagerProgressMethod<PAYLOAD_T extends BasicJobPayload> = (
    this: SimulatedJobManager<PAYLOAD_T>,
    payload?: Partial<PAYLOAD_T>
) => Promise<{
    header: Readonly<WorkflowJobHeader>
    payload: PAYLOAD_T
}>

export class SimulatedJobManager<PAYLOAD_T extends BasicJobPayload> implements BaseJobManager<PAYLOAD_T> {
    readonly header: Readonly<WorkflowJobHeader> = {
        at: Date.now(),
        lost_deadline_ms: 0,
        progress_deadline_ms: 0,
        retries: { max: 0, exponent: 0, initial_backoff_ms: 0 },
    }
    jobKey: Readonly<WorkflowJobKey>
    public Progress: SimulatedJobManagerProgressMethod<PAYLOAD_T>

    constructor(
        public job: PAYLOAD_T,
        progressImpl: SimulatedJobManagerProgressMethod<PAYLOAD_T>,
        jobKey?: WorkflowJobKey,
    ) {
        this.Progress = progressImpl?.bind(this)
        this.jobKey = jobKey ?? { job_id: "" }
    }

    async Wait(_duration: number): Promise<number | undefined> {
        return undefined
    }

    async GetJob(): Promise<{ header: Readonly<WorkflowJobHeader>; payload: PAYLOAD_T }> {
        return { header: this.header, payload: this.job }
    }
}
