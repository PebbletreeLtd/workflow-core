/**
 * Workflow metrics counter.
 * 
 * Pure in-memory sliding-window accumulator for pick stats and per-job-type
 * execution/duration/outcome counts. No external dependencies.
 */

import { WorkflowClock } from "./workflowClock"

const division_count = 20

export interface iJobSummary {
    totals: {
        executed: number
        duration: number
    }
    outcomes: { [P: string]: number }
}

export type iWorkflowCounter = {
    picks: {
        requested: number
        got: number
        cycles: number
        cycleUtilisationTotal: number
        conflicts: number
    }
    jobs: { [name: string]: iJobSummary }
}

const zeroValue = (): iWorkflowCounter => {
    return {
        jobs: {},
        picks: {
            cycleUtilisationTotal: 0,
            cycles: 0,
            got: 0,
            requested: 0,
            conflicts: 0
        }
    }
}

const merge = (val1: iWorkflowCounter, val2: Partial<iWorkflowCounter>): iWorkflowCounter => {
    const ret: iWorkflowCounter = {
        picks: {
            cycles: val1.picks.cycles + (val2.picks?.cycles || 0),
            cycleUtilisationTotal: val1.picks.cycleUtilisationTotal + (val2.picks?.cycleUtilisationTotal || 0),
            got: val1.picks.got + (val2.picks?.got || 0),
            conflicts: val1.picks.conflicts + (val2.picks?.conflicts || 0),
            requested: val1.picks.requested + (val2.picks?.requested || 0),
        },
        jobs: {}
    }
    const all_job_keys = Array.from(new Set([...Object.keys(val1.jobs), ...Object.keys(val2.jobs || {})]))
    for (const k of all_job_keys) {
        const job_val_1 = val1.jobs[k as any] || { outcomes: {}, totals: { duration: 0, executed: 0 } }
        const job_val_2 = (val2.jobs || {})[k as any] || { outcomes: {}, totals: { duration: 0, executed: 0 } }

        const merged = ret.jobs[k as any] = {
            totals: {
                duration: job_val_1.totals.duration + job_val_2.totals.duration,
                executed: job_val_1.totals.executed + job_val_2.totals.executed,
            },
            outcomes: {}
        }
        const outcome_keys = Array.from(new Set([...Object.keys(job_val_1.outcomes), ...Object.keys(job_val_2.outcomes)]))
        for (const outcome_key of outcome_keys) {
            (merged.outcomes as any)[outcome_key] = ((job_val_1.outcomes as any)[outcome_key] || 0) + ((job_val_2.outcomes as any)[outcome_key] || 0)
        }
    }
    return ret
}

export class WorkflowCounter {
    private arrayStartTime: number;
    private division_history: iWorkflowCounter[] = []
    private division_history_aggregate: iWorkflowCounter | undefined = undefined
    private currentCounter: iWorkflowCounter = zeroValue()
    constructor(private options: {
        counter_duration_ms: number,
        clock: WorkflowClock,
    }) {
        this.arrayStartTime = options.clock.now()
    }





    private ResetDivisions() {
        const at = this.options.clock.now()
        const projectedArrayStartTime = at - (this.options.counter_duration_ms)
        const missedBeats = Math.min(division_count + 1, Math.floor((projectedArrayStartTime - this.arrayStartTime) / (this.options.counter_duration_ms / division_count)))
        if (missedBeats <= 0) return
        for (let i = 0; i < missedBeats; i++) {
            if (this.division_history.length >= division_count)
                this.division_history.shift()
            this.division_history.push(this.currentCounter)
            this.currentCounter = zeroValue()
        }
        this.arrayStartTime = projectedArrayStartTime
        this.division_history_aggregate = zeroValue()
        for (const h of this.division_history) {
            this.division_history_aggregate = merge(h, this.division_history_aggregate)
        }
    }

    pegValue(value: Partial<iWorkflowCounter>) {
        this.ResetDivisions()
        this.currentCounter = merge(this.currentCounter, value)
    }

    getCurrentValue() {
        const ret = this.division_history_aggregate || this.currentCounter
        const { cycleUtilisationTotal, ...picks } = ret.picks
        const utilisation = picks.cycles ? cycleUtilisationTotal / picks.cycles : 0
        return {
            picks: { ...picks, utilisation },
            jobs: ret.jobs,
            sampleDuration: this.options.counter_duration_ms
        }
    }
}
