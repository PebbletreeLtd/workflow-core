/**
 * Repeat schedule computation.
 * 
 * Computes the next occurrence of a repeating workflow job schedule.
 * Pure logic — no database or external dependencies.
 */
import { WorkflowClock } from "./workflowClock"
import type { RepeatSchedule, WorkflowJobHeader } from "./workflowTypes"
import { namedDuration } from "./workflowTypes"

/**
 * Given a job header with a repeatSchedule, compute the next schedule entry.
 * Returns the updated RepeatSchedule with nextDate advanced to the future,
 * or undefined if the schedule is exhausted (past until date/count, or type "none").
 */
export function computeNextSchedule(header: WorkflowJobHeader, clock: WorkflowClock): RepeatSchedule | undefined {
    let next_schedule = header.repeatSchedule
    let counter = 1000
    while (next_schedule) {
        if (counter-- < 0) {
            console.error("Bombing out on bad schedule entry", next_schedule)
            return undefined
        }
        next_schedule = ((schedule: RepeatSchedule): RepeatSchedule | undefined => {
            let repeatOn: Date = new Date(schedule.nextDate)
            switch (schedule.type) {
                case "periodic": {
                    const at = clock.now() + namedDuration(schedule.unit, schedule.period)
                    repeatOn = new Date(at)
                    break
                }
                case "daily":
                    repeatOn.setDate(repeatOn.getDate() + 1)
                    break
                case "annually":
                    repeatOn.setFullYear(repeatOn.getFullYear() + 1)
                    repeatOn.setMonth(schedule.month)
                    repeatOn.setDate(schedule.date)
                    while (repeatOn.getDate() != schedule.date) {
                        repeatOn.setDate(repeatOn.getDate() - 1)
                    }
                    break
                case "monthlyDate":
                    repeatOn.setMonth(repeatOn.getMonth() + 1)
                    repeatOn.setDate(schedule.date)
                    while (repeatOn.getDate() != schedule.date) {
                        repeatOn.setDate(repeatOn.getDate() - 1)
                    }
                    break
                case "monthlyDay": {
                    repeatOn.setDate(1)
                    repeatOn.setMonth(repeatOn.getMonth() + 1)
                    const monthIn = repeatOn.getMonth()
                    let firstMatchingDayDelta = schedule.weekDay - repeatOn.getDay()
                    if (firstMatchingDayDelta < 0) {
                        firstMatchingDayDelta = 7 + firstMatchingDayDelta
                    }
                    repeatOn.setDate(firstMatchingDayDelta + 7 * (schedule.week - 1))
                    while (repeatOn.getMonth() != monthIn) {
                        repeatOn.setDate(repeatOn.getDate() - 7)
                    }
                    break
                }
                case "weekly": {
                    let i = schedule.weekDay.length + 2
                    while (true) {
                        repeatOn.setDate(repeatOn.getDate() + 1)
                        if (schedule.weekDay[repeatOn.getDay()]) {
                            break
                        }
                        if (i-- < 0) {
                            return undefined
                        }
                    }
                    break
                }
                case "none":
                    return undefined
                default: {
                    const r: never = schedule
                    console.error(r)
                }
            }
            switch (schedule.until.format) {
                case "count":
                    schedule.until.value--
                    if (schedule.until.value < 0) return undefined
                    break
                case "date":
                    if (repeatOn > new Date(schedule.until.value)) return undefined
            }
            schedule.nextDate = repeatOn.getTime()
            return schedule
        })(next_schedule)
        if (next_schedule && next_schedule.nextDate > clock.now()) return next_schedule
    }
    return undefined
}
