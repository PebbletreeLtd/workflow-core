export interface WorkflowClockTimerCancel {
    cancel: () => void
}
export interface WorkflowClock {
    now(): number,
    sleep: (ms: number) => Promise<void>,
    setTimer: (fn: () => void, ms: number) => WorkflowClockTimerCancel
    setTimerInterval: (fn: () => void, ms: number) => WorkflowClockTimerCancel
}

export const defaultWorkflowClock: WorkflowClock = {
    now: () => Date.now(),
    sleep: (ms: number) => new Promise(resolve => setTimeout(resolve, ms).unref()),
    setTimer: (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        globalThis[`set${`Timeout`}`]
        const id = setTimeout(fn, ms).unref()
        return { cancel: () => clearTimeout(id) }
    },
    setTimerInterval: (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        const id = setInterval(fn, ms).unref()
        return { cancel: () => clearInterval(id) }
    }
}