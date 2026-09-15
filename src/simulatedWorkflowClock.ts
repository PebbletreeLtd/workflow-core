/**
 * Deterministic in-memory WorkflowClock for tests and simulations.
 *
 * Time does not advance on its own — callers drive it via `advance(ms)` or
 * `setTime(ms)`. `sleep`, `setTimeout`, and `setInterval` register pending
 * entries that fire (in deadline order, FIFO for ties) as the clock advances.
 *
 * Exported so downstream consumers can use it to test their own job runners.
 */
import type { WorkflowClock, WorkflowClockTimerCancel } from "./workflowClock"

interface Pending {
    at: number
    seq: number
    fire: () => void
    cancelled?: boolean
}

export class SimulatedWorkflowClock implements WorkflowClock {
    private nowMs: number
    private pending: Pending[] = []
    private seqCounter = 0

    constructor(startMs = 0) {
        this.nowMs = startMs
    }

    now = (): number => this.nowMs

    sleep = (ms: number): Promise<void> => {
        return new Promise<void>(resolve => {
            this.enqueue(this.nowMs + ms, resolve)
        })
    }

    setTimer = (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        const entry = this.enqueue(this.nowMs + ms, fn)
        return { cancel: () => { entry.cancelled = true } }
    }

    setTimerInterval = (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        let cancelled = false
        let current: Pending
        const tick = () => {
            if (cancelled) return
            try { fn() } finally {
                if (!cancelled) current = this.enqueue(this.nowMs + ms, tick)
            }
        }
        current = this.enqueue(this.nowMs + ms, tick)
        return {
            cancel: () => {
                cancelled = true
                current.cancelled = true
            }
        }
    }

    /**
     * Advance the clock by `ms`, firing any pending sleeps/timers whose
     * deadline is reached, in deadline order (FIFO on ties). Yields to the
     * microtask queue between firings so awaiters can schedule new work.
     */
    async advance(ms: number): Promise<void> {
        if (ms < 0) throw new Error("SimulatedWorkflowClock.advance: ms must be >= 0")
        const target = this.nowMs + ms
        // Loop until no more pendings are due at or before `target`.
        // New pendings may be scheduled by fired callbacks; keep draining.
        while (true) {
            const next = this.takeNextDue(target)
            if (!next) break
            this.nowMs = next.at
            next.fire()
            await flushMicrotasks()
        }
        this.nowMs = target
        await flushMicrotasks()
    }

    /** Jump the clock without firing pending timers. Rarely what you want; use `advance` in most cases. */
    setTime(ms: number): void {
        this.nowMs = ms
    }

    /** Count of scheduled (not yet fired, not cancelled) entries. */
    get pendingCount(): number {
        return this.pending.reduce((n, p) => n + (p.cancelled ? 0 : 1), 0)
    }

    private enqueue(at: number, fire: () => void): Pending {
        const entry: Pending = { at, seq: this.seqCounter++, fire }
        this.pending.push(entry)
        return entry
    }

    private takeNextDue(target: number): Pending | undefined {
        let bestIdx = -1
        let best: Pending | undefined
        for (let i = 0; i < this.pending.length; i++) {
            const p = this.pending[i]!
            if (p.cancelled) continue
            if (p.at > target) continue
            if (!best || p.at < best.at || (p.at === best.at && p.seq < best.seq)) {
                best = p
                bestIdx = i
            }
        }
        if (bestIdx >= 0) this.pending.splice(bestIdx, 1)
        // Opportunistically prune cancelled entries so the list doesn't grow unbounded
        this.pending = this.pending.filter(p => !p.cancelled)
        return best
    }
}

/**
 * Drain the microtask queue by yielding several times. Also runs a
 * `setImmediate` tick so any I/O-scheduled work (e.g. Promise resolutions
 * queued from another turn) is picked up before the next clock step.
 */
async function flushMicrotasks(): Promise<void> {
    for (let i = 0; i < 10; i++) await Promise.resolve()
    await new Promise<void>(resolve => setImmediate(resolve))
}
