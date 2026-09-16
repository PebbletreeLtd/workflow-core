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
import { noopDebugLog, type WorkflowDebugLog } from "./workflowTypes"

interface Pending {
    at: number
    seq: number
    kind: "sleep" | "timer" | "interval"
    label?: string
    fire: () => void
    cancelled?: boolean
}

export interface SimulatedWorkflowClockOptions {
    startMs?: number
    debugLog?: WorkflowDebugLog
}

export class SimulatedWorkflowClock implements WorkflowClock {
    private nowMs: number
    private pending: Pending[] = []
    private seqCounter = 0
    debugLog: WorkflowDebugLog

    constructor(startOrOptions: number | SimulatedWorkflowClockOptions = 0) {
        const opts: SimulatedWorkflowClockOptions =
            typeof startOrOptions === "number" ? { startMs: startOrOptions } : startOrOptions
        this.nowMs = opts.startMs ?? 0
        this.debugLog = opts.debugLog ?? noopDebugLog
        this.debugLog("simClock.init", { startMs: this.nowMs })
    }

    now = (): number => this.nowMs

    sleep = (ms: number, opts?: { advanceIfSupported?: boolean }): Promise<void> => {
        if (opts?.advanceIfSupported) {
            this.debugLog("simClock.sleep.advance", { ms, from: this.nowMs })
            return this.advance(ms)
        }
        const dueAt = this.nowMs + ms
        const seq = this.seqCounter
        this.debugLog("simClock.sleep.enter", { ms, dueAt, seq })
        return new Promise<void>(resolve => {
            this.enqueue(dueAt, "sleep", () => {
                this.debugLog("simClock.sleep.resolve", { dueAt, seq })
                resolve()
            })
        })
    }

    setTimer = (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        const dueAt = this.nowMs + ms
        const seq = this.seqCounter
        this.debugLog("simClock.setTimer.schedule", { ms, dueAt, seq })
        const entry = this.enqueue(dueAt, "timer", () => {
            this.debugLog("simClock.setTimer.fire", { dueAt, seq })
            fn()
        })
        return {
            cancel: () => {
                if (!entry.cancelled) this.debugLog("simClock.setTimer.cancel", { dueAt, seq })
                entry.cancelled = true
            }
        }
    }

    setTimerInterval = (fn: () => void, ms: number): WorkflowClockTimerCancel => {
        let cancelled = false
        let current: Pending
        const intervalSeq = this.seqCounter
        this.debugLog("simClock.setTimerInterval.schedule", { ms, seq: intervalSeq })
        const tick = () => {
            if (cancelled) return
            this.debugLog("simClock.setTimerInterval.tick", { seq: intervalSeq, at: this.nowMs })
            try { fn() } finally {
                if (!cancelled) current = this.enqueue(this.nowMs + ms, "interval", tick)
            }
        }
        current = this.enqueue(this.nowMs + ms, "interval", tick)
        return {
            cancel: () => {
                if (!cancelled) this.debugLog("simClock.setTimerInterval.cancel", { seq: intervalSeq })
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
        this.debugLog("simClock.advance.enter", {
            ms,
            from: this.nowMs,
            target,
            pendingBefore: this.pendingCount,
        })
        let fired = 0
        // Loop until no more pendings are due at or before `target`.
        // New pendings may be scheduled by fired callbacks; keep draining.
        while (true) {
            const next = this.takeNextDue(target)
            if (!next) break
            this.nowMs = next.at
            next.fire()
            fired++
            await flushMicrotasks()
        }
        this.nowMs = target
        await flushMicrotasks()
        this.debugLog("simClock.advance.exit", {
            nowMs: this.nowMs,
            fired,
            pendingAfter: this.pendingCount,
        })
    }

    /** Jump the clock without firing pending timers. Rarely what you want; use `advance` in most cases. */
    setTime(ms: number): void {
        this.debugLog("simClock.setTime", { from: this.nowMs, to: ms, pendingCount: this.pendingCount })
        this.nowMs = ms
    }

    /** Count of scheduled (not yet fired, not cancelled) entries. */
    get pendingCount(): number {
        return this.pending.reduce((n, p) => n + (p.cancelled ? 0 : 1), 0)
    }

    /** Snapshot of currently scheduled entries (not cancelled), sorted by due time. Useful for stall diagnostics. */
    pendingSnapshot(): Array<{ at: number; seq: number; kind: Pending["kind"] }> {
        return this.pending
            .filter(p => !p.cancelled)
            .map(p => ({ at: p.at, seq: p.seq, kind: p.kind }))
            .sort((a, b) => a.at - b.at || a.seq - b.seq)
    }

    private enqueue(at: number, kind: Pending["kind"], fire: () => void): Pending {
        const entry: Pending = { at, seq: this.seqCounter++, kind, fire }
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
