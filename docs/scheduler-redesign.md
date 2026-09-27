# Scheduler redesign — speculative

Status: **under discussion, not implemented**. This captures the design from the
chat thread on 2026-09-20/21 for review. Everything is provisional.

## Why redesign

The current token-ring picker (see [workflowPicker.ts](../src/workflowPicker.ts),
[workflowEngine.ts](../src/workflowEngine.ts)) has three properties we want to
change:

1. **Polling.** Every server holds the token in rotation and scans for ready
   jobs whether or not any exist. Baseline DB load is non-zero at rest; tail
   latency for "run this immediately" jobs is proportional to token rotation
   period.
2. **Implicit clock coordination.** Writers stamp absolute `at` from their own
   clock; every reader compares against its own clock to decide readiness.
   Cross-server clock skew is neither bounded nor detectable in the design.
3. **Adoption conflicts.** Phase II adoption (code 1020) grows with server
   count. Current mitigation is a bounded retry loop.

We *want to keep* the token ring's <1s dead-server detection.

## Goals

- Workers do not poll when there is no work.
- Exactly one clock is authoritative for scheduling. Any skew between servers
  never affects correctness.
- Workers own no scheduling state and make no scheduling decisions.
- Preserve <1s dead-server detection.
- Simplify the worker code path substantially.

## Shape

One elected leader is the sole clock authority and dispatcher. Workers execute
what they're told. Token ring is used only for leader election and worker
liveness.

```
   external writer
        │
        ▼
  rel(vs, id) → {delay_ms, payload, caps}          ─┐
  ingest_counter (atomic-add)                       │
                                                    │  writer surface
                                                    │  (external + internal
                                                    │   producers)
                                                   ─┘

        ▲ watch(ingest_counter)
        │
  ┌─────┴─────────────────────────────────────┐
  │  LEADER (elected via token ring)          │
  │                                           │
  │   ingest loop  ── writes ─► abs(at, id)   │──► persistent scheduled table
  │   prefill loop ── reads ── abs            │
  │   dispatch loop── reads ── abs, writes ─► │──► dispatch[worker_id]
  │                                           │
  │   in-memory: min-heap of (at, id)         │
  │              + Set<id> for dedup          │
  └───────────────────────────────────────────┘
                              │
                              ▼
                    watch(dispatch[W])
                              │
                    ┌─────────┴─────────┐
                    │  WORKER (many)    │
                    │  run + write      │
                    │  outcome          │
                    └───────────────────┘
```

## Storage schema

One logical subspace, key-type union:

- `rel(versionstamp, id) → {delay_ms, payload, caps}` — writer append-only.
  Deleted by leader on ingest.
- `abs(at_ms, id) → {payload, caps, execution_id?, at_lost_deadline?}` —
  leader-owned. Persistent from ingest to completion.

Auxiliary:

- `ingest_counter` — single atomic-int key. Writer atomic-add signals the
  leader; leader arm-sleep txn reads it with a conflict range.
- `dispatch[worker_id]` — per-worker append-only queue keyed by versionstamp.
  Values reference `abs` entries.

## Writer path

Given `delay_ms`, in one txn:

1. Insert `rel[versionstamp, id] = {delay_ms, payload, caps}`.
2. If `delay_ms < X_ingest + safety_margin`, `atomic_add(ingest_counter, 1)`.

That's it. Writers never read, never see absolute time, never touch `abs`.

The `safety_margin` covers writer-side commit-latency variance (not clock
drift — there is no inter-server clock dependency in this design). If a
writer's own commit takes longer than expected, its `delay_ms` still counts
from *when the entry lands in `rel`*, not from the writer's local `now`. The
margin is chosen to comfortably exceed realistic p99 commit latency.

## Leader — ingest loop

Single event loop. One txn per turn, taking one of two shapes:

```
txn:
  entries = snapshot_scan(rel, limit=N)
  if entries.length > 0:
    for e in entries:
      abs[leader_now + e.delay_ms, e.id] = {payload, caps}
      delete rel[e.vs, e.id]
    commit
    // post-commit, on the same event loop:
    for e in entries where e.absolute_at ≤ leader.current_horizon:
      heap.tryInsert(e.absolute_at, e.id)  // Set<id> dedup
    // immediate re-loop
  else:
    addReadConflictKey(ingest_counter)
    w = watch(ingest_counter)
    commit
    await Promise.race([w, timer(X_ingest)])
    // re-loop
```

**Arm-conflict handling.** If the arm txn 1020s (a writer bumped
`ingest_counter` during our snapshot-empty read), retry up to `arm_retry_cap`
(3). If still failing, skip the arm entirely and re-loop immediately — under
sustained near-term write load the re-loop's snapshot-scan of `rel` will
return non-empty (writers are actively populating it), so the "give up on
arming" fallback degrades into productive draining, not a spin.

**Snapshot-scan** is deliberate: it avoids conflicting with writers on the
`rel` range itself. The single-key `ingest_counter` conflict is the only
signal that needs to reach the leader synchronously.

## Leader — prefill loop

```
timer every H/2:
  next = snapshot_range_read(abs, [leader.current_horizon, leader_now + H])
  for e in next:
    heap.tryInsert(e.at, e.id)      // Set<id> dedup, no-op if present
  leader.current_horizon = leader_now + H
```

No conflicts, no watches — the leader is the sole writer to `abs`, so it's
reading its own past state. Any entry the leader wrote to `abs` in this
window either:

- was ingested with `absolute_at ≤ current_horizon`, in which case the ingest
  loop's post-commit hook already inserted it into the heap; or
- will be captured by the next prefill sweep.

The `Set<id>` dedup handles the race where both paths try to insert the same
entry.

Cold start (after leader failover) uses the same code path with
`current_horizon = leader_now`.

## Leader — dispatch loop

```
timer on heap.min.at:
  while heap.min.at ≤ leader_now:
    (at, id) = heap.pop()
    txn:
      job = abs.get(at, id)
      if !job or job.execution_id: continue  // reassigned or missing
      worker = pick_worker(job.caps)
      abs.set(at, id, {
        ...job,
        execution_id: worker,
        at_lost_deadline: leader_now + lost_deadline_ms,
      })
      dispatch[worker].append({abs_key: (at, id), payload: job.payload})
      commit
```

Worker selection reads the token-ring registration table for live workers +
their capabilities. Least-loaded-of-capable is a reasonable first-cut policy.

If `pick_worker` returns none (no live worker supports this capability), the
leader writes a terminal failed outcome directly to `abs` and stops — this
replaces today's adopt-to-fail dance.

## Worker path

Per worker, one loop:

```
loop:
  entries = dispatch[self].drain()
  if entries.length == 0:
    w = watch(dispatch[self].head_key)
    await w
    continue
  for e in entries:
    outcome = run(e.payload)
    txn:
      write outcome
      delete abs[e.abs_key]
      delete dispatch[self][e.vs]
      commit
```

No clock reasoning, no scanning, no adoption. Watch on the worker's own
dispatch queue only.

## Failover

**Leader dies:**
- Token ring detects (<1s) and elects a new leader.
- New leader boot:
  1. Prefill from `leader_now` → heap populated with the near-horizon.
  2. Run ingest drain → any un-drained `rel` entries flow through.
  3. Recovery sweep of `abs`: entries with `execution_id` past
     `at_lost_deadline` where the executor is now dead — reset to pending
     (same shape as [resetLostJobs](../src/workflowPicker.ts#L308-L360)).
  4. Enter normal loops.
- During the failover window: writers keep writing to `rel`; workers keep
  executing whatever's already in their dispatch queues.

**Worker dies:**
- Token ring detects.
- Leader scans `abs` for entries with `execution_id == dead_worker`, clears
  the field, re-inserts into heap.
- Leader clears `dispatch[dead_worker]`.

## Kept, changed, dropped

**Kept from today:**
- Token ring package (`@pebbletree/tokenring`) — scoped down to election +
  liveness.
- `execution_id` + `lost_deadline_ms` semantics on the job record.
- The general shape of `resetLostJobs`.

**Changed:**
- Storage: `at`-indexed job table → `rel` + `abs`.
- Writers write `delay_ms`, not absolute `at`.
- No token payload, no `averageWorkload`, no `runningRatioToAverage`, no
  batch-size adaptation.

**Dropped:**
- `WorkflowPicker` per-server scan and Phase II adopt loop.
- `capabilityBuffer` bitmap encoding (capabilities read from registration
  values directly).
- `PickContext.ringState` and `isProvisional` — leader has direct read access
  to the registration table.

## Configuration

| Knob                | Meaning                                          | Starting default |
| ------------------- | ------------------------------------------------ | ---------------- |
| `X_ingest`          | Leader ingest loop's max sleep                   | 60 s             |
| `safety_margin`     | Extra threshold for writer wake-decision         | 5 s              |
| `H`                 | In-memory horizon window                         | 10 min           |
| `arm_retry_cap`     | Ingest arm conflicts before fall-through re-loop | 3                |
| `dispatch_batch`    | Max jobs dispatched per dispatch-loop turn       | 100              |
| `lost_deadline_ms`  | (unchanged from today)                           | as-is            |

## Open questions

1. **Single-leader throughput ceiling.** All ingest + all dispatch through one
   process. Target peak insert/sec and dispatch/sec? Above what number does
   leadership need to shard, and by what key (segment? capability? hash of
   id?) without breaking dead-server detection SLA?

2. **Failover cold-start cost.** New leader must snapshot-read
   `abs[leader_now, leader_now + H]`. Range read size scales with the density
   of scheduled jobs in H. Worth measuring under realistic data before
   trusting the failover latency claim.

3. **Ingest batch size N.** Too small → many round-trips under load. Too
   large → risk hitting FDB txn size limits (10MB, 5s) and increased latency
   for other loops. Adaptive? Fixed?

4. **Prefill trigger.** Timer every `H/2` is coarse. Should prefill be
   triggered by heap-drain past a watermark instead (e.g. when heap has < 10%
   of its post-prefill size)?

5. **Per-engine multi-table support.** Today `WorkflowEngine` supports
   multiple `WorkflowPicker`s. Does one leader own all tables' `rel`/`abs`,
   or is there one election per (segment, table)? Simpler: leader per
   segment, owns all tables. Confirm segment semantics still hold.

6. **Unsupported-type failure path.** Design says leader writes a terminal
   failed outcome directly at dispatch. That changes the code path job
   outcomes travel through — verify it hits the same after-hooks
   (`OnAfterWorkflowJob`) that today's adopt-to-fail path does.

7. **`safety_margin` calibration.** What's a defensible number for
   worst-case FDB commit latency in real workload? 5s is a guess.

8. **Worker back-pressure.** No cap on `dispatch[W]` depth. If a worker is
   slow, its queue grows unbounded. Do we want a per-worker inflight cap?
   Where — leader (refuses to dispatch), worker (stops draining), or both?

9. **Ingest-commit + heap-insert atomicity.** The post-commit heap insert is
   not atomic with the ingest txn. Leader crash between ingest commit and
   heap insert leaves the entry in `abs` but not in memory. Next prefill
   catches it. Acceptable, but verify.

10. **`dispatch_batch` per turn.** With the timer on `heap.min.at`, a single
    turn could try to dispatch a large backlog if the leader is behind.
    Bound per-turn to keep the event loop responsive.

11. **Test harness.** Current [tests/harness.ts](../src/tests/harness.ts) and
    [tests/setup.ts](../src/tests/setup.ts) simulate the token ring with
    `simulatedJobRunner` and `simulatedWorkflowClock`. Does the harness
    model extend to leader election + watches + per-worker queues, or does
    it need substantial rework?

12. **Multi-writer to `rel`.** No conflict on `rel` writes because keys are
    versionstamped and unique. Confirm this is true for whatever storage
    adapter we build (i.e., the storage layer must expose versionstamp
    generation).

13. **In-flight state on dispatch.** Currently proposed: leader marks
    `abs.execution_id = worker` in the same txn as the dispatch queue write.
    On worker completion the `abs` entry is deleted. Alternative: leader
    deletes `abs` at dispatch and the dispatch queue entry is the sole
    durable record. First option keeps failover recovery uniform with today
    (`resetLostJobs`). Confirm which we want.
