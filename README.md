# @pebbletree/workflow-core

Database-agnostic workflow engine for TypeScript. Manages the full lifecycle of
background jobs — scanning, picking, executing, retrying, scheduling, and
logging — with pluggable storage adapters, capability-based routing, and
distributed coordination via a token ring.

## Features

- **Pick → Run → Outcome lifecycle** — scan for ready jobs, atomically adopt
  them, execute via typed runner classes, and handle success/retry/fatal paths.
- **Pluggable storage** — implement the `WorkflowJobStorage` interface for any
  database (FoundationDB, Postgres, SQLite, in-memory, …).
- **Token ring coordination** — extends `TokenRingWorkDistributor` from
  `@pebbletree/tokenring` for distributed work distribution, liveness
  detection, and automatic recovery of jobs from unresponsive servers.
- **Capability routing** — bitmap-encoded capability sets let a multi-server
  ring decide which nodes can run which job types. Capabilities use string
  type names and are encoded/decoded via helper functions.
- **Retry & backoff** — configurable per-job retry policy with exponential
  backoff. Exhausted retries escalate to fatal. Use `WorkflowUtils.retryPolicy()`
  to compute a policy from a target duration.
- **Repeat scheduling** — daily, weekly, monthly (by date or day-of-week),
  annual, and periodic repeat schedules with count/date/forever termination.
- **Progress & liveness** — jobs report progress; a deadline timer detects
  stalled executions. Pushback timers extend the lease automatically.
- **Readoption detection** — if another executor steals the job mid-run, the
  original detects it and aborts cleanly.
- **Structured logging** — job outcomes and custom log entries are written
  through the storage adapter's log subspace.
- **Metrics** — `WorkflowCounter` tracks pick statistics and per-job-type
  execution counts, durations, and outcomes in a sliding window.
- **Error classes** — `JobError` and `JobFatalError` let runners signal
  recoverable and fatal failures with structured outcomes.
- **In-memory testing** — ships with `SimulatedJobRunner` for unit-testing
  runners without any database, plus an `InMemoryJobStorage` adapter used by
  the built-in test suite.

## Installation

```bash
npm install @pebbletree/workflow-core
```

## Quick start

```ts
import {
  WorkflowEngine,
  WorkflowPicker,
  JobRunner,
  capabilitiesToBuffer,
  type BasicJobPayload,
  type WorkflowStorageTransaction,
  type WorkflowEngineOptions,
  type WorkflowPickerArgs,
  type JobRunnerConstructor,
} from "@pebbletree/workflow-core"

// 1. Define your payload type
interface MyPayload extends BasicJobPayload {
  type: "send-email" | "generate-report"
  to?: string
}

// Shorthand for the transaction type used by your storage adapter.
// If your adapter extends WorkflowStorageTransaction, use that type here.
type TXN = WorkflowStorageTransaction<MyPayload>

// 2. All possible job type values, sorted (used for bitmap encoding)
const allTypes: MyPayload["type"][] = ["generate-report", "send-email"]

// 3. Write runners as JobRunner subclasses
class SendEmailRunner extends JobRunner<MyPayload, "send-email", TXN> {
  async runJob() {
    const { payload } = await this.GetJob()
    // ... send the email using payload.to ...
    await this.Progress() // keep the lease alive for long jobs
  }
}

class GenerateReportRunner extends JobRunner<MyPayload, "generate-report", TXN> {
  async runJob() {
    const { payload } = await this.GetJob()
    // ... build the report ...
  }
}

// 4. Create a storage adapter (implement WorkflowJobStorage for your DB)
const storage = createMyDatabaseAdapter<MyPayload>()

// 5. Create a picker (one per job table)
const picker = new WorkflowPicker<MyPayload, TXN>({
  storage,
  batchSize: 10,
  idealMaxRunning: 50,
})

// 6. Create the engine (extends TokenRingWorkDistributor)
//    WorkflowEngine is abstract — subclass it and register runners
//    via the curried AddRunner() helper inside InitialiseRunners().
class MyEngine extends WorkflowEngine<MyPayload, TXN> {
  InitialiseRunners() {
    this.AddRunner("send-email")(SendEmailRunner)
    this.AddRunner("generate-report")(GenerateReportRunner)
  }
}

const engine = new MyEngine({
  pickers: [picker],
  allSortedCapabilities: allTypes,
  segment_name: "my-workflow",
  issuer_id: "server-1",
  ringConfig: myRingConfig,
  ringStorage: myRingStorageTransactionFactory,
})

// The engine automatically picks and runs jobs when it receives a
// token from the ring. canRunType is derived from the registered
// runners — no need to supply it yourself. You can also pick manually:
await engine.pick({
  executorId: "server-1",
  averageWorkload: 5,
  currentRunning: 3,
})

// 7. Clean up
engine.Destroy()
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│            WorkflowEngine                                │
│            (extends TokenRingWorkDistributor)             │
│                                                          │
│   onToken()  ──►  pick()                                 │
│                     │                                    │
│                     ▼                                    │
│              WorkflowPicker (one per job table)          │
│                │  scan via txn.at.getRangeSnapshot()     │
│                │  filter by canRunType() (from engine)   │
│                │  adopt atomically (optimistic lock)     │
│                ▼                                         │
│              startJob()  ──►  runJob() hook              │
│                │  resolve JobRunner subclass from map    │
│                │  instantiate runner with storage ref    │
│                │  runner.Run()                           │
│                ▼                                         │
│              JobRunner                                   │
│                │  GetJob() / Progress() / Wait()         │
│                │  progress timers & pushback             │
│                │  retry / reschedule / fatal             │
│                │  repeat schedule computation            │
│                │  outcome logging via WriteJobLog()      │
│                ▼                                         │
│              WorkflowJobStorage (adapter)                │
│                doTn ──► WorkflowStorageTransaction       │
│                  txn.job   (get/set/clear jobs)          │
│                  txn.at    (scan ready jobs by time)     │
│                  txn.executor (find jobs by executor)    │
│                  txn.jobLogKey (append log entries)      │
│                                                          │
│   onServerUnresponsive()  ──►  resetLostJobs()           │
│              reclaims jobs from dead ring members         │
└──────────────────────────────────────────────────────────┘
```

## Core concepts

### Storage adapter

Implement `WorkflowJobStorage<PAYLOAD_T, TXN>` for your database. Each
adapter instance maps to one job table. The interface has a single member:

| Member | Purpose |
|---|---|
| `doTn` | Transaction factory — all reads/writes happen inside transactions |

All storage operations are performed through `WorkflowStorageTransaction<PAYLOAD_T>`,
which is passed to the `doTn` callback. The transaction exposes four typed
sub-objects:

| Sub-object | Methods | Purpose |
|---|---|---|
| `txn.job` | `get`, `snapshotGet`, `set`, `clear` | Read/write/delete job documents by `job_id` |
| `txn.at` | `getRangeSnapshot` | Scan ready jobs by `header.at` timestamp |
| `txn.executor` | `getRangeAllStartsWith` | Find jobs by `execution_id` (dead-server recovery) |
| `txn.jobLogKey` | `set` | Append structured log entries |

See `InMemoryJobStorage` for a complete reference implementation.

### Capabilities

Three helper functions — `capabilitiesToBuffer`, `bufferToCapabilities`, and
`mergeCapabilityBuffers` — encode string-typed job capabilities as compact
bitmaps. The engine encodes each server's supported types when joining the
token ring, and the ring merges bitmaps so every member knows which types the
cluster can handle. If a job type is unsupported by the entire ring, the
picker still adopts it (failing fast so queues don't back up).

### Job runners

Runners are subclasses of `JobRunner<PAYLOAD_T, T, TXN>`. Implement the abstract
`runJob()` method to define the work. Inside `runJob()` you have access to:

| Method | Purpose |
|---|---|
| `this.GetJob()` | Read the current job (starts progress/pushback timers) |
| `this.Progress(payload?)` | Report progress, extend the lease, optionally update payload |
| `this.Wait(ms)` | Wait up to 2 minutes, auto-progressing. Longer waits return a reschedule timestamp |
| `this.store` | Direct access to the storage adapter |
| `this.jobKey` | The key of the running job |
| `this.jobType` | The job's type string |

Return `void` for normal completion, or a future timestamp (`number`) to
reschedule the job to a specific time.

**Overridable hooks** (protected):

| Method | Purpose |
|---|---|
| `WriteJobLog(txn, outcome)` | Customise how outcomes are written to the log subspace |
| `onFatalError({ job, jobKey, outcome })` | Called on fatal/vanished/rescheduled-error outcomes |
| `onJobOutcome({ outcome, payload, jobKey, txn })` | Called on every outcome — useful for notifications |

**Static:**

| Member | Purpose |
|---|---|
| `JobRunner.JobRunningCount` | `{ total, types }` — current running job counts across all runners |

### Job errors

Throw `JobError` or `JobFatalError` from `runJob()` to control the outcome:

- `throw new JobFatalError("reason")` — marks the job as permanently failed.
- `throw new JobError({ type: "custom-recoverable", message: "..." })` — triggers a retry (respecting the retry policy).

### Job outcomes

Every job execution resolves to a `WorkflowJobOutcome`:

| Outcome | Meaning |
|---|---|
| `success` | Job completed normally |
| `fatal-error` | Unrecoverable failure (retries exhausted or fatal throw) |
| `rescheduled-error` | Recoverable error — job rescheduled for retry with backoff |
| `vanished` | Job disappeared from storage mid-run |
| `readopted` | Another executor claimed the job (original aborts) |

### Engine hooks

`WorkflowEngine` provides a virtual `runJob()` method that wraps every job
execution. Override it to add tracing, logging, or other cross-cutting concerns:

```ts
class MyEngine extends WorkflowEngine<MyPayload, TXN> {
  // ...
  override runJob(args: {
    jobKey: WorkflowJobKey
    payload: MyPayload
    fn: () => Promise<WorkflowJobOutcome>
  }) {
    console.log("Starting job", args.jobKey.job_id, args.payload.type)
    return args.fn()
  }
}
```

### Repeat schedules

Jobs can carry a `repeatSchedule` in their header. On successful completion (or
exhausted retries), the schedule is advanced and the job is rescheduled
automatically. Supported types: `daily`, `weekly`, `monthlyDate`, `monthlyDay`,
`annually`, `periodic`, `none`.

## Utilities

### Duration helpers

Convenience functions for expressing durations in milliseconds:

```ts
import { durationSeconds, durationMinutes, durationHours, durationDays, namedDuration } from "@pebbletree/workflow-core"

durationSeconds(30)  // 30_000
durationMinutes(5)   // 300_000
durationHours(1)     // 3_600_000
durationDays(7)      // 604_800_000
namedDuration("hours", 2) // 7_200_000
```

### Retry policy helper

`WorkflowUtils.retryPolicy()` computes a `WorkflowRetryPolicy` from an initial
backoff and a target total duration:

```ts
import { WorkflowUtils } from "@pebbletree/workflow-core"

// Exponential backoff starting at 1 s, covering ~5 minutes total
const policy = WorkflowUtils.retryPolicy(1000, 300_000)
// → { max: N, initial_backoff_ms: 1000, exponent: 2 }
```

### Metrics counter

`WorkflowCounter` is a sliding-window accumulator that tracks pick statistics
and per-job-type execution/duration/outcome counts. The engine uses it
internally and can print periodic summaries via `summaryIntervalMs`.

```ts
import { WorkflowCounter } from "@pebbletree/workflow-core"

WorkflowCounter.Create({ name: "main", counter_duration_ms: 60_000 })
const stats = WorkflowCounter.getValue("main")
// stats.picks → { requested, got, cycles, conflicts, utilisation }
// stats.jobs  → { [type]: { totals: { executed, duration }, outcomes: { ... } } }
```

### Schedule computation

`computeNextSchedule(header)` advances a job's repeat schedule to the next
future occurrence, or returns `undefined` if the schedule is exhausted.

## Module map

| File | Description |
|---|---|
| `workflowEngine.ts` | High-level orchestrator: extends token ring, picks jobs, resolves runners, manages execution |
| `workflowPicker.ts` | Scans storage for ready jobs, filters by capability, adopts atomically |
| `jobRunner.ts` | Runs a single job: progress timers, pushback, retry logic, outcome logging |
| `workflowCapabilities.ts` | Bitmap encode/decode/merge for string-typed capabilities |
| `workflowStorageAdapter.ts` | Storage interface (implement for your database) |
| `workflowTypes.ts` | All shared type definitions, duration helpers |
| `schedule.ts` | Repeat schedule computation (pure logic) |
| `inMemoryStorage.ts` | In-memory `WorkflowJobStorage` implementation for testing |
| `counter.ts` | In-memory sliding-window metrics accumulator |
| `jobErrors.ts` | `JobError` and `JobFatalError` classes |
| `simulatedJobRunner.ts` | In-memory job runner for unit-testing runner subclasses |

## Testing

Tests use [Vitest](https://vitest.dev) with an in-memory storage adapter — no
external databases needed.

```bash
# Run tests (quiet — console output suppressed)
npm test

# Run tests with application console output visible
npm run test:verbose

# Watch mode
npm run test:watch
```

## Scripts

| Script | Command |
|---|---|
| `npm run build` | Compile TypeScript to `dist/` |
| `npm test` | Run tests once |
| `npm run test:verbose` | Run tests with console output (`VERBOSE=1`) |
| `npm run test:watch` | Run tests in watch mode |

## License

MIT — see [LICENSE](LICENSE).
