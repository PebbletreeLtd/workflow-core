# @pebbletree/workflow-core

Database-agnostic workflow engine for TypeScript. Manages the full lifecycle of
background jobs — scanning, picking, executing, retrying, scheduling, and
logging — with pluggable storage adapters and capability-based routing.

## Features

- **Pick → Run → Outcome lifecycle** — scan for ready jobs, atomically adopt
  them, execute via typed runner functions, and handle success/retry/fatal paths.
- **Pluggable storage** — implement the `WorkflowJobStorage` interface for any
  database (FoundationDB, Postgres, SQLite, in-memory, …).
- **Capability routing** — bitmap-encoded capability sets let a multi-server
  ring decide which nodes can run which job types.
- **Retry & backoff** — configurable per-job retry policy with exponential
  backoff. Exhausted retries escalate to fatal.
- **Repeat scheduling** — daily, weekly, monthly (by date or day-of-week),
  annual, and periodic repeat schedules with count/date/forever termination.
- **Progress & liveness** — jobs report progress; a deadline timer detects
  stalled executions. Pushback timers extend the lease automatically.
- **Readoption detection** — if another executor steals the job mid-run, the
  original detects it and aborts cleanly.
- **Structured logging** — job outcomes and custom log entries are written
  through the storage adapter.
- **In-memory testing** — ships with `SimulatedJobManager` for unit-testing
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
  WorkflowCapabilities,
  type WorkflowConfig,
  type JobRunnerFunction,
  type BasicJobPayload,
} from "@pebbletree/workflow-core"

// 1. Define your payload type
interface MyPayload extends BasicJobPayload {
  type: "send-email" | "generate-report"
  to?: string
}

// 2. Map string type names → numeric enum values
const enumLookup: Record<string, number> = {
  "send-email": 1,
  "generate-report": 2,
}

// 3. Write runners
const sendEmail: JobRunnerFunction<MyPayload> = async (mgr) => {
  const { payload } = await mgr.GetJob()
  // ... send the email ...
  await mgr.Progress() // keep the lease alive for long jobs
}

const generateReport: JobRunnerFunction<MyPayload> = async (mgr) => {
  const { payload } = await mgr.GetJob()
  // ... build the report ...
}

// 4. Register capabilities
const capabilities = new WorkflowCapabilities<MyPayload>({
  allSortedCapabilities: [1, 2],
  runners: new Map<number, JobRunnerFunction<MyPayload>>([
    [1, sendEmail],
    [2, generateReport],
  ]),
})

// 5. Create a storage adapter (implement WorkflowJobStorage for your DB)
const storage = createMyDatabaseAdapter()

// 6. Wire everything together
const config: WorkflowConfig = {
  workflow_server_reregister_time_ms: 60_000,
  workflow_token_ack_timeout_ms: 500,
  workflow_batch_size: 10,
  workflow_ideal_maximum_jobs_running: 50,
  workflow_max_job_retention_age_days: 30,
  workflow_supress_job_outcome_logs: ["success"],
}

const picker = new WorkflowPicker<MyPayload>({
  storage,
  capabilities,
  config,
  enumLookup,
})

const engine = new WorkflowEngine<MyPayload>({
  pickers: [picker],
  capabilities,
  config,
  enumLookup,
})

// 7. Call engine.pick() on your scheduling trigger (e.g. token ring, timer)
await engine.pick({
  executorId: "server-1",
  averageWorkload: 5,
  currentRunning: 3,
})

// 8. Clean up
engine.destroy()
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  WorkflowEngine                     │
│                                                     │
│   pick()  ──►  WorkflowPicker (one per table)       │
│                  │  scanReadyJobs (snapshot read)    │
│                  │  filter by WorkflowCapabilities   │
│                  │  adoptJob (atomic claim)          │
│                  ▼                                   │
│              startJob()                              │
│                  │  resolve runner from capabilities │
│                  │  create JobManager                │
│                  │  mgr.Run(runner)                  │
│                  ▼                                   │
│              JobManager                              │
│                  │  progress timers & pushback       │
│                  │  retry / reschedule / fatal       │
│                  │  repeat schedule computation      │
│                  │  outcome logging                  │
│                  ▼                                   │
│              WorkflowJobStorage (adapter)            │
│                  getJob / updateJob / adoptJob       │
│                  scanReadyJobs / writeLog            │
│                  runInTransaction                    │
└─────────────────────────────────────────────────────┘
```

## Core concepts

### Storage adapter

Implement `WorkflowJobStorage<PAYLOAD_T>` for your database. Each adapter
instance maps to one job table. Methods are self-transactional — no leaked
transaction handles.

Key methods:

| Method | Purpose |
|---|---|
| `getJob` | Read a job by key |
| `scanReadyJobs` | Snapshot-scan for jobs with `header.at` in a time range |
| `adoptJob` | Atomically claim a job (optimistic lock on `header.at`) |
| `updateJob` | Read-modify-write with `execution_id` validation |
| `resetOrphanedJobs` | Reclaim jobs from a dead executor |
| `writeLog` | Persist a structured log entry |
| `runInTransaction` | Group reads/writes atomically |

The transactional view (`WorkflowJobStorageTransaction`) adds `setJob`,
`getJobByJobId`, and an optional `processJobOutcome` hook for post-processing
within the same transaction.

### Capabilities

`WorkflowCapabilities` maps numeric job type IDs to runner functions and
encodes the set as a compact bitmap buffer. This enables multi-server rings to
merge capability bitmaps and determine which types the entire ring supports.

### Job outcomes

| Outcome | Meaning |
|---|---|
| `success` | Job completed normally |
| `rescheduled-error` | Recoverable error; job rescheduled with decremented retries |
| `fatal-error` | Unrecoverable error or retries exhausted; job cleared |
| `vanished` | Job disappeared from storage mid-execution |
| `readopted` | Another executor claimed the job; this run aborts |

### Repeat schedules

Jobs can carry a `repeatSchedule` in their header. On successful completion (or
exhausted retries), the schedule is advanced and the job is rescheduled
automatically. Supported types: `daily`, `weekly`, `monthlyDate`, `monthlyDay`,
`annually`, `periodic`, `none`.

### Error types

Runners signal errors by throwing `JobError` or `JobFatalError`:

```ts
import { JobError, JobFatalError } from "@pebbletree/workflow-core"

// Recoverable — will retry if retries remain
throw new JobError({ type: "custom-recoverable", message: "API timeout" })

// Fatal — clears the job immediately
throw new JobFatalError("Unrecoverable: invalid payload")
```

## Module map

| File | Description |
|---|---|
| `workflowEngine.ts` | High-level orchestrator: picks jobs, resolves runners, manages execution |
| `workflowPicker.ts` | Scans storage for ready jobs, filters by capability, adopts atomically |
| `jobManager.ts` | Runs a single job: progress timers, pushback, retry logic, outcome logging |
| `workflowCapabilities.ts` | Maps numeric job types to runners; bitmap encode/decode |
| `workflowStorageAdapter.ts` | Storage interface (implement for your database) |
| `workflowTypes.ts` | All shared type definitions, duration helpers |
| `schedule.ts` | Repeat schedule computation (pure logic) |
| `capabilityBuffer.ts` | Low-level bitmap encode/decode/merge |
| `counter.ts` | In-memory sliding-window metrics accumulator |
| `jobErrors.ts` | `JobError` and `JobFatalError` classes |
| `simulatedJobManager.ts` | In-memory job manager stub for unit-testing runners |

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
