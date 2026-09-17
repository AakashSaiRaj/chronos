# Chronos — Distributed Workflow Engine

A workflow orchestration engine in the spirit of Temporal: you register a DAG of
tasks, start an execution, and a pool of workers executes the tasks in dependency
order. Every piece of state lives in PostgreSQL, so an execution survives the
process that started it.

**Phases 1 and 2 are complete**: a locally runnable engine with workers executing
persisted workflows, made fault-tolerant with retries, lease reclamation, worker
failure detection, and dead-letter handling. Phases 3–4 add cloud-native
deployment and observability — see [What's next](#whats-next).

```
Workflow "order_pipeline"
        │
   Task A ──▶ Task B ──▶ Task C
 charge     reserve      send
 payment    inventory    receipt
```

---

## Quick start

Requires Go 1.26+, podman, `jq`, and `curl`.

```bash
make db-up          # start PostgreSQL in podman (loopback :55432)
make demo           # register the workflow above, run it, show persisted state
```

`make demo` starts the control plane and two workers, runs the pipeline, then
reads the result back out of PostgreSQL to show that nothing was held in memory.

To see the durability and fault-tolerance claims actually tested:

```bash
make recovery-demo  # SIGKILL the engine mid-workflow; a new process finishes it
make failure-demo   # SIGKILL a worker mid-task; another one takes over.
                    # Also: retries with backoff, dead-lettering, operator replay.
```

Other entry points:

```bash
make help              # all targets
make test              # unit tests, no database needed
make test-integration  # everything, against the podman PostgreSQL
make stack-up          # postgres + server + 2 workers, all in containers
make psql              # inspect the database directly
make dlq               # show the dead letter queue
```

---

## Architecture

```
                        ┌──────────────────────────────┐
   client ──── REST ───▶│        chronos-server        │
                        │                              │
                        │  ┌────────┐   ┌───────────┐  │
                        │  │  API   │──▶│  Service  │  │  orchestration,
                        │  └────────┘   └─────┬─────┘  │  validation
                        │                     │        │
                        │  ┌───────────┐      │        │
                        │  │  Engine   │◀ nudge        │  reacts to what
                        │  │ (sweeper) │      │        │  workers reported
                        │  └─────┬─────┘      │        │
                        │  ┌───────────┐      │        │
                        │  │  Reaper   │      │        │  reacts to the
                        │  │ (detector)│      │        │  absence of reports
                        │  └─────┬─────┘      │        │
                        └────────┼────────────┼────────┘
                                 │            │
                                 ▼            ▼
                        ┌──────────────────────────────┐
                        │          PostgreSQL          │
                        │  definitions · executions    │
                        │  tasks (= the queue)         │
                        │  history · workers           │
                        └──────────────────────────────┘
                                 ▲
                                 │ (never directly)
   chronos-worker ─── REST ──────┘
   poll → run activity → renew lease → report
```

Four moving parts:

- **API** (`internal/api`) — HTTP/JSON transport. Parses, validates, maps domain
  errors to status codes. No business logic.
- **Service + Engine** (`internal/engine`) — `Service` handles request-driven
  operations; `Engine` runs the scheduling loop that advances executions.
- **Reaper** (`internal/engine/reaper.go`) — detects the failures nobody reports:
  lapsed leases and workers that stopped heartbeating.
- **Store** (`internal/store`) — all SQL. Repositories plus the task queue.

The Engine and Reaper are separate on purpose. The engine reacts to state a worker
*reported*; the reaper reacts to the absence of a report. Each has exactly one
trigger condition, and the reaper runs on its own (slower) cadence because it is a
timeout detector.

Workers (`internal/worker`) are separate processes that reach the engine only
over the API. They hold no workflow state and need no database credentials, which
is what makes them trivially replaceable and horizontally scalable.

### Execution lifecycle

```
POST /v1/executions          execution row committed as PENDING, request returns
        │
engine sweep                 materialize tasks, PENDING → RUNNING
        │
engine sweep                 tasks whose deps are COMPLETED → SCHEDULED
        │                    (input resolved and persisted at this moment)
        │
POST /v1/tasks/poll          worker claims one task, RUNNING + lease + claim token
        │
worker runs the activity
        │
POST /v1/tasks/{id}/heartbeat  lease renewed; cancellation relayed back
        │
POST /v1/tasks/{id}/complete task COMPLETED, engine nudged
        │
engine sweep                 next tasks become ready … until all COMPLETED
        │
                             execution COMPLETED, output = terminal task outputs
```

The engine loop is a single idempotent function: read the execution and its
tasks, decide what is now possible, write it. Nothing is remembered between
passes.

When things go wrong instead:

```
task FAILED, attempts remain     → engine requeues at now + backoff(attempt)
task FAILED, attempts exhausted  → DEAD_LETTER, execution FAILED
task FAILED, declared permanent  → DEAD_LETTER immediately, budget untouched
lease lapsed, budget remains     → reaper requeues, charged to the lease budget
lease lapsed, budget exhausted   → DEAD_LETTER
worker heartbeat lapsed          → worker DEAD, all its tasks reclaimed at once
operator replays a parked task   → task SCHEDULED, execution FAILED → RUNNING
```

---

## Design decisions

### PostgreSQL is the task queue

The `tasks` table *is* the queue. Claiming a task is one statement:

```sql
UPDATE tasks SET state = 'RUNNING', claim_token = $tok, lease_expires_at = …
WHERE id = (
    SELECT id FROM tasks
    WHERE state = 'SCHEDULED' AND task_queue = $q AND activity = ANY($acts)
    ORDER BY scheduled_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING …
```

`SKIP LOCKED` is what makes it a real queue: concurrent claimants lock different
rows and never block each other, so N workers polling at once get N different
tasks.

The target stack names Kafka or SQS, and Phase 1 deliberately does not use them.
Scheduling a task is a state transition on a row the engine is already writing,
so with a table-backed queue the enqueue commits in the *same transaction* as the
decision that caused it. An external broker would split that into two writes and
require an outbox to stay consistent. Phase 1 gets exactly-once enqueue for free
and runs with one fewer system to operate.

The tradeoff is a throughput ceiling tied to PostgreSQL commit latency, and the
queue competing with application queries for the same database. Phase 4 measures
where that ceiling actually is. The queue operations are confined to
`internal/store/tasks.go` and reached through interfaces declared by their
consumers, so substituting SQS later is a contained change.

### No in-memory run state

The engine never caches what a workflow is doing. Each pass re-derives
everything from rows. This is what the brief's "recoverable from persisted state"
requirement demands, and it is why `make recovery-demo` works: a `SIGKILL`ed
engine is replaced by a process that has never seen the execution and reaches the
identical conclusion.

It also makes the engine horizontally scalable for free, since there is no state
to synchronize between replicas.

The cost is re-reading state on every pass. Bounded queries over partial indexes
on live executions keep that cheap, and completed history never appears in the
engine's scan.

### Polling is the correctness mechanism; nudges are an optimization

The engine sweeps on a timer (250 ms default). After a durable write, the service
also calls `Nudge()` to trigger a sweep immediately.

The split is deliberate. Nudges are a non-blocking, coalescing, best-effort
signal — dropping one costs latency, never correctness, because the next sweep
rediscovers the work in the database. Systems that make the signal load-bearing
have to make signal delivery reliable, which is a much harder problem.

### Concurrency: per-execution locks, contention-free hot paths

Several engine replicas can sweep the same database. Each takes a row lock on the
execution it is working on with `FOR UPDATE SKIP LOCKED`, so an execution is
processed by exactly one replica at a time and contention degrades into "someone
else has it, move on" rather than blocking.

The worker-facing paths — claim a task, report a result — deliberately take *no*
execution-wide lock. They lock only the single task row involved.

That last point took a wrong turn worth recording. History originally carried a
dense per-execution sequence number, which meant every append had to read
`MAX(sequence)` first. That read-modify-write forced concurrent writers to
serialize on the execution row, so claiming two tasks of the *same* workflow
became mutually blocking. Making the retry optimistic instead only converted the
problem into a thundering herd and deadlocks under load. The fix was to drop the
dense counter: history is ordered by its `BIGSERIAL` id, which is monotonic per
execution without being contiguous. Appends became pure inserts with no
coordination, and the claim path became both simpler and parallel. Ordering was
all the audit trail ever needed. `TestConcurrentHistoryAppendsDoNotContend` and
`TestConcurrentWorkersNeverShareATask` pin the resulting behaviour.

### Idempotency is enforced by database constraints

Three operations must tolerate being retried, and each is guarded by a
constraint rather than an application-level check, because "read then decide" is
not safe across replicas:

| Operation | Mechanism | Effect |
|---|---|---|
| Register a workflow | `UNIQUE (name, version)` + canonical spec hash | identical spec → no-op; changed spec → 409 |
| Start an execution | partial `UNIQUE (workflow_name, idempotency_key)` | replay resolves to the same execution |
| Report a task result | `claim_token` must match in the `WHERE` clause | replay is a no-op; a superseded worker is rejected |

The claim token is the interesting one. Each claim mints a fresh token and only
discloses it to the worker that won the lease. Reporting requires presenting it,
so a slow worker whose task was reassigned cannot overwrite the new attempt's
result — it gets `ErrStaleClaim` and abandons the task. A duplicate report of the
*same* attempt is recognized as a replay and returns the already-persisted task,
which is what lets the client retry safely after a lost response.

Because all three are genuinely idempotent, the Go client retries transient
failures blind.

### Workflow versions are immutable

Registering a changed spec under an existing version is a 409. Executions
reference the version they ran against, so mutating it would make persisted
history unreplayable and let two runs of "v1" mean different things. The spec
hash is computed over a canonicalized form, so reordering tasks or reformatting
JSON is correctly treated as no change.

### Task inputs are resolved at schedule time, not claim time

When a task becomes ready, the engine computes its full input — workflow input,
static task input, and each dependency's output — and persists it. A worker
receives a self-contained payload and never reads upstream state itself.

This means a task's input is frozen the moment it is enqueued, so a re-claim
after a worker crash replays byte-identical input rather than re-deriving it from
state that may have moved on.

### Two attempt budgets, not one

This is the decision the whole failure story rests on, and it is not obvious.

`maxAttempts` is the *activity's* budget: how many times the user is willing for
their code to run. A worker being SIGKILLed is not the activity failing — it is
the infrastructure failing. Charging a crash to that budget would mean a task with
`maxAttempts: 1` could never survive a worker restart, which is precisely the
scenario Phase 2 exists to handle. Conversely, with no bound at all, a task that
reliably kills whatever worker picks it up would be requeued forever.

So there are two counters. A lease expiry grants back the attempt the lost claim
consumed and is charged to `maxLeaseExpiries` (default 3) instead:

```
attempt            every execution actually started      (never reset)
maxAttempts        activity failures allowed             (grows on reclaim/replay)
leaseExpiryCount   times the worker vanished             (bounded separately)
```

`attempt` therefore stays an honest lifetime count. A non-zero `leaseExpiryCount`
is how an operator tells "this work was redone because a worker died" apart from
"the activity kept failing" — something the error string cannot express.

### The database clock is the only clock

Every deadline — lease expiry, retry backoff, heartbeat staleness — is evaluated
against `now()` inside the same statement as the write it guards. Nothing compares
an application-server clock against a database timestamp.

The first implementation did inject a Go clock into the engine and reaper, which
was convenient for tests and wrong for production: the detection queries used the
database clock while the decisions used the injected one, so under clock skew a
replica running 30s fast would reap leases that were still live. Removing the
injection made that class of bug unrepresentable.

The consequence for tests is that time cannot be faked by swapping a clock. They
simulate elapsed time by backdating the timestamps the real queries read
(`internal/testsupport/failure.go`), which exercises the production path exactly.

### A worker reporting late still wins

The reaper's requeue and dead-letter writes carry `AND lease_expires_at < now()`.
If a worker submits a result in the instant between the reaper listing its task
and writing the reclaim, the guard rejects the write and the report stands.

Discarding completed work would be worse than a slightly late reclaim, and the
reaper's own transaction writes no history until after that guard passes — an event
saying "lease expired" next to a task that completed normally is worse than no
event at all.

### Lease renewal, not long leases

A worker renews its lease every third of the lease duration while an activity runs,
and the server floors the lease at the task's own timeout plus a grace period.

The alternative — giving every task a lease long enough for the slowest possible
activity — means a crashed worker's task stays stuck for that entire duration.
Renewal inverts it: a live worker keeps proving it is alive, so the detection
window is one heartbeat interval wide regardless of how long the work takes. The
server-side floor closes the other hole, where a worker asking for a 5s lease on a
120s task would have its own work reaped out from under it.

### Cancellation is a request, not an order

The control plane cannot reach into another process and stop it. What it can do is
set a flag, which the worker collects on its next lease renewal and uses to cancel
the activity's context — and invalidate the claim, so any result the worker
produces anyway is refused.

Reusing the renewal channel means cancellation costs no extra round trip: the
worker is already checking in.

### Dead-lettering with a way back

A task that exhausts its budget is parked in `DEAD_LETTER` rather than retried
forever or silently dropped. It stays in the `tasks` table — not a separate queue —
so its full attempt history sits alongside the failure, which is what an operator
needs in order to decide anything.

`POST /v1/tasks/{id}/replay` returns it to the queue and revives its execution.
That is the only path out of `WORKFLOW_FAILED`, and it requires an explicit
request: the engine never resumes a run it gave up on. Replay also restores the
sibling tasks the failure had canceled, or the replayed task would succeed into a
graph that could never finish.

### Other choices worth noting

- **Dependencies are vendored.** `vendor/` is committed, so builds need no module
  proxy. The container build runs with `--network=none`.
- **Liveness and readiness are separate.** `/healthz` never touches the database;
  restarting a pod because PostgreSQL is briefly unreachable only adds churn.
  `/readyz` does check, and fails fast.
- **Unknown JSON fields are rejected.** Silently ignoring a misspelled
  `maxAttemps` would let a client believe it configured a retry policy that was
  never applied.
- **An empty queue returns 204, not 404.** "Nothing to do" is a polling worker's
  normal steady state and should not pollute error metrics.
- **A failure is retryable unless a worker says otherwise.** Assuming permanence
  would silently skip the retry policy the workflow author configured. Workers may
  not claim `LEASE_EXPIRED` or `WORKER_DEAD` — those are conclusions only the
  control plane can draw, and a worker reporting them would be reporting on its
  own death.
- **Interfaces are declared where they are consumed.** `api.Service` lives in the
  API package, not the engine, and a compile-time assertion keeps the two in
  step.
- **The image is distroless and non-root**, ~20 MB, static binaries.

---

## Data model

```
workflow_definitions          immutable versioned DAG (spec as JSONB + hash)
  └─ workflow_executions      one run: state, input, output, idempotency key
       ├─ tasks               the DAG instance AND the queue
       └─ history_events      append-only audit trail
workers                       registration + liveness (heartbeats)
```

State machines are explicit, and every transition is validated before it is
written (`internal/domain/state.go`):

```
Workflow:  PENDING ──▶ RUNNING ──▶ COMPLETED
               │           ├─────▶ FAILED ──┐
               └───────────┴─────▶ CANCELED │
                           ◀─────────────────┘  (operator replay only)

Task:      PENDING ──▶ SCHEDULED ──▶ RUNNING ──▶ COMPLETED
              ▲             ▲           │ ├───▶ FAILED
              │             │           │ │        │
              │             └───────────┘ │        │  retry (attempts remain)
              │              requeue      │        ▼
              │             (lease lapsed)│    DEAD_LETTER
              │                           │        │
              │                           └────────┤  budget exhausted
              │                                    │
              │        ┌───────────────────────────┘  operator replay
              │        ▼
              └─── CANCELED                        (replay restores siblings)
```

Task state changes are compare-and-swap: the expected current state is part of
the `WHERE` clause, so a replica that lost a race learns it lost instead of
clobbering the winner's write.

`FAILED` is deliberately not terminal — the retry pass revives it. `DEAD_LETTER`
*is* terminal to the engine, which is what stops a hopeless task from retrying
forever; only an explicit replay moves it. The two backwards edges
(`WORKFLOW_FAILED → RUNNING` and `TASK_CANCELED → PENDING`) exist solely for
operator replay and are unreachable from engine code.

---

## API

```
GET    /healthz                              liveness (no DB dependency)
GET    /readyz                               readiness (checks DB)

POST   /v1/workflows                         register a version (idempotent)
GET    /v1/workflows                         list
GET    /v1/workflows/{name}                  latest version
GET    /v1/workflows/{name}/versions/{v}     exact version

POST   /v1/executions                        start (Idempotency-Key supported)
GET    /v1/executions                        list (workflowName, state, paging)
GET    /v1/executions/{id}                   execution + task graph
GET    /v1/executions/{id}/history           audit trail (afterId cursor)
POST   /v1/executions/{id}/cancel            cancel

POST   /v1/workers                           register (upsert by name)
GET    /v1/workers                           list
POST   /v1/workers/{id}/heartbeat            refresh liveness

POST   /v1/tasks/poll                        claim next task (204 when idle)
POST   /v1/tasks/{id}/complete               report success (claim token required)
POST   /v1/tasks/{id}/fail                   report failure (claim token required)
POST   /v1/tasks/{id}/heartbeat              renew lease; relays cancelRequested

GET    /v1/dead-letter                       parked tasks (workflowName, activity)
POST   /v1/tasks/{id}/replay                 requeue a parked task, revive its run
```

Errors use one envelope, with a stable machine-readable code:

```json
{ "error": { "code": "stale_claim", "message": "…", "requestId": "…" } }
```

### Example

```bash
API=http://127.0.0.1:8088

curl -X POST $API/v1/workflows -H 'Content-Type: application/json' -d '{
  "name": "order_pipeline",
  "version": 1,
  "tasks": [
    { "name": "task_a", "activity": "charge_payment" },
    { "name": "task_b", "activity": "reserve_inventory", "dependsOn": ["task_a"] },
    { "name": "task_c", "activity": "send_receipt",      "dependsOn": ["task_b"] }
  ]
}'

curl -X POST $API/v1/executions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001' \
  -d '{"workflowName":"order_pipeline",
       "input":{"orderId":"1001","customer":"buyer@example.com",
                "amount":129.99,"sku":"widget-blue","quantity":3}}'

curl -s $API/v1/executions/$EXEC_ID | jq .
curl -s $API/v1/executions/$EXEC_ID/history | jq -r '.events[].eventType'

# Triage and remediate failures
curl -s $API/v1/dead-letter | jq '.items[] | {task: .task.name, failureReason}'
curl -X POST $API/v1/tasks/$TASK_ID/replay -d '{"extraAttempts":1}'
```

### Configuring reliability

Retry behaviour is per task, and every field is optional:

```json
{
  "name": "task_a",
  "activity": "charge_payment",
  "maxAttempts": 5,
  "timeoutSeconds": 30,
  "maxLeaseExpiries": 3,
  "retryPolicy": {
    "initialIntervalMs": 1000,
    "backoffCoefficient": 2,
    "maxIntervalMs": 60000,
    "jitterPercent": 20
  }
}
```

Omitting `retryPolicy` entirely means the defaults (1s, doubling, capped at 60s,
20% jitter). It also leaves the workflow's spec hash unchanged, which is why
adding retries in Phase 2 did not invalidate a single workflow registered in
Phase 1 — a guarantee pinned by a golden-hash test.

Jitter is additive only, so a retry never fires *earlier* than the backoff curve
intends. `jitterPercent: 0` makes the schedule exactly reproducible.

---

## Writing a workflow

A workflow is a DAG; a task names an *activity*, which is a Go function
registered on a worker.

```go
registry := worker.NewRegistry()

registry.Register("charge_payment", func(ctx context.Context, in worker.ActivityInput) (any, error) {
    var order Order
    if err := in.UnmarshalWorkflowInput(&order); err != nil {
        return nil, err
    }
    // Derive the id from the input rather than randomly, so a re-run of this
    // task does not produce a second distinct charge.
    return PaymentResult{ChargeID: "chg_" + hash(order.OrderID)}, nil
})

registry.Register("reserve_inventory", func(ctx context.Context, in worker.ActivityInput) (any, error) {
    var payment PaymentResult
    if err := in.UnmarshalUpstream("task_a", &payment); err != nil {
        return nil, err
    }
    …
})
```

Chronos leases a task to one worker at a time, but a worker that crashes after
performing a side effect and before reporting will have its task re-executed. So
**at-least-once is the honest contract for an activity body**, and activities
should be idempotent. `internal/worker/activities` shows the pattern: results are
derived deterministically from input.

Validation rejects malformed graphs up front — unknown dependencies, duplicate
names, self-dependencies, and cycles (all problems reported at once, so a client
does not discover them one restart at a time).

---

## Configuration

Everything is environment-driven and validated at startup, so a misconfigured
process fails immediately rather than at first use.

**Server**

| Variable | Default | Notes |
|---|---|---|
| `CHRONOS_DATABASE_URL` | `postgres://chronos:chronos@127.0.0.1:55432/chronos?sslmode=disable` | |
| `CHRONOS_HTTP_ADDR` | `:8088` | |
| `CHRONOS_ENGINE_ENABLED` | `true` | `false` gives an API-only replica |
| `CHRONOS_ENGINE_POLL_INTERVAL` | `250ms` | sweep cadence |
| `CHRONOS_ENGINE_BATCH_SIZE` | `200` | executions per sweep |
| `CHRONOS_REAPER_ENABLED` | `true` | `false` disables failure detection |
| `CHRONOS_REAPER_INTERVAL` | `1s` | must be < `CHRONOS_WORKER_TIMEOUT` |
| `CHRONOS_REAPER_BATCH_SIZE` | `100` | tasks or workers reclaimed per pass |
| `CHRONOS_WORKER_TIMEOUT` | `45s` | heartbeat lapse before a worker is DEAD |
| `CHRONOS_MIGRATE_ON_START` | `true` | prefer a migration Job in a cluster |
| `CHRONOS_DB_MAX_CONNS` / `_MIN_CONNS` | `20` / `2` | |
| `CHRONOS_LOG_LEVEL` / `_FORMAT` | `info` / `text` | `json` for production |

**Worker**

| Variable | Default | Notes |
|---|---|---|
| `CHRONOS_SERVER_URL` | `http://127.0.0.1:8088` | |
| `CHRONOS_WORKER_NAME` | `worker-<hostname>` | stable across restarts by design |
| `CHRONOS_TASK_QUEUE` | `default` | |
| `CHRONOS_WORKER_CONCURRENCY` | `4` | independent claimants |
| `CHRONOS_WORKER_LEASE_DURATION` | `30s` | server floors it at the task timeout |
| `CHRONOS_WORKER_HEARTBEAT_INTERVAL` | `10s` | must be < lease duration |
| `CHRONOS_WORKER_TASK_TIMEOUT` | `5m` | per-attempt bound when a task declares none |

Ports are non-default (`55432`, `8088`) and bound to loopback so Chronos does not
collide with a system PostgreSQL or another local stack.

---

## Testing

318 tests, 77.2% statement coverage across `internal/...`.

```bash
make test              # unit only — no database, no containers
make test-integration  # adds database-backed tests
make cover             # HTML coverage report
```

| Package | Coverage | What is tested |
|---|---|---|
| `internal/domain` | 97.0% | DAG validation, cycle detection, state machines, spec hashing, backoff curve |
| `internal/api` | 84.0% | routing, decoding, error mapping, idempotency, token disclosure, DLQ, heartbeat |
| `internal/store` | 75.9% | queue semantics, idempotency, locking, lease reclaim, worker liveness |
| `internal/engine` | 60.2% | end-to-end execution, recovery, all failure scenarios, concurrency |
| `internal/config` | 96.9% | env parsing and cross-field validation |
| `internal/client` | 65.6% | retry classification, backoff, sentinel errors |
| `internal/worker` | 31.8% | registry, activity input decoding |

Integration tests run against real PostgreSQL, not a fake, because the
correctness argument rests on `SKIP LOCKED`, partial unique indexes, and row-level
locking. They skip themselves when no database is configured, so `go test ./...`
still passes without one. Each test *package* gets its own PostgreSQL schema,
since `go test ./...` runs packages in parallel and shared tables would make them
interfere.

The tests that carry the most weight:

Durability and concurrency:

- `TestExecutionRecoversFromPersistedStateAfterEngineRestart` — a discarded engine
  is replaced and the workflow finishes, with completed work preserved
- `TestConcurrentWorkersNeverShareATask` — 20 workers, one workflow, no task
  claimed twice
- `TestConcurrentEnginesDoNotDoubleSchedule` — 4 replicas sweeping at once, each
  execution started exactly once
- `TestCreateExecutionDeduplicatesUnderConcurrency` — 8 racing identical starts
  converge on one execution

Failure scenarios (`internal/engine/failure_integration_test.go`):

- `TestWorkerCrashLeaseExpiresAnotherWorkerContinues` — the brief's exact sequence,
  including the crashed worker later being refused
- `TestLeaseExpiryDoesNotConsumeTheActivityAttemptBudget` — a `maxAttempts: 1` task
  survives losing its worker
- `TestRepeatedWorkerLossEventuallyDeadLetters` — but not forever
- `TestRetryUsesExponentialBackoff` — 1s, 2s, 4s, and not claimable early
- `TestOnlyOneWorkerCanReportAfterReassignment` — three generations of worker, one
  accepted result
- `TestDuplicateReportsUnderConcurrencyAreIdempotent` — 8 concurrent replays, one
  history entry
- `TestWorkerReportBeatsTheReaper` — a just-in-time report is not discarded
- `TestConcurrentReapersReclaimEachTaskOnce` — the reaper is safe on every replica
- `TestDeadWorkerDetectionReclaimsItsTasks` — one lapsed heartbeat reclaims
  everything that worker held
- `TestReplayResumesAFailedWorkflow` — resumes rather than restarts
- `TestPendingRetrySurvivesEngineRestart` — a backoff delay is a timestamp, not a
  timer
- `TestAddingRetrySupportDidNotChangeSpecHashes` — Phase 1 workflows still
  re-register cleanly

---

## Project layout

```
cmd/chronos-server/        API + engine
cmd/chronos-worker/        task executor
internal/domain/           entities, state machines, DAG validation (no I/O)
internal/store/            PostgreSQL: repositories, queue, migrations
internal/engine/           scheduling loop, failure reaper, control-plane service
internal/api/              HTTP transport
internal/client/           Go SDK
internal/worker/           poll loop, activity registry, example activities
internal/config/           env configuration
internal/testsupport/      integration test harness + failure injection helpers
scripts/                   demo.sh, recovery-demo.sh, failure-demo.sh
```

---

## Requirements

### Phase 1

| Requirement | Where |
|---|---|
| Workflow definition and registration | `domain/workflow.go`, `store/definitions.go` |
| Workflow execution API | `api/handlers.go`, `engine/service.go` |
| Workflow/task state machine | `domain/state.go` + DB check constraints |
| Task queues | `store/tasks.go` (`SKIP LOCKED`) |
| Worker registration and polling | `store/workers.go`, `worker/worker.go` |
| PostgreSQL persistence | `store/migrations/0001_init.sql` |
| Workflow execution history | `store/history.go` |
| REST API | `internal/api` |
| Idempotency | spec hash, partial unique index, claim tokens |
| Unit + integration tests | 215 tests, 76.8% coverage |
| Recoverable from persisted state | `make recovery-demo` |

`REST/gRPC` is delivered as REST. gRPC would add code generation and a second
transport for the same `engine.Service` interface without exercising anything new;
the service boundary is already transport-agnostic, so it remains a drop-in
addition.

### Phase 2

| Requirement | Where |
|---|---|
| Task retries with exponential backoff | `domain/retry.go`, `engine.processFailedTasks` |
| Task timeouts | worker-side per-attempt bound + server-side lease floor |
| Dead-letter handling | `DEAD_LETTER` state, `GET /v1/dead-letter`, replay |
| Worker heartbeats | `POST /v1/workers/{id}/heartbeat`, worker heartbeat loop |
| Worker failure detection | `store.MarkStaleWorkersDead`, `Reaper.detectDeadWorkers` |
| Task lease / visibility timeout | `RenewLease`, `Reaper.reapExpiredLeases` |
| Duplicate task execution protection | claim tokens + guarded compare-and-swap writes |
| Workflow cancellation | control-plane cancel + cooperative signal via heartbeat |
| Concurrent task execution | `SKIP LOCKED` claims, per-worker concurrency slots |
| Scheduler recovery | stateless engine and reaper; delays persisted as timestamps |
| Automated failure tests | `failure_integration_test.go`, `make failure-demo` |

### Known boundaries

Stated plainly, because they are deliberate scope decisions rather than oversights:

- **At-least-once, not exactly-once.** A worker that performs a side effect and
  then dies before reporting will have its task re-executed. Chronos guarantees a
  task is leased to one worker at a time and that at most one result is *accepted*;
  it cannot make an arbitrary side effect atomic with the report. Activities must
  be idempotent, and `internal/worker/activities` shows the pattern.
- **Cancellation is cooperative.** A worker learns about it on its next lease
  renewal, so an activity that ignores its context will run to completion. Its
  result is refused either way.
- **Dead-letter replay is manual.** There is no automatic re-drive; that is the
  point of parking a task, but it does mean the queue needs watching (`make dlq`).
- **Retry policy is per task, not per error class.** A worker can mark a single
  failure non-retryable, but a workflow cannot declare "retry timeouts, not
  validation errors" declaratively.
- **Single queue backend.** PostgreSQL only, by the reasoning above.
- **No metrics or tracing yet.** Failure detection is observable through history
  events and structured logs; Prometheus and OpenTelemetry are Phase 4.

---

## What's next

- **Phase 3** — Terraform-provisioned AWS, EKS deployment, HPA, readiness and
  liveness probes, resource limits, rolling deploys, secrets, IAM least privilege,
  CI/CD through GitHub Actions
- **Phase 4** — OpenTelemetry tracing, Prometheus metrics (queue depth, worker
  utilization, retry and failure rates), Grafana dashboards, alerting, and load
  testing with documented throughput, p50/p95/p99 latency, and measured recovery
  time after worker failure
