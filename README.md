# Chronos

A workflow orchestration engine in the spirit of Temporal. You register a DAG of
tasks, start an execution, and a pool of workers runs the tasks in dependency order.
All state lives in PostgreSQL, so an execution outlives the process that started it.

```
Workflow "order_pipeline"
        │
   Task A ──▶ Task B ──▶ Task C
 charge     reserve      send
 payment    inventory    receipt
```

Written in Go. Roughly 366 tests, a Terraform/EKS deployment, and Prometheus and
OpenTelemetry instrumentation. Design notes are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), load test results in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

Two things to know before you read further. The AWS infrastructure has been
validated but never applied — I had no account to run it against, so `terraform
apply` has never executed. And the performance numbers come from a single laptop, so
treat the relative findings as the useful part and the absolute throughput as
indicative.

## Quick start

Needs Go 1.26+, podman, `jq`, `curl`.

```bash
make db-up   # PostgreSQL in podman, loopback :55432
make demo    # register the workflow above, run it, read the result back from the DB
```

`make demo` brings up the control plane and two workers, runs the pipeline, then
queries PostgreSQL directly to show nothing was held in memory.

The failure behaviour is the interesting part, and there are scripts for it:

```bash
make recovery-demo  # SIGKILL the engine mid-workflow; a fresh process finishes it
make failure-demo   # SIGKILL a worker mid-task; retries, dead-lettering, replay
make trace-demo     # one workflow, one trace, spanning server and worker
make loadtest       # throughput, latency, scaling, recovery timing
```

Everything else:

```bash
make help
make test                 # unit tests, no database
make test-integration     # everything, against the podman PostgreSQL
make stack-up             # postgres + server + 2 workers, containerized
make psql                 # poke at the database
make dlq                  # dead letter queue
make queue                # live queue depth and pool saturation
make deploy-validate      # check Terraform + Kubernetes config, no AWS needed
make k8s-render ENV=prod  # what would actually be applied
```

## How it works

```
                        ┌──────────────────────────────┐
   client ──── REST ───▶│        chronos-server        │
                        │                              │
                        │  ┌────────┐   ┌───────────┐  │
                        │  │  API   │──▶│  Service  │  │
                        │  └────────┘   └─────┬─────┘  │
                        │                     │        │
                        │  ┌───────────┐      │        │  reacts to what
                        │  │  Engine   │◀ nudge        │  workers reported
                        │  │ (sweeper) │      │        │
                        │  └─────┬─────┘      │        │
                        │  ┌───────────┐      │        │  reacts to the
                        │  │  Reaper   │      │        │  absence of reports
                        │  │ (detector)│      │        │
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

- `internal/api` — HTTP/JSON. Parsing, validation, mapping domain errors to status
  codes. No business logic.
- `internal/engine` — `Service` handles request-driven operations, `Engine` runs the
  sweep loop that advances executions.
- `internal/engine/reaper.go` — finds the failures nobody reports: lapsed leases,
  workers that stopped heartbeating.
- `internal/store` — all the SQL, including the queue.

Engine and reaper are separate because their triggers are opposites. The engine acts
on state a worker reported; the reaper acts on the absence of a report. The reaper
also runs on a slower cadence, being a timeout detector.

Workers are separate processes that only ever talk to the API. No workflow state, no
database credentials, which is what makes them disposable.

### Lifecycle

```
POST /v1/executions          execution row committed as PENDING, request returns
        │
engine sweep                 materialize tasks, PENDING → RUNNING
        │
engine sweep                 tasks whose deps are COMPLETED → SCHEDULED
        │                    (input resolved and persisted here)
        │
POST /v1/tasks/poll          worker claims one task: RUNNING + lease + claim token
        │
worker runs the activity
        │
POST /v1/tasks/{id}/heartbeat  renew lease; cancellation relayed back
        │
POST /v1/tasks/{id}/complete   task COMPLETED, engine nudged
        │
engine sweep                 next tasks become ready … until all COMPLETED
        │
                             execution COMPLETED, output = terminal task outputs
```

The sweep is one idempotent function: read the execution and its tasks, work out
what's now possible, write it. Nothing carries over between passes.

When it goes wrong:

```
task FAILED, attempts remain     → requeued at now + backoff(attempt)
task FAILED, attempts exhausted  → DEAD_LETTER, execution FAILED
task FAILED, declared permanent  → DEAD_LETTER now, attempt budget untouched
lease lapsed, budget remains     → reaper requeues, charged to the lease budget
lease lapsed, budget exhausted   → DEAD_LETTER
worker heartbeat lapsed          → worker DEAD, all its tasks reclaimed together
operator replays a parked task   → task SCHEDULED, execution FAILED → RUNNING
```

## Design notes

The full set is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The ones that
actually explain how this thing behaves:

### PostgreSQL is the queue

The `tasks` table *is* the queue. A claim is one statement:

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

`SKIP LOCKED` is what makes it a queue rather than a table people fight over.
Concurrent claimants lock different rows, so N workers polling at once get N
different tasks.

Kafka or SQS would be the conventional choice. I didn't use one because scheduling a
task is a state transition on a row the engine is already writing, so a table-backed
queue commits the enqueue in the same transaction as the decision that caused it. An
external broker splits that into two writes and needs an outbox to stay consistent.

The bill comes due on throughput: an empty poll still costs a full transaction, so
idle workers eat capacity the scheduler needs. That turned out to be the measured
ceiling — see [what the load test found](#what-the-load-test-found). Queue operations
live in `internal/store/tasks.go` behind interfaces declared by their consumers, so
swapping in SQS later is contained.

### Nothing is held in memory

The engine caches nothing about what a workflow is doing. Every pass re-derives it
from rows, which is why `make recovery-demo` works: a `SIGKILL`ed engine is replaced
by a process that has never seen the execution and reaches the same conclusion. It
also makes replicas free, since there's no state to synchronize.

The engine sweeps on a 250ms timer, and the service also calls `Nudge()` after a
durable write to sweep immediately. Nudges are best-effort — dropping one costs
latency, not correctness, because the next sweep finds the work anyway. Making the
signal load-bearing would mean having to make signal delivery reliable, which is a
much harder problem than a timer.

### Idempotency comes from constraints, not application checks

Three operations have to tolerate replay, and each is guarded in the database,
because "read then decide" isn't safe across replicas:

| Operation | Mechanism | Effect |
|---|---|---|
| Register a workflow | `UNIQUE (name, version)` + canonical spec hash | identical spec is a no-op, changed spec is a 409 |
| Start an execution | partial `UNIQUE (workflow_name, idempotency_key)` | replay resolves to the same execution |
| Report a task result | `claim_token` in the `WHERE` clause | replay is a no-op, superseded worker is rejected |

The claim token earns its keep. Each claim mints a fresh one and only hands it to the
worker that won the lease, so a slow worker whose task got reassigned can't overwrite
the new attempt — it gets `ErrStaleClaim` and drops the work. A duplicate report of
the *same* attempt is recognized as a replay and returns the persisted task, which is
what makes client retries safe after a lost response.

### Two attempt budgets

The decision the whole failure story rests on, and it isn't obvious.

`maxAttempts` is the *activity's* budget: how many times you're willing for your code
to run. A worker getting SIGKILLed isn't the activity failing, it's the
infrastructure failing. Charging a crash to that budget would mean a task with
`maxAttempts: 1` could never survive a worker restart. But with no bound at all, a
task that reliably kills whatever picks it up gets requeued forever.

So there are two counters. A lease expiry gives back the attempt the lost claim
consumed and charges `maxLeaseExpiries` (default 3) instead:

```
attempt            executions actually started       (never reset)
maxAttempts        activity failures allowed         (grows on reclaim/replay)
leaseExpiryCount   times the worker vanished         (bounded separately)
```

`attempt` stays an honest lifetime count, and a non-zero `leaseExpiryCount` is how
you tell "this was redone because a worker died" from "the activity kept failing" —
which the error string can't express.

### The database clock is the only clock

Every deadline is evaluated against `now()` in the same statement as the write it
guards. Nothing compares an app-server clock to a database timestamp.

The first version injected a Go clock into the engine and reaper. Convenient for
tests, wrong in production: detection queries used the database clock while decisions
used the injected one, so a replica running 30s fast would reap leases that were
still live. Removing the injection made that unrepresentable. Tests now backdate the
timestamps the real queries read (`internal/testsupport/failure.go`) instead of
swapping a clock.

### A wrong turn worth recording

History originally carried a dense per-execution sequence number, so every append
read `MAX(sequence)` first. That read-modify-write made concurrent writers serialize
on the execution row, which meant claiming two tasks of the *same* workflow became
mutually blocking. Making the retry optimistic just converted it into a thundering
herd and then deadlocks under load.

The fix was to drop the dense counter and order history by its `BIGSERIAL` id, which
is monotonic per execution without being contiguous. Appends became plain inserts,
the claim path got simpler and genuinely parallel, and ordering was all the audit
trail ever needed.

### Smaller things

- Reaper writes carry `AND lease_expires_at < now()`, so a worker reporting in the
  instant between the reaper listing its task and reclaiming it still wins. Throwing
  away completed work is worse than a slightly late reclaim.
- A worker renews its lease every third of the duration rather than taking a lease
  long enough for the slowest possible activity — otherwise a crashed worker's task
  stays stuck for that whole duration.
- Cancellation is a flag the worker collects on its next renewal, so it costs no
  extra round trip. It also invalidates the claim, so a result produced anyway is
  refused.
- A dead-lettered task stays in the `tasks` table with its full attempt history, not
  in a separate queue. Replay is the only route out of a failed workflow, and it
  restores the sibling tasks the failure canceled.
- `/healthz` never touches the database; restarting a pod because PostgreSQL blinked
  only adds churn. `/readyz` does check.
- Unknown JSON fields are rejected, so a misspelled `maxAttemps` doesn't let a client
  believe it configured a retry policy that never applied.
- An empty queue returns 204, not 404. It's a polling worker's normal state.
- Dependencies are vendored and `vendor/` is committed, so builds need no module
  proxy and the container build runs with `--network=none`.

## Data model

```
workflow_definitions          immutable versioned DAG (spec as JSONB + hash)
  └─ workflow_executions      one run: state, input, output, idempotency key
       ├─ tasks               the DAG instance AND the queue
       └─ history_events      append-only audit trail
workers                       registration + liveness
```

State machines are explicit and every transition is validated before it's written
(`internal/domain/state.go`):

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

Task state changes are compare-and-swap: the expected current state is in the `WHERE`
clause, so a replica that loses a race finds out instead of clobbering the winner.

`FAILED` isn't terminal, since the retry pass revives it. `DEAD_LETTER` is terminal
to the engine, which is what stops a hopeless task retrying forever. The two
backwards edges exist only for operator replay and are unreachable from engine code.

## API

```
GET    /healthz                              liveness (no DB dependency)
GET    /readyz                               readiness (checks DB)
GET    /metrics                              Prometheus

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
POST   /v1/tasks/{id}/heartbeat              renew lease, relays cancelRequested

GET    /v1/dead-letter                       parked tasks
POST   /v1/tasks/{id}/replay                 requeue a parked task, revive its run
```

One error envelope, with a stable machine-readable code:

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

curl -s $API/v1/dead-letter | jq '.items[] | {task: .task.name, failureReason}'
curl -X POST $API/v1/tasks/$TASK_ID/replay -d '{"extraAttempts":1}'
```

### Retry configuration

Per task, every field optional:

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

Omitting `retryPolicy` gives the defaults (1s, doubling, capped at 60s, 20% jitter)
and leaves the spec hash unchanged, so adding retry support to this project didn't
invalidate a single already-registered workflow. There's a golden-hash test pinning
that.

Jitter is additive only, so a retry never fires *earlier* than the curve intends.
`jitterPercent: 0` makes the schedule exactly reproducible.

## Writing a workflow

A task names an *activity*, which is a Go function registered on a worker.

```go
registry := worker.NewRegistry()

registry.Register("charge_payment", func(ctx context.Context, in worker.ActivityInput) (any, error) {
    var order Order
    if err := in.UnmarshalWorkflowInput(&order); err != nil {
        return nil, err
    }
    // Derive the id from the input rather than randomly, so a re-run doesn't
    // produce a second distinct charge.
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

Chronos leases a task to one worker at a time, but a worker that performs a side
effect and dies before reporting will have its task re-run. At-least-once is the
honest contract for an activity body, so activities should be idempotent.
`internal/worker/activities` shows the pattern: results derived deterministically
from input.

Malformed graphs are rejected up front — unknown dependencies, duplicate names,
self-dependencies, cycles. All problems are reported at once rather than one restart
at a time.

## Observability

`/metrics` on all three processes, around 32 series. Labels are workflow, activity,
state and task queue, all bounded by the number of registered workflows. No execution
IDs or task IDs in labels.

Tracing is OpenTelemetry. The interesting part is that a workflow's spans come from
different processes minutes apart, so there's no live call chain for a `traceparent`
header to travel along. The W3C context is persisted on the execution row and handed
back on the poll response, which is what makes one workflow one trace. `make
trace-demo` shows the result.

Spans are exported to the structured log stream rather than over OTLP. A real
exporter drags in grpc, protobuf and genproto, and this project vendors its
dependencies so it builds offline. The log exporter implements
`sdktrace.SpanExporter`, so span IDs, propagation and parent links are all real, and
a collector's filelog receiver can reconstruct OTLP. Swapping in `otlptracegrpc` is a
change to one function.

Dashboard in `deploy/observability/`, alert rules and scrape config in
`deploy/k8s/base/monitoring.yaml`. Alerts only fire on things the system can't fix
itself, so retries, lease expiries and stale claims aren't alerted on.

### What the load test found

`make loadtest` produces the numbers in [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
The short version, from 600 executions × 3 tasks per run on one machine:

| | |
|---|---|
| Peak throughput | 66 workflows/s, 199 tasks/s |
| Submit latency | 12–20ms p50, under 50ms p99, flat even under a 16s backlog |
| Recovery, lease expiry | 6.19s (5s lease + 1s reaper interval) |
| Recovery, worker death | 45.40s (`CHRONOS_WORKER_TIMEOUT`) |

Throughput is flat from 1 to 4 workers, and the reason is the useful bit. The queue
never exceeded depth 44, the oldest task never waited past 0.21s, and at most 8 of 16
slots were ever busy. Workers were idle waiting for work to exist.

The claim timing explains it. Of 30,354 claim transactions only 5,430 returned a
task; the other 82% found an empty queue and still paid for a full PostgreSQL
transaction, burning 77.6s of database time to say "no". Adding workers adds pollers
competing with the scheduler that creates the work they're waiting for, which is also
why polling 12× more often made throughput *worse*.

`LISTEN`/`NOTIFY` is the fix. `Engine.Nudge()` is already the notification point, so
it's confined to the poll path, and long-polling the existing endpoint would capture
most of the benefit without changing the client contract. I haven't done it —
it changes the queue design rather than the instrumentation, and finding it was the
point of measuring.

End-to-end latency for a chain is mostly scheduling delay, not work. Each hop waits
on both the engine sweep and a worker poll, so a 3-task chain has a ~750ms floor
before any activity runs. Chains cost latency even when the tasks are instant; fan
out instead if you care.

## Configuration

Environment-driven, validated at startup so a misconfigured process fails
immediately rather than at first use.

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
| `CHRONOS_OBSERVER_INTERVAL` | `5s` | how often DB-derived gauges refresh |
| `CHRONOS_DB_MAX_CONNS` / `_MIN_CONNS` | `20` / `2` | |
| `CHRONOS_LOG_LEVEL` / `_FORMAT` | `info` / `text` | `json` for production |

**Worker**

| Variable | Default | Notes |
|---|---|---|
| `CHRONOS_SERVER_URL` | `http://127.0.0.1:8088` | |
| `CHRONOS_WORKER_NAME` | `worker-<hostname>` | stable across restarts on purpose |
| `CHRONOS_TASK_QUEUE` | `default` | |
| `CHRONOS_WORKER_CONCURRENCY` | `4` | independent claimants |
| `CHRONOS_WORKER_LEASE_DURATION` | `30s` | server floors it at the task timeout |
| `CHRONOS_WORKER_HEARTBEAT_INTERVAL` | `10s` | must be < lease duration |
| `CHRONOS_WORKER_TASK_TIMEOUT` | `5m` | per-attempt bound when a task declares none |
| `CHRONOS_WORKER_HEALTH_ADDR` | `:8090` | probes and `/metrics` share this listener |

**Tracing and metrics** (both binaries)

| Variable | Default | Notes |
|---|---|---|
| `CHRONOS_METRICS_ENABLED` | `true` | |
| `CHRONOS_TRACING_ENABLED` | `false` | at ratio 1.0 it emits a line per span |
| `CHRONOS_TRACE_SAMPLE_RATIO` | `1.0` | head sampling; the knob for span volume |
| `CHRONOS_TRACE_EXPORTER` | `log` | `log` or `none` |
| `CHRONOS_SERVICE_NAME` | per binary | separates API, scheduler and worker spans |
| `CHRONOS_ENVIRONMENT` | `local` | tags spans so dev and prod stay separable |

Two behaviours that surprised me and might surprise you:

The span stream isn't gated by `CHRONOS_LOG_LEVEL`. It has its own logger and is
always JSON, since it's machine-consumed. Enabling tracing is already a request for
the data. (Spans used to go through the app logger at `Debug`, so a process at the
default `info` level exported every one of them into a discard and tracing appeared
to do nothing.)

The effective lease is `GREATEST(requested, task_timeout + 15s)`. Raising
`CHRONOS_WORKER_LEASE_DURATION` therefore does *not* shorten recovery for a
long-running task, because its lease is already longer than that.
`CHRONOS_WORKER_TIMEOUT` is the bound that matters there.

Ports are non-default (`55432`, `8088`) and bound to loopback, so Chronos doesn't
collide with a system PostgreSQL or another local stack.

## Testing

366 tests, 81.2% statement coverage.

```bash
make test              # unit only, no database or containers
make test-integration  # adds the database-backed tests
make cover             # HTML coverage report
```

Integration tests run against real PostgreSQL rather than a fake, because the
correctness argument rests on `SKIP LOCKED`, partial unique indexes and row-level
locking — none of which a fake exercises. They skip themselves when no database is
configured, so `go test ./...` still passes without one. Each test *package* gets its
own schema, since `go test ./...` runs packages in parallel and shared tables would
make them interfere.

The ones carrying the most weight:

- `TestExecutionRecoversFromPersistedStateAfterEngineRestart` — a discarded engine is
  replaced and the workflow finishes, completed work preserved
- `TestConcurrentWorkersNeverShareATask` — 20 workers, one workflow, no double claim
- `TestConcurrentEnginesDoNotDoubleSchedule` — 4 replicas sweeping at once
- `TestWorkerCrashLeaseExpiresAnotherWorkerContinues` — including the crashed worker
  later being refused
- `TestLeaseExpiryDoesNotConsumeTheActivityAttemptBudget` — a `maxAttempts: 1` task
  survives losing its worker
- `TestOnlyOneWorkerCanReportAfterReassignment` — three generations of worker, one
  accepted result
- `TestWorkerReportBeatsTheReaper` — a just-in-time report isn't discarded
- `TestConcurrentReapersReclaimEachTaskOnce` — the reaper is safe on every replica
- `TestRetryUsesExponentialBackoff` — 1s, 2s, 4s, and not claimable early
- `TestPendingRetrySurvivesEngineRestart` — a backoff delay is a timestamp, not a
  timer

## Layout

```
cmd/chronos-server/        API + engine + reaper + observer (and -migrate mode)
cmd/chronos-worker/        task executor
cmd/chronos-loadtest/      load generator
internal/domain/           entities, state machines, DAG validation (no I/O)
internal/store/            PostgreSQL: repositories, queue, migrations
internal/engine/           sweep loop, reaper, gauge observer, service
internal/api/              HTTP transport
internal/client/           Go SDK
internal/worker/           poll loop, activity registry, example activities
internal/config/           env configuration
internal/telemetry/        metrics, tracing, HTTP instrumentation (leaf package)
internal/testsupport/      integration harness + failure injection
docs/                      ARCHITECTURE.md, PERFORMANCE.md
scripts/                   demo, recovery-demo, failure-demo, trace-demo,
                           loadtest, validate-deploy
deploy/terraform/          VPC, EKS, RDS, IAM/IRSA, ECR, optional Redis + SQS
deploy/k8s/                Kustomize base + dev/prod overlays, monitoring
deploy/observability/      Grafana dashboard
.github/workflows/         ci.yml, cd.yml, terraform.yml
```

## Deployment

Terraform provisions AWS, Kustomize deploys onto EKS, GitHub Actions runs both.
[deploy/README.md](deploy/README.md) has the detail, including the cluster add-ons
Terraform doesn't manage. A few choices that aren't obvious from the manifests:

The API, scheduler and workers are the same image with different flags — the API runs
with engine and reaper off, the scheduler runs with them on and nothing pointing at
it. They scale independently because API load follows request volume while scheduling
load follows the number of live executions.

Two scheduler replicas, no leader election. The engine locks each execution with
`SKIP LOCKED` and the reaper's writes are guarded compare-and-swaps, so replicas
don't conflict, and one replica would mean a single node failure stops scheduling
everywhere.

Only workers autoscale, because only workers are safe to kill: losing one means its
lease lapses and the reaper reassigns the task. The HPA is asymmetric for the same
reason — scale-up is aggressive since a spare worker costs one idle poll, scale-down
waits ten minutes because a worker removed mid-activity has its task redone.

Migrations run as a Job rather than on startup. `store.Migrate` is safe to run
concurrently, so every replica migrating on boot would work, but a bad migration
would crash-loop the fleet instead of failing the deploy at a named step.

There is no Kubernetes Secret. The DSN is mounted from Secrets Manager by the CSI
driver using each pod's own IRSA identity and read via `CHRONOS_DATABASE_URL_FILE`,
so there's nothing to `kubectl get secret` and the value never enters an environment
variable. The worker IAM role grants nothing at all, deliberately — workers need no
AWS access, so a compromised worker can't read what the API pods can.

## Limitations

- **At-least-once, not exactly-once.** A worker that performs a side effect then dies
  before reporting will have its task re-run. Chronos guarantees a task is leased to
  one worker at a time and that at most one result is *accepted*; it can't make an
  arbitrary side effect atomic with the report.
- **Cancellation is cooperative.** A worker finds out on its next lease renewal, so
  an activity that ignores its context runs to completion. Its result is refused
  either way.
- **Dead-letter replay is manual.** No automatic re-drive — that's rather the point
  of parking a task, but it does mean watching the queue.
- **Retry policy is per task, not per error class.** A worker can mark one failure
  non-retryable, but a workflow can't declare "retry timeouts, not validation
  errors".
- **PostgreSQL is the only queue backend**, for the reasons above, and polling is the
  measured throughput ceiling.
- **Performance numbers are from one laptop**, where PostgreSQL, the control plane
  and the workers share 11 cores. The relative findings are the useful part.
- **The worker HPA scales on CPU**, which is a proxy for queue depth and a poor one
  for I/O-bound activities: a worker blocked on a slow downstream uses no CPU while
  its queue grows. `chronos_queue_depth` now exists to replace it, but wiring it up
  needs prometheus-adapter or KEDA in the cluster.
- **No cluster autoscaler.** The node group has min/max bounds but nothing moves
  `desired_size`, so the HPA can only scale within existing node capacity.
- **The infrastructure has never been applied.** Terraform validates and the
  manifests render, but no AWS account was used. `terraform validate` checks syntax
  and provider schemas; it won't catch an insufficient IAM policy, an IRSA trust
  policy that doesn't match, or an add-on version conflict. Treat the first apply as
  an exercise expected to surface problems.
- **No soak testing.** Every run is seconds to tens of seconds. Nothing here says
  anything about table growth, autovacuum, or a week of uptime.
- **No signals, timers or child workflows.** Chronos orchestrates a declarative
  graph; it doesn't replay imperative code, so it also needs no determinism sandbox.
