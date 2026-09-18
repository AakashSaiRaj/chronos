# Chronos architecture

A distributed workflow orchestration engine: callers register workflows as task
graphs, start executions, and workers run the tasks. Durability, ordering, retries
and failure recovery are the engine's problem, not the caller's.

This document covers the shape of the system and the decisions behind it, including
the ones that were wrong first. Measured behaviour is in
[PERFORMANCE.md](PERFORMANCE.md).

## The system

```
                       ┌──────────────────────────────────────┐
   caller ──REST──────▶│  API  (internal/api)                 │
                       │  validate, idempotency, DTO mapping  │
                       └───────────────┬──────────────────────┘
                                       │
                       ┌───────────────▼──────────────────────┐
                       │  Service  (internal/engine/service)  │
                       │  transport-agnostic use cases        │
                       └───────────────┬──────────────────────┘
                                       │
        ┌──────────────────────────────┼───────────────────────────────┐
        │                              │                               │
┌───────▼────────┐          ┌──────────▼─────────┐          ┌──────────▼─────────┐
│  Engine        │          │  Reaper            │          │  Observer          │
│  materialize   │          │  expired leases,   │          │  queue gauges,     │
│  + schedule    │          │  dead workers      │          │  pool saturation   │
└───────┬────────┘          └──────────┬─────────┘          └──────────┬─────────┘
        │                              │                               │
        └──────────────────────────────┼───────────────────────────────┘
                                       │
                       ┌───────────────▼──────────────────────┐
                       │  Store  (internal/store)             │
                       │  the only SQL in the system          │
                       └───────────────┬──────────────────────┘
                                       │
                       ┌───────────────▼──────────────────────┐
                       │  PostgreSQL                          │
                       │  workflow_definitions                │
                       │  workflow_executions                 │
                       │  tasks         ← this IS the queue   │
                       │  history_events                      │
                       │  workers                             │
                       └───────────────▲──────────────────────┘
                                       │
                       ┌───────────────┴──────────────────────┐
   activities ◀────────│  Workers  (internal/worker)          │
                       │  poll → execute → report             │
                       │  lease renewal, cooperative cancel   │
                       └──────────────────────────────────────┘
                              (reach the DB only via the API)
```

Three processes, one binary each:

- **`chronos-server`** — API + engine + reaper + observer. Each can be disabled by
  environment variable, so the same image runs as an API-only replica or a
  scheduler-only replica. Phase 3 deploys it both ways.
- **`chronos-worker`** — polls for tasks, runs activities, reports outcomes. Holds no
  database credentials and no database route; its IAM role grants nothing.
- **`chronos-loadtest`** — the measurement client behind PERFORMANCE.md.

## The decisions that shaped it

### PostgreSQL is the queue

The `tasks` table is the work queue. A worker claims with
`SELECT … FOR UPDATE SKIP LOCKED LIMIT 1` inside the same transaction that writes the
`TASK_STARTED` history event.

Chosen because enqueueing a task and recording the state change that caused it are
then **one commit**. With a separate broker they are two systems, and every
interesting failure lives in the gap between them: a task enqueued but not recorded
runs twice, a task recorded but not enqueued never runs. Avoiding that gap usually
means an outbox and a relay — more moving parts than the broker saved.

What it costs is that an idle worker is not free: an empty poll is a full
transaction. At 82% empty polls that is the measured throughput ceiling, and it is
why adding workers stops helping. See
[PERFORMANCE.md](PERFORMANCE.md#worker-scaling-it-doesnt-and-that-is-the-finding).

SQS and Redis modules exist in `deploy/terraform/optional.tf` behind
`enable_sqs` / `enable_redis`, both false. They are there to make the swap a
deployment decision rather than a rewrite, not because it is needed.

### History is ordered by `BIGSERIAL`, not a per-execution sequence

The first implementation gave each execution a dense sequence: event 1, 2, 3 within
that workflow. It read better and it was wrong.

A dense counter needs read-modify-write — `SELECT max(seq) … + 1` — inside the claim
transaction. That serialized concurrent claims against the same execution, which is
exactly what `SKIP LOCKED` exists to avoid. Adding optimistic retry made it worse: a
thundering herd of retries on a hot workflow, then deadlocks.
`TestConcurrentWorkersNeverShareATask` is what surfaced it.

A global `BIGSERIAL` gives total order within an execution — which is the only
ordering anything needs — for free, with no coordination. The sequence has gaps
across executions, which nothing cares about.

### The database clock is the only clock

Every deadline — lease expiry, retry backoff, worker liveness — is evaluated in SQL
against `now()`, in the same statement as the write it guards.

An earlier version injected a Go clock for testability. That version had a real bug:
detection used the database's clock while the *decision* used the injected one, so
clock skew between replicas could reap a live lease. There is no correct way to
inject a clock into a system where several processes must agree what time it is.

The cost is that tests cannot fake time. They backdate the rows the real queries read
(`internal/testsupport/failure.go`), so the production code path runs unmodified
against a real clock. A test that wants "this worker went silent two minutes ago"
writes exactly that.

### Two attempt budgets, not one

- `max_attempts` — the activity ran and failed. Its own fault.
- `max_lease_expiries` (default 3) — the worker vanished mid-task. Infrastructure's
  fault.

Separate because conflating them means a rolling deploy consumes the retry budget of
every in-flight task. A task with `maxAttempts: 1` must still survive a worker
restart: it never got its one attempt. So a lease expiry **returns** the consumed
attempt and charges a different budget.

### Guarded writes, so a late worker cannot win

Every reclaim carries `AND lease_expires_at < now()` in its `WHERE` clause, and no
history event is written until the guard has passed. A worker whose report is in
flight when the reaper fires still wins, because the reaper's update matches nothing.

Getting the order wrong here is subtle: an earlier version appended the history event
before the guarded write, so a rejected reclaim still left a `TASK_LEASE_EXPIRED`
event behind describing something that never happened.

### Claim tokens make reporting idempotent

A claim mints a fresh token; reporting requires it. A superseded worker's report is
refused (`ErrStaleClaim`) rather than silently overwriting a result the current owner
produced. This is what makes blind client retries safe, which in turn is what lets
the client retry at all.

### Trace context lives on the execution row

The one piece of distributed tracing that HTTP header propagation cannot solve. A
workflow's spans come from different processes at different times: the API accepts
it, the scheduler enqueues tasks seconds later, a worker claims one minutes after
that. There is no live call chain to propagate a header along.

So the W3C `traceparent` is persisted on `workflow_executions` (migration 0003) and
returned on the poll response. Every later span, in whatever process, is created as a
child of it. One workflow is one trace across all three components — verified
end to end: a 12-span trace spanning server and worker, with activity spans parented
to the originating request.

### Workers are not trusted

A worker has no database credentials, no network route to port 5432 (the
NetworkPolicy says so explicitly rather than relying on the absence of a password),
and an IAM role that grants nothing. Everything goes through the API, so claim
tokens, state transitions and idempotency are enforced in one place. A compromised
worker can do what the API lets it do and nothing else.

## Observability

Three signals, each answering a question the others cannot.

**Metrics** (`/metrics` on all three components, Prometheus). ~32 series. Labels are
workflow, activity, state and task queue — all bounded by the number of registered
workflows. Never an execution ID, task ID or worker name: an ID in a label is the
standard way to take Prometheus down. Attempt counts above five collapse into `6+`.

Database-derived gauges are refreshed by a background poller rather than computed on
scrape, because putting a query on the scrape path means a slow database becomes
scrape timeouts precisely when the metrics are most needed. Gauges are `Reset()`
before each refresh so a label combination that stops existing stops reporting —
without that, a drained queue keeps publishing its peak depth forever. Connection
pool stats *are* collected on scrape, being an in-memory read.

**Traces** (OpenTelemetry). Spans are exported to the structured log stream rather
than over OTLP: the gRPC exporter pulls in grpc, protobuf and genproto, and this
project vendors its dependencies so it can build with no network access. The exporter
implements `sdktrace.SpanExporter`, so span IDs, W3C propagation and parent links are
all real, and a collector's filelog receiver reconstructs OTLP from them. Swapping in
`otlptracegrpc` is a change to one function.

**Logs** (`slog`, JSON in production). The span stream has its own logger,
deliberately independent of the application log level.

Dashboard and alerts: `deploy/observability/`, `deploy/k8s/base/monitoring.yaml`.
Alerts fire only on conditions the system cannot resolve itself — not on retries,
lease expiries or stale claims, all of which are normal and self-healing.

### Three bugs that only appeared when it ran

Instrumentation that compiles is not instrumentation that works.

**Spans were exported into a discard.** They went through the application logger at
`Debug`, so a process at the default `info` level emitted nothing.
`CHRONOS_TRACING_ENABLED=true` appeared to do nothing at all, with no indication why.
Enabling tracing is already an explicit request for the data, so it is no longer
gated a second time by a log level; volume is controlled by the sample ratio, which
is the knob that belongs to tracing.

**Span names contained task UUIDs.** `otelhttp` wraps the mux, so it creates the span
*before* routing — `r.Pattern` is still empty and the only name available is the raw
path. The result was `POST /v1/tasks/13de3e6c-…/complete`: one span name per task,
which makes latency-by-endpoint impossible and bloats the backend's name index. This
is the tracing equivalent of an ID in a metric label. Names are now set from the route
pattern after the mux has matched.

**Idle workers flooded the tracing backend.** Workers poll continuously, so an idle
queue produced 463 single-span root traces in two minutes from one worker, each
describing nothing. Polls and heartbeats are now excluded on both the inbound and
outbound side. 576 empty polls now produce zero spans, and nothing is lost: the
interesting span is the activity execution that follows a successful poll, and that
one is parented to the workflow's trace.

A fourth was found in the deployment rather than the code: the namespace is
default-deny and neither the scheduler nor the workers had an ingress rule, so the
PodMonitors could not have scraped them. The failure mode is silent — targets simply
stay down and Chronos reports nothing wrong — which is why `allow-metrics-scrape`
exists and is cross-referenced from both files.

## Layering

```
domain     ← no dependencies. States, validation, retry policy, spec hashing.
store      ← domain. All SQL. Nothing above it writes a query.
engine     ← domain, store, telemetry. Scheduling, reaping, use cases.
api        ← engine, domain, telemetry. HTTP only: DTOs, status codes, middleware.
client     ← domain. What a worker uses to reach the API.
worker     ← client, domain, telemetry. Activity execution.
telemetry  ← leaf. Imports nothing from Chronos, so it cannot create a cycle.
```

Dependencies point one way. `telemetry` being a leaf is what lets the store report
query latency without importing it — the store takes a callback instead.

`Service` exists so the use cases do not live in HTTP handlers. Adding gRPC would
mean a new transport package calling the same `Service`; no engine code would change.
Not done — REST is sufficient here and a second transport with no second client is
speculative.

## What is deliberately absent

- **Signals, timers, child workflows.** Temporal has them. Out of scope for a task
  graph.
- **Workflow-as-code determinism.** Chronos orchestrates a declarative graph; it does
  not replay imperative code, so it needs no determinism sandbox.
- **gRPC.** See above.
- **A real OTLP exporter.** Constrained by offline vendoring, isolated to one
  function.
- **`LISTEN`/`NOTIFY` for the poll path.** The measured ceiling, and the first thing
  to change if this needed to scale further. It is a Phase 1 architectural change,
  not Phase 4 instrumentation — and finding it was the point of Phase 4.
