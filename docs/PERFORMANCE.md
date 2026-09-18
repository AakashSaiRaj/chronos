# Chronos performance

Every number here was measured by `scripts/loadtest.sh`, which is committed so the
figures can be reproduced rather than taken on trust. Nothing is extrapolated and
nothing is from a different shape of run than the one described.

```
make loadtest            # full suite, writes reports to /tmp/chronos-loadtest
```

## Test environment

A laptop, not a cluster. That matters for reading the absolute numbers: the API,
the scheduler, every worker and PostgreSQL all share 11 cores, so they compete with
each other in a way they would not in a real deployment. The *relative* results —
which resource saturates first, how throughput responds to more workers — are the
useful part, and they are what the analysis below is about.

| | |
|---|---|
| Machine | Apple M3 Pro, 11 cores, 18 GB, macOS 26.6.2 |
| Go | 1.27.1 |
| PostgreSQL | 17.11 in podman 6.1.1, `max_connections=200`, `shared_buffers=128MB` |
| Chronos pool | `CHRONOS_DB_MAX_CONNS=20` per server replica |
| Workload | 600 executions per run, 3 sequential `noop` tasks each (1800 tasks) |
| Submission | 64 concurrent submitters, closed loop |

The activity is `noop`, deliberately. The order-pipeline activities validate input
and read named upstream outputs, so they would cap the chain at three tasks and fold
JSON validation into the measurement. With a free activity, whatever time remains is
Chronos's own cost — scheduling, queueing, claiming, committing — which is what is
being measured.

Throughput is computed from the load generator's **monotonic** clock. The database's
wall-clock view is recorded alongside it as a cross-check, and a disagreement is
flagged rather than published; see [Measuring this correctly](#measuring-this-correctly).

## Headline results

| Configuration | Workflows/s | Tasks/s | e2e p50 | e2e p99 | Submit p50 | Submit p99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 worker × 4 slots, 250 ms polls | 64.9 | 194.8 | 5.9 s | 8.9 s | 15.0 ms | 49.2 ms |
| 2 workers × 4 slots, 250 ms polls | **66.3** | **198.8** | 5.8 s | 8.7 s | 11.9 ms | 19.1 ms |
| 4 workers × 4 slots, 250 ms polls | 63.4 | 190.2 | 6.1 s | 9.1 s | 15.6 ms | 24.5 ms |
| 4 workers × 4 slots, 20 ms polls | 50.1 | 150.4 | 7.7 s | 11.7 s | 13.6 ms | 27.0 ms |
| Saturation: 1200 executions, 64 submitters | 45.9 | 137.7 | 16.7 s | 25.5 s | — | — |

All runs completed 100% of submitted executions with zero failures.

Peak observed throughput: **66 workflows/s, 199 tasks/s.**

### Submission latency is the number a caller actually waits on

`POST /v1/executions` stays at **12–20 ms p50 and under 50 ms p99** across every
configuration, including the saturation run where end-to-end latency was 16 seconds.

That separation is the design working as intended. Accepting a workflow is one
insert; running it is somebody else's problem, later. A caller is never made to wait
on the backlog, and a queue 20 seconds deep does not become a 20-second API. This is
the main argument for a durable queue over synchronous orchestration.

## Worker scaling: it doesn't, and that is the finding

Throughput is flat from 1 to 4 workers — 64.9, 66.3, 63.4 workflows/s — and the
variation between them is smaller than the run-to-run noise. Sixteen worker slots
did no more work than four.

The instinctive reading is that something is contended. The metrics say something
more specific, and more interesting: **the workers were never the constraint, so
there was nothing for more of them to do.** Sampled during a saturation run:

```
chronos_queue_depth                              peak 44
chronos_queue_oldest_claimable_age_seconds       peak 0.21
chronos_tasks_running                            peak 8   (of 16 slots)
chronos_db_pool_total_connections                20   (= pool ceiling)
chronos_db_pool_empty_acquires_total             3980
```

The queue never built up, the oldest task never waited more than a fifth of a
second, and at most half the slots were ever occupied. Workers were sitting idle
waiting for work to exist.

So where did the time go? The `claim_task` timing added in Phase 4 answers it:

| Operation | Transactions | Total DB time | Mean |
|---|---:|---:|---:|
| `claim_task` | 30,354 | 94.5 s | 3.11 ms |
| `report_task_completed` | 5,430 | 19.7 s | 3.63 ms |

5,430 of those 30,354 claim transactions returned a task. **The other 24,924 —
82.1% — found nothing and still cost a full PostgreSQL transaction**, consuming
77.6 seconds of database time to answer "no".

That is the bottleneck. Chronos uses the `tasks` table as its queue, and a worker
discovers work by running `SELECT … FOR UPDATE SKIP LOCKED` against it. An empty
poll is indistinguishable in cost from a successful one: same transaction, same row
lock acquisition, same commit. Adding workers adds pollers, and pollers contend for
the same table and the same 20-connection pool as the scheduler that creates the
work they are waiting for. Past the point where workers can keep up, each additional
worker subtracts from the capacity available to the scheduler.

The 20 ms poll run is the confirming experiment. Polling 12× more often made
throughput **worse** — 50.1 vs 63.4 workflows/s — because it multiplied the empty
transactions without making any additional work available. If workers had been the
constraint, tighter polling would have helped.

This is the trade accepted in Phase 1: PostgreSQL-as-queue buys transactional
enqueue with the workflow state, no second system to keep consistent, and
`SKIP LOCKED` correctness under concurrency. What it costs is that idle capacity is
not free. A dedicated broker gives you a blocking receive, where an idle consumer
costs nothing.

### The fix, if this needed to scale further

`LISTEN`/`NOTIFY`. PostgreSQL can wake a waiting connection when a task becomes
claimable, so a worker with no work holds an idle connection instead of running a
transaction every 250 ms. Empty polls go to roughly zero and the 77.6 seconds spent
answering "no" goes back to doing work. The engine already has the notification
point — `Engine.Nudge()` is called on every task report — so the change is confined
to the poll path, and long-polling the existing HTTP endpoint would get most of the
benefit without changing the client contract at all.

Not done here because it is a Phase 1 architectural change rather than Phase 4
instrumentation, and because the measurement matters more than the optimization: the
point of this phase was to be able to find this, and the numbers above are that
capability working.

## End-to-end latency is scheduling delay, not work

A 3-task chain of no-ops takes ~5.9 s at p50. The activities themselves account for
almost none of it.

Each hop in a chain has to wait for two independent loops:

- the engine's sweep, to notice the dependency is satisfied and make the next task
  claimable — `CHRONOS_ENGINE_POLL_INTERVAL`, 250 ms
- a worker's poll, to pick it up — `CHRONOS_WORKER_POLL_INTERVAL`, 250 ms

Averaging half an interval each, that is ~250 ms per hop and ~750 ms for a 3-task
chain before any work happens. The remainder is queueing: at 66 workflows/s with
600 in flight, most of a workflow's life is spent waiting behind others.

The consequence worth knowing is that **chain depth costs latency even when the
tasks are instant**. A 10-task chain has ten scheduling round trips in it. Workflows
that need low latency should fan out rather than chain, and `queue_wait` vs
`task_duration` on the dashboard is what tells the two apart:

- `task_duration` high → the activity is slow. Optimize the activity.
- `queue_wait` high, utilization high → not enough workers. Scale the Deployment.
- `queue_wait` high, utilization low → scheduling delay or poll interval. Scaling
  workers will not help, and may hurt.

## Failure recovery

A worker holding a task was `SIGKILL`ed, so it could neither report nor release the
lease, and the wait until another worker ran the same task was measured. Both
detectors were exercised, because which one fires depends on the task and reporting
only one number would misdescribe the system:

| Detector | Measured | Bound by |
|---|---:|---|
| Lease expiry | **6.19 s** | 5 s lease + 1 s reaper interval |
| Worker-death detection | **45.40 s** | `CHRONOS_WORKER_TIMEOUT` (45 s default) |

Both landed within half a second of their theoretical bound.

The lever that decides which one fires is not obvious. The effective lease is

```
GREATEST(requested_lease, timeout_seconds + 15s)
```

The grace term exists so a task that legitimately runs for its entire declared
budget does not have its lease reaped at the exact moment it finishes — which would
hand live work to a second worker. The consequence is that a task declaring a 300 s
timeout gets a 315 s lease, so lease expiry cannot possibly reclaim it and
worker-death detection is the only thing that will.

**So recovery time for long-running tasks is governed by `CHRONOS_WORKER_TIMEOUT`,
not by the lease.** A deployment that tunes lease duration expecting faster recovery
for long tasks would be tuning the wrong knob. `CHRONOS_WORKER_TIMEOUT` must in turn
stay comfortably above the heartbeat interval, or a single dropped heartbeat evicts
a healthy worker; the config layer rejects combinations that violate this.

In both cases the task was re-run to completion on the surviving worker with no lost
or duplicated side effects, which is the property the two-budget design
(`max_attempts` for activity failures, `max_lease_expiries` for infrastructure
failures) exists to protect.

## Measuring this correctly

Two measurement bugs produced convincing, wrong numbers before the results above
were trustworthy. Both are worth recording, because both would have been published
as findings.

**A closed laptop looked like a scalability limit.** An early run reported 0.8
workflows/s with a 364-second window. Throughput was being derived from the
difference between the database's `created_at` and `completed_at`, and the host had
suspended mid-run: PostgreSQL's wall clock advanced across the sleep, Go's monotonic
clock did not. The tell was that only two progress lines had been printed for what
was supposedly a six-minute drain. Throughput is now taken from the monotonic clock,
the server-side window is reported beside it, and a disagreement beyond
`2 × window + 5s` prints a `CLOCK JUMP DETECTED` warning instead of a number. The
script also re-execs under `caffeinate` so it cannot happen in the first place.

**The load generator was the bottleneck it was measuring.** `drain` originally
issued one `GET /v1/executions/{id}` per pending execution per pass. Watching 600
executions meant 600 sequential round trips per pass, so the generator's own latency
sat inside the measurement window as a constant — which is precisely the shape that
makes throughput look unresponsive to added workers. It now pages
`GET /v1/executions?workflowName=…`, two requests per pass, and gets the timestamps
from the same response.

The second bug had a cause worth noting on its own: `Client.do` takes `headers` in
the argument position that looks like it should take query parameters. Passing a
filter map there compiles, silently sends the filter as HTTP headers, and returns an
unfiltered default page — so the generator waited forever for executions it was
never asking about. `TestListExecutionsSendsFiltersAsQueryParameters` pins it.

Warm-up executions are also drained rather than merely submitted, so they are not
competing for worker slots during the measured run.

## Limits of these numbers

- **One host.** PostgreSQL, the control plane and the workers share 11 cores. A real
  deployment separates them, and the absolute throughput would differ.
- **Free activities.** `noop` isolates Chronos's overhead. Real activities make
  network calls, at which point worker count matters and the conclusion about
  scaling changes.
- **Single scheduler.** The engine was one replica. Multiple schedulers are safe
  (`TestConcurrentEnginesDoNotDoubleSchedule`) but sweep throughput with several was
  not measured.
- **Default pool.** 20 connections per replica. The pool showed 3,980 empty acquires,
  so a larger pool would likely raise the ceiling — bounded by PostgreSQL's own
  `max_connections` divided by replica count, which is the trade the
  `ChronosDatabasePoolExhausted` alert exists to surface.
- **No sustained soak.** Runs are seconds to tens of seconds. Nothing here says
  anything about behaviour over hours, table growth, or autovacuum.
