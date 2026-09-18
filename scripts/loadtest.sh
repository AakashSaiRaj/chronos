#!/usr/bin/env bash
#
# Chronos Phase 4 performance run.
#
# Produces the numbers in docs/PERFORMANCE.md. It is a script rather than a
# checklist so the figures can be reproduced rather than trusted, and so the
# conditions they were measured under are recorded next to them.
#
# Four things are measured, each answering a different question:
#
#   1. Worker scaling      -- does adding workers add throughput, and where does it
#                             stop paying off?
#   2. Latency floor       -- how much of end-to-end latency is polling delay rather
#                             than work? Measured by re-running with tight intervals.
#   3. Failure recovery    -- how long after a worker is killed does its in-flight
#                             task get picked up by someone else?
#   4. Bottleneck evidence -- which resource is actually saturated, read from the
#                             metrics rather than guessed.
#
# Usage: scripts/loadtest.sh [executions-per-run]
set -euo pipefail

cd "$(dirname "$0")/.."

EXECUTIONS="${1:-300}"
TASKS_PER_WORKFLOW=3
SERVER_URL="http://127.0.0.1:8088"
DB_URL="${CHRONOS_DATABASE_URL:-postgres://chronos:chronos@127.0.0.1:55432/chronos?sslmode=disable}"
RESULTS_DIR="${RESULTS_DIR:-/tmp/chronos-loadtest}"
BIN_DIR="${RESULTS_DIR}/bin"

mkdir -p "${RESULTS_DIR}" "${BIN_DIR}"

# Track every process this script starts so a failure cannot leave orphans behind
# holding leases and skewing the next run.
PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

section() {
  echo
  echo "=============================================================="
  echo "  $*"
  echo "=============================================================="
}

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }
}
require jq
require curl
require podman

# Keep the host awake for the duration.
#
# Not a convenience: a macOS suspend mid-run leaves the database's wall clock
# advancing across the gap while the load generator's monotonic clock does not,
# which silently turned a healthy 30 workflows/sec run into a reported 0.8. The
# generator now detects and flags that, but preventing it is better than
# annotating it.
if command -v caffeinate >/dev/null 2>&1 && [[ -z "${CHRONOS_LOADTEST_CAFFEINATED:-}" ]]; then
  echo "re-executing under caffeinate to prevent host sleep during the run"
  export CHRONOS_LOADTEST_CAFFEINATED=1
  exec caffeinate -i -s "$0" "$@"
fi

section "Building binaries"
export GOPROXY=off GOFLAGS=-mod=vendor
go build -o "${BIN_DIR}/chronos-server"   ./cmd/chronos-server
go build -o "${BIN_DIR}/chronos-worker"   ./cmd/chronos-worker
go build -o "${BIN_DIR}/chronos-loadtest" ./cmd/chronos-loadtest
echo "built into ${BIN_DIR}"

podman ps --format '{{.Names}}' | grep -qx chronos-postgres || {
  echo "chronos-postgres is not running; start it with: make db-up" >&2
  exit 1
}

# A clean slate. Old executions would distort the state gauges, and old DEAD worker
# rows would make the worker-count assertions ambiguous.
section "Resetting durable state"
podman exec chronos-postgres psql -U chronos -d chronos -q -c "
  TRUNCATE history_events, tasks, workflow_executions, workers CASCADE;" >/dev/null
echo "truncated history_events, tasks, executions, workers"

# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------

start_server() {
  local poll_interval="${1:-250ms}"
  CHRONOS_DATABASE_URL="${DB_URL}" \
  CHRONOS_ENGINE_POLL_INTERVAL="${poll_interval}" \
  CHRONOS_LOG_LEVEL=warn \
  CHRONOS_LOG_FORMAT=json \
    "${BIN_DIR}/chronos-server" > "${RESULTS_DIR}/server.log" 2>&1 &
  PIDS+=($!)
  SERVER_PID=$!

  local i
  for i in $(seq 1 60); do
    if curl -fsS "${SERVER_URL}/readyz" >/dev/null 2>&1; then
      echo "server ready (engine poll ${poll_interval})"
      return 0
    fi
    sleep 0.5
  done
  echo "server did not become ready" >&2
  tail -20 "${RESULTS_DIR}/server.log" >&2
  exit 1
}

stop_server() {
  [[ -n "${SERVER_PID:-}" ]] || return 0
  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
}

WORKER_PIDS=()

start_workers() {
  local count="$1" concurrency="${2:-4}" poll_interval="${3:-250ms}"
  local i
  WORKER_PIDS=()
  for i in $(seq 1 "${count}"); do
    CHRONOS_SERVER_URL="${SERVER_URL}" \
    CHRONOS_WORKER_NAME="lt-worker-${i}" \
    CHRONOS_WORKER_CONCURRENCY="${concurrency}" \
    CHRONOS_WORKER_POLL_INTERVAL="${poll_interval}" \
    CHRONOS_WORKER_HEALTH_ADDR=":$((8200 + i))" \
    CHRONOS_LOG_LEVEL=warn \
    CHRONOS_LOG_FORMAT=json \
      "${BIN_DIR}/chronos-worker" > "${RESULTS_DIR}/worker-${i}.log" 2>&1 &
    WORKER_PIDS+=($!)
    PIDS+=($!)
  done

  # Wait for registration, so the load test does not start before the fleet is
  # actually able to claim -- otherwise the first seconds of every run measure
  # worker startup instead of throughput.
  local j active
  for j in $(seq 1 60); do
    active=$(curl -fsS "${SERVER_URL}/v1/workers" \
      | jq '[.items[] | select(.state == "ACTIVE")] | length')
    [[ "${active}" -ge "${count}" ]] && { echo "${count} worker(s) active, ${concurrency} slot(s) each"; return 0; }
    sleep 0.5
  done
  echo "workers did not register (saw ${active:-0} of ${count})" >&2
  exit 1
}

stop_workers() {
  local pid
  for pid in "${WORKER_PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  for pid in "${WORKER_PIDS[@]:-}"; do
    wait "${pid}" 2>/dev/null || true
  done
  WORKER_PIDS=()
  # Clear registrations so the next scenario's worker count assertion is exact.
  podman exec chronos-postgres psql -U chronos -d chronos -q -c "TRUNCATE workers;" >/dev/null
}

run_load() {
  local label="$1" out="$2" workflow="$3"
  "${BIN_DIR}/chronos-loadtest" \
    -server "${SERVER_URL}" \
    -executions "${EXECUTIONS}" \
    -tasks "${TASKS_PER_WORKFLOW}" \
    -concurrency 32 \
    -warmup 10 \
    -workflow "${workflow}" \
    -label "${label}" \
    -json "${out}"
}

# ---------------------------------------------------------------------------
# 1. Worker scaling
# ---------------------------------------------------------------------------

section "Scenario 1: worker scaling (${EXECUTIONS} executions x ${TASKS_PER_WORKFLOW} tasks)"

start_server 250ms
for workers in 1 2 4; do
  echo
  echo "--- ${workers} worker(s) ---"
  start_workers "${workers}" 4 250ms
  run_load "${workers} worker(s) x 4 slots, 250ms polls" \
    "${RESULTS_DIR}/scale-${workers}.json" "lt_scale_${workers}"
  stop_workers
done

# ---------------------------------------------------------------------------
# 2. Latency floor: how much of end-to-end time is polling delay?
# ---------------------------------------------------------------------------

section "Scenario 2: latency floor with tight poll intervals"
echo "A no-op chain does almost no work, so its end-to-end time is nearly all"
echo "scheduling and polling delay. Re-running with 20ms intervals isolates how"
echo "much of the latency is the poll interval rather than capacity."
stop_server
start_server 20ms
start_workers 4 4 20ms
run_load "4 worker(s) x 4 slots, 20ms polls" \
  "${RESULTS_DIR}/tight-polls.json" "lt_tight"
stop_workers

# ---------------------------------------------------------------------------
# 3. Bottleneck evidence, read from metrics
# ---------------------------------------------------------------------------

section "Scenario 3: saturation evidence under sustained load"
start_workers 4 4 20ms

# Load in the background so the metrics can be sampled while the queue is busy;
# a scrape taken after the run would show an idle system and prove nothing.
"${BIN_DIR}/chronos-loadtest" -server "${SERVER_URL}" \
  -executions "$((EXECUTIONS * 2))" -tasks "${TASKS_PER_WORKFLOW}" \
  -concurrency 64 -warmup 0 -workflow lt_saturate \
  -label "saturation probe" -json "${RESULTS_DIR}/saturation.json" \
  > "${RESULTS_DIR}/saturation.log" 2>&1 &
LOAD_PID=$!
PIDS+=("${LOAD_PID}")

sleep 3
echo
echo "--- metrics sampled mid-run ---"
curl -fsS "${SERVER_URL}/metrics" > "${RESULTS_DIR}/metrics-midrun.txt"
grep -E '^chronos_(queue_depth|queue_oldest_claimable_age_seconds|tasks_running|db_pool_(acquired|idle|max|total)_connections|db_pool_empty_acquires_total)' \
  "${RESULTS_DIR}/metrics-midrun.txt" || true
echo
echo "worker utilization (busy/total per worker):"
for i in 1 2 3 4; do
  busy=$(curl -fsS "http://127.0.0.1:$((8200 + i))/metrics" 2>/dev/null \
    | awk '/^chronos_worker_slots_busy /{print $2}')
  total=$(curl -fsS "http://127.0.0.1:$((8200 + i))/metrics" 2>/dev/null \
    | awk '/^chronos_worker_slots_total /{print $2}')
  echo "  lt-worker-${i}: ${busy:-?}/${total:-?}"
done

wait "${LOAD_PID}" 2>/dev/null || true
tail -20 "${RESULTS_DIR}/saturation.log"

echo
echo "--- database time by operation (p95 estimated from buckets) ---"
curl -fsS "${SERVER_URL}/metrics" > "${RESULTS_DIR}/metrics-after.txt"
grep -E '^chronos_db_query_duration_seconds_(count|sum)' "${RESULTS_DIR}/metrics-after.txt" \
  | sed 's/^chronos_db_query_duration_seconds_//' || true
stop_workers

# ---------------------------------------------------------------------------
# 4. Failure recovery: kill a worker holding a lease
# ---------------------------------------------------------------------------

section "Scenario 4: recovery time after a worker is killed mid-task"
cat <<'EXPLAIN'
A long activity is claimed, the holding worker is SIGKILLed so it can neither
report nor release, and the wait until another worker runs the task is measured.

There are two independent detectors, and which one fires first depends on the
task, so both are measured:

  a) Lease expiry. The effective lease is
     GREATEST(requested_lease, timeout_seconds + 15s) -- the grace term exists so
     a task that legitimately runs for its whole budget does not have its lease
     reaped at the moment it finishes. With no declared task timeout the lease is
     just what the worker asked for, so this path is fast.

  b) Worker-death detection. Fires at CHRONOS_WORKER_TIMEOUT (45s by default),
     independent of the task. This is the backstop that matters for long-running
     tasks, whose lease is deliberately longer than any sensible detection delay.

A task with a 300s timeout has a 315s lease, so (b) reclaims it, not (a). Reading
only one of these numbers would badly misdescribe the system.
EXPLAIN

LEASE_SECONDS=5

# One slot each, so the task under test is definitely the one the killed worker
# held and there is exactly one other worker able to pick it up.
start_recovery_worker() {
  local name="$1" port="$2"
  CHRONOS_SERVER_URL="${SERVER_URL}" \
  CHRONOS_WORKER_NAME="${name}" \
  CHRONOS_WORKER_CONCURRENCY=1 \
  CHRONOS_WORKER_POLL_INTERVAL=20ms \
  CHRONOS_WORKER_LEASE_DURATION="${LEASE_SECONDS}s" \
  CHRONOS_WORKER_HEARTBEAT_INTERVAL=1s \
  CHRONOS_WORKER_HEALTH_ADDR=":${port}" \
  CHRONOS_LOG_LEVEL=info CHRONOS_LOG_FORMAT=json \
    "${BIN_DIR}/chronos-worker" > "${RESULTS_DIR}/worker-${name}.log" 2>&1 &
  echo $!
}

wait_for_workers() {
  local want="$1" i active
  for i in $(seq 1 60); do
    active=$(curl -fsS "${SERVER_URL}/v1/workers" \
      | jq "[.items[] | select(.state==\"ACTIVE\")] | length")
    [[ "${active}" -ge "${want}" ]] && return 0
    sleep 0.5
  done
  echo "  only ${active:-0} of ${want} workers active" >&2
  return 1
}

restart_recovery_workers() {
  kill "${VICTIM_PID:-}" "${SURVIVOR_PID:-}" 2>/dev/null || true
  sleep 1
  podman exec chronos-postgres psql -U chronos -d chronos -q -c "TRUNCATE workers;" >/dev/null
  VICTIM_PID=$(start_recovery_worker lt-victim 8210)
  SURVIVOR_PID=$(start_recovery_worker lt-survivor 8211)
  PIDS+=("${VICTIM_PID}" "${SURVIVOR_PID}")
  wait_for_workers 2
}

VICTIM_PID=$(start_recovery_worker lt-victim 8210)
SURVIVOR_PID=$(start_recovery_worker lt-survivor 8211)
PIDS+=("${VICTIM_PID}" "${SURVIVOR_PID}")
wait_for_workers 2
echo "two workers active"

# measure_recovery <name> <timeout-seconds-json> <expected-detector>
#
# timeout_seconds is the lever that decides which detector wins, because the lease
# is GREATEST(requested, timeout_seconds + 15). Passing null leaves the task with no
# declared timeout, so the lease is exactly what the worker asked for.
measure_recovery() {
  local name="$1" timeout_json="$2" expected="$3"
  local workflow="lt_recovery_${name}"

  echo
  echo "--- ${name}: expecting ${expected} to fire first ---"

  local timeout_field=""
  [[ "${timeout_json}" != "null" ]] && timeout_field="\"timeoutSeconds\": ${timeout_json},"

  curl -fsS -X POST "${SERVER_URL}/v1/workflows" -H 'Content-Type: application/json' -d "{
    \"name\": \"${workflow}\", \"version\": 1, \"taskQueue\": \"default\",
    \"tasks\": [{ \"name\": \"task_1\", \"activity\": \"sleep\",
                \"input\": { \"duration\": \"120s\" },
                ${timeout_field}
                \"maxAttempts\": 5,
                \"maxLeaseExpiries\": 5,
                \"retryPolicy\": { \"initialIntervalMs\": 100, \"backoffCoefficient\": 1.0 } }]
  }" >/dev/null

  local exec_id
  exec_id=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
    -H 'Content-Type: application/json' \
    -d "{\"workflowName\":\"${workflow}\",\"input\":{}}" | jq -r .id)

  # Wait for the claim, and record both the holder and the lease the server granted.
  local holder="" lease_expires=""
  local i
  for i in $(seq 1 200); do
    holder=$(curl -fsS "${SERVER_URL}/v1/executions/${exec_id}" | jq -r '.tasks[0].workerId // ""')
    [[ -n "${holder}" ]] && break
    sleep 0.1
  done
  [[ -n "${holder}" ]] || { echo "  task was never claimed" >&2; return 1; }

  lease_expires=$(curl -fsS "${SERVER_URL}/v1/executions/${exec_id}" \
    | jq -r '.tasks[0].leaseExpiresAt // ""')
  echo "  claimed by ${holder}, lease until ${lease_expires}"

  local kill_pid
  if [[ "${holder}" == "lt-victim" ]]; then kill_pid="${VICTIM_PID}"; else kill_pid="${SURVIVOR_PID}"; fi

  # SIGKILL, not SIGTERM: a graceful stop lets the worker report, which is the
  # opposite of the failure being measured.
  local kill_at
  kill_at=$(date +%s.%N)
  kill -9 "${kill_pid}" 2>/dev/null || true
  echo "  SIGKILLed ${holder}"

  # The recovery signal is a TASK_STARTED naming a different worker. History is the
  # durable record, so unlike polling task state it cannot be missed between polls.
  local recovered_at="" new_holder=""
  for i in $(seq 1 900); do
    new_holder=$(curl -fsS "${SERVER_URL}/v1/executions/${exec_id}/history" \
      | jq -r "[.events[] | select(.eventType==\"TASK_STARTED\") | .payload.workerId]
               | unique | map(select(. != \"${holder}\")) | .[0] // \"\"" 2>/dev/null || true)
    if [[ -n "${new_holder}" ]]; then
      recovered_at=$(date +%s.%N)
      break
    fi
    sleep 0.1
  done

  if [[ -z "${recovered_at}" ]]; then
    echo "  NOT reclaimed within 90s" >&2
    return 1
  fi

  local seconds
  seconds=$(echo "${recovered_at} ${kill_at}" | awk '{printf "%.2f", $1 - $2}')
  echo "  recovered by ${new_holder} after ${seconds}s"

  jq -n --arg scenario "${name}" --arg killed "${holder}" --arg new "${new_holder}" \
        --arg seconds "${seconds}" --arg lease "${LEASE_SECONDS}" \
        --arg expected "${expected}" --arg timeout "${timeout_json}" \
        '{scenario:$scenario, detector:$expected, killedWorker:$killed, recoveredBy:$new,
          requestedLeaseSeconds:($lease|tonumber), taskTimeoutSeconds:$timeout,
          recoverySeconds:($seconds|tonumber)}' \
    > "${RESULTS_DIR}/recovery-${name}.json"

  # Leave nothing running: a 120s sleep would hold a slot well past this script.
  curl -fsS -X POST "${SERVER_URL}/v1/executions/${exec_id}/cancel" \
    -H 'Content-Type: application/json' -d '{"reason":"loadtest cleanup"}' >/dev/null 2>&1 || true

  # Restart the killed worker so the next measurement has two workers again.
  restart_recovery_workers
}

# No declared task timeout, so the lease is the 5s the worker asked for and lease
# expiry is the fast path. With a 300s timeout the lease becomes 315s and only
# worker-death detection can reclaim it.
measure_recovery "lease-expiry" "null" "lease expiry (5s lease + 1s reaper interval)"
measure_recovery "worker-death" "300" "worker-death detection (45s CHRONOS_WORKER_TIMEOUT)"

kill "${VICTIM_PID}" "${SURVIVOR_PID}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

section "Summary"
printf '%-38s %12s %12s %12s %12s\n' "scenario" "wf/s" "tasks/s" "e2e p50" "e2e p99"
for f in "${RESULTS_DIR}"/scale-*.json "${RESULTS_DIR}/tight-polls.json" "${RESULTS_DIR}/saturation.json"; do
  [[ -f "${f}" ]] || continue
  jq -r '[.label, (.workflowsPerSec|tostring), (.tasksPerSec|tostring),
          (.endToEndLatency.p50Ms|tostring), (.endToEndLatency.p99Ms|tostring)] | @tsv' "${f}" \
    | awk -F'\t' '{printf "%-38s %12.1f %12.1f %10.0fms %10.0fms\n", $1, $2, $3, $4, $5}'
done

echo
echo "submit latency (the only latency a caller waits on):"
for f in "${RESULTS_DIR}"/scale-*.json "${RESULTS_DIR}/tight-polls.json"; do
  [[ -f "${f}" ]] || continue
  jq -r '"  \(.label): p50=\(.submitLatency.p50Ms)ms p95=\(.submitLatency.p95Ms)ms p99=\(.submitLatency.p99Ms)ms"' "${f}"
done

echo
echo "failure recovery (SIGKILL a worker holding a task -> running elsewhere):"
for f in "${RESULTS_DIR}"/recovery-*.json; do
  [[ -f "${f}" ]] || continue
  jq -r '"  \(.scenario): \(.recoverySeconds)s via \(.detector)"' "${f}"
done

echo
echo "raw reports in ${RESULTS_DIR}"
stop_server
echo "done"
