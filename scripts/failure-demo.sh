#!/usr/bin/env bash
#
# Failure-injection demo for Chronos Phase 2.
#
# Three scenarios against the real binaries:
#
#   1. A worker is SIGKILLed mid-task. Its lease lapses, the reaper reclaims the
#      task, a second worker picks it up, and the workflow completes. This is the
#      sequence from the project brief:
#
#          worker crashes -> lease expires -> another worker claims -> continues
#
#   2. A task fails repeatedly and is retried with exponential backoff, then
#      dead-lettered once its attempts run out.
#
#   3. The dead-lettered task is replayed by an operator.
#
# Requires: a running Postgres (make db-up), jq, curl.

set -euo pipefail

readonly HTTP_PORT="${HTTP_PORT:-8088}"
readonly PG_PORT="${PG_PORT:-55432}"
readonly SERVER_URL="http://127.0.0.1:${HTTP_PORT}"
readonly DATABASE_URL="${CHRONOS_DATABASE_URL:-postgres://chronos:chronos@127.0.0.1:${PG_PORT}/chronos?sslmode=disable}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LOG_DIR="${REPO_ROOT}/.demo-logs"

# Tight timings so failure detection is observable in seconds rather than minutes.
readonly WORKER_LEASE_SECONDS=3
readonly REAPER_INTERVAL=500ms
readonly WORKER_TIMEOUT=8s

PIDS=""
track_pid() { PIDS="${PIDS} $1"; }

cleanup() {
  local status=$?
  echo
  echo "──────────────────────────────────────────────────────────────"
  echo "Shutting down demo processes"
  for pid in ${PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -TERM "${pid}" 2>/dev/null || true
  done
  sleep 1
  for pid in ${PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -KILL "${pid}" 2>/dev/null || true
  done
  [[ ${status} -ne 0 ]] && echo && echo "Demo failed (exit ${status}). Logs in ${LOG_DIR}/"
  exit "${status}"
}
trap cleanup EXIT INT TERM

section() {
  echo
  echo "══════════════════════════════════════════════════════════════"
  echo "  $*"
  echo "══════════════════════════════════════════════════════════════"
}

step() { echo; echo "── $* ──"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: $1 is required" >&2; exit 1; }
}
require jq
require curl
require go

cd "${REPO_ROOT}"
mkdir -p "${LOG_DIR}"

psql_value() {
  podman exec chronos-postgres psql -U chronos -d chronos -tAc "$1" 2>/dev/null | tr -d '\r' | tr -d '[:space:]'
}

psql_table() {
  podman exec chronos-postgres psql -U chronos -d chronos -c "$1" 2>/dev/null | sed 's/^/    /'
}

reset_workflow() {
  podman exec chronos-postgres psql -U chronos -d chronos -q -c "
    DELETE FROM workflow_executions WHERE workflow_name = '$1';
    DELETE FROM workflow_definitions WHERE name = '$1';" >/dev/null 2>&1 || true
}

task_field() {
  psql_value "SELECT $3 FROM tasks WHERE execution_id='$1' AND name='$2';"
}

exec_state() {
  psql_value "SELECT state FROM workflow_executions WHERE id='$1';"
}

# await_value polls a psql expression until it equals the expected value.
await_value() {
  local query="$1" expected="$2" label="$3" limit="${4:-60}"
  local current=""
  for _ in $(seq 1 "${limit}"); do
    current="$(psql_value "${query}")"
    if [[ "${current}" == "${expected}" ]]; then
      echo "    ${label}: ${current}"
      return 0
    fi
    sleep 0.5
  done
  echo "    TIMED OUT waiting for ${label} to become ${expected} (last: ${current})" >&2
  return 1
}

start_worker() {
  local name="$1"
  CHRONOS_SERVER_URL="${SERVER_URL}" \
  CHRONOS_WORKER_NAME="${name}" \
  CHRONOS_WORKER_CONCURRENCY=1 \
  CHRONOS_WORKER_LEASE_DURATION="${WORKER_LEASE_SECONDS}s" \
  CHRONOS_WORKER_HEARTBEAT_INTERVAL=1s \
  CHRONOS_LOG_LEVEL=info \
    ./bin/chronos-worker >"${LOG_DIR}/failure-${name}.log" 2>&1 &
  WORKER_PID=$!
  track_pid "${WORKER_PID}"
}

# ---------------------------------------------------------------------------

section "Setup"
podman exec chronos-postgres pg_isready -U chronos -d chronos >/dev/null 2>&1 || {
  echo "error: Chronos PostgreSQL is not reachable. Run: make db-up" >&2
  exit 1
}
export GOFLAGS=-mod=vendor
go build -o bin/chronos-server ./cmd/chronos-server
go build -o bin/chronos-worker ./cmd/chronos-worker
echo "binaries built; postgres ready"

CHRONOS_DATABASE_URL="${DATABASE_URL}" \
CHRONOS_HTTP_ADDR=":${HTTP_PORT}" \
CHRONOS_REAPER_INTERVAL="${REAPER_INTERVAL}" \
CHRONOS_WORKER_TIMEOUT="${WORKER_TIMEOUT}" \
CHRONOS_LOG_LEVEL=info \
  ./bin/chronos-server >"${LOG_DIR}/failure-server.log" 2>&1 &
track_pid $!

echo -n "waiting for the control plane "
for _ in $(seq 1 60); do
  curl -fsS "${SERVER_URL}/readyz" >/dev/null 2>&1 && { echo "ready"; break; }
  echo -n "."
  sleep 0.5
done
curl -fsS "${SERVER_URL}/readyz" >/dev/null
echo "reaper interval ${REAPER_INTERVAL}, worker timeout ${WORKER_TIMEOUT}, worker lease ${WORKER_LEASE_SECONDS}s"

# ===========================================================================
section "Scenario 1 — worker crash, lease expiry, handoff"
# ===========================================================================

reset_workflow "crash_recovery"

# task_a sleeps long enough that a worker can be killed mid-flight. No
# timeoutSeconds, so the lease is exactly what the worker requests and expires
# quickly once it stops renewing.
curl -fsS -X POST "${SERVER_URL}/v1/workflows" \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "crash_recovery",
        "version": 1,
        "description": "A slow task, so a worker can be killed while holding it",
        "tasks": [
          { "name": "task_a", "activity": "sleep", "input": {"duration": "5s"},
            "maxAttempts": 1, "maxLeaseExpiries": 3,
            "retryPolicy": {"initialIntervalMs": 200, "jitterPercent": 0} },
          { "name": "task_b", "activity": "noop", "dependsOn": ["task_a"] }
        ]
      }' | jq -c '{name, tasks: [.tasks[].name]}'

step "Starting worker-victim only"
start_worker "worker-victim"
VICTIM_PID="${WORKER_PID}"
echo "  worker-victim pid ${VICTIM_PID}"
sleep 2

EXEC_ID=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"crash_recovery","input":{"run":"crash"}}' | jq -r .id)
echo "  execution ${EXEC_ID}"

step "Waiting for worker-victim to claim task_a"
await_value \
  "SELECT worker_id FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';" \
  "worker-victim" "task_a holder"

echo "  note maxAttempts=1: the activity is allowed exactly one try"
psql_table "SELECT name, state, attempt, max_attempts, lease_expiry_count, worker_id
            FROM tasks WHERE execution_id='${EXEC_ID}' ORDER BY name;"

step "SIGKILL worker-victim while it holds task_a"
kill -KILL "${VICTIM_PID}" 2>/dev/null || true
wait "${VICTIM_PID}" 2>/dev/null || true
echo "  worker-victim is gone. It reported nothing and will renew nothing."
echo "  its lease has ~${WORKER_LEASE_SECONDS}s left to run."

step "The reaper notices the lapsed lease"
await_value \
  "SELECT state FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';" \
  "SCHEDULED" "task_a state"

psql_table "SELECT name, state, attempt, max_attempts, lease_expiry_count,
                   last_failure_reason, worker_id
            FROM tasks WHERE execution_id='${EXEC_ID}' ORDER BY name;"

LEASE_EXPIRIES=$(task_field "${EXEC_ID}" task_a lease_expiry_count)
MAX_ATTEMPTS=$(task_field "${EXEC_ID}" task_a max_attempts)
echo "  lease_expiry_count=${LEASE_EXPIRIES}, max_attempts=${MAX_ATTEMPTS}"
echo "  The lost claim's attempt was granted back and the loss charged to the"
echo "  separate lease-expiry budget — otherwise maxAttempts=1 could never survive"
echo "  a worker restart."
[[ "${LEASE_EXPIRIES}" == "1" ]] || { echo "expected one lease expiry" >&2; exit 1; }

step "Starting worker-rescue"
start_worker "worker-rescue"
echo "  worker-rescue pid ${WORKER_PID}"

step "Does the workflow finish?"
await_value "SELECT state FROM workflow_executions WHERE id='${EXEC_ID}';" \
  "COMPLETED" "execution state" 120

psql_table "SELECT name, state, attempt, worker_id
            FROM tasks WHERE execution_id='${EXEC_ID}' ORDER BY name;"
echo "  task_a was executed twice: once by the worker that died, once by its"
echo "  replacement. Only the second attempt's result was accepted."

step "History across the failure"
curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}/history" \
  | jq -r '.events[] | "  \(.id)\t\(.eventType)\t\(.taskName // "-")"' \
  | column -t -s $'\t'

# ===========================================================================
section "Scenario 2 — retries with backoff, then dead-letter"
# ===========================================================================

reset_workflow "retry_demo"

curl -fsS -X POST "${SERVER_URL}/v1/workflows" \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "retry_demo",
        "version": 1,
        "description": "A task that always fails, to show the retry curve",
        "tasks": [
          { "name": "task_a", "activity": "always_fail",
            "input": {"message": "simulated downstream outage"},
            "maxAttempts": 3,
            "retryPolicy": {
              "initialIntervalMs": 1000,
              "backoffCoefficient": 2,
              "maxIntervalMs": 10000,
              "jitterPercent": 0
            } }
        ]
      }' | jq -c '{name, retry: .tasks[0].retryPolicy, maxAttempts: .tasks[0].maxAttempts}'

RETRY_EXEC=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"retry_demo","input":{}}' | jq -r .id)
echo "  execution ${RETRY_EXEC}"
echo "  expect attempt 1, wait 1s, attempt 2, wait 2s, attempt 3, then dead-letter"

step "Watching the attempts"
LAST_ATTEMPT=0
for _ in $(seq 1 80); do
  ATTEMPT=$(task_field "${RETRY_EXEC}" task_a attempt)
  STATE=$(task_field "${RETRY_EXEC}" task_a state)
  if [[ "${ATTEMPT}" != "${LAST_ATTEMPT}" ]]; then
    echo "    attempt ${ATTEMPT} (state ${STATE})"
    LAST_ATTEMPT="${ATTEMPT}"
  fi
  [[ "${STATE}" == "DEAD_LETTER" ]] && break
  sleep 0.5
done

await_value "SELECT state FROM tasks WHERE execution_id='${RETRY_EXEC}' AND name='task_a';" \
  "DEAD_LETTER" "task_a state"
await_value "SELECT state FROM workflow_executions WHERE id='${RETRY_EXEC}';" \
  "FAILED" "execution state"

psql_table "SELECT name, state, attempt, max_attempts, retryable,
                   last_failure_reason, left(error, 40) AS error
            FROM tasks WHERE execution_id='${RETRY_EXEC}';"

step "The retry schedule, as recorded in history"
curl -fsS "${SERVER_URL}/v1/executions/${RETRY_EXEC}/history" \
  | jq -r '.events[] | select(.eventType == "TASK_RETRY_SCHEDULED")
           | "  backoff \(.payload.backoffMs)ms after attempt \(.payload.attempt) (\(.payload.attemptsRemaining) left)"'

step "The dead letter queue"
curl -fsS "${SERVER_URL}/v1/dead-letter" \
  | jq '{total, items: [.items[] | {
        task: .task.name, activity: .task.activity,
        attempts: .task.attempt, failureReason, error: .task.error }]}'

# ===========================================================================
section "Scenario 3 — operator replay"
# ===========================================================================

DLQ_TASK=$(curl -fsS "${SERVER_URL}/v1/dead-letter" | jq -r '.items[0].task.id')
echo "replaying dead-lettered task ${DLQ_TASK}"
echo "(the activity is still broken, so it will fail again — in practice you would"
echo " fix the cause first. What this shows is the remediation path itself.)"

BEFORE_ATTEMPTS=$(task_field "${RETRY_EXEC}" task_a attempt)
BEFORE_STATE=$(exec_state "${RETRY_EXEC}")
echo "  before replay: execution=${BEFORE_STATE}, task attempts=${BEFORE_ATTEMPTS}"

REPLAY=$(curl -fsS -X POST "${SERVER_URL}/v1/tasks/${DLQ_TASK}/replay" \
  -H 'Content-Type: application/json' -d '{"extraAttempts":1}')
echo "${REPLAY}" | jq -c '{name, state, attempt, maxAttempts}'

REPLAY_STATE=$(echo "${REPLAY}" | jq -r .state)
[[ "${REPLAY_STATE}" == "SCHEDULED" ]] || {
  echo "expected the replayed task to be requeued, got ${REPLAY_STATE}" >&2; exit 1; }

step "It runs again, then returns to the dead letter queue"
# The activity still fails, so the revived run is short-lived. Rather than race
# the transient RUNNING state, assert on the durable evidence it leaves behind.
await_value "SELECT state FROM tasks WHERE execution_id='${RETRY_EXEC}' AND name='task_a';" \
  "DEAD_LETTER" "task_a state" 60

AFTER_ATTEMPTS=$(task_field "${RETRY_EXEC}" task_a attempt)
echo "    attempts before replay: ${BEFORE_ATTEMPTS}, after: ${AFTER_ATTEMPTS}"
[[ "${AFTER_ATTEMPTS}" -gt "${BEFORE_ATTEMPTS}" ]] || {
  echo "expected the replay to actually re-execute the task" >&2; exit 1; }
echo "    attempt is a lifetime count of executions, so it kept climbing rather"
echo "    than resetting — the row stays an honest record of what really ran."

step "The history proves the run was revived and re-failed"
curl -fsS "${SERVER_URL}/v1/executions/${RETRY_EXEC}/history" \
  | jq -r '.events[] | select(.eventType | test("REPLAY|DEAD_LETTER|WORKFLOW"))
           | "  \(.id)\t\(.eventType)\t\(.payload.resumedFromDeadLetter // .payload.reason // "-")"' \
  | column -t -s $'\t'

REPLAY_EVENTS=$(curl -fsS "${SERVER_URL}/v1/executions/${RETRY_EXEC}/history" \
  | jq '[.events[] | select(.eventType == "TASK_REPLAYED")] | length')
RESUME_EVENTS=$(curl -fsS "${SERVER_URL}/v1/executions/${RETRY_EXEC}/history" \
  | jq '[.events[] | select(.payload.resumedFromDeadLetter != null)] | length')
echo
echo "    TASK_REPLAYED events: ${REPLAY_EVENTS}, resume events: ${RESUME_EVENTS}"
[[ "${REPLAY_EVENTS}" -ge 1 && "${RESUME_EVENTS}" -ge 1 ]] || {
  echo "expected the replay and the revival to both be recorded" >&2; exit 1; }
echo "    replay is the only thing that moves an execution out of FAILED, and it"
echo "    only ever happens because an operator asked."

psql_table "SELECT name, state, attempt, max_attempts, lease_expiry_count
            FROM tasks WHERE execution_id='${RETRY_EXEC}';"

# ===========================================================================
section "Failure demo complete"
# ===========================================================================
cat <<EOF
What was demonstrated:

  1. A SIGKILLed worker's task was reclaimed and finished by another worker.
     Nothing reported the failure — the control plane inferred it from silence,
     which is the only signal a crashed process leaves behind.

  2. Failures were retried on an exponential curve and then parked, rather than
     retried forever or dropped.

  3. A parked task was replayed on request, reviving the run it had failed.

Two decisions worth noting:

  * A lost worker is charged to a task's lease-expiry budget, not its activity
    attempt budget. A crash is not the activity failing, so maxAttempts=1 still
    survives a worker restart — while a task that reliably destroys workers is
    still eventually parked.

  * Every deadline is evaluated against the database clock, in the same statement
    as the write it guards. One clock for the cluster means a replica with a
    drifted clock cannot reap a live lease, and a worker reporting just as its
    lease lapses still wins.

  Logs: ${LOG_DIR}/
EOF
