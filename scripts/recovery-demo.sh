#!/usr/bin/env bash
#
# Recovery demo: proves a Chronos workflow is recoverable from persisted state.
#
# The engine is SIGKILLed mid-workflow — no graceful shutdown, no chance to flush
# anything — and a brand-new process picks the workflow up and drives it to
# completion. The replacement engine has never seen the execution and holds no
# in-memory state about it; everything it needs it reads from PostgreSQL.
#
# The first task is executed by curl rather than by a worker process. That is
# deliberate: it keeps the crash window free of any leased task, so the demo
# isolates the property Phase 1 guarantees (scheduling state is recoverable) from
# the one Phase 2 adds (reclaiming a task whose worker died mid-execution). It
# also shows the worker protocol is nothing more than HTTP.
#
# Requires: a running Postgres (make db-up), jq, curl.

set -euo pipefail

readonly HTTP_PORT="${HTTP_PORT:-8088}"
readonly PG_PORT="${PG_PORT:-55432}"
readonly SERVER_URL="http://127.0.0.1:${HTTP_PORT}"
readonly DATABASE_URL="${CHRONOS_DATABASE_URL:-postgres://chronos:chronos@127.0.0.1:${PG_PORT}/chronos?sslmode=disable}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LOG_DIR="${REPO_ROOT}/.demo-logs"

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

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: $1 is required" >&2; exit 1; }
}
require jq
require curl
require go

cd "${REPO_ROOT}"
mkdir -p "${LOG_DIR}"

start_server() {
  local label="$1"
  CHRONOS_DATABASE_URL="${DATABASE_URL}" \
  CHRONOS_HTTP_ADDR=":${HTTP_PORT}" \
  CHRONOS_LOG_LEVEL=info \
    ./bin/chronos-server >"${LOG_DIR}/recovery-server-${label}.log" 2>&1 &
  SERVER_PID=$!
  track_pid "${SERVER_PID}"

  for _ in $(seq 1 60); do
    curl -fsS "${SERVER_URL}/readyz" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "server (${label}) never became ready" >&2
  return 1
}

start_worker() {
  local name="$1"
  CHRONOS_SERVER_URL="${SERVER_URL}" \
  CHRONOS_WORKER_NAME="${name}" \
  CHRONOS_WORKER_CONCURRENCY=2 \
  CHRONOS_LOG_LEVEL=info \
    ./bin/chronos-worker >"${LOG_DIR}/recovery-${name}.log" 2>&1 &
  track_pid $!
}

psql_value() {
  podman exec chronos-postgres psql -U chronos -d chronos -tAc "$1" 2>/dev/null | tr -d '\r'
}

task_state() {
  psql_value "SELECT state FROM tasks WHERE execution_id='$1' AND name='$2';" | tr -d '[:space:]'
}

exec_state() {
  psql_value "SELECT state FROM workflow_executions WHERE id='$1';" | tr -d '[:space:]'
}

show_persisted_state() {
  echo "  workflow_executions:"
  podman exec chronos-postgres psql -U chronos -d chronos -c \
    "SELECT state, started_at IS NOT NULL AS started, completed_at IS NOT NULL AS finished
     FROM workflow_executions WHERE id='$1';" 2>/dev/null | sed 's/^/    /'
  echo "  tasks:"
  podman exec chronos-postgres psql -U chronos -d chronos -c \
    "SELECT name, state, attempt, worker_id, output
     FROM tasks WHERE execution_id='$1' ORDER BY name;" 2>/dev/null | sed 's/^/    /'
}

# await_state polls until the execution reaches a terminal state, printing only
# when the state actually changes so the log stays readable.
await_terminal_state() {
  local exec_id="$1" limit="$2" last="" current=""
  for _ in $(seq 1 "${limit}"); do
    current="$(exec_state "${exec_id}")"
    if [[ "${current}" != "${last}" ]]; then
      echo "  state -> ${current}"
      last="${current}"
    fi
    case "${current}" in COMPLETED|FAILED|CANCELED) echo "${current}"; return 0 ;; esac
    sleep 0.2
  done
  echo "${current}"
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

# reset_demo_workflow clears prior runs of this one workflow so the script is
# re-runnable. It is needed because workflow versions are immutable by design:
# re-registering a changed spec under an existing version is a 409, which is the
# right behaviour for a real client but inconvenient for a demo that evolves.
reset_demo_workflow() {
  local name="$1"
  podman exec chronos-postgres psql -U chronos -d chronos -q -c "
    DELETE FROM workflow_executions WHERE workflow_name = '${name}';
    DELETE FROM workflow_definitions WHERE name = '${name}';" >/dev/null 2>&1 || true
}

section "1. Start engine generation A — deliberately with no workers"
reset_demo_workflow "recovery_demo"
start_server "A"
SERVER_A_PID="${SERVER_PID}"
echo "engine A pid ${SERVER_A_PID}"

curl -fsS -X POST "${SERVER_URL}/v1/workflows" \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "recovery_demo",
        "version": 1,
        "description": "Three-task chain used to demonstrate crash recovery",
        "tasks": [
          { "name": "task_a", "activity": "noop" },
          { "name": "task_b", "activity": "noop", "dependsOn": ["task_a"] },
          { "name": "task_c", "activity": "noop", "dependsOn": ["task_b"] }
        ]
      }' | jq -c '{name, version, tasks: [.tasks[].name]}'

EXEC_ID=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"recovery_demo","input":{"run":"recovery"}}' | jq -r .id)
echo "started execution ${EXEC_ID}"

echo -n "waiting for the engine to schedule task_a "
for _ in $(seq 1 50); do
  [[ "$(task_state "${EXEC_ID}" task_a)" == "SCHEDULED" ]] && { echo "done"; break; }
  echo -n "."
  sleep 0.2
done
[[ "$(task_state "${EXEC_ID}" task_a)" == "SCHEDULED" ]] || {
  echo "task_a was never scheduled" >&2; exit 1; }

section "2. Execute task_a over plain HTTP (standing in for a worker)"
CLAIM=$(curl -fsS -X POST "${SERVER_URL}/v1/tasks/poll" \
  -H 'Content-Type: application/json' \
  -d '{"workerId":"curl-worker","taskQueue":"default","leaseSeconds":60}')
TASK_A_ID=$(echo "${CLAIM}" | jq -r .id)
CLAIM_TOKEN=$(echo "${CLAIM}" | jq -r .claimToken)
echo "${CLAIM}" | jq -c '{name, activity, state, attempt, workerId}'

curl -fsS -X POST "${SERVER_URL}/v1/tasks/${TASK_A_ID}/complete" \
  -H 'Content-Type: application/json' \
  -d "{\"claimToken\":\"${CLAIM_TOKEN}\",\"output\":{\"step\":\"a\",\"via\":\"curl\"}}" \
  | jq -c '{name, state, output}'

echo -n "waiting for the engine to schedule task_b "
for _ in $(seq 1 50); do
  [[ "$(task_state "${EXEC_ID}" task_b)" == "SCHEDULED" ]] && { echo "done"; break; }
  echo -n "."
  sleep 0.2
done
[[ "$(task_state "${EXEC_ID}" task_b)" == "SCHEDULED" ]] || {
  echo "task_b was never scheduled" >&2; exit 1; }
echo "task_b is queued and unclaimed — this is our crash window"

section "3. SIGKILL engine A"
kill -KILL "${SERVER_A_PID}" 2>/dev/null || true
# Reap the killed child quietly so bash does not print its own job notice.
wait "${SERVER_A_PID}" 2>/dev/null || true
sleep 1

if curl -fsS --max-time 2 "${SERVER_URL}/readyz" >/dev/null 2>&1; then
  echo "engine A is somehow still serving; aborting" >&2
  exit 1
fi
echo "engine A is gone. The API no longer answers. Nothing was flushed on the way out."

section "4. What survived the crash (straight from PostgreSQL)"
show_persisted_state "${EXEC_ID}"

MID_EXEC_STATE="$(exec_state "${EXEC_ID}")"
echo
echo "  execution=${MID_EXEC_STATE}  task_a=$(task_state "${EXEC_ID}" task_a)" \
     " task_b=$(task_state "${EXEC_ID}" task_b)  task_c=$(task_state "${EXEC_ID}" task_c)"
[[ "${MID_EXEC_STATE}" == "RUNNING" ]] || {
  echo "expected a mid-flight execution, got ${MID_EXEC_STATE}" >&2; exit 1; }

# Capture task_a's result so we can prove completed work is not redone.
TASK_A_OUTPUT_BEFORE=$(psql_value "SELECT output::text FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';")
TASK_A_DONE_BEFORE=$(psql_value "SELECT completed_at FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';")
EVENTS_BEFORE=$(psql_value "SELECT count(*) FROM history_events WHERE execution_id='${EXEC_ID}';" | tr -d '[:space:]')
echo "  ${EVENTS_BEFORE} history events were durable at the moment of the crash"

section "5. Start engine generation B — a brand new process"
start_server "B"
echo "engine B pid ${SERVER_PID}; it has never seen execution ${EXEC_ID}"
echo "everything it does next is reconstructed from the rows above"
start_worker "worker-recovery-B"
echo "started worker-recovery-B (a different worker than the one that ran task_a)"

section "6. Does the workflow finish?"
STATE="$(await_terminal_state "${EXEC_ID}" 150 | tail -1)"
if [[ "${STATE}" != "COMPLETED" ]]; then
  echo "recovery FAILED: execution ended as ${STATE}" >&2
  show_persisted_state "${EXEC_ID}"
  exit 1
fi
echo "  the replacement engine finished a workflow it never started"

section "7. Verifying recovery resumed rather than restarted"
show_persisted_state "${EXEC_ID}"

TASK_A_OUTPUT_AFTER=$(psql_value "SELECT output::text FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';")
TASK_A_DONE_AFTER=$(psql_value "SELECT completed_at FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';")
TASK_A_ATTEMPTS=$(psql_value "SELECT attempt FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';" | tr -d '[:space:]')
TASK_A_WORKER=$(psql_value "SELECT worker_id FROM tasks WHERE execution_id='${EXEC_ID}' AND name='task_a';" | tr -d '[:space:]')

echo
[[ "${TASK_A_OUTPUT_BEFORE}" == "${TASK_A_OUTPUT_AFTER}" ]] || {
  echo "FAIL: task_a's output changed across the restart" >&2; exit 1; }
[[ "${TASK_A_DONE_BEFORE}" == "${TASK_A_DONE_AFTER}" ]] || {
  echo "FAIL: task_a was re-run after the restart" >&2; exit 1; }
[[ "${TASK_A_ATTEMPTS}" == "1" ]] || {
  echo "FAIL: task_a ran ${TASK_A_ATTEMPTS} times; completed work must not be redone" >&2; exit 1; }
[[ "${TASK_A_WORKER}" == "curl-worker" ]] || {
  echo "FAIL: task_a's worker attribution changed to ${TASK_A_WORKER}" >&2; exit 1; }

echo "  task_a: attempt=1, output and completion time unchanged, still attributed"
echo "          to curl-worker — completed work was preserved, not repeated"
echo "  task_b and task_c were executed by worker-recovery-B, after the crash"

section "8. History across the crash boundary"
curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}/history" \
  | jq -r '.events[] | "  \(.id)\t\(.eventType)\t\(.taskName // "-")"' \
  | column -t -s $'\t'

EVENTS_AFTER=$(psql_value "SELECT count(*) FROM history_events WHERE execution_id='${EXEC_ID}';" | tr -d '[:space:]')
echo
echo "  ${EVENTS_BEFORE} events before the crash, ${EVENTS_AFTER} after recovery — one"
echo "  continuous history for a single execution, spanning two engine processes."

section "Recovery demo complete"
cat <<EOF
A workflow was interrupted by SIGKILL and completed by a different process.

What made that possible:
  * every state change commits to PostgreSQL before it is acted on
  * the engine derives its next action from rows, never from memory
  * task materialization and state transitions are idempotent, so a scheduling
    pass that was cut off part-way is safe to simply redo

Phase 1 boundary, stated plainly: a task that was leased to a worker when that
worker died stays RUNNING until its lease lapses, and Phase 1 has no sweeper to
notice. Lease expiry, requeueing, retries with backoff, and worker failure
detection are Phase 2 — the schema already carries lease_expires_at, attempt,
and max_attempts to support them.
EOF
