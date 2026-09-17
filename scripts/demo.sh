#!/usr/bin/env bash
#
# End-to-end Chronos Phase 1 demo.
#
# Brings up the control plane and two workers, registers the Task A -> B -> C
# example workflow, runs it, and prints the persisted state and history that
# resulted. Everything it shows is read back out of PostgreSQL, not from
# in-process memory.
#
# Requires: a running Postgres (make db-up), jq, curl.

set -euo pipefail

readonly HTTP_PORT="${HTTP_PORT:-8088}"
readonly PG_PORT="${PG_PORT:-55432}"
readonly SERVER_URL="http://127.0.0.1:${HTTP_PORT}"
readonly DATABASE_URL="${CHRONOS_DATABASE_URL:-postgres://chronos:chronos@127.0.0.1:${PG_PORT}/chronos?sslmode=disable}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LOG_DIR="${REPO_ROOT}/.demo-logs"

# Tracked as a space-separated string rather than an array: macOS still ships
# bash 3.2, where empty-array expansion under `set -u` and negative indices are
# both unavailable.
PIDS=""

track_pid() {
  PIDS="${PIDS} $1"
}

cleanup() {
  local status=$?
  echo
  echo "──────────────────────────────────────────────────────────────"
  echo "Shutting down demo processes"
  for pid in ${PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -TERM "${pid}" 2>/dev/null || true
  done
  # Give the graceful shutdown path a moment before forcing anything.
  sleep 1
  for pid in ${PIDS}; do
    kill -0 "${pid}" 2>/dev/null && kill -KILL "${pid}" 2>/dev/null || true
  done
  if [[ ${status} -ne 0 ]]; then
    echo
    echo "Demo failed (exit ${status}). Logs are in ${LOG_DIR}/"
  fi
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
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required but not installed" >&2
    exit 1
  }
}

require jq
require curl
require go

cd "${REPO_ROOT}"
mkdir -p "${LOG_DIR}"

section "1. Checking PostgreSQL"
if ! pg_ready=$(podman exec chronos-postgres pg_isready -U chronos -d chronos 2>&1); then
  echo "error: Chronos PostgreSQL is not reachable. Run: make db-up" >&2
  echo "  ${pg_ready}" >&2
  exit 1
fi
echo "postgres ready on 127.0.0.1:${PG_PORT}"

section "2. Building binaries"
export GOFLAGS=-mod=vendor
go build -o bin/chronos-server ./cmd/chronos-server
go build -o bin/chronos-worker ./cmd/chronos-worker
echo "built bin/chronos-server and bin/chronos-worker"

section "3. Starting the control plane (API + engine)"
CHRONOS_DATABASE_URL="${DATABASE_URL}" \
CHRONOS_HTTP_ADDR=":${HTTP_PORT}" \
CHRONOS_LOG_LEVEL=info \
  ./bin/chronos-server >"${LOG_DIR}/server.log" 2>&1 &
SERVER_PID=$!
track_pid "${SERVER_PID}"
echo "chronos-server pid ${SERVER_PID}, logs at ${LOG_DIR}/server.log"

echo -n "waiting for readiness "
for _ in $(seq 1 60); do
  if curl -fsS "${SERVER_URL}/readyz" >/dev/null 2>&1; then
    echo "ready"
    break
  fi
  echo -n "."
  sleep 0.5
done
curl -fsS "${SERVER_URL}/readyz" | jq -c .

section "4. Starting two workers"
# Two workers on the same queue, so the queue's SKIP LOCKED claim path is
# genuinely exercised rather than trivially serialized.
for i in 1 2; do
  CHRONOS_SERVER_URL="${SERVER_URL}" \
  CHRONOS_WORKER_NAME="worker-demo-${i}" \
  CHRONOS_WORKER_CONCURRENCY=2 \
  CHRONOS_LOG_LEVEL=info \
    ./bin/chronos-worker >"${LOG_DIR}/worker-${i}.log" 2>&1 &
  worker_pid=$!
  track_pid "${worker_pid}"
  echo "chronos-worker worker-demo-${i} pid ${worker_pid}"
done
sleep 2

echo
echo "Registered workers:"
curl -fsS "${SERVER_URL}/v1/workers" | jq '.items[] | {name, taskQueue, state, activities}'

section "5. Registering the order_pipeline workflow (Task A -> B -> C)"

# Clear prior runs of this one workflow so the script is re-runnable. Needed
# because workflow versions are immutable by design: re-registering a changed
# spec under an existing version is a 409, which is right for a real client but
# inconvenient for a demo whose spec may evolve.
podman exec chronos-postgres psql -U chronos -d chronos -q -c "
  DELETE FROM workflow_executions WHERE workflow_name = 'order_pipeline';
  DELETE FROM workflow_definitions WHERE name = 'order_pipeline';" >/dev/null 2>&1 || true

read -r -d '' WORKFLOW <<'JSON' || true
{
  "name": "order_pipeline",
  "version": 1,
  "description": "Charge the payment, reserve inventory, then send a receipt",
  "taskQueue": "default",
  "tasks": [
    { "name": "task_a", "activity": "charge_payment" },
    { "name": "task_b", "activity": "reserve_inventory", "dependsOn": ["task_a"] },
    { "name": "task_c", "activity": "send_receipt",      "dependsOn": ["task_b"] }
  ]
}
JSON

curl -fsS -X POST "${SERVER_URL}/v1/workflows" \
  -H 'Content-Type: application/json' -d "${WORKFLOW}" \
  | jq '{name, version, taskQueue, specHash, tasks: [.tasks[] | {name, activity, dependsOn}]}'

echo
echo "Re-registering the identical definition (must be idempotent, HTTP 200 not 201):"
status=$(curl -fsS -o /dev/null -w '%{http_code}' -X POST "${SERVER_URL}/v1/workflows" \
  -H 'Content-Type: application/json' -d "${WORKFLOW}")
echo "  HTTP ${status}"
[[ "${status}" == "200" ]] || { echo "expected 200 on re-registration, got ${status}" >&2; exit 1; }

section "6. Starting an execution"
EXECUTION=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-1001' \
  -d '{
        "workflowName": "order_pipeline",
        "input": {
          "orderId": "1001",
          "customer": "buyer@example.com",
          "amount": 129.99,
          "currency": "USD",
          "sku": "widget-blue",
          "quantity": 3
        }
      }')
EXEC_ID=$(echo "${EXECUTION}" | jq -r .id)
echo "${EXECUTION}" | jq '{id, workflowName, workflowVersion, state, idempotencyKey}'
echo
echo "execution id: ${EXEC_ID}"

echo
echo "Replaying the same request with the same Idempotency-Key:"
REPLAY_ID=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-1001' \
  -d '{"workflowName":"order_pipeline","input":{"orderId":"1001","amount":129.99}}' | jq -r .id)
echo "  returned execution id: ${REPLAY_ID}"
if [[ "${REPLAY_ID}" != "${EXEC_ID}" ]]; then
  echo "IDEMPOTENCY VIOLATED: replay created a different execution" >&2
  exit 1
fi
echo "  same execution — no duplicate run was created"

section "7. Waiting for the workflow to finish"
STATE="PENDING"
for _ in $(seq 1 100); do
  STATE=$(curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}" | jq -r .state)
  printf '\r  state: %-10s' "${STATE}"
  case "${STATE}" in
    COMPLETED|FAILED|CANCELED) break ;;
  esac
  sleep 0.2
done
echo

if [[ "${STATE}" != "COMPLETED" ]]; then
  echo "workflow did not complete (final state: ${STATE})" >&2
  curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}" | jq .
  exit 1
fi

section "8. Final execution state (read back from PostgreSQL)"
curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}" \
  | jq '{
      id, state, workflowName, workflowVersion,
      startedAt, completedAt,
      output,
      tasks: [.tasks[] | {name, activity, state, attempt, workerId, startedAt, completedAt}]
    }'

section "9. Execution history (the durable audit trail)"
curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}/history" \
  | jq -r '.events[] | "  \(.id)\t\(.eventType)\t\(.taskName // "-")"' \
  | column -t -s $'\t'

section "10. Proving the state is durable, not in-memory"
echo "Querying PostgreSQL directly:"
podman exec chronos-postgres psql -U chronos -d chronos -x -c "
  SELECT workflow_name, state, error, output
  FROM workflow_executions WHERE id = '${EXEC_ID}';" 2>/dev/null | sed 's/^/  /'

echo "Task rows:"
podman exec chronos-postgres psql -U chronos -d chronos -c "
  SELECT name, state, attempt, worker_id, depends_on
  FROM tasks WHERE execution_id = '${EXEC_ID}' ORDER BY name;" 2>/dev/null | sed 's/^/  /'

echo "Event count:"
podman exec chronos-postgres psql -U chronos -d chronos -t -c "
  SELECT count(*) || ' history events persisted'
  FROM history_events WHERE execution_id = '${EXEC_ID}';" 2>/dev/null | sed 's/^/  /'

section "Demo complete"
cat <<EOF
The workflow ran Task A -> Task B -> Task C in dependency order, executed by
worker processes that reached the engine only over the REST API. Every state
transition is committed to PostgreSQL before it is acted on, which is why
'make recovery-demo' can kill the engine mid-run and have it resume.

  API:      ${SERVER_URL}
  Logs:     ${LOG_DIR}/
  Inspect:  make psql
EOF
