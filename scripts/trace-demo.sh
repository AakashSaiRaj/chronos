#!/usr/bin/env bash
#
# Chronos Phase 4: show that one workflow produces one connected trace.
#
# This is the claim worth demonstrating rather than asserting. A workflow's spans
# are produced by different processes at different times — the API accepts the
# execution, the scheduler enqueues its tasks, a worker claims one later — so there
# is no live call chain for a traceparent header to travel along. The trace only
# holds together because the W3C context is persisted on the execution row
# (migration 0003) and handed back to the worker on the poll response.
#
# The script starts a server and a worker with tracing on, runs a three-task chain,
# then reassembles every span carrying that workflow's trace ID from both processes
# and prints the tree.
set -euo pipefail

cd "$(dirname "$0")/.."

SERVER_URL="${SERVER_URL:-http://127.0.0.1:8088}"
WORK_DIR="${WORK_DIR:-/tmp/chronos-trace-demo}"
mkdir -p "${WORK_DIR}"

SERVER_LOG="${WORK_DIR}/server.log"
WORKER_LOG="${WORK_DIR}/worker.log"

PIDS=()
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for tool in jq curl podman; do
  command -v "${tool}" >/dev/null 2>&1 || { echo "missing required tool: ${tool}" >&2; exit 1; }
done

podman ps --format '{{.Names}}' | grep -qx chronos-postgres || {
  echo "chronos-postgres is not running; start it with: make db-up" >&2
  exit 1
}

echo "building..."
export GOPROXY=off GOFLAGS=-mod=vendor
go build -o "${WORK_DIR}/chronos-server" ./cmd/chronos-server
go build -o "${WORK_DIR}/chronos-worker" ./cmd/chronos-worker

# Tracing on, sampling everything. The span stream is JSON on stderr regardless of
# the application log format, because it is machine-consumed: a collector's filelog
# receiver is what would turn these lines back into OTLP.
echo "starting server and worker with tracing enabled..."
CHRONOS_TRACING_ENABLED=true \
CHRONOS_TRACE_SAMPLE_RATIO=1.0 \
CHRONOS_SERVICE_NAME=chronos-api \
CHRONOS_LOG_LEVEL=warn \
  "${WORK_DIR}/chronos-server" > "${SERVER_LOG}" 2>&1 &
PIDS+=($!)

for _ in $(seq 1 60); do
  curl -fsS "${SERVER_URL}/readyz" >/dev/null 2>&1 && break
  sleep 0.5
done
curl -fsS "${SERVER_URL}/readyz" >/dev/null || { echo "server never became ready" >&2; exit 1; }

CHRONOS_TRACING_ENABLED=true \
CHRONOS_TRACE_SAMPLE_RATIO=1.0 \
CHRONOS_SERVICE_NAME=chronos-worker \
CHRONOS_WORKER_NAME=trace-demo-worker \
CHRONOS_WORKER_HEALTH_ADDR=:8095 \
CHRONOS_LOG_LEVEL=warn \
  "${WORK_DIR}/chronos-worker" > "${WORKER_LOG}" 2>&1 &
PIDS+=($!)

for _ in $(seq 1 60); do
  active=$(curl -fsS "${SERVER_URL}/v1/workers" \
    | jq '[.items[] | select(.state == "ACTIVE")] | length' 2>/dev/null || echo 0)
  [[ "${active}" -ge 1 ]] && break
  sleep 0.5
done
echo "worker registered"

# A fresh workflow name each run, so re-running is not a 409 on an immutable version.
WORKFLOW="trace_demo_$(date +%s)"
curl -fsS -X POST "${SERVER_URL}/v1/workflows" -H 'Content-Type: application/json' -d "{
  \"name\": \"${WORKFLOW}\", \"version\": 1, \"taskQueue\": \"default\",
  \"tasks\": [
    { \"name\": \"task_a\", \"activity\": \"charge_payment\" },
    { \"name\": \"task_b\", \"activity\": \"reserve_inventory\", \"dependsOn\": [\"task_a\"] },
    { \"name\": \"task_c\", \"activity\": \"send_receipt\",      \"dependsOn\": [\"task_b\"] }
  ]
}" >/dev/null
echo "registered ${WORKFLOW}"

EXEC_ID=$(curl -fsS -X POST "${SERVER_URL}/v1/executions" \
  -H 'Content-Type: application/json' \
  -d "{\"workflowName\":\"${WORKFLOW}\",\"input\":{
        \"orderId\":\"trace-1\",\"customer\":\"buyer@example.com\",
        \"amount\":42.50,\"currency\":\"USD\",\"sku\":\"widget-blue\",\"quantity\":2}}" \
  | jq -r .id)

STATE=PENDING
for _ in $(seq 1 120); do
  STATE=$(curl -fsS "${SERVER_URL}/v1/executions/${EXEC_ID}" | jq -r .state)
  case "${STATE}" in COMPLETED|FAILED|CANCELED) break ;; esac
  sleep 0.25
done
echo "execution ${EXEC_ID} finished: ${STATE}"
[[ "${STATE}" == "COMPLETED" ]] || { echo "workflow did not complete" >&2; exit 1; }

# The trace ID is read from the execution row, which is the whole point: the durable
# record is what ties the processes together.
TRACEPARENT=$(podman exec chronos-postgres psql -U chronos -d chronos -tA \
  -c "SELECT traceparent FROM workflow_executions WHERE id = '${EXEC_ID}'")
TRACE_ID=$(echo "${TRACEPARENT}" | cut -d- -f2)

echo
echo "traceparent persisted on the execution row:"
echo "  ${TRACEPARENT}"
echo "  trace id: ${TRACE_ID}"

# Spans are batched, so give the exporter a moment to flush.
sleep 6

echo
echo "=============================================================="
echo "  Every span carrying trace ${TRACE_ID:0:16}…"
echo "  (both processes, ordered by start time)"
echo "=============================================================="
printf '%-34s %-9s %8s  %s\n' NAME KIND DURATION SPAN/PARENT
cat "${SERVER_LOG}" "${WORKER_LOG}" \
  | grep '"stream":"spans"' \
  | jq -r --arg t "${TRACE_ID}" \
      'select(.traceId == $t)
       | [.startTime, .name, .kind, (.durationMs|tostring), .spanId, (.parentSpanId // "ROOT")]
       | @tsv' \
  | sort \
  | awk -F'\t' '{printf "%-34s %-9s %6sms  %s <- %s\n", $2, $3, $4, $5, $6}'

TOTAL=$(cat "${SERVER_LOG}" "${WORKER_LOG}" | grep '"stream":"spans"' \
  | jq -r --arg t "${TRACE_ID}" 'select(.traceId == $t) | .spanId' | wc -l | tr -d ' ')

echo
echo "  ${TOTAL} spans, one trace, two processes."
echo
echo "Note what is absent: no span per task poll. Workers poll continuously, so"
echo "tracing them would bury this trace under hundreds of single-span traces that"
echo "describe nothing. Polls are filtered; the activity spans above are parented to"
echo "the originating request instead, which is the causal story worth keeping."
echo
echo "Poll volume is still visible, as a metric rather than a trace:"
curl -fsS http://127.0.0.1:8095/metrics | grep '^chronos_worker_polls_total' | sed 's/^/  /'

echo
echo "raw span streams: ${SERVER_LOG}, ${WORKER_LOG}"
