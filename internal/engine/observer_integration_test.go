package engine_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

func TestObserverPublishesQueueGaugesFromTheDatabase(t *testing.T) {
	h := newHarness(t)
	metrics := telemetry.NewMetrics()
	observer := engine.NewObserver(h.store, metrics, engine.ObserverConfig{}, testsupport.Logger())

	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"amount":1}`, "observer-gauges")
	h.sweep(t)

	require.NoError(t, observer.Refresh(h.ctx))
	body := scrapeMetrics(t, metrics)

	// The first task is claimable, so it must appear as backlog under its own
	// activity — this is the gauge an operator scales the worker pool on.
	assert.Contains(t, body,
		`chronos_queue_depth{activity="charge_payment",task_queue="default"} 1`)
	assert.Contains(t, body, `chronos_workflow_executions{state="RUNNING"} 1`)
	assert.NotEmpty(t, exec.ID)

	// The pool collector is registered on the first gauge refresh and reads on
	// scrape, since pool stats are in-memory and free.
	assert.Contains(t, body, "chronos_db_pool_max_connections")
}

// TestObserverResetsGaugesWhenWorkDrains pins the stale-gauge bug. A Prometheus
// gauge keeps its last value forever unless something overwrites it, so a label
// combination that stops existing — a drained queue, an activity with no pending
// work — would keep reporting the backlog it had at its peak. A dashboard would
// then show a permanent phantom queue that no amount of scaling could clear.
func TestObserverResetsGaugesWhenWorkDrains(t *testing.T) {
	h := newHarness(t)
	metrics := telemetry.NewMetrics()
	observer := engine.NewObserver(h.store, metrics, engine.ObserverConfig{}, testsupport.Logger())

	h.register(t, linearSpec())
	h.start(t, "order_pipeline", `{"amount":1}`, "observer-reset")
	h.sweep(t)

	require.NoError(t, observer.Refresh(h.ctx))
	require.Contains(t, scrapeMetrics(t, metrics),
		`chronos_queue_depth{activity="charge_payment",task_queue="default"} 1`)

	// Drain the queue by claiming the task, so nothing is claimable any more.
	claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID:      "worker-observer",
		TaskQueue:     "default",
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, "charge_payment", claimed.Activity)

	require.NoError(t, observer.Refresh(h.ctx))
	body := scrapeMetrics(t, metrics)

	// The claimable series must be gone entirely, not merely reporting a smaller
	// number and certainly not still reporting 1.
	assert.NotContains(t, body, `chronos_queue_depth{activity="charge_payment"`)
	// The work did not vanish, it moved: it is now leased.
	assert.Contains(t, body, `chronos_tasks_running{task_queue="default"} 1`)
}

// TestPollTaskPropagatesTraceparentToTheWorker covers the link that makes a
// workflow one trace rather than a scatter of orphan spans: the execution's stored
// trace context has to reach the worker, because there is no live call chain
// between the process that scheduled the task and the one that runs it.
func TestPollTaskPropagatesTraceparentToTheWorker(t *testing.T) {
	h := newHarness(t)

	def := h.register(t, linearSpec())
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	exec, _, err := h.store.CreateExecution(h.ctx, store.StartExecutionParams{
		Definition:  def,
		Traceparent: traceparent,
	})
	require.NoError(t, err)
	require.Equal(t, traceparent, exec.Traceparent)
	h.sweep(t)

	claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID:      "worker-trace",
		TaskQueue:     "default",
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)

	assert.Equal(t, traceparent, claimed.Traceparent,
		"the claimed task must carry its execution's trace context")
}

func TestPollTaskLeavesTraceparentEmptyWhenUntraced(t *testing.T) {
	h := newHarness(t)

	h.register(t, linearSpec())
	h.start(t, "order_pipeline", `{"amount":1}`, "observer-untraced")
	h.sweep(t)

	claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID:      "worker-untraced",
		TaskQueue:     "default",
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)

	// Tracing off must produce "" rather than a malformed header: that is the other
	// value migration 0003's CHECK constraint accepts.
	assert.Empty(t, claimed.Traceparent)
}

func TestObserverWithoutMetricsIsANoOp(t *testing.T) {
	h := newHarness(t)
	observer := engine.NewObserver(h.store, nil, engine.ObserverConfig{}, testsupport.Logger())

	// Metrics are optional, so the observer has to tolerate being wired up without
	// them rather than panicking on a nil registry.
	assert.NoError(t, observer.Refresh(h.ctx))
	assert.NoError(t, observer.Run(h.ctx))
}

func scrapeMetrics(t *testing.T, m *telemetry.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.MetricsHandler(testsupport.Logger()).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.True(t, strings.Contains(body, "chronos_"), "scrape produced no chronos series")
	return body
}
