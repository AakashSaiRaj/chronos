package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func TestNewMetricsRegistersEverySeriesOnce(t *testing.T) {
	// Two independent sets must be constructible in one process. They would panic
	// on duplicate registration if the registry were global, which is what makes
	// tests order-dependent.
	first := NewMetrics()
	second := NewMetrics()

	require.NotNil(t, first)
	require.NotNil(t, second)
	require.NotSame(t, first.Registry(), second.Registry())
}

func TestMetricsExposeChronosNamespace(t *testing.T) {
	m := NewMetrics()
	m.WorkflowsStarted.WithLabelValues("order_pipeline").Inc()

	body := scrape(t, m)

	require.Contains(t, body, `chronos_workflow_executions_started_total{workflow="order_pipeline"} 1`)
	// Runtime metrics come from the Go and process collectors, and are the first
	// thing to check when latency rises for no visible reason.
	require.Contains(t, body, "go_goroutines")
}

func TestObserveDBQueryRecordsLatencyAndErrorsSeparately(t *testing.T) {
	m := NewMetrics()

	m.ObserveDBQuery("claim_task", 3*time.Millisecond, nil)
	m.ObserveDBQuery("claim_task", 4*time.Millisecond, errors.New("connection reset"))

	// Both calls are timed under one series; only the failing one increments the
	// error counter, so a dashboard can show latency and error rate for the same
	// operation without the failures being invisible in the timing.
	body := scrape(t, m)
	assert.Contains(t, body, `chronos_db_query_duration_seconds_count{operation="claim_task"} 2`)
	assert.InDelta(t, 1, testutil.ToFloat64(m.DBQueryErrors.WithLabelValues("claim_task")), 0.001)
}

func TestObserveDBQueryOnNilMetricsIsSafe(t *testing.T) {
	// Metrics are optional, and every call site would otherwise need a nil check.
	var m *Metrics
	assert.NotPanics(t, func() {
		m.ObserveDBQuery("claim_task", time.Millisecond, nil)
		m.SetWorkerSlots(4, 2)
	})
}

func TestSetWorkerSlotsPublishesUtilizationInputs(t *testing.T) {
	m := NewMetrics()
	m.SetWorkerSlots(8, 3)

	// Utilization is deliberately left as a ratio of two exported series rather
	// than precomputed, so it can be aggregated across a fleet.
	assert.InDelta(t, 8, testutil.ToFloat64(m.WorkerSlotsTotal), 0.001)
	assert.InDelta(t, 3, testutil.ToFloat64(m.WorkerSlotsBusy), 0.001)
}

func TestAttemptLabelCollapsesHighAttemptCounts(t *testing.T) {
	// Attempt numbers are unbounded in principle, and one series per attempt would
	// grow without limit. Beyond a handful the only interesting fact is "many".
	for attempt, want := range map[int]string{1: "1", 5: "5", 6: "6+", 97: "6+"} {
		assert.Equal(t, want, attemptLabel(attempt), "attempt %d", attempt)
	}
}

func TestGaugeResetClearsVanishedLabelCombinations(t *testing.T) {
	m := NewMetrics()
	m.QueueDepth.WithLabelValues("default", "charge_payment").Set(12)
	require.Contains(t, scrape(t, m), `chronos_queue_depth{activity="charge_payment",task_queue="default"} 12`)

	// The observer resets before each refresh. Without it, a drained queue would
	// keep reporting its last value forever and a dashboard would show a permanent
	// phantom backlog.
	m.QueueDepth.Reset()

	assert.NotContains(t, scrape(t, m), "chronos_queue_depth{")
}

func TestMetricsHandlerReportsDisabledWhenNil(t *testing.T) {
	var m *Metrics
	rec := httptest.NewRecorder()
	m.MetricsHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	// A build without metrics must fail the scrape visibly rather than serve an
	// empty page, which would look like a system doing no work at all.
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------------------------------------------------------------------------
// Trace context propagation
// ---------------------------------------------------------------------------

func TestTraceparentRoundTripsThroughTheDurableQueue(t *testing.T) {
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0})

	ctx, span := tracing.Tracer().Start(context.Background(), "workflow")
	traceparent := TraceparentFrom(ctx)
	span.End()

	// Shape is the W3C header, which is what makes it storable and what the CHECK
	// constraint in migration 0003 enforces.
	require.Regexp(t, `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`, traceparent)

	// Rebuilt in a *fresh* context, standing in for a different process reading the
	// value back out of Postgres minutes later. This is the mechanism that keeps a
	// workflow one trace instead of a scatter of orphan spans.
	revived := ContextFromTraceparent(context.Background(), traceparent)
	child := trace.SpanContextFromContext(revived)

	require.True(t, child.IsValid())
	assert.Equal(t, TraceIDFrom(ctx), child.TraceID().String())
	assert.True(t, child.IsRemote(), "revived context should be treated as a remote parent")
}

func TestTraceparentFromIsEmptyWithoutASpan(t *testing.T) {
	initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0})

	// An untraced execution must store "" rather than a malformed header, which is
	// the other value migration 0003's CHECK permits.
	assert.Empty(t, TraceparentFrom(context.Background()))
}

func TestContextFromTraceparentIgnoresEmptyAndMalformedValues(t *testing.T) {
	initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0})

	for _, tc := range []string{"", "garbage", "00-tooshort-x-01"} {
		ctx := ContextFromTraceparent(context.Background(), tc)
		assert.False(t, trace.SpanContextFromContext(ctx).IsValid(),
			"expected no span context for %q", tc)
	}
}

func TestTracingDisabledStillPropagatesContext(t *testing.T) {
	// The propagator is installed even when tracing is off, so a service with
	// tracing disabled in the middle of a call chain does not break the trace for
	// the services either side of it.
	initTracing(t, TracingConfig{Enabled: false})

	incoming := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ContextFromTraceparent(context.Background(), incoming)

	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", TraceIDFrom(ctx))
	assert.Equal(t, incoming, TraceparentFrom(ctx))
}

func TestTracingDisabledProducesNonRecordingSpans(t *testing.T) {
	initTracing(t, TracingConfig{Enabled: false})

	_, span := Start(context.Background(), "activity.charge_payment")
	defer span.End()

	// Instrumentation calls stay valid rather than needing a branch at every call
	// site; they just cost nothing.
	assert.False(t, span.IsRecording())
}

func TestExporterNoneDisablesTracingEvenWhenEnabled(t *testing.T) {
	tracing := initTracing(t, TracingConfig{Enabled: true, Exporter: "none"})

	_, span := tracing.Tracer().Start(context.Background(), "workflow")
	defer span.End()

	assert.False(t, span.IsRecording())
}

// ---------------------------------------------------------------------------
// Span export
// ---------------------------------------------------------------------------

// TestSpansAreExportedAtDefaultLogLevel pins the regression that made tracing
// look completely broken: spans were emitted through the application logger at
// Debug level, so a process at the default `info` level exported every span into a
// discard. Enabling tracing appeared to do nothing at all.
func TestSpansAreExportedAtDefaultLogLevel(t *testing.T) {
	var out bytes.Buffer
	tracing := initTracing(t, TracingConfig{
		Enabled:     true,
		ServiceName: "chronos-test",
		SampleRatio: 1.0,
		SpanOutput:  &out,
	})

	_, span := tracing.Tracer().Start(context.Background(), "activity.charge_payment",
		trace.WithAttributes(ActivityAttr("charge_payment"), AttemptAttr(2)))
	span.End()

	require.NoError(t, tracing.Shutdown(context.Background()))

	line := firstSpanLine(t, out.String())
	assert.Equal(t, "span", line["msg"])
	assert.Equal(t, "activity.charge_payment", line["name"])
	assert.Equal(t, "spans", line["stream"], "span lines must be routable away from app logs")
	assert.Equal(t, "charge_payment", line["chronos.activity"])
	assert.Equal(t, "2", line["chronos.attempt"])
	assert.NotEmpty(t, line["traceId"])
	assert.NotEmpty(t, line["spanId"])
}

func TestExportedSpanRecordsParentAndError(t *testing.T) {
	var out bytes.Buffer
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0, SpanOutput: &out})

	ctx, parent := tracing.Tracer().Start(context.Background(), "workflow")
	_, child := tracing.Tracer().Start(ctx, "activity.charge_payment")
	End(child, errors.New("card declined"))
	parent.End()

	require.NoError(t, tracing.Shutdown(context.Background()))

	spans := spanLinesByName(t, out.String())
	require.Contains(t, spans, "activity.charge_payment")
	require.Contains(t, spans, "workflow")

	failed := spans["activity.charge_payment"]
	assert.Equal(t, "Error", failed["statusCode"])
	assert.Equal(t, "card declined", failed["statusMessage"])
	// The parent link is what reconstructs the tree; without it the spans are just
	// a flat list that happens to share a trace ID.
	assert.Equal(t, spans["workflow"]["spanId"], failed["parentSpanId"])
}

func TestEndTreatsCancellationAsNonError(t *testing.T) {
	var out bytes.Buffer
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0, SpanOutput: &out})

	_, span := tracing.Tracer().Start(context.Background(), "activity.slow")
	End(span, context.Canceled)

	require.NoError(t, tracing.Shutdown(context.Background()))

	// A routine deploy cancels in-flight work. Marking those spans as errors would
	// make the error rate spike on every rollout and mean nothing.
	assert.NotEqual(t, "Error", firstSpanLine(t, out.String())["statusCode"])
}

func TestEndOnNilSpanIsSafe(t *testing.T) {
	assert.NotPanics(t, func() { End(nil, errors.New("boom")) })
}

func TestShutdownOnDisabledTracingIsSafe(t *testing.T) {
	tracing := initTracing(t, TracingConfig{Enabled: false})
	assert.NoError(t, tracing.Shutdown(context.Background()))

	var nilTracing *Tracing
	assert.NoError(t, nilTracing.Shutdown(context.Background()))
}

// ---------------------------------------------------------------------------
// HTTP instrumentation
// ---------------------------------------------------------------------------

// TestShouldTraceExcludesHighVolumeChatter pins the other regression worth
// guarding: the task poll used to be traced, and because workers poll
// continuously an idle system produced hundreds of single-span root traces that
// described nothing while burying the traces someone wanted.
func TestShouldTraceExcludesHighVolumeChatter(t *testing.T) {
	untraced := []string{
		"/metrics", "/healthz", "/readyz",
		"/v1/tasks/poll",
		"/v1/tasks/3f6c4e5a-1111-2222-3333-444455556666/heartbeat",
		"/v1/workers/3f6c4e5a-1111-2222-3333-444455556666/heartbeat",
	}
	for _, path := range untraced {
		assert.False(t, shouldTrace(httptest.NewRequest(http.MethodPost, path, nil)),
			"%s should not be traced", path)
	}

	traced := []string{
		"/v1/executions",
		"/v1/workflows",
		"/v1/tasks/3f6c4e5a-1111-2222-3333-444455556666/complete",
		"/v1/tasks/3f6c4e5a-1111-2222-3333-444455556666/fail",
	}
	for _, path := range traced {
		assert.True(t, shouldTrace(httptest.NewRequest(http.MethodPost, path, nil)),
			"%s should be traced", path)
	}
}

func TestTemplatePathRemovesIDsFromSpanNames(t *testing.T) {
	// Span names are the primary grouping key in a tracing backend. A UUID in the
	// name yields one name per task and makes latency-by-endpoint impossible.
	cases := map[string]string{
		"/v1/tasks/3f6c4e5a-1111-2222-3333-444455556666/complete": "/v1/tasks/{id}/complete",
		"/v1/executions/3F6C4E5A-1111-2222-3333-444455556666":     "/v1/executions/{id}",
		"/v1/executions": "/v1/executions",
		"/v1/workflows/order_pipeline/versions/1": "/v1/workflows/order_pipeline/versions/1",
	}
	for path, want := range cases {
		assert.Equal(t, want, TemplatePath(path), path)
	}
}

func TestInstrumentHandlerSkipsFilteredPathsAndTracesTheRest(t *testing.T) {
	var out bytes.Buffer
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0, SpanOutput: &out})

	handler := InstrumentHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), "chronos-test")

	for _, path := range []string{"/v1/tasks/poll", "/metrics", "/v1/executions"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, "filtered requests must still be served: %s", path)
	}

	require.NoError(t, tracing.Shutdown(context.Background()))

	// Exactly one span: the poll and the scrape were filtered, the execution was not.
	spans := spanLines(t, out.String())
	require.Len(t, spans, 1)
	assert.Equal(t, "/v1/executions", spans[0]["url.path"])

	// Left unnamed by otelhttp, which can only see the method before routing. This
	// is exactly why the API adds withSpanRoute — see TestSetSpanRouteReplacesPathWithRoutePattern.
	assert.Equal(t, "POST", spans[0]["name"])
}

func TestSetSpanRouteReplacesPathWithRoutePattern(t *testing.T) {
	var out bytes.Buffer
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0, SpanOutput: &out})

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Stands in for the API's withSpanRoute middleware, which runs after the
		// mux has matched and so is the earliest point the pattern is known.
		SetSpanRoute(r, "POST /v1/tasks/{id}/complete")
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	InstrumentHandler(inner, "chronos-test").ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost,
			"/v1/tasks/3f6c4e5a-1111-2222-3333-444455556666/complete", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, tracing.Shutdown(context.Background()))

	line := firstSpanLine(t, out.String())
	assert.Equal(t, "POST /v1/tasks/{id}/complete", line["name"])
	assert.Equal(t, "POST /v1/tasks/{id}/complete", line["http.route"])
	assert.NotContains(t, line["name"], "3f6c4e5a")
}

func TestSetSpanRouteIsSafeWithoutARecordingSpan(t *testing.T) {
	initTracing(t, TracingConfig{Enabled: false})

	req := httptest.NewRequest(http.MethodPost, "/v1/executions", nil)
	assert.NotPanics(t, func() {
		SetSpanRoute(req, "POST /v1/executions")
		SetSpanRoute(req, "")
	})
}

func TestInstrumentTransportPropagatesTraceparentAndSkipsPolls(t *testing.T) {
	tracing := initTracing(t, TracingConfig{Enabled: true, SampleRatio: 1.0})

	headers := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clientFor := func() *http.Client {
		return &http.Client{Transport: InstrumentTransport(nil)}
	}

	ctx, span := tracing.Tracer().Start(context.Background(), "activity.charge_payment")
	defer span.End()
	wantTrace := TraceIDFrom(ctx)

	// A traced call carries the header, which is what joins the worker's span to
	// the server's handling of the same request.
	do(t, ctx, clientFor(), srv.URL+"/v1/tasks/x/complete")
	got := <-headers
	require.NotEmpty(t, got)
	assert.Contains(t, got, wantTrace)

	// A poll is filtered, so no span is created for it and no header is injected.
	do(t, ctx, clientFor(), srv.URL+"/v1/tasks/poll")
	assert.Empty(t, <-headers)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// initTracing installs a provider and restores the previous global state, so
// tracing tests do not leak configuration into one another through otel's globals.
func initTracing(t *testing.T, cfg TracingConfig) *Tracing {
	t.Helper()

	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})

	tracing, err := InitTracing(cfg, discardLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tracing.Shutdown(context.Background()) })
	return tracing
}

func do(t *testing.T, ctx context.Context, c *http.Client, url string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	require.NoError(t, err)
	resp, err := c.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.MetricsHandler(discardLogger()).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

// firstSpanLine returns the first exported span as a flat map of string values.
func firstSpanLine(t *testing.T, output string) map[string]string {
	t.Helper()
	lines := spanLines(t, output)
	require.NotEmpty(t, lines, "no span was exported")
	return lines[0]
}

func spanLinesByName(t *testing.T, output string) map[string]map[string]string {
	t.Helper()
	byName := map[string]map[string]string{}
	for _, line := range spanLines(t, output) {
		byName[line["name"]] = line
	}
	return byName
}

func spanLines(t *testing.T, output string) []map[string]string {
	t.Helper()

	var lines []map[string]string
	for _, raw := range strings.Split(strings.TrimSpace(output), "\n") {
		if raw == "" {
			continue
		}
		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &parsed), "span line is not JSON: %s", raw)
		if parsed["msg"] != "span" {
			continue
		}
		flat := make(map[string]string, len(parsed))
		for k, v := range parsed {
			flat[k] = valueToString(v)
		}
		lines = append(lines, flat)
	}
	return lines
}

// valueToString flattens a decoded JSON value so assertions can compare against
// plain strings. Span attributes arrive as strings (the exporter emits
// attribute.Value.Emit()) while durations arrive as numbers.
func valueToString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		encoded, _ := json.Marshal(typed)
		return string(encoded)
	}
}

// discardLogger keeps test output readable: these tests assert on the span stream,
// not on application logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}
