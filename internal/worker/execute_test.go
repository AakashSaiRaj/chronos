package worker

// Internal test: Worker.execute is unexported, and it is the function that carries
// Phase 4's tracing behaviour, so it is exercised directly rather than through a
// full worker run.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/config"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// A fixed upstream trace, standing in for the context stored on the execution row
// when the workflow was accepted.
const (
	upstreamTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	upstreamTraceparent = "00-" + upstreamTraceID + "-00f067aa0ba902b7-01"
)

// TestExecuteJoinsTheWorkflowTrace is the point of persisting a traceparent on the
// execution: the worker runs the activity in a different process, minutes after the
// workflow was accepted, so there is no live call chain to propagate a header
// along. If the stored context is not picked up here, every attempt becomes an
// orphan root span and a workflow's trace falls apart into unrelated fragments.
func TestExecuteJoinsTheWorkflowTrace(t *testing.T) {
	spans, w, srv := newExecuteFixture(t, func(_ context.Context, _ ActivityInput) (any, error) {
		return map[string]any{"ok": true}, nil
	})
	defer srv.Close()

	w.execute(context.Background(), discardLogger(), taskFor(upstreamTraceparent))

	activity := requireSpan(t, spans, "activity.charge_payment")
	assert.Equal(t, upstreamTraceID, activity["traceId"],
		"the activity span must land in the workflow's trace, not a new one")
	assert.Equal(t, "00f067aa0ba902b7", activity["parentSpanId"],
		"the activity span's parent must be the stored span")

	// Attributes let a trace be filtered by workflow shape without parsing names.
	assert.Equal(t, "charge_payment", activity["chronos.activity"])
	assert.Equal(t, "task_a", activity["chronos.task"])
	assert.Equal(t, "3", activity["chronos.attempt"])
}

// TestExecuteReportIsAChildOfTheActivitySpan checks the other half of the link:
// reporting the outcome must hang off the activity span rather than starting a
// second, disconnected trace. The full chain is
// activity -> outbound client call -> the control plane's handling of it, and all
// three have to share the workflow's trace for the trace to be readable.
func TestExecuteReportIsAChildOfTheActivitySpan(t *testing.T) {
	spans, w, srv := newExecuteFixture(t, func(_ context.Context, _ ActivityInput) (any, error) {
		return nil, nil
	})
	defer srv.Close()

	w.execute(context.Background(), discardLogger(), taskFor(upstreamTraceparent))

	activity := requireSpan(t, spans, "activity.charge_payment", "internal")
	// Client and server spans share a name here, so they are told apart by kind.
	clientSpan := requireSpan(t, spans, "POST /v1/tasks/{id}/complete", "client")
	serverSpan := requireSpan(t, spans, "POST /v1/tasks/{id}/complete", "server")

	assert.Equal(t, activity["spanId"], clientSpan["parentSpanId"],
		"the report call must be a child of the activity")
	assert.Equal(t, clientSpan["spanId"], serverSpan["parentSpanId"],
		"the control plane must continue the worker's trace, not start its own")

	// One trace end to end, across the process boundary.
	for _, span := range []map[string]string{activity, clientSpan, serverSpan} {
		assert.Equal(t, upstreamTraceID, span["traceId"], span["name"]+"/"+span["kind"])
	}

	// The span name is templated, so one name covers every task rather than one
	// span name per task ID.
	assert.NotContains(t, clientSpan["name"], taskID.String())
}

func TestExecuteRecordsActivityFailureOnTheSpan(t *testing.T) {
	spans, w, srv := newExecuteFixture(t, func(_ context.Context, _ ActivityInput) (any, error) {
		return nil, errors.New("card declined")
	})
	defer srv.Close()

	w.execute(context.Background(), discardLogger(), taskFor(upstreamTraceparent))

	activity := requireSpan(t, spans, "activity.charge_payment")
	assert.Equal(t, "Error", activity["statusCode"])
	assert.Equal(t, "card declined", activity["statusMessage"])
}

// TestExecuteStartsItsOwnTraceWhenUntraced covers the tracing-disabled path: an
// empty traceparent must not produce a malformed parent or drop the span, it just
// becomes a root.
func TestExecuteStartsItsOwnTraceWhenUntraced(t *testing.T) {
	spans, w, srv := newExecuteFixture(t, func(_ context.Context, _ ActivityInput) (any, error) {
		return nil, nil
	})
	defer srv.Close()

	w.execute(context.Background(), discardLogger(), taskFor(""))

	activity := requireSpan(t, spans, "activity.charge_payment")
	assert.NotEmpty(t, activity["traceId"])
	assert.NotEqual(t, upstreamTraceID, activity["traceId"])
	assert.Empty(t, activity["parentSpanId"], "an untraced task's span is a root")
}

func TestExecuteTracksSlotUtilizationAroundTheActivity(t *testing.T) {
	// The worker is needed inside the activity to sample the counter, but it does
	// not exist until after the fixture is built, so it is captured by pointer.
	var worker *Worker
	var busyDuringRun int64

	_, w, srv := newExecuteFixture(t, func(_ context.Context, _ ActivityInput) (any, error) {
		// Sampled from inside the activity, which is the only window in which the
		// slot is genuinely occupied.
		busyDuringRun = worker.busySlots.Load()
		return nil, nil
	})
	defer srv.Close()
	worker = w
	w.metrics = telemetry.NewMetrics()

	w.execute(context.Background(), discardLogger(), taskFor(""))

	assert.EqualValues(t, 1, busyDuringRun, "the slot must be counted busy while the activity runs")
	assert.EqualValues(t, 0, w.busySlots.Load(), "the slot must be released afterwards")
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

// newExecuteFixture wires a worker with tracing enabled against a stub control
// plane, and returns the buffer the span exporter writes to.
func newExecuteFixture(t *testing.T, fn ActivityFunc) (*bytes.Buffer, *Worker, *httptest.Server) {
	t.Helper()

	spans := &bytes.Buffer{}
	tracing := initTracing(t, spans)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The stub stands in for the API, including the route-pattern span naming the
		// real server applies, so the assertions cover the naming path too.
		telemetry.SetSpanRoute(r, "POST /v1/tasks/{id}/complete")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"` + taskID.String() + `","state":"COMPLETED"}`))
	}))
	// Instrumented so the report call produces a client span, matching production.
	srv.Config.Handler = telemetry.InstrumentHandler(srv.Config.Handler, "stub-api")

	registry := NewRegistry().Register("charge_payment", fn)

	c, err := client.New(client.Config{
		BaseURL:       srv.URL,
		Timeout:       5 * time.Second,
		Logger:        discardLogger(),
		WrapTransport: telemetry.InstrumentTransport,
	})
	require.NoError(t, err)

	w, err := New(config.Worker{
		ServerURL:      srv.URL,
		Name:           "worker-trace-test",
		TaskQueue:      "default",
		Concurrency:    4,
		PollInterval:   10 * time.Millisecond,
		LeaseDuration:  time.Minute,
		TaskTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
	}, c, registry, discardLogger())
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, tracing.Shutdown(context.Background())) })
	return spans, w, srv
}

var taskID = uuid.MustParse("13de3e6c-37da-40e5-a3ce-fa09959f845c")

func taskFor(traceparent string) *client.TaskResponse {
	return &client.TaskResponse{
		ID:          taskID,
		ExecutionID: uuid.MustParse("b50c22c8-74e4-4d42-9e93-96c7d24c3d51"),
		Name:        "task_a",
		Activity:    "charge_payment",
		Attempt:     3,
		MaxAttempts: 3,
		TaskQueue:   "default",
		ClaimToken:  uuid.New().String(),
		Input:       json.RawMessage(`{"workflowInput":{"amount":1}}`),
		Traceparent: traceparent,
	}
}

// initTracing installs a real tracer provider writing spans to buf, and restores
// otel's globals afterwards so tests do not leak configuration into one another.
func initTracing(t *testing.T, buf io.Writer) *telemetry.Tracing {
	t.Helper()

	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})

	tracing, err := telemetry.InitTracing(telemetry.TracingConfig{
		Enabled:     true,
		ServiceName: "chronos-worker-test",
		SampleRatio: 1.0,
		SpanOutput:  buf,
	}, discardLogger())
	require.NoError(t, err)
	return tracing
}

// requireSpan flushes pending spans and returns the one matching name, and kind
// when kind is non-empty. Kind is needed because a client span and the server span
// it triggers share a name once both are templated to the same route.
func requireSpan(t *testing.T, buf *bytes.Buffer, name string, kind ...string) map[string]string {
	t.Helper()

	// Spans are batched, so nothing is guaranteed to be in the buffer until the
	// provider is flushed.
	flusher, ok := otel.GetTracerProvider().(interface {
		ForceFlush(context.Context) error
	})
	require.True(t, ok, "tracer provider does not support ForceFlush")
	require.NoError(t, flusher.ForceFlush(context.Background()))

	wantKind := ""
	if len(kind) > 0 {
		wantKind = kind[0]
	}

	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &parsed), "span line is not JSON: %s", raw)
		if parsed["msg"] != "span" || parsed["name"] != name {
			continue
		}
		flat := map[string]string{}
		for k, v := range parsed {
			if s, ok := v.(string); ok {
				flat[k] = s
			}
		}
		if wantKind != "" && flat["kind"] != wantKind {
			continue
		}
		return flat
	}
	t.Fatalf("no span named %q (kind %q) was exported; buffer:\n%s", name, wantKind, buf.String())
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}
