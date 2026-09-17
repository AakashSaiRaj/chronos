package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName identifies Chronos's instrumentation scope in emitted spans.
const TracerName = "github.com/AakashSaiRaj/chronos"

// TracingConfig configures the tracer provider.
type TracingConfig struct {
	// Enabled turns tracing on. When false a no-op provider is installed, so every
	// instrumentation call in the codebase stays valid and costs almost nothing.
	Enabled bool
	// ServiceName distinguishes the API, scheduler, and worker in a trace.
	ServiceName string
	// ServiceVersion is the build version.
	ServiceVersion string
	// Environment is dev/prod, so traces from different environments do not get
	// confused for one another.
	Environment string
	// SampleRatio is the head sampling probability. 1.0 traces everything, which is
	// right for development and wrong for a busy production system.
	SampleRatio float64
	// Exporter selects where spans go: "log" or "none".
	Exporter string
	// SpanOutput is where the log exporter writes spans. Defaults to stderr.
	// Injectable so tests can capture the span stream.
	SpanOutput io.Writer
}

// Tracing holds an initialized tracer provider.
type Tracing struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	logger   *slog.Logger
}

// InitTracing installs a global tracer provider and W3C context propagator.
//
// On the exporter choice: spans are written to the structured log stream rather
// than pushed over OTLP. That is a deliberate constraint of this build — the OTLP
// gRPC exporter pulls in grpc, protobuf, and genproto, and this project vendors
// its dependencies so it can build with no network access. Emitting spans as
// structured logs keeps the whole OTel model intact (real span and trace IDs,
// real W3C propagation, real parent/child relationships) and a collector's
// filelog receiver can turn them back into OTLP.
//
// Swapping in otlptracegrpc is a change to this function and nothing else: every
// call site uses the OTel API, not the exporter.
func InitTracing(cfg TracingConfig, logger *slog.Logger) (*Tracing, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// The propagator is installed even when tracing is disabled. Otherwise a
	// disabled service in the middle of a call chain would silently break the
	// trace for the services on either side of it.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if !cfg.Enabled || strings.EqualFold(cfg.Exporter, "none") {
		// A no-op provider, not nil. Instrumentation calls remain valid and the
		// branch-per-call-site alternative would litter the codebase.
		otel.SetTracerProvider(noop.NewTracerProvider())
		logger.Info("tracing disabled")
		return &Tracing{tracer: otel.Tracer(TracerName), logger: logger}, nil
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = "chronos"
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 1.0
	}

	exporter := &logSpanExporter{logger: newSpanLogger(cfg.SpanOutput)}

	provider := sdktrace.NewTracerProvider(
		// Batching, not synchronous export: a span must never make the request it
		// describes slower, and a blocked exporter must not block the engine.
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		// ParentBased so a sampling decision made upstream is honoured. Without it
		// a trace would be sampled independently at each hop and end up with holes.
		sdktrace.WithSampler(
			sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio)),
		),
		sdktrace.WithResource(buildResource(cfg)),
	)

	otel.SetTracerProvider(provider)
	logger.Info("tracing enabled",
		"service", cfg.ServiceName, "exporter", "log", "sampleRatio", cfg.SampleRatio)

	return &Tracing{provider: provider, tracer: provider.Tracer(TracerName), logger: logger}, nil
}

// Tracer returns the tracer to instrument with.
func (t *Tracing) Tracer() trace.Tracer {
	if t == nil || t.tracer == nil {
		return otel.Tracer(TracerName)
	}
	return t.tracer
}

// Shutdown flushes pending spans.
//
// Worth calling on the shutdown path: with a batching exporter, the spans
// describing whatever went wrong just before a crash are exactly the ones still
// sitting in the buffer.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return t.provider.Shutdown(shutdownCtx)
}

// buildResource describes *this process* on every span it emits. Without it a
// trace spanning the API, the scheduler, and three workers would not say which
// span came from where, which is most of the value of tracing a distributed system.
func buildResource(cfg TracingConfig) *resource.Resource {
	host, _ := os.Hostname()
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.HostName(host),
		// deployment.environment.name spelled explicitly: the generated helper for
		// it is absent from this semconv version, and hand-writing the conventional
		// key is better than inventing a non-standard one a backend will not group on.
		attribute.String("deployment.environment.name", cfg.Environment),
	)
}

// ---------------------------------------------------------------------------
// Span helpers
// ---------------------------------------------------------------------------

// Start begins a span using the global tracer.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(TracerName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// End closes a span, recording err when non-nil.
//
// Centralized so that "what counts as an error on a span" is decided once.
// Notably, a context cancellation during shutdown is not an error: marking every
// span red during a routine deploy would make the error rate meaningless.
func End(span trace.Span, err error) {
	if span == nil {
		return
	}
	defer span.End()

	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		span.SetStatus(codes.Unset, "canceled")
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// TraceparentFrom serializes the current span context as a W3C traceparent
// header value, or "" when there is no recording span.
//
// This is what lets a trace survive the durable task queue. An execution's tasks
// are scheduled by one process, minutes later, and executed by another; without
// carrying the trace context through the database, a workflow would appear as a
// scatter of unrelated single-span traces instead of one causal story.
func TraceparentFrom(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextFromTraceparent rebuilds a span context from a stored traceparent, so a
// span created now becomes a child of the workflow that started hours ago.
func ContextFromTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// TraceIDFrom returns the current trace ID, or "" if untraced. Used to stamp the
// trace ID onto log lines so a log and a trace can be joined.
func TraceIDFrom(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// ---------------------------------------------------------------------------
// Log span exporter
// ---------------------------------------------------------------------------

// logSpanExporter writes finished spans to the structured log stream.
//
// It implements sdktrace.SpanExporter, which is a two-method interface, so this is
// a genuine OTel exporter rather than a parallel logging scheme. The emitted
// fields are the OTLP span fields, which is what makes a collector able to
// reconstruct real spans from them.
type logSpanExporter struct {
	logger *slog.Logger
}

// newSpanLogger builds the logger the span exporter writes to.
//
// Deliberately independent of the application logger, for two reasons that were
// both found the hard way by turning tracing on and seeing nothing at all.
//
// First, the level. Spans used to be emitted through the application logger at
// Debug, which meant a process running at the default `info` level exported every
// span into a discard. `CHRONOS_TRACING_ENABLED=true` appeared to do nothing, with
// no indication why. Enabling tracing is already an explicit request for this
// data, so the span stream is not gated a second time by a log level; volume is
// controlled by the sample ratio, which is the knob that actually belongs to
// tracing.
//
// Second, the format. This stream is machine-consumed — a collector's filelog
// receiver parses it back into OTLP — so it is always JSON, even when the
// application logs are text for human reading. A span rendered as text prose is
// useless to both audiences.
//
// The `stream: spans` field lets a log pipeline route these lines away from the
// application logs rather than having to pattern-match on message text.
func newSpanLogger(out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stderr
	}
	handler := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(handler).With("stream", "spans")
}

func (e *logSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for _, span := range spans {
		sc := span.SpanContext()

		attrs := []any{
			"traceId", sc.TraceID().String(),
			"spanId", sc.SpanID().String(),
			"name", span.Name(),
			"kind", span.SpanKind().String(),
			"durationMs", span.EndTime().Sub(span.StartTime()).Milliseconds(),
			"startTime", span.StartTime().UTC().Format(time.RFC3339Nano),
		}
		if parent := span.Parent(); parent.IsValid() {
			attrs = append(attrs, "parentSpanId", parent.SpanID().String())
		}
		if status := span.Status(); status.Code != codes.Unset {
			attrs = append(attrs, "statusCode", status.Code.String())
			if status.Description != "" {
				attrs = append(attrs, "statusMessage", status.Description)
			}
		}
		for _, attr := range span.Attributes() {
			attrs = append(attrs, string(attr.Key), attr.Value.Emit())
		}

		// Info on a dedicated span logger, so this line is emitted whenever tracing
		// is enabled regardless of the application's log level. See newSpanLogger.
		e.logger.Info("span", attrs...)
	}
	return nil
}

func (e *logSpanExporter) Shutdown(context.Context) error { return nil }

// Common span attribute keys, defined once so a dashboard query does not have to
// guess whether it is workflow or workflow_name.
func WorkflowAttr(name string) attribute.KeyValue { return attribute.String("chronos.workflow", name) }
func ActivityAttr(name string) attribute.KeyValue { return attribute.String("chronos.activity", name) }
func TaskAttr(name string) attribute.KeyValue     { return attribute.String("chronos.task", name) }
func ExecutionAttr(id fmt.Stringer) attribute.KeyValue {
	return attribute.String("chronos.execution_id", id.String())
}
func AttemptAttr(n int) attribute.KeyValue { return attribute.Int("chronos.attempt", n) }
func StateAttr(s string) attribute.KeyValue {
	return attribute.String("chronos.state", s)
}
