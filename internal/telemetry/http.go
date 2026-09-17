package telemetry

import (
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// MetricsHandler serves the Prometheus exposition endpoint.
//
// Served from Chronos's own registry rather than the default one, so the exposed
// series are exactly the ones declared in metrics.go.
func (m *Metrics) MetricsHandler(logger *slog.Logger) http.Handler {
	if m == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics are disabled", http.StatusNotFound)
		})
	}

	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A broken collector must not silently truncate the scrape: log it and
		// still serve whatever else succeeded, so one bad metric does not blind
		// the entire dashboard.
		ErrorHandling: promhttp.ContinueOnError,
		ErrorLog:      promLogger{logger: logger},
		// Bounds a slow collector. Without it, a scrape can hang for the full
		// Prometheus timeout and the resulting gap looks like the process died.
		Timeout: 8 * time.Second,
		// Concurrent scrapes are the norm with more than one Prometheus, and
		// serialising them would make each wait on the other.
		EnableOpenMetrics: true,
	})
}

type promLogger struct{ logger *slog.Logger }

func (l promLogger) Println(v ...any) {
	if l.logger != nil {
		l.logger.Warn("metrics collection error", "detail", v)
	}
}

// shouldTrace decides whether a request is worth a span.
//
// Applied to both the inbound handler and the outbound transport, so the two sides
// of a call agree and a suppressed request does not produce a half-trace.
//
// What is excluded, and why it matters more than it sounds: the task poll. Workers
// long-poll continuously, so on an idle queue the poll is by far the most frequent
// request in the system — a measured run produced 463 empty polls in two minutes
// from a single worker, each one a distinct root trace containing exactly one span
// and describing nothing. Multiplied by a worker fleet that is a flood of noise
// that buries the traces someone actually wants, and it is billed per span by
// every hosted tracing backend.
//
// Nothing is lost by dropping it. A poll that returns work is interesting, but the
// interesting part is the activity execution that follows, and that span is
// parented to the workflow's own trace via the stored traceparent — a much more
// useful placement than a root span per poll. See Worker.execute.
//
// Lease and worker heartbeats are excluded on the same grounds: fixed-rate
// chatter whose only signal (did it fail) is already a metric.
func shouldTrace(r *http.Request) bool {
	path := r.URL.Path
	switch path {
	case "/metrics", "/healthz", "/readyz", "/v1/tasks/poll":
		return false
	}
	// Heartbeat paths carry an ID, so they are matched by suffix.
	return !strings.HasSuffix(path, "/heartbeat")
}

// InstrumentHandler wraps an HTTP handler with tracing.
//
// Span names are corrected to the route pattern by the API's own middleware once
// routing has happened — see api.withSpanRoute. They cannot be set here: this
// wrapper runs before the mux matches, so r.Pattern is still empty and the only
// name available at this point is the raw path, complete with UUIDs.
func InstrumentHandler(h http.Handler, service string) http.Handler {
	return otelhttp.NewHandler(h, service, otelhttp.WithFilter(shouldTrace))
}

// SetSpanRoute renames the current span to a low-cardinality route pattern and
// records it as http.route.
//
// Necessary because span names are the primary grouping key in every tracing
// backend, and the name otelhttp can derive before routing is the raw path. That
// produced names like "POST /v1/tasks/13de3e6c-…/complete" — a distinct span name
// per task, which makes latency-by-endpoint aggregation impossible and bloats the
// backend's name index. This is the tracing equivalent of putting an ID in a
// metric label.
func SetSpanRoute(r *http.Request, pattern string) {
	if pattern == "" {
		return
	}
	span := trace.SpanFromContext(r.Context())
	if !span.IsRecording() {
		return
	}
	span.SetName(pattern)
	span.SetAttributes(semconv.HTTPRoute(pattern))
}

// InstrumentTransport wraps an HTTP round tripper so outbound calls continue the
// caller's trace.
//
// This is what joins a worker's spans to the control plane's: the traceparent
// header is injected here, and the API's handler extracts it, so a worker's task
// report and the server's handling of it end up in the same trace automatically.
//
// The same shouldTrace filter applies, so a worker's poll loop does not mint a
// root span per poll.
func InstrumentTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base,
		otelhttp.WithFilter(shouldTrace),
		// otelhttp names client spans just "HTTP POST" by default, which says
		// nothing about which call it was. Naming them by templated path makes a
		// trace readable without reintroducing per-ID span names.
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + TemplatePath(r.URL.Path)
		}),
	)
}

// uuidSegment matches a path segment that is a UUID.
var uuidSegment = regexp.MustCompile(
	`/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// TemplatePath replaces UUID path segments with {id}.
//
// The client side has no router to ask for a route pattern — it builds concrete
// paths — so the low-cardinality name has to be recovered from the path itself.
// Without this, every task report would produce its own span name and client-side
// latency could not be aggregated per endpoint, which is the same failure mode
// SetSpanRoute fixes on the server side.
func TemplatePath(path string) string {
	return uuidSegment.ReplaceAllString(path, "/{id}")
}
