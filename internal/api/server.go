// Package api exposes Chronos's control plane over HTTP/JSON.
//
// The layer is deliberately thin: it parses and validates wire input, delegates
// to engine.Service, and maps domain errors onto status codes. No scheduling or
// persistence logic lives here, which is what allows the engine to be driven
// just as well from tests or a future gRPC front end.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// Server wires routes onto an engine service.
type Server struct {
	svc     Service
	logger  *slog.Logger
	handler http.Handler
	version string
	metrics *telemetry.Metrics
}

// Options configures a Server.
type Options struct {
	Logger *slog.Logger
	// Version is reported by /healthz to make deployments identifiable.
	Version string
	// Metrics enables the /metrics endpoint. Nil omits the route entirely rather
	// than serving an empty one, so a misconfigured scrape fails loudly.
	Metrics *telemetry.Metrics
	// ServiceName labels spans, distinguishing the API from the scheduler.
	ServiceName string
}

// NewServer builds the HTTP handler tree.
func NewServer(svc Service, opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := opts.Version
	if version == "" {
		version = "dev"
	}

	service := opts.ServiceName
	if service == "" {
		service = "chronos-api"
	}

	s := &Server{svc: svc, logger: logger, version: version, metrics: opts.Metrics}

	mux := s.routes()

	// Tracing is the outermost layer so a span covers the whole request, including
	// time spent in the middleware below it. The request-ID middleware sits inside
	// it so log lines can carry both the request ID and the trace ID.
	s.handler = telemetry.InstrumentHandler(
		chain(mux,
			// Outermost of the inner chain, so the span carries its route before any
			// other middleware runs or logs against it.
			withSpanRoute(mux),
			withRequestID,
			withRecovery(logger),
			withAccessLog(logger),
			withBodyLimit,
		),
		service,
	)
	return s
}

// withSpanRoute renames the request's span from the raw path to its route pattern.
//
// This has to happen here rather than in the tracing wrapper because of an
// ordering problem: the span is created outside the mux, so at creation time the
// request has not been routed yet and net/http has not populated r.Pattern. The
// only name available that early is the literal path, which for this API means
// span names like "POST /v1/tasks/13de3e6c-37da-40e5-a3ce-fa09959f845c/complete" —
// one distinct span name per task, so no tracing backend can group or aggregate
// them.
//
// mux.Handler performs the route lookup without serving, which is what makes the
// pattern available at this point. It costs a second match per request, which is a
// map lookup against a table of two dozen patterns — cheap next to the database
// work every one of these endpoints goes on to do.
func withSpanRoute(mux *http.ServeMux) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, pattern := mux.Handler(r); pattern != "" {
				telemetry.SetSpanRoute(r, pattern)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Handler exposes the fully wrapped handler, e.g. for httptest.
func (s *Server) Handler() http.Handler { return s.handler }

// routes declares the API surface. Method and path patterns come from
// net/http's router, so no third-party mux is needed.
//
// Returns the concrete *http.ServeMux rather than http.Handler because
// withSpanRoute needs mux.Handler to resolve a request's route pattern.
func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()

	// Liveness and readiness are separate on purpose: a pod with a temporarily
	// unreachable database is not ready to serve, but killing and restarting it
	// would not help, so liveness must not depend on the database.
	mux.HandleFunc("GET /healthz", s.handleLive)
	mux.HandleFunc("GET /readyz", s.handleReady)

	// Prometheus scrape endpoint. Registered only when metrics are enabled, so a
	// scrape against a build without them fails visibly instead of reporting zero.
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.MetricsHandler(s.logger))
	}

	// Workflow definitions.
	mux.HandleFunc("POST /v1/workflows", s.handleRegisterWorkflow)
	mux.HandleFunc("GET /v1/workflows", s.handleListWorkflows)
	mux.HandleFunc("GET /v1/workflows/{name}", s.handleGetLatestWorkflow)
	mux.HandleFunc("GET /v1/workflows/{name}/versions/{version}", s.handleGetWorkflowVersion)

	// Executions.
	mux.HandleFunc("POST /v1/executions", s.handleStartExecution)
	mux.HandleFunc("GET /v1/executions", s.handleListExecutions)
	mux.HandleFunc("GET /v1/executions/{id}", s.handleGetExecution)
	mux.HandleFunc("GET /v1/executions/{id}/history", s.handleGetHistory)
	mux.HandleFunc("POST /v1/executions/{id}/cancel", s.handleCancelExecution)

	// Workers.
	mux.HandleFunc("POST /v1/workers", s.handleRegisterWorker)
	mux.HandleFunc("GET /v1/workers", s.handleListWorkers)
	mux.HandleFunc("POST /v1/workers/{id}/heartbeat", s.handleHeartbeat)

	// Task dispatch. These are the worker-facing data-plane endpoints.
	mux.HandleFunc("POST /v1/tasks/poll", s.handlePollTask)
	mux.HandleFunc("POST /v1/tasks/{id}/complete", s.handleCompleteTask)
	mux.HandleFunc("POST /v1/tasks/{id}/fail", s.handleFailTask)
	// Lease renewal doubles as the cooperative-cancellation channel.
	mux.HandleFunc("POST /v1/tasks/{id}/heartbeat", s.handleHeartbeatTask)

	// Dead letter queue: the operator surface for tasks that could not succeed.
	mux.HandleFunc("GET /v1/dead-letter", s.handleListDeadLetter)
	mux.HandleFunc("POST /v1/tasks/{id}/replay", s.handleReplayTask)

	return mux
}

// NewHTTPServer wraps a handler in an http.Server with timeouts chosen for
// Chronos's traffic mix.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:    addr,
		Handler: h,
		// Generous read/write timeouts: task payloads are small but a worker on
		// a slow link should not have its result rejected.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		// Long idle timeout so worker poll connections stay pooled instead of
		// reconnecting on every poll.
		IdleTimeout: 120 * time.Second,
	}
}
