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
)

// Server wires routes onto an engine service.
type Server struct {
	svc     Service
	logger  *slog.Logger
	handler http.Handler
	version string
}

// Options configures a Server.
type Options struct {
	Logger *slog.Logger
	// Version is reported by /healthz to make deployments identifiable.
	Version string
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

	s := &Server{svc: svc, logger: logger, version: version}
	s.handler = chain(s.routes(),
		withRequestID,
		withRecovery(logger),
		withAccessLog(logger),
		withBodyLimit,
	)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Handler exposes the fully wrapped handler, e.g. for httptest.
func (s *Server) Handler() http.Handler { return s.handler }

// routes declares the API surface. Method and path patterns come from
// net/http's router, so no third-party mux is needed.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Liveness and readiness are separate on purpose: a pod with a temporarily
	// unreachable database is not ready to serve, but killing and restarting it
	// would not help, so liveness must not depend on the database.
	mux.HandleFunc("GET /healthz", s.handleLive)
	mux.HandleFunc("GET /readyz", s.handleReady)

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
