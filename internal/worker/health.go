package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// health tracks a worker's liveness and readiness for Kubernetes probes.
//
// A worker has no inbound API of its own, so without this there is nothing for a
// probe to ask. Exec probes were the alternative and are worse: they fork a
// process on every check and cannot see whether the worker is actually talking to
// the control plane.
//
// The distinction matters as much here as it does for the server. Liveness means
// "this process is not wedged"; restarting it is a sensible remedy. Readiness
// means "this worker is registered and polling"; a worker that cannot reach the
// control plane is not ready, but restarting it would not help, so liveness must
// not depend on that.
type health struct {
	registered atomic.Bool
	// lastPollUnixNano records the last poll attempt that got an answer, whether
	// or not it carried a task. Staleness here means the control plane has gone
	// away, not that the queue is empty.
	lastPollUnixNano atomic.Int64
	// pollStaleAfter is how long without a successful poll marks the worker
	// unready.
	pollStaleAfter time.Duration
}

func newHealth(pollStaleAfter time.Duration) *health {
	if pollStaleAfter <= 0 {
		pollStaleAfter = time.Minute
	}
	return &health{pollStaleAfter: pollStaleAfter}
}

func (h *health) markRegistered() { h.registered.Store(true) }

func (h *health) markPolled() { h.lastPollUnixNano.Store(time.Now().UnixNano()) }

// ready reports whether the worker is usefully participating in the fleet.
func (h *health) ready() (bool, string) {
	if !h.registered.Load() {
		return false, "not yet registered with the control plane"
	}
	last := h.lastPollUnixNano.Load()
	if last == 0 {
		return false, "no successful poll yet"
	}
	if since := time.Since(time.Unix(0, last)); since > h.pollStaleAfter {
		return false, "last successful poll was " + since.Round(time.Second).String() + " ago"
	}
	return true, "polling"
}

// healthHandler serves the probe endpoints.
func (h *health) handler(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	write := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			logger.Debug("encode health response failed", "error", err)
		}
	}

	// Liveness deliberately ignores control-plane reachability: a worker that
	// cannot reach the API is not broken, and restarting it would only add churn
	// to an already degraded system.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		ready, detail := h.ready()
		if !ready {
			write(w, http.StatusServiceUnavailable,
				map[string]string{"status": "unavailable", "detail": detail})
			return
		}
		write(w, http.StatusOK, map[string]string{"status": "ok", "detail": detail})
	})

	return mux
}

// ServeHealth starts the probe endpoints and returns once they are listening.
//
// It must be called *before* the worker waits for the control plane, not after.
// A worker starting while the API is still coming up is a normal cold start, and
// during that window liveness has to succeed (the process is fine) while readiness
// fails (it is not registered yet). If the probe endpoint only appeared after
// registration, liveness would fail too and Kubernetes would kill a worker that
// was doing exactly the right thing — turning a slow start into a crash loop.
//
// Binding is done synchronously so a port conflict is reported to the caller
// rather than surfacing later as unexplained probe failures.
func (w *Worker) ServeHealth(ctx context.Context) error {
	if w.cfg.HealthAddr == "" {
		w.logger.Warn("worker health endpoint disabled; Kubernetes probes will not work")
		return nil
	}

	listener, err := net.Listen("tcp", w.cfg.HealthAddr)
	if err != nil {
		return err
	}
	w.logger.Info("worker health endpoint listening", "addr", listener.Addr().String())

	srv := &http.Server{
		Handler:           w.health.handler(w.logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			w.logger.Error("worker health endpoint failed", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	return nil
}
