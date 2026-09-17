// Command chronos-worker runs a Chronos task worker.
//
// The worker reaches the engine only over the REST API, so it needs no database
// credentials and can be scaled independently of the control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/config"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
	"github.com/AakashSaiRaj/chronos/internal/worker"
	"github.com/AakashSaiRaj/chronos/internal/worker/activities"
)

// startupWait bounds how long the worker waits for the control plane to become
// ready before giving up, so a worker that starts before the server in a compose
// or Kubernetes rollout does not crash-loop.
const startupWait = 60 * time.Second

// version is injected at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "chronos-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	logger := config.NewLogger(cfg.Logging)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var metrics *telemetry.Metrics
	if cfg.Telemetry.MetricsEnabled {
		metrics = telemetry.NewMetrics()
	}

	tracing, err := telemetry.InitTracing(telemetry.TracingConfig{
		Enabled:        cfg.Telemetry.TracingEnabled,
		ServiceName:    cfg.Telemetry.ServiceName,
		ServiceVersion: version,
		Environment:    cfg.Telemetry.Environment,
		SampleRatio:    cfg.Telemetry.TraceSampleRatio,
		Exporter:       cfg.Telemetry.TraceExporter,
	}, logger)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		if err := tracing.Shutdown(context.Background()); err != nil {
			logger.Warn("flushing traces failed", "error", err)
		}
	}()

	api, err := client.New(client.Config{
		BaseURL: cfg.ServerURL,
		Timeout: cfg.RequestTimeout,
		// Instrumented transport: the traceparent header is injected on every
		// outbound call, so a worker's spans join the control plane's trace rather
		// than forming an island. Wrapping preserves the client's connection-pool
		// tuning, which a polling worker depends on.
		WrapTransport: telemetry.InstrumentTransport,
		// Retries are safe because every mutating call is idempotent: task
		// reports are guarded by their claim token.
		MaxRetries: 3,
		Logger:     logger,
	})
	if err != nil {
		return err
	}

	registry := activities.Register(worker.NewRegistry())

	w, err := worker.New(cfg, api, registry, logger)
	if err != nil {
		return err
	}
	w = w.WithMetrics(metrics)

	// Probes come up before anything that can block. A worker waiting for the
	// control plane during a cold start must answer liveness (the process is
	// healthy) while failing readiness (it is not registered yet); if the endpoint
	// only appeared after registration, Kubernetes would kill it mid-wait.
	if err := w.ServeHealth(ctx); err != nil {
		return fmt.Errorf("start worker health endpoint: %w", err)
	}

	if err := waitForServer(ctx, api, logger); err != nil {
		return err
	}

	if err := w.Run(ctx); err != nil {
		return err
	}

	logger.Info("worker shut down cleanly")
	return nil
}

// waitForServer polls readiness until the control plane answers.
func waitForServer(ctx context.Context, api *client.Client, logger *slog.Logger) error {
	deadline := time.Now().Add(startupWait)
	attempt := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := api.Ready(probeCtx)
		cancel()
		if err == nil {
			return nil
		}

		attempt++
		if time.Now().After(deadline) {
			return fmt.Errorf("control plane not ready after %s: %w", startupWait, err)
		}
		logger.Info("waiting for control plane", "attempt", attempt, "error", err)

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
