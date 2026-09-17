// Command chronos-server runs the Chronos control plane: the REST API and the
// workflow scheduling engine.
//
// The two are colocated for local convenience but decoupled in code, so
// CHRONOS_ENGINE_ENABLED=false yields an API-only replica and the scheduler can
// be scaled independently in a cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AakashSaiRaj/chronos/internal/api"
	"github.com/AakashSaiRaj/chronos/internal/config"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// version is injected at build time via -ldflags "-X main.version=...". It is
// reported by /healthz so a running deployment is identifiable.
var version = "dev"

func main() {
	// -healthcheck lets the container image probe itself without needing curl in
	// the final image, which keeps it distroless-friendly.
	healthcheck := flag.String("healthcheck", "", "probe the given URL and exit 0 when healthy")
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	if *healthcheck != "" {
		if err := probe(*healthcheck); err != nil {
			fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		// slog may not be configured yet, so write plainly to stderr.
		fmt.Fprintf(os.Stderr, "chronos-server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadServer()
	if err != nil {
		return err
	}
	// An explicit CHRONOS_VERSION wins; otherwise report the build-time value.
	if cfg.Version == "dev" && version != "dev" {
		cfg.Version = version
	}
	logger := config.NewLogger(cfg.Logging)
	slog.SetDefault(logger)

	// Cancel on SIGINT/SIGTERM so Kubernetes and Ctrl-C both trigger the same
	// graceful shutdown path.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting chronos-server",
		"version", cfg.Version, "addr", cfg.HTTPAddr, "engineEnabled", cfg.EngineEnabled)

	db, err := store.Open(ctx, store.Config{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        cfg.DBMaxConns,
		MinConns:        cfg.DBMinConns,
		ConnectTimeout:  cfg.DBConnectTimeout,
		MaxConnLifetime: cfg.DBMaxConnLifetime,
	}, logger)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer db.Close()
	logger.Info("database connected")

	if cfg.MigrateOnStart {
		if err := store.Migrate(ctx, db.Pool(), logger); err != nil {
			return fmt.Errorf("apply migrations: %w", err)
		}
	}

	eng := engine.New(db, engine.Config{
		PollInterval: cfg.EnginePollInterval,
		BatchSize:    cfg.EngineBatchSize,
	}, logger)

	// The service nudges the engine after durable writes so scheduling latency
	// is not bounded by the poll interval.
	svc := engine.NewService(db, eng, logger)
	srv := api.NewHTTPServer(cfg.HTTPAddr, api.NewServer(svc, api.Options{
		Logger:  logger,
		Version: cfg.Version,
	}))

	// Bind before declaring startup complete, so a port conflict fails fast
	// instead of surfacing as an unexplained lack of traffic.
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	if cfg.EngineEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := eng.Run(ctx); err != nil {
				errCh <- fmt.Errorf("engine: %w", err)
			}
		}()
	} else {
		logger.Warn("engine disabled; this replica serves the API only")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("http server listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	// Wait for a shutdown signal or a fatal subsystem error.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("fatal error, shutting down", "error", err)
		stop()
		shutdownHTTP(srv, cfg.ShutdownTimeout, logger)
		wg.Wait()
		return err
	}

	// Stop accepting new requests, then let in-flight ones drain. The engine
	// stops via ctx; any execution it was mid-sweep on simply rolls back and is
	// re-derived from persisted state by whoever runs next.
	shutdownHTTP(srv, cfg.ShutdownTimeout, logger)
	wg.Wait()

	logger.Info("chronos-server stopped")
	return nil
}

func shutdownHTTP(srv *http.Server, timeout time.Duration, logger *slog.Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful shutdown incomplete, closing connections", "error", err)
		_ = srv.Close()
	}
}

// probe implements the -healthcheck mode.
func probe(url string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
}
