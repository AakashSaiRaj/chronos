// Package config loads process configuration from the environment.
//
// Everything is environment-driven with explicit defaults so the same binary
// runs unchanged from a laptop, a container, or a Kubernetes pod — the Phase 3
// deployment target. Values are validated at startup so a misconfigured process
// fails immediately rather than at first use.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultDatabaseURL matches the podman-compose Postgres in compose.yaml.
const DefaultDatabaseURL = "postgres://chronos:chronos@127.0.0.1:55432/chronos?sslmode=disable"

// DefaultServerURL matches the server's default bind address.
const DefaultServerURL = "http://127.0.0.1:8088"

// Logging is shared by every binary.
type Logging struct {
	Level  slog.Level
	Format string // "json" or "text"
}

// Server configures the API + engine process.
type Server struct {
	DatabaseURL string
	HTTPAddr    string

	DBMaxConns        int32
	DBMinConns        int32
	DBConnectTimeout  time.Duration
	DBMaxConnLifetime time.Duration

	EnginePollInterval time.Duration
	EngineBatchSize    int
	// EngineEnabled allows running an API-only replica, so the scheduler can be
	// scaled independently of the API in Phase 3.
	EngineEnabled bool

	// ReaperEnabled turns on failure detection. Separate from the engine so the
	// two can be scaled or rolled independently.
	ReaperEnabled bool
	// ReaperInterval is how often expired leases and silent workers are swept
	// for. It bounds how long a lost task waits before being reclaimed.
	ReaperInterval time.Duration
	// ReaperBatchSize bounds how much one reaper pass reclaims.
	ReaperBatchSize int
	// WorkerTimeout is how long a worker's heartbeat may lapse before it is
	// declared dead. Must be comfortably larger than a worker's heartbeat
	// interval so one dropped request cannot evict a healthy worker.
	WorkerTimeout time.Duration
	// MigrateOnStart applies pending migrations during startup. Convenient
	// locally; in a cluster a dedicated migration Job is preferable.
	MigrateOnStart bool

	ShutdownTimeout time.Duration
	Version         string
	Logging         Logging
}

// Worker configures a task-executing process.
type Worker struct {
	ServerURL string
	Name      string
	TaskQueue string

	// Concurrency is how many tasks this worker executes at once.
	Concurrency int
	// PollInterval is the backoff between polls that found no work.
	PollInterval time.Duration
	// LeaseDuration is how long the worker asks to hold a claimed task.
	LeaseDuration time.Duration
	// HeartbeatInterval is how often liveness is refreshed.
	HeartbeatInterval time.Duration
	// TaskTimeout bounds a single activity invocation when the task itself
	// declares no timeout.
	TaskTimeout time.Duration

	// HealthAddr is where the worker serves liveness and readiness probes. A
	// worker has no other inbound surface, so without this Kubernetes has nothing
	// to ask. Empty disables it.
	HealthAddr string

	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	Logging         Logging
}

// LoadServer reads server configuration from the environment.
func LoadServer() (Server, error) {
	var problems []string
	collect := func(err error) {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}

	cfg := Server{
		HTTPAddr: envString("CHRONOS_HTTP_ADDR", ":8088"),
		Version:  envString("CHRONOS_VERSION", "dev"),
	}

	// The DSN may arrive as a file, which is how a secret manager delivers it.
	databaseURL, err := envSecret("CHRONOS_DATABASE_URL", DefaultDatabaseURL)
	collect(err)
	cfg.DatabaseURL = databaseURL

	maxConns, err := envInt("CHRONOS_DB_MAX_CONNS", 20, 1, 1000)
	collect(err)
	cfg.DBMaxConns = int32(maxConns) //nolint:gosec // bounded above by envInt

	minConns, err := envInt("CHRONOS_DB_MIN_CONNS", 2, 0, 1000)
	collect(err)
	cfg.DBMinConns = int32(minConns)

	cfg.DBConnectTimeout, err = envDuration("CHRONOS_DB_CONNECT_TIMEOUT", 10*time.Second)
	collect(err)
	cfg.DBMaxConnLifetime, err = envDuration("CHRONOS_DB_MAX_CONN_LIFETIME", time.Hour)
	collect(err)

	cfg.EnginePollInterval, err = envDuration("CHRONOS_ENGINE_POLL_INTERVAL", 250*time.Millisecond)
	collect(err)
	cfg.EngineBatchSize, err = envInt("CHRONOS_ENGINE_BATCH_SIZE", 200, 1, 10_000)
	collect(err)
	cfg.EngineEnabled, err = envBool("CHRONOS_ENGINE_ENABLED", true)
	collect(err)

	cfg.ReaperEnabled, err = envBool("CHRONOS_REAPER_ENABLED", true)
	collect(err)
	cfg.ReaperInterval, err = envDuration("CHRONOS_REAPER_INTERVAL", time.Second)
	collect(err)
	cfg.ReaperBatchSize, err = envInt("CHRONOS_REAPER_BATCH_SIZE", 100, 1, 10_000)
	collect(err)
	cfg.WorkerTimeout, err = envDuration("CHRONOS_WORKER_TIMEOUT", 45*time.Second)
	collect(err)

	cfg.MigrateOnStart, err = envBool("CHRONOS_MIGRATE_ON_START", true)
	collect(err)
	cfg.ShutdownTimeout, err = envDuration("CHRONOS_SHUTDOWN_TIMEOUT", 20*time.Second)
	collect(err)

	cfg.Logging, err = loadLogging()
	collect(err)

	if cfg.DatabaseURL == "" {
		problems = append(problems, "CHRONOS_DATABASE_URL must not be empty")
	}
	if cfg.DBMinConns > cfg.DBMaxConns {
		problems = append(problems,
			fmt.Sprintf("CHRONOS_DB_MIN_CONNS (%d) must not exceed CHRONOS_DB_MAX_CONNS (%d)",
				cfg.DBMinConns, cfg.DBMaxConns))
	}
	// Sweeping less often than the detection threshold would mean a dead worker
	// could go unnoticed for far longer than WorkerTimeout implies.
	if cfg.ReaperInterval >= cfg.WorkerTimeout {
		problems = append(problems, fmt.Sprintf(
			"CHRONOS_REAPER_INTERVAL (%s) must be shorter than CHRONOS_WORKER_TIMEOUT (%s)",
			cfg.ReaperInterval, cfg.WorkerTimeout))
	}

	if len(problems) > 0 {
		return Server{}, fmt.Errorf("invalid server configuration: %s", strings.Join(problems, "; "))
	}
	return cfg, nil
}

// LoadWorker reads worker configuration from the environment.
func LoadWorker() (Worker, error) {
	var problems []string
	collect := func(err error) {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}

	cfg := Worker{
		ServerURL:  strings.TrimRight(envString("CHRONOS_SERVER_URL", DefaultServerURL), "/"),
		Name:       envString("CHRONOS_WORKER_NAME", defaultWorkerName()),
		TaskQueue:  envString("CHRONOS_TASK_QUEUE", "default"),
		HealthAddr: envString("CHRONOS_WORKER_HEALTH_ADDR", ":8090"),
	}

	var err error
	cfg.Concurrency, err = envInt("CHRONOS_WORKER_CONCURRENCY", 4, 1, 1024)
	collect(err)
	cfg.PollInterval, err = envDuration("CHRONOS_WORKER_POLL_INTERVAL", 250*time.Millisecond)
	collect(err)
	cfg.LeaseDuration, err = envDuration("CHRONOS_WORKER_LEASE_DURATION", 30*time.Second)
	collect(err)
	cfg.HeartbeatInterval, err = envDuration("CHRONOS_WORKER_HEARTBEAT_INTERVAL", 10*time.Second)
	collect(err)
	cfg.TaskTimeout, err = envDuration("CHRONOS_WORKER_TASK_TIMEOUT", 5*time.Minute)
	collect(err)
	cfg.RequestTimeout, err = envDuration("CHRONOS_WORKER_REQUEST_TIMEOUT", 15*time.Second)
	collect(err)
	cfg.ShutdownTimeout, err = envDuration("CHRONOS_SHUTDOWN_TIMEOUT", 20*time.Second)
	collect(err)

	cfg.Logging, err = loadLogging()
	collect(err)

	if cfg.ServerURL == "" {
		problems = append(problems, "CHRONOS_SERVER_URL must not be empty")
	}
	if cfg.Name == "" {
		problems = append(problems, "CHRONOS_WORKER_NAME must not be empty")
	}
	// A lease shorter than the heartbeat interval would let leases lapse while
	// the worker still believes it is healthy.
	if cfg.LeaseDuration > 0 && cfg.HeartbeatInterval >= cfg.LeaseDuration {
		problems = append(problems, fmt.Sprintf(
			"CHRONOS_WORKER_HEARTBEAT_INTERVAL (%s) must be shorter than CHRONOS_WORKER_LEASE_DURATION (%s)",
			cfg.HeartbeatInterval, cfg.LeaseDuration))
	}

	if len(problems) > 0 {
		return Worker{}, fmt.Errorf("invalid worker configuration: %s", strings.Join(problems, "; "))
	}
	return cfg, nil
}

func loadLogging() (Logging, error) {
	format := strings.ToLower(envString("CHRONOS_LOG_FORMAT", "text"))
	if format != "text" && format != "json" {
		return Logging{}, fmt.Errorf("CHRONOS_LOG_FORMAT must be 'text' or 'json', got %q", format)
	}

	var level slog.Level
	raw := envString("CHRONOS_LOG_LEVEL", "info")
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return Logging{}, fmt.Errorf("CHRONOS_LOG_LEVEL %q is not a valid level", raw)
	}
	return Logging{Level: level, Format: format}, nil
}

// NewLogger builds the process logger.
func NewLogger(l Logging) *slog.Logger {
	opts := &slog.HandlerOptions{Level: l.Level}
	if l.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

// defaultWorkerName derives a stable-per-host identity so a worker that restarts
// reuses its registration instead of leaking a new row each time.
func defaultWorkerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "worker-local"
	}
	return "worker-" + host
}

func envString(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envSecret reads a value that may be supplied indirectly via <KEY>_FILE.
//
// Secret managers mount credentials as files, not environment variables — for
// good reason: an env var is visible in `kubectl describe`, in a crash dump, and
// to anything that can read /proc/<pid>/environ, and it cannot be rotated without
// restarting the process. Supporting the file form means the DSN never has to
// pass through the environment at all.
//
// The direct value still wins when set, so local development and compose stay
// unchanged.
func envSecret(key, def string) (string, error) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v, nil
	}

	path := strings.TrimSpace(os.Getenv(key + "_FILE"))
	if path == "" {
		return def, nil
	}

	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s_FILE: read %s: %w", key, path, err)
	}
	value := strings.TrimSpace(string(body))
	if value == "" {
		return "", fmt.Errorf("%s_FILE: %s is empty", key, path)
	}
	return value, nil
}

func envInt(key string, def, min, max int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", key, raw)
	}
	if v < min || v > max {
		return 0, fmt.Errorf("%s must be between %d and %d, got %d", key, min, max, v)
	}
	return v, nil
}

func envBool(key string, def bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean, got %q", key, raw)
	}
	return v, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration like '250ms' or '30s', got %q", key, raw)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", key, raw)
	}
	return v, nil
}
