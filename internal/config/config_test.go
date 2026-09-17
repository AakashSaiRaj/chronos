package config_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/config"
)

// setEnv sets environment variables for one test and restores them afterwards.
// t.Setenv already handles restoration and forbids t.Parallel, which is what we
// want since configuration is process-global.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

// clearEnv blanks every Chronos variable so a test starts from documented
// defaults regardless of what the developer has exported in their shell.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"CHRONOS_DATABASE_URL", "CHRONOS_HTTP_ADDR", "CHRONOS_VERSION",
		"CHRONOS_DB_MAX_CONNS", "CHRONOS_DB_MIN_CONNS", "CHRONOS_DB_CONNECT_TIMEOUT",
		"CHRONOS_DB_MAX_CONN_LIFETIME", "CHRONOS_ENGINE_POLL_INTERVAL",
		"CHRONOS_ENGINE_BATCH_SIZE", "CHRONOS_ENGINE_ENABLED", "CHRONOS_MIGRATE_ON_START",
		"CHRONOS_SHUTDOWN_TIMEOUT", "CHRONOS_LOG_LEVEL", "CHRONOS_LOG_FORMAT",
		"CHRONOS_SERVER_URL", "CHRONOS_WORKER_NAME", "CHRONOS_TASK_QUEUE",
		"CHRONOS_WORKER_CONCURRENCY", "CHRONOS_WORKER_POLL_INTERVAL",
		"CHRONOS_WORKER_LEASE_DURATION", "CHRONOS_WORKER_HEARTBEAT_INTERVAL",
		"CHRONOS_WORKER_TASK_TIMEOUT", "CHRONOS_WORKER_REQUEST_TIMEOUT",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadServerDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := config.LoadServer()
	require.NoError(t, err)

	require.Equal(t, config.DefaultDatabaseURL, cfg.DatabaseURL)
	require.Equal(t, ":8088", cfg.HTTPAddr)
	require.Equal(t, "dev", cfg.Version)
	require.Equal(t, int32(20), cfg.DBMaxConns)
	require.Equal(t, int32(2), cfg.DBMinConns)
	require.Equal(t, 250*time.Millisecond, cfg.EnginePollInterval)
	require.Equal(t, 200, cfg.EngineBatchSize)
	require.True(t, cfg.EngineEnabled, "the engine must run by default")
	require.True(t, cfg.MigrateOnStart, "migrations must apply by default for local runs")
	require.Equal(t, slog.LevelInfo, cfg.Logging.Level)
	require.Equal(t, "text", cfg.Logging.Format)
}

func TestLoadServerReadsEnvironment(t *testing.T) {
	clearEnv(t)
	setEnv(t, map[string]string{
		"CHRONOS_DATABASE_URL":         "postgres://u:p@db:5432/chronos",
		"CHRONOS_HTTP_ADDR":            ":9000",
		"CHRONOS_VERSION":              "1.2.3",
		"CHRONOS_DB_MAX_CONNS":         "50",
		"CHRONOS_DB_MIN_CONNS":         "5",
		"CHRONOS_ENGINE_POLL_INTERVAL": "1s",
		"CHRONOS_ENGINE_BATCH_SIZE":    "500",
		"CHRONOS_ENGINE_ENABLED":       "false",
		"CHRONOS_MIGRATE_ON_START":     "false",
		"CHRONOS_SHUTDOWN_TIMEOUT":     "45s",
		"CHRONOS_LOG_LEVEL":            "debug",
		"CHRONOS_LOG_FORMAT":           "json",
	})

	cfg, err := config.LoadServer()
	require.NoError(t, err)

	require.Equal(t, "postgres://u:p@db:5432/chronos", cfg.DatabaseURL)
	require.Equal(t, ":9000", cfg.HTTPAddr)
	require.Equal(t, "1.2.3", cfg.Version)
	require.Equal(t, int32(50), cfg.DBMaxConns)
	require.Equal(t, int32(5), cfg.DBMinConns)
	require.Equal(t, time.Second, cfg.EnginePollInterval)
	require.Equal(t, 500, cfg.EngineBatchSize)
	require.False(t, cfg.EngineEnabled, "an API-only replica must be configurable")
	require.False(t, cfg.MigrateOnStart)
	require.Equal(t, 45*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, slog.LevelDebug, cfg.Logging.Level)
	require.Equal(t, "json", cfg.Logging.Format)
}

// TestLoadServerRejectsBadValues checks that misconfiguration fails at startup
// rather than at first use, when it would be much harder to diagnose.
func TestLoadServerRejectsBadValues(t *testing.T) {
	tests := map[string]struct {
		env    map[string]string
		expect string
	}{
		"non-numeric max conns": {
			map[string]string{"CHRONOS_DB_MAX_CONNS": "lots"}, "must be an integer"},
		"max conns out of range": {
			map[string]string{"CHRONOS_DB_MAX_CONNS": "0"}, "must be between"},
		"bad duration": {
			map[string]string{"CHRONOS_ENGINE_POLL_INTERVAL": "soon"}, "must be a duration"},
		"negative duration": {
			map[string]string{"CHRONOS_ENGINE_POLL_INTERVAL": "-5s"}, "must be positive"},
		"bad boolean": {
			map[string]string{"CHRONOS_ENGINE_ENABLED": "maybe"}, "must be a boolean"},
		"bad log level": {
			map[string]string{"CHRONOS_LOG_LEVEL": "chatty"}, "not a valid level"},
		"bad log format": {
			map[string]string{"CHRONOS_LOG_FORMAT": "xml"}, "must be 'text' or 'json'"},
		"min conns exceeds max": {
			map[string]string{"CHRONOS_DB_MIN_CONNS": "30", "CHRONOS_DB_MAX_CONNS": "10"},
			"must not exceed"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, tc.env)

			_, err := config.LoadServer()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expect)
		})
	}
}

// TestLoadServerReportsAllProblemsAtOnce saves an operator from fixing
// configuration one variable per restart.
func TestLoadServerReportsAllProblemsAtOnce(t *testing.T) {
	clearEnv(t)
	setEnv(t, map[string]string{
		"CHRONOS_DB_MAX_CONNS":         "nope",
		"CHRONOS_ENGINE_POLL_INTERVAL": "soon",
		"CHRONOS_LOG_FORMAT":           "xml",
	})

	_, err := config.LoadServer()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "CHRONOS_DB_MAX_CONNS")
	require.Contains(t, msg, "CHRONOS_ENGINE_POLL_INTERVAL")
	require.Contains(t, msg, "CHRONOS_LOG_FORMAT")
}

func TestLoadWorkerDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := config.LoadWorker()
	require.NoError(t, err)

	require.Equal(t, config.DefaultServerURL, cfg.ServerURL)
	require.NotEmpty(t, cfg.Name, "a worker must always have an identity")
	require.Equal(t, "default", cfg.TaskQueue)
	require.Equal(t, 4, cfg.Concurrency)
	require.Equal(t, 250*time.Millisecond, cfg.PollInterval)
	require.Equal(t, 30*time.Second, cfg.LeaseDuration)
	require.Equal(t, 10*time.Second, cfg.HeartbeatInterval)
	require.Less(t, cfg.HeartbeatInterval, cfg.LeaseDuration,
		"the default heartbeat must be well inside the default lease")
}

func TestLoadWorkerNormalizesServerURL(t *testing.T) {
	clearEnv(t)
	setEnv(t, map[string]string{"CHRONOS_SERVER_URL": "http://chronos:8080/"})

	cfg, err := config.LoadWorker()
	require.NoError(t, err)
	require.Equal(t, "http://chronos:8080", cfg.ServerURL,
		"a trailing slash must be trimmed so paths do not double up")
}

// TestLoadWorkerRejectsHeartbeatAtOrBeyondLease catches a configuration that
// would let leases lapse while the worker still believed it was healthy.
func TestLoadWorkerRejectsHeartbeatAtOrBeyondLease(t *testing.T) {
	for _, heartbeat := range []string{"30s", "45s"} {
		clearEnv(t)
		setEnv(t, map[string]string{
			"CHRONOS_WORKER_LEASE_DURATION":     "30s",
			"CHRONOS_WORKER_HEARTBEAT_INTERVAL": heartbeat,
		})

		_, err := config.LoadWorker()
		require.Error(t, err, "heartbeat %s vs lease 30s must be rejected", heartbeat)
		require.Contains(t, err.Error(), "must be shorter than")
	}
}

func TestLoadWorkerRejectsBadValues(t *testing.T) {
	tests := map[string]map[string]string{
		"zero concurrency":    {"CHRONOS_WORKER_CONCURRENCY": "0"},
		"huge concurrency":    {"CHRONOS_WORKER_CONCURRENCY": "100000"},
		"bad poll interval":   {"CHRONOS_WORKER_POLL_INTERVAL": "often"},
		"bad request timeout": {"CHRONOS_WORKER_REQUEST_TIMEOUT": "-1s"},
	}

	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			setEnv(t, env)

			_, err := config.LoadWorker()
			require.Error(t, err)
		})
	}
}

func TestNewLoggerHonoursFormatAndLevel(t *testing.T) {
	textLogger := config.NewLogger(config.Logging{Level: slog.LevelWarn, Format: "text"})
	require.NotNil(t, textLogger)
	require.True(t, textLogger.Enabled(nil, slog.LevelWarn))
	require.False(t, textLogger.Enabled(nil, slog.LevelInfo),
		"a warn-level logger must drop info records")

	jsonLogger := config.NewLogger(config.Logging{Level: slog.LevelDebug, Format: "json"})
	require.NotNil(t, jsonLogger)
	require.True(t, jsonLogger.Enabled(nil, slog.LevelDebug))
}
