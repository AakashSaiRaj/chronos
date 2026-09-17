// Package testsupport provides shared helpers for Chronos integration tests.
//
// Integration tests need a real PostgreSQL: the engine's correctness rests on
// SKIP LOCKED, partial unique indexes, and row-level locking, none of which a
// fake would exercise. Tests skip themselves when no database is configured so
// `go test ./...` still passes on a machine without one.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AakashSaiRaj/chronos/internal/store"
)

// DatabaseURLEnv names the environment variable holding the test database DSN.
const DatabaseURLEnv = "CHRONOS_TEST_DATABASE_URL"

// DefaultTestDatabaseURL points at the podman-compose Postgres from the
// Makefile. Tests fall back to it so `make test-integration` needs no exports.
const DefaultTestDatabaseURL = "postgres://chronos:chronos@127.0.0.1:55432/chronos?sslmode=disable"

var (
	setupOnce sync.Once
	setupErr  error
	schema    string

	resolveOnce sync.Once
	resolvedDSN string
)

// DatabaseURL returns the configured test DSN, or "" when integration tests
// should be skipped.
func DatabaseURL() string {
	resolveOnce.Do(func() {
		if v := strings.TrimSpace(os.Getenv(DatabaseURLEnv)); v != "" {
			resolvedDSN = v
			return
		}
		// Only fall back to the local default when it is actually reachable; a
		// hard-coded DSN must never turn "no database" into a confusing failure.
		if reachable(DefaultTestDatabaseURL) {
			resolvedDSN = DefaultTestDatabaseURL
		}
	})
	return resolvedDSN
}

func reachable(dsn string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s, err := store.Open(ctx, store.Config{
		DatabaseURL:    dsn,
		MaxConns:       1,
		ConnectTimeout: 3 * time.Second,
	}, Logger())
	if err != nil {
		return false
	}
	s.Close()
	return true
}

// NewStore returns a migrated, empty store for a test, skipping the test when no
// database is available.
//
// Each test *package* gets its own PostgreSQL schema, derived from the test
// binary name. That matters because `go test ./...` runs packages in parallel:
// without per-package isolation, one package's cleanup would truncate tables
// another package was actively using. Within a package, tables are truncated
// before each test, so database-backed tests must not call t.Parallel.
func NewStore(t testing.TB) *store.Store {
	t.Helper()

	dsn := DatabaseURL()
	if dsn == "" {
		t.Skipf("skipping integration test: set %s (or run `make db-up`) to enable", DatabaseURLEnv)
	}

	setupOnce.Do(func() {
		schema = schemaName()
		setupErr = prepareSchema(dsn, schema)
	})
	if setupErr != nil {
		t.Fatalf("prepare test schema: %v", setupErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := open(ctx, dsn, schema, 10)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(s.Close)

	Truncate(t, s)
	return s
}

// open connects with search_path pinned to the test schema, so every unqualified
// table name in production SQL resolves inside this package's sandbox.
func open(ctx context.Context, dsn, schema string, maxConns int32) (*store.Store, error) {
	scoped, err := withSearchPath(dsn, schema)
	if err != nil {
		return nil, err
	}
	return store.Open(ctx, store.Config{
		DatabaseURL:    scoped,
		MaxConns:       maxConns,
		ConnectTimeout: 5 * time.Second,
	}, Logger())
}

// withSearchPath rewrites a DSN so connections start with the test schema first
// on their search_path. public stays on the path for anything PostgreSQL
// installs there.
func withSearchPath(dsn, schema string) (string, error) {
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		return "", fmt.Errorf("parse test DSN %q: %w", dsn, err)
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "search_path=" + schema + ",public", nil
}

// prepareSchema creates the package's schema and applies migrations into it.
func prepareSchema(dsn, schema string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// CREATE SCHEMA does not depend on search_path, so a single scoped pool can
	// both create the schema and then build the tables inside it.
	s, err := open(ctx, dsn, schema, 4)
	if err != nil {
		return err
	}
	defer s.Close()

	if _, err := s.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}
	if err := store.Migrate(ctx, s.Pool(), Logger()); err != nil {
		return fmt.Errorf("migrate schema %s: %w", schema, err)
	}
	return nil
}

var unsafeSchemaChars = regexp.MustCompile(`[^a-z0-9_]+`)

// schemaName derives a stable schema name from the test binary, e.g. the store
// package's binary "store.test" becomes "chronos_test_store". Stability means
// repeated runs reuse the schema and skip re-migrating.
func schemaName() string {
	base := filepath.Base(os.Args[0])
	base = strings.TrimSuffix(base, ".test")
	base = strings.TrimSuffix(base, ".exe")
	base = unsafeSchemaChars.ReplaceAllString(strings.ToLower(base), "_")
	if base == "" {
		base = "default"
	}
	return "chronos_test_" + base
}

// Truncate empties all Chronos tables in the test schema, leaving the schema in
// place.
func Truncate(t testing.TB, s *store.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := s.Pool().Exec(ctx, `
		TRUNCATE history_events, tasks, workflow_executions, workflow_definitions, workers
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}
}
