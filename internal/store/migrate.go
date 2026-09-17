package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migration is a single forward-only schema change.
type migration struct {
	Name string
	SQL  string
}

// advisoryLockKey namespaces the migration lock. Any value works as long as it
// is stable; this is the low 63 bits of a hash of "chronos.migrations".
const advisoryLockKey int64 = 0x6368726f6e6f7301

// Migrate applies any un-applied migrations in lexical filename order.
//
// It is safe to call concurrently from every replica at startup: a session-level
// advisory lock serializes the runners, and each migration plus its ledger row
// commit in one transaction so a crash can never leave a half-applied version
// recorded as done. Applied migrations are checksummed, so editing a migration
// that has already run is reported as an error instead of silently diverging
// environments.
func Migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		// Best effort: releasing on a broken connection is moot because the
		// lock dies with the session anyway.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       TEXT        PRIMARY KEY,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT name, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var name, checksum string
		if err := rows.Scan(&name, &checksum); err != nil {
			rows.Close()
			return fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[name] = checksum
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	pending := 0
	for _, m := range migrations {
		sum := checksum(m.SQL)
		if existing, ok := applied[m.Name]; ok {
			if existing != sum {
				return fmt.Errorf(
					"migration %s was already applied with checksum %s but now hashes to %s: "+
						"migrations are immutable, add a new one instead",
					m.Name, existing, sum)
			}
			continue
		}

		if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("apply migration %s: %w", m.Name, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (name, checksum) VALUES ($1, $2)`,
				m.Name, sum); err != nil {
				return fmt.Errorf("record migration %s: %w", m.Name, err)
			}
			return nil
		}); err != nil {
			return err
		}

		pending++
		if logger != nil {
			logger.Info("applied migration", "migration", m.Name)
		}
	}

	if logger != nil {
		logger.Info("schema up to date", "applied", pending, "total", len(migrations))
	}
	return nil
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("glob migrations: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no embedded migrations found")
	}
	sort.Strings(entries)

	out := make([]migration, 0, len(entries))
	for _, path := range entries {
		body, err := migrationFS.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", path, err)
		}
		out = append(out, migration{
			Name: strings.TrimPrefix(path, "migrations/"),
			SQL:  string(body),
		})
	}
	return out, nil
}

func checksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
