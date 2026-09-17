package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// PostgreSQL error codes we translate into domain errors.
const (
	pgUniqueViolation     = "23505"
	pgForeignKeyViolation = "23503"
	pgCheckViolation      = "23514"
	pgSerializationFail   = "40001"
	pgDeadlockDetected    = "40P01"
)

// Querier is the subset of pgx shared by *pgxpool.Pool and pgx.Tx. Repository
// methods target this so the same code runs inside or outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store is the durable state gateway. A Store is either pool-backed (each call
// is its own implicit transaction) or tx-backed (created by WithTx, so a group
// of calls commits atomically).
type Store struct {
	db     Querier
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// Config holds connection pool tuning.
type Config struct {
	DatabaseURL     string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
}

// Open connects to PostgreSQL and verifies the connection with a ping.
func Open(ctx context.Context, cfg Config, logger *slog.Logger) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}
	return &Store{db: pool, pool: pool, logger: logger}, nil
}

// Pool exposes the underlying pool for health checks and migrations.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Ping verifies the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if s.pool == nil {
		return errors.New("store has no pool")
	}
	return s.pool.Ping(ctx)
}

// WithTx runs fn inside a single transaction, committing on nil and rolling
// back on error or panic. The Store handed to fn is bound to the transaction.
//
// Nested calls reuse the enclosing transaction rather than opening a second
// one, so a repository method that needs atomicity can be called safely from
// inside a larger transactional operation.
func (s *Store) WithTx(ctx context.Context, fn func(*Store) error) error {
	if _, alreadyInTx := s.db.(pgx.Tx); alreadyInTx {
		return fn(s)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		// Rollback after a successful commit is a no-op, so this is safe as an
		// unconditional cleanup and it also covers the panic path.
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()

	if err := fn(&Store{db: tx, pool: s.pool, logger: s.logger}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// translateError maps driver-level failures onto domain sentinels so callers
// never have to know about PostgreSQL error codes.
func translateError(err error, what string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", what, domain.ErrNotFound)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgUniqueViolation:
			return fmt.Errorf("%s: %w (constraint %s)", what, domain.ErrAlreadyExists, pgErr.ConstraintName)
		case pgForeignKeyViolation:
			return fmt.Errorf("%s: %w: referenced row missing (constraint %s)", what, domain.ErrConflict, pgErr.ConstraintName)
		case pgCheckViolation:
			return fmt.Errorf("%s: %w: check %s rejected the write", what, domain.ErrValidation, pgErr.ConstraintName)
		case pgSerializationFail, pgDeadlockDetected:
			return fmt.Errorf("%s: %w: concurrent update, retry", what, domain.ErrConflict)
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}

// IsRetryable reports whether an error is a transient concurrency failure that
// the caller may retry as-is.
func IsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgSerializationFail || pgErr.Code == pgDeadlockDetected
	}
	return false
}

// isUniqueViolation reports whether err is a raw unique-constraint violation.
// It inspects the driver error directly, before translateError wraps it, so
// optimistic-retry paths can distinguish a lost race from a real fault.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
