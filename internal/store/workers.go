package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

const workerColumns = `id, name, task_queue, activities, state, registered_at, last_heartbeat_at,
	declared_dead_at`

// RegisterWorkerParams describes a worker announcing itself.
type RegisterWorkerParams struct {
	// Name is the worker's stable logical identity, e.g. a pod name. Reusing it
	// across restarts is intentional.
	Name       string
	TaskQueue  string
	Activities []string
}

// RegisterWorker upserts a worker record.
//
// Registration keys on the worker's logical name so a restarted worker reclaims
// its existing identity instead of leaking a new row on every crash loop. The
// registration is what tells the control plane which activities are servable on
// a queue, and Phase 2's failure detection builds on last_heartbeat_at.
func (s *Store) RegisterWorker(ctx context.Context, p RegisterWorkerParams) (*domain.Worker, error) {
	if p.Name == "" {
		return nil, fmt.Errorf("%w: worker name is required", domain.ErrValidation)
	}
	if p.TaskQueue == "" {
		p.TaskQueue = domain.DefaultTaskQueue
	}
	activities := p.Activities
	if activities == nil {
		activities = []string{}
	}

	// A worker that was previously declared dead and has now come back is
	// resurrected rather than left marked dead: registration is itself proof of
	// life, and the alternative is a permanently unusable worker name.
	row := s.db.QueryRow(ctx, `
		INSERT INTO workers (id, name, task_queue, activities, state)
		VALUES ($1, $2, $3, $4, 'ACTIVE')
		ON CONFLICT (name) DO UPDATE SET
			task_queue        = EXCLUDED.task_queue,
			activities        = EXCLUDED.activities,
			state             = 'ACTIVE',
			declared_dead_at  = NULL,
			last_heartbeat_at = now()
		RETURNING `+workerColumns,
		uuid.New(), p.Name, p.TaskQueue, activities)

	w, err := scanWorker(row)
	if err != nil {
		return nil, translateError(err, "register worker")
	}
	return w, nil
}

// Heartbeat refreshes a worker's liveness timestamp, clearing a dead marking if
// the worker has come back.
func (s *Store) Heartbeat(ctx context.Context, workerID uuid.UUID) (*domain.Worker, error) {
	row := s.db.QueryRow(ctx, `
		UPDATE workers SET
			last_heartbeat_at = now(),
			state             = 'ACTIVE',
			declared_dead_at  = NULL
		WHERE id = $1
		RETURNING `+workerColumns, workerID)
	w, err := scanWorker(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("heartbeat worker %s", workerID))
	}
	return w, nil
}

// MarkStaleWorkersDead declares ACTIVE workers dead once their heartbeat has
// lapsed by more than threshold, returning the ones it changed.
//
// Detection is deliberately separate from lease expiry. A lease tells you a
// specific task went quiet; a lapsed heartbeat tells you an entire worker did,
// which lets the reaper reclaim all of its tasks at once instead of waiting for
// each lease to time out individually. The threshold should be a few heartbeat
// intervals so a single dropped request does not evict a healthy worker.
func (s *Store) MarkStaleWorkersDead(ctx context.Context, threshold time.Duration, limit int) ([]domain.Worker, error) {
	if threshold <= 0 {
		threshold = time.Minute
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.Query(ctx, `
		UPDATE workers SET
			state            = 'DEAD',
			declared_dead_at = now()
		WHERE id IN (
			SELECT id FROM workers
			WHERE state = 'ACTIVE'
			  AND last_heartbeat_at < now() - ($1::int * interval '1 millisecond')
			ORDER BY last_heartbeat_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+workerColumns,
		threshold.Milliseconds(), limit)
	if err != nil {
		return nil, translateError(err, "mark stale workers dead")
	}
	defer rows.Close()

	out := []domain.Worker{}
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, translateError(err, "scan stale worker")
		}
		out = append(out, *w)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate stale workers")
	}
	return out, nil
}

// GetWorker fetches a worker by ID.
func (s *Store) GetWorker(ctx context.Context, id uuid.UUID) (*domain.Worker, error) {
	row := s.db.QueryRow(ctx, `SELECT `+workerColumns+` FROM workers WHERE id = $1`, id)
	w, err := scanWorker(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get worker %s", id))
	}
	return w, nil
}

// ListWorkers returns registered workers, optionally filtered by queue.
func (s *Store) ListWorkers(ctx context.Context, taskQueue string, limit int) ([]domain.Worker, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+workerColumns+` FROM workers
		WHERE ($1 = '' OR task_queue = $1)
		ORDER BY name ASC
		LIMIT $2`, taskQueue, limit)
	if err != nil {
		return nil, translateError(err, "list workers")
	}
	defer rows.Close()

	out := []domain.Worker{}
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, translateError(err, "scan worker")
		}
		out = append(out, *w)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate workers")
	}
	return out, nil
}

func scanWorker(row scanner) (*domain.Worker, error) {
	var (
		w            domain.Worker
		state        string
		activities   []string
		registered   time.Time
		heartbeat    time.Time
		declaredDead *time.Time
	)
	if err := row.Scan(&w.ID, &w.Name, &w.TaskQueue, &activities, &state,
		&registered, &heartbeat, &declaredDead); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, err
	}
	w.State = domain.WorkerState(state)
	w.Activities = activities
	w.RegisteredAt = registered
	w.LastHeartbeatAt = heartbeat
	w.DeclaredDeadAt = declaredDead
	return &w, nil
}
