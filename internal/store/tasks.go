package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

const taskColumns = `id, execution_id, name, activity, state, depends_on, input, output, error,
	attempt, max_attempts, timeout_seconds, task_queue, claim_token, worker_id,
	lease_expires_at, scheduled_at, created_at, updated_at, started_at, completed_at`

// MaterializeTasks writes the execution's task graph to durable storage.
//
// It is idempotent by way of the (execution_id, name) unique constraint: if the
// engine crashes after materializing some tasks, the next pass inserts only the
// missing ones. Tasks always land in PENDING so that scheduling — computing
// inputs from upstream outputs and enqueueing — is a single code path handled by
// the dispatcher.
func (s *Store) MaterializeTasks(ctx context.Context, exec *domain.WorkflowExecution, spec *domain.WorkflowSpec) (int, error) {
	order, err := spec.TopologicalOrder()
	if err != nil {
		return 0, err
	}

	inserted := 0
	for _, name := range order {
		ts, ok := spec.Task(name)
		if !ok {
			return inserted, fmt.Errorf("%w: task %q missing from spec", domain.ErrValidation, name)
		}

		staticInput := json.RawMessage(`{}`)
		if len(ts.Input) > 0 {
			staticInput = ts.Input
		}
		// depends_on is NOT NULL; a nil Go slice would be sent as SQL NULL, so
		// normalize the "no dependencies" case to an empty array.
		dependsOn := ts.DependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}

		tag, err := s.db.Exec(ctx, `
			INSERT INTO tasks
				(id, execution_id, name, activity, state, depends_on, input,
				 max_attempts, timeout_seconds, task_queue)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (execution_id, name) DO NOTHING`,
			uuid.New(), exec.ID, ts.Name, ts.Activity, domain.TaskPending,
			dependsOn, staticInput, ts.MaxAttempts, ts.TimeoutSeconds, exec.TaskQueue)
		if err != nil {
			return inserted, translateError(err, fmt.Sprintf("materialize task %q", ts.Name))
		}
		inserted += int(tag.RowsAffected())
	}
	return inserted, nil
}

// ListTasks returns an execution's tasks ordered by name for stable output.
func (s *Store) ListTasks(ctx context.Context, executionID uuid.UUID) ([]domain.Task, error) {
	rows, err := s.db.Query(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE execution_id = $1 ORDER BY name ASC`, executionID)
	if err != nil {
		return nil, translateError(err, "list tasks")
	}
	defer rows.Close()

	out := []domain.Task{}
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, translateError(err, "scan task")
		}
		out = append(out, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate tasks")
	}
	return out, nil
}

// GetTask fetches a task by ID.
func (s *Store) GetTask(ctx context.Context, id uuid.UUID) (*domain.Task, error) {
	row := s.db.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id)
	t, err := scanTask(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get task %s", id))
	}
	return t, nil
}

// ScheduleTask enqueues a PENDING task with its resolved runtime input.
//
// This is the enqueue half of the task queue. Because the queue is a table, the
// enqueue commits in the same transaction as the state change that justified it,
// so there is no window where a task is "scheduled but not visible" or
// "published but not persisted" — the dual-write problem an external broker
// would introduce.
func (s *Store) ScheduleTask(ctx context.Context, taskID uuid.UUID, input json.RawMessage, runAt time.Time) (*domain.Task, error) {
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if !json.Valid(input) {
		return nil, fmt.Errorf("%w: task input is not valid JSON", domain.ErrValidation)
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state        = 'SCHEDULED',
			input        = $2,
			scheduled_at = $3,
			updated_at   = now()
		WHERE id = $1 AND state IN ('PENDING', 'FAILED')
		RETURNING `+taskColumns,
		taskID, []byte(input), runAt)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: task %s is not schedulable", domain.ErrInvalidStateTransition, taskID)
		}
		return nil, translateError(err, fmt.Sprintf("schedule task %s", taskID))
	}
	return t, nil
}

// ClaimParams describes a worker's request for work.
type ClaimParams struct {
	// TaskQueue restricts the claim to one queue.
	TaskQueue string
	// Activities restricts the claim to activities the worker implements. Empty
	// means the worker accepts any activity on the queue.
	Activities []string
	// WorkerID identifies the claimant for observability and lease ownership.
	WorkerID string
	// LeaseDuration is how long the claim is valid before Phase 2's sweeper may
	// reclaim it.
	LeaseDuration time.Duration
}

func (p *ClaimParams) applyDefaults() []string {
	if p.TaskQueue == "" {
		p.TaskQueue = domain.DefaultTaskQueue
	}
	if p.LeaseDuration <= 0 {
		p.LeaseDuration = 30 * time.Second
	}
	if p.Activities == nil {
		return []string{}
	}
	return p.Activities
}

// ClaimTask atomically pops the next eligible task and leases it to a worker.
// Returns domain.ErrNotFound when the queue has nothing eligible.
//
// The inner SELECT ... FOR UPDATE SKIP LOCKED is what makes this a real queue.
// Concurrent claimants each lock a distinct row and never block on one another,
// so N workers polling in parallel receive N different tasks — including tasks
// belonging to the same workflow, because nothing here locks the execution.
func (s *Store) ClaimTask(ctx context.Context, p ClaimParams) (*domain.Task, error) {
	activities := p.applyDefaults()

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state            = 'RUNNING',
			worker_id        = $3,
			claim_token      = $4,
			lease_expires_at = now() + ($5::int * interval '1 second'),
			attempt          = attempt + 1,
			started_at       = COALESCE(started_at, now()),
			updated_at       = now()
		WHERE id = (
			SELECT id FROM tasks
			WHERE state = 'SCHEDULED'
			  AND task_queue = $1
			  AND (cardinality($2::text[]) = 0 OR activity = ANY ($2::text[]))
			  AND (scheduled_at IS NULL OR scheduled_at <= now())
			  AND attempt < max_attempts
			ORDER BY scheduled_at ASC NULLS FIRST, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+taskColumns,
		p.TaskQueue, activities, p.WorkerID, uuid.New(), int(p.LeaseDuration.Seconds()))

	t, err := scanTask(row)
	if err != nil {
		return nil, translateError(err, "claim task")
	}
	return t, nil
}

// CompleteTask records a successful attempt.
//
// The claim token must match, which is what protects durable state from a
// superseded worker: if a slow worker's lease expired and the task was handed to
// someone else, the stale worker's report is rejected rather than overwriting
// the new attempt. A duplicate report of the *same* attempt is treated as an
// idempotent replay and returns the already-persisted task.
func (s *Store) CompleteTask(ctx context.Context, taskID uuid.UUID, claimToken uuid.UUID, output json.RawMessage) (*domain.Task, error) {
	var outputArg any
	if len(output) > 0 {
		if !json.Valid(output) {
			return nil, fmt.Errorf("%w: task output is not valid JSON", domain.ErrValidation)
		}
		outputArg = []byte(output)
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state            = 'COMPLETED',
			output           = $3,
			error            = '',
			completed_at     = now(),
			lease_expires_at = NULL,
			updated_at       = now()
		WHERE id = $1 AND state = 'RUNNING' AND claim_token = $2
		RETURNING `+taskColumns,
		taskID, claimToken, outputArg)

	t, err := scanTask(row)
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, translateError(err, fmt.Sprintf("complete task %s", taskID))
	}
	return s.resolveUnmatchedReport(ctx, taskID, claimToken, domain.TaskCompleted)
}

// FailTask records a failed attempt. Attempt accounting already happened at
// claim time, so a task with attempts remaining stays eligible for Phase 2's
// retry policy to revive; with none remaining it is terminal for the run.
func (s *Store) FailTask(ctx context.Context, taskID uuid.UUID, claimToken uuid.UUID, failure string) (*domain.Task, error) {
	if failure == "" {
		failure = "task failed without a reported reason"
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state            = 'FAILED',
			error            = $3,
			completed_at     = now(),
			lease_expires_at = NULL,
			updated_at       = now()
		WHERE id = $1 AND state = 'RUNNING' AND claim_token = $2
		RETURNING `+taskColumns,
		taskID, claimToken, failure)

	t, err := scanTask(row)
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, translateError(err, fmt.Sprintf("fail task %s", taskID))
	}
	return s.resolveUnmatchedReport(ctx, taskID, claimToken, domain.TaskFailed)
}

// resolveUnmatchedReport explains why a guarded task report matched no row and
// converts it into either an idempotent success or a precise error.
func (s *Store) resolveUnmatchedReport(
	ctx context.Context,
	taskID uuid.UUID,
	claimToken uuid.UUID,
	intended domain.TaskState,
) (*domain.Task, error) {
	current, err := s.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	// Same attempt, already recorded with the intended outcome: replay.
	if current.ClaimToken != nil && *current.ClaimToken == claimToken && current.State == intended {
		return current, nil
	}
	// Same attempt but a different terminal outcome was already recorded.
	if current.ClaimToken != nil && *current.ClaimToken == claimToken {
		return nil, fmt.Errorf("%w: task %s already recorded as %s, cannot record %s",
			domain.ErrConflict, taskID, current.State, intended)
	}
	// A different attempt owns the task now.
	return nil, fmt.Errorf("%w: task %s is in state %s and is not held by claim %s",
		domain.ErrStaleClaim, taskID, current.State, claimToken)
}

// CancelIncompleteTasks marks every non-terminal task of an execution CANCELED.
// Running tasks are included: their claim tokens are cleared so a late report
// from the worker is rejected as stale rather than resurrecting the execution.
func (s *Store) CancelIncompleteTasks(ctx context.Context, executionID uuid.UUID) (int, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE tasks SET
			state            = 'CANCELED',
			claim_token      = NULL,
			lease_expires_at = NULL,
			completed_at     = now(),
			updated_at       = now()
		WHERE execution_id = $1
		  AND state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED')`, executionID)
	if err != nil {
		return 0, translateError(err, "cancel incomplete tasks")
	}
	return int(tag.RowsAffected()), nil
}

func scanTask(row scanner) (*domain.Task, error) {
	var (
		t         domain.Task
		state     string
		dependsOn []string
		input     []byte
		output    []byte
		claim     *uuid.UUID
		lease     *time.Time
		scheduled *time.Time
		created   time.Time
		updated   time.Time
		started   *time.Time
		completed *time.Time
	)
	if err := row.Scan(
		&t.ID, &t.ExecutionID, &t.Name, &t.Activity, &state, &dependsOn,
		&input, &output, &t.Error, &t.Attempt, &t.MaxAttempts, &t.TimeoutSeconds,
		&t.TaskQueue, &claim, &t.WorkerID, &lease, &scheduled,
		&created, &updated, &started, &completed,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, err
	}

	t.State = domain.TaskState(state)
	t.DependsOn = dependsOn
	t.Input = json.RawMessage(input)
	if len(output) > 0 {
		t.Output = json.RawMessage(output)
	}
	t.ClaimToken = claim
	t.LeaseExpiresAt = lease
	t.ScheduledAt = scheduled
	t.CreatedAt = created
	t.UpdatedAt = updated
	t.StartedAt = started
	t.CompletedAt = completed
	return &t, nil
}
