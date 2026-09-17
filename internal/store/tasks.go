package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

const taskColumns = `id, execution_id, name, activity, state, depends_on, input, output, error,
	attempt, max_attempts, timeout_seconds, task_queue, claim_token, worker_id,
	lease_expires_at, scheduled_at, created_at, updated_at, started_at, completed_at,
	retry_initial_interval_ms, retry_backoff_coefficient, retry_max_interval_ms,
	retry_jitter_percent, retryable, cancel_requested, dead_lettered_at,
	lease_expiry_count, max_lease_expiries, last_failure_reason`

// leaseGraceSeconds is added to a task's own timeout when deriving the minimum
// lease. Without it a task that legitimately runs for its full budget would have
// its lease lapse at the exact moment it finishes, and the reaper would hand the
// work to a second worker.
const leaseGraceSeconds = 15

// MaterializeTasks writes the execution's task graph to durable storage.
//
// It is idempotent by way of the (execution_id, name) unique constraint: if the
// engine crashes after materializing some tasks, the next pass inserts only the
// missing ones. Tasks always land in PENDING so that scheduling — computing
// inputs from upstream outputs and enqueueing — is a single code path handled by
// the dispatcher.
//
// The retry policy is resolved here and stored on the row, so the task's retry
// schedule is reproducible from the row alone and unaffected by later changes to
// the engine's defaults.
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
		retry := ts.RetryPolicy.Resolve()
		maxLeaseExpiries := ts.MaxLeaseExpiries
		if maxLeaseExpiries <= 0 {
			maxLeaseExpiries = domain.DefaultMaxLeaseExpiries
		}

		tag, err := s.db.Exec(ctx, `
			INSERT INTO tasks
				(id, execution_id, name, activity, state, depends_on, input,
				 max_attempts, timeout_seconds, task_queue,
				 retry_initial_interval_ms, retry_backoff_coefficient,
				 retry_max_interval_ms, retry_jitter_percent, max_lease_expiries)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT (execution_id, name) DO NOTHING`,
			uuid.New(), exec.ID, ts.Name, ts.Activity, domain.TaskPending,
			dependsOn, staticInput, ts.MaxAttempts, ts.TimeoutSeconds, exec.TaskQueue,
			int(retry.InitialInterval/time.Millisecond), retry.BackoffCoefficient,
			int(retry.MaxInterval/time.Millisecond), retry.JitterPercent, maxLeaseExpiries)
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
	return collectTasks(rows)
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
// This is the enqueue half of the task queue, used for a task's first attempt.
// Retries and requeues go through RequeueTask, which preserves the already
// resolved input. Because the queue is a table, the enqueue commits in the same
// transaction as the state change that justified it, so there is no window where
// a task is "scheduled but not visible" or "published but not persisted" — the
// dual-write problem an external broker would introduce.
func (s *Store) ScheduleTask(ctx context.Context, taskID uuid.UUID, input json.RawMessage, delay time.Duration) (*domain.Task, error) {
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if !json.Valid(input) {
		return nil, fmt.Errorf("%w: task input is not valid JSON", domain.ErrValidation)
	}
	if delay < 0 {
		delay = 0
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state        = 'SCHEDULED',
			input        = $2,
			scheduled_at = now() + ($3::bigint * interval '1 microsecond'),
			updated_at   = now()
		WHERE id = $1 AND state = 'PENDING'
		RETURNING `+taskColumns,
		taskID, []byte(input), delay.Microseconds())

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: task %s is not schedulable", domain.ErrInvalidStateTransition, taskID)
		}
		return nil, translateError(err, fmt.Sprintf("schedule task %s", taskID))
	}
	return t, nil
}

// RequeueReason describes why a task is going back on the queue.
type RequeueReason struct {
	// From constrains which state the task must currently be in, making the
	// update a compare-and-swap against a concurrent writer.
	From domain.TaskState
	// Failure categorizes the cause, recorded on the row for operators.
	Failure domain.FailureReason
	// Error is a human-readable explanation.
	Error string
	// CountsAsLeaseExpiry increments the task's lease-expiry counter, which is
	// what distinguishes "a worker vanished" from "the activity failed".
	CountsAsLeaseExpiry bool
	// GrantExtraAttempts raises max_attempts by this much. Used by lease
	// reclamation (to give back the attempt a lost claim consumed) and by operator
	// replay, so that attempt stays an honest lifetime count of executions.
	GrantExtraAttempts int
	// RequireLeaseExpired adds "and the lease really is expired" to the guard.
	// The reaper sets it so the check happens against the database clock in the
	// same statement as the write, closing the window in which a worker's
	// just-in-time report would otherwise be overwritten.
	RequireLeaseExpired bool
}

// RequeueTask puts a task back on the queue after the given delay, preserving its
// already-resolved input so a re-run replays byte-identical input.
//
// The delay is applied against the database clock rather than the caller's. Time
// is decided in one place for the whole cluster: an app server whose clock has
// drifted must not be able to reap a lease early or park a retry for an hour.
//
// The claim token is cleared deliberately. A worker that was holding this task
// and reports late will then find no matching claim and be told its claim is
// stale, rather than getting a confusing conflict about task state.
func (s *Store) RequeueTask(ctx context.Context, taskID uuid.UUID, delay time.Duration, reason RequeueReason) (*domain.Task, error) {
	if reason.From != "" {
		if err := domain.TransitionTask(reason.From, domain.TaskScheduled); err != nil {
			return nil, err
		}
	}
	if delay < 0 {
		delay = 0
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state               = 'SCHEDULED',
			scheduled_at        = now() + ($2::bigint * interval '1 microsecond'),
			claim_token         = NULL,
			worker_id           = '',
			lease_expires_at    = NULL,
			completed_at        = NULL,
			dead_lettered_at    = NULL,
			cancel_requested    = FALSE,
			retryable           = TRUE,
			error               = $3,
			last_failure_reason = $4,
			lease_expiry_count  = lease_expiry_count + CASE WHEN $5 THEN 1 ELSE 0 END,
			max_attempts        = max_attempts + $6,
			updated_at          = now()
		WHERE id = $1
		  AND ($7 = '' OR state = $7)
		  AND state IN ('RUNNING', 'FAILED', 'DEAD_LETTER')
		  AND (NOT $8 OR (lease_expires_at IS NOT NULL AND lease_expires_at < now()))
		RETURNING `+taskColumns,
		taskID, delay.Microseconds(), reason.Error, string(reason.Failure),
		reason.CountsAsLeaseExpiry, reason.GrantExtraAttempts, string(reason.From),
		reason.RequireLeaseExpired)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: task %s could not be requeued from state %q",
				domain.ErrInvalidStateTransition, taskID, reason.From)
		}
		return nil, translateError(err, fmt.Sprintf("requeue task %s", taskID))
	}
	return t, nil
}

// DeadLetterTask parks a task that has run out of attempts, or whose failure was
// declared permanent, for operator attention.
//
// Dead-lettering is terminal to the engine: without it, a task that can never
// succeed would either retry forever or be silently dropped. Keeping the row
// (rather than moving it to a separate table) preserves the full attempt history
// alongside the failure, which is what an operator needs to decide whether to
// replay it.
func (s *Store) DeadLetterTask(ctx context.Context, taskID uuid.UUID, failure domain.FailureReason, message string) (*domain.Task, error) {
	return s.DeadLetterTaskGuarded(ctx, taskID, failure, message, false)
}

// DeadLetterTaskGuarded is DeadLetterTask with an optional requirement that the
// task's lease really has lapsed, checked against the database clock in the same
// statement as the write. The reaper uses it so a worker reporting just in time
// still wins.
func (s *Store) DeadLetterTaskGuarded(
	ctx context.Context,
	taskID uuid.UUID,
	failure domain.FailureReason,
	message string,
	requireLeaseExpired bool,
) (*domain.Task, error) {
	if message == "" {
		message = "task exhausted its attempt budget"
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state               = 'DEAD_LETTER',
			dead_lettered_at    = now(),
			completed_at        = COALESCE(completed_at, now()),
			claim_token         = NULL,
			lease_expires_at    = NULL,
			cancel_requested    = FALSE,
			error               = $2,
			last_failure_reason = $3,
			updated_at          = now()
		WHERE id = $1
		  AND state IN ('RUNNING', 'FAILED')
		  AND (NOT $4 OR (lease_expires_at IS NOT NULL AND lease_expires_at < now()))
		RETURNING `+taskColumns,
		taskID, message, string(failure), requireLeaseExpired)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: task %s cannot be dead-lettered from its current state",
				domain.ErrInvalidStateTransition, taskID)
		}
		return nil, translateError(err, fmt.Sprintf("dead-letter task %s", taskID))
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
	// LeaseDuration is how long the claim is valid before the reaper may reclaim
	// it. The server raises it to cover the task's own timeout.
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
//
// The lease is never shorter than the task's own timeout plus a grace period,
// regardless of what the worker asked for. A worker requesting a 30s lease for a
// task allowed 300s would otherwise have its lease reaped mid-run and the work
// handed to a second worker — a duplicate execution caused purely by
// misconfiguration.
func (s *Store) ClaimTask(ctx context.Context, p ClaimParams) (*domain.Task, error) {
	activities := p.applyDefaults()

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state            = 'RUNNING',
			worker_id        = $3,
			claim_token      = $4,
			lease_expires_at = now() + (GREATEST(
				$5::int,
				CASE WHEN timeout_seconds > 0 THEN timeout_seconds + $6::int ELSE 0 END
			) * interval '1 second'),
			attempt          = attempt + 1,
			cancel_requested = FALSE,
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
		p.TaskQueue, activities, p.WorkerID, uuid.New(),
		int(p.LeaseDuration.Seconds()), leaseGraceSeconds)

	t, err := scanTask(row)
	if err != nil {
		return nil, translateError(err, "claim task")
	}
	return t, nil
}

// RenewLease extends a running task's lease and reports whether cancellation has
// been requested.
//
// This is the mechanism that makes long-running tasks safe without giving every
// task a pessimistically huge lease: a live worker keeps proving it is alive, and
// a dead one stops, so the reaper's window stays short. It doubles as the
// cooperative-cancellation channel, since it is already a periodic round trip
// the worker makes while holding the task.
func (s *Store) RenewLease(ctx context.Context, taskID, claimToken uuid.UUID, extendBy time.Duration) (*domain.Task, error) {
	if extendBy <= 0 {
		extendBy = 30 * time.Second
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			lease_expires_at = now() + (GREATEST(
				$3::int,
				CASE WHEN timeout_seconds > 0 THEN timeout_seconds + $4::int ELSE 0 END
			) * interval '1 second'),
			updated_at       = now()
		WHERE id = $1 AND state = 'RUNNING' AND claim_token = $2
		RETURNING `+taskColumns,
		taskID, claimToken, int(extendBy.Seconds()), leaseGraceSeconds)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			// The lease is gone: either it was reaped and reassigned, or the task
			// was canceled. Either way the worker must stop.
			return nil, fmt.Errorf("%w: task %s is no longer leased to claim %s",
				domain.ErrStaleClaim, taskID, claimToken)
		}
		return nil, translateError(err, fmt.Sprintf("renew lease on task %s", taskID))
	}
	return t, nil
}

// ListExpiredLeaseTaskIDs returns tasks whose lease has lapsed.
//
// No lock is taken: the reaper re-checks the condition inside a guarded update
// per task, so two reapers racing on the same task simply means one of them
// finds nothing to do.
func (s *Store) ListExpiredLeaseTaskIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT id FROM tasks
		WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at < now()
		ORDER BY lease_expires_at ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, translateError(err, "list expired leases")
	}
	defer rows.Close()
	return collectUUIDs(rows, "expired lease task id")
}

// ListTaskIDsHeldByWorkers returns running tasks leased to any of the named
// workers. Used to reclaim work from a worker that heartbeat detection has
// declared dead, without waiting for each lease to lapse on its own.
func (s *Store) ListTaskIDsHeldByWorkers(ctx context.Context, workerNames []string, limit int) ([]uuid.UUID, error) {
	if len(workerNames) == 0 {
		return []uuid.UUID{}, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(ctx, `
		SELECT id FROM tasks
		WHERE state = 'RUNNING' AND worker_id = ANY ($1::text[])
		ORDER BY started_at ASC
		LIMIT $2`, workerNames, limit)
	if err != nil {
		return nil, translateError(err, "list tasks held by workers")
	}
	defer rows.Close()
	return collectUUIDs(rows, "task id held by worker")
}

// ListRetryableTaskIDs returns failed tasks the engine should consider retrying.
func (s *Store) ListRetryableTaskIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(ctx, `
		SELECT id FROM tasks
		WHERE state = 'FAILED'
		ORDER BY updated_at ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, translateError(err, "list retryable tasks")
	}
	defer rows.Close()
	return collectUUIDs(rows, "retryable task id")
}

// DeadLetterFilter narrows a dead letter queue listing.
type DeadLetterFilter struct {
	WorkflowName string
	Activity     string
	Limit        int
	Offset       int
}

// ListDeadLetterTasks returns parked tasks, most recently dead-lettered first.
func (s *Store) ListDeadLetterTasks(ctx context.Context, f DeadLetterFilter) ([]domain.Task, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+prefixColumns("t", taskColumns)+`
		FROM tasks t
		JOIN workflow_executions e ON e.id = t.execution_id
		WHERE t.state = 'DEAD_LETTER'
		  AND ($1 = '' OR e.workflow_name = $1)
		  AND ($2 = '' OR t.activity = $2)
		ORDER BY t.dead_lettered_at DESC
		LIMIT $3 OFFSET $4`,
		f.WorkflowName, f.Activity, f.Limit, f.Offset)
	if err != nil {
		return nil, translateError(err, "list dead letter tasks")
	}
	defer rows.Close()
	return collectTasks(rows)
}

// CountDeadLetterTasks returns the size of the dead letter queue.
func (s *Store) CountDeadLetterTasks(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE state = 'DEAD_LETTER'`).Scan(&n)
	if err != nil {
		return 0, translateError(err, "count dead letter tasks")
	}
	return n, nil
}

// RequestTaskCancellation flags an execution's running tasks so their workers
// abandon them at the next lease renewal.
//
// Cancellation of a task already executing inside a worker can only ever be a
// request: the control plane cannot reach into another process and stop it. What
// it can do is stop pretending the result matters, which is why the caller also
// invalidates the claims.
func (s *Store) RequestTaskCancellation(ctx context.Context, executionID uuid.UUID) (int, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE tasks SET cancel_requested = TRUE, updated_at = now()
		WHERE execution_id = $1 AND state = 'RUNNING' AND cancel_requested = FALSE`, executionID)
	if err != nil {
		return 0, translateError(err, "request task cancellation")
	}
	return int(tag.RowsAffected()), nil
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
			state               = 'COMPLETED',
			output              = $3,
			error               = '',
			last_failure_reason = '',
			completed_at        = now(),
			lease_expires_at    = NULL,
			cancel_requested    = FALSE,
			updated_at          = now()
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

// FailParams describes a reported task failure.
type FailParams struct {
	// Error is the worker's explanation.
	Error string
	// Reason categorizes the failure.
	Reason domain.FailureReason
	// Retryable is false when the worker knows another attempt cannot help, for
	// example a malformed input. Such a task dead-letters immediately rather than
	// burning its remaining attempts on a foregone conclusion.
	Retryable bool
}

// FailTask records a failed attempt. Attempt accounting already happened at
// claim time; whether another attempt follows is decided by the engine's retry
// pass, which reads the task's persisted policy.
func (s *Store) FailTask(ctx context.Context, taskID uuid.UUID, claimToken uuid.UUID, p FailParams) (*domain.Task, error) {
	if p.Error == "" {
		p.Error = "task failed without a reported reason"
	}
	if p.Reason == "" {
		p.Reason = domain.FailureActivityError
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks SET
			state               = 'FAILED',
			error               = $3,
			last_failure_reason = $4,
			retryable           = $5,
			completed_at        = now(),
			lease_expires_at    = NULL,
			cancel_requested    = FALSE,
			updated_at          = now()
		WHERE id = $1 AND state = 'RUNNING' AND claim_token = $2
		RETURNING `+taskColumns,
		taskID, claimToken, p.Error, string(p.Reason), p.Retryable)

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
	// A different attempt owns the task now, or the claim was invalidated by a
	// reap or a cancellation.
	return nil, fmt.Errorf("%w: task %s is in state %s and is not held by claim %s",
		domain.ErrStaleClaim, taskID, current.State, claimToken)
}

// CancelIncompleteTasks marks every non-terminal task of an execution CANCELED.
//
// Running tasks are included and their claim tokens cleared, so a late report
// from a worker that was mid-flight is rejected as stale rather than resurrecting
// a canceled execution. Dead-lettered tasks are included too, since leaving them
// parked under a canceled workflow would misrepresent them as actionable.
func (s *Store) CancelIncompleteTasks(ctx context.Context, executionID uuid.UUID) (int, error) {
	return s.CancelIncompleteTasksExcept(ctx, executionID, uuid.Nil)
}

// CancelIncompleteTasksExcept is CancelIncompleteTasks with one task spared.
//
// The engine uses it when failing a run: the dead-lettered task that caused the
// failure must stay dead-lettered so it remains visible in the dead letter queue
// and replayable, while its now-unreachable siblings are cleaned up.
func (s *Store) CancelIncompleteTasksExcept(ctx context.Context, executionID, spare uuid.UUID) (int, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE tasks SET
			state            = 'CANCELED',
			claim_token      = NULL,
			lease_expires_at = NULL,
			dead_lettered_at = NULL,
			cancel_requested = FALSE,
			completed_at     = now(),
			updated_at       = now()
		WHERE execution_id = $1
		  AND ($2::uuid IS NULL OR id <> $2)
		  AND state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED', 'DEAD_LETTER')`,
		executionID, nullableUUID(spare))
	if err != nil {
		return 0, translateError(err, "cancel incomplete tasks")
	}
	return int(tag.RowsAffected()), nil
}

// nullableUUID maps the zero UUID onto SQL NULL, so "no exclusion" can be
// expressed without a second query.
func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

// RestoreCanceledTasks returns an execution's canceled tasks to PENDING.
//
// Used only by operator replay. When a run failed, the engine canceled the tasks
// that could no longer be reached; reviving the run has to undo that, or the
// replayed task would succeed into a graph that can never complete. Attempt
// counters are left alone so the row stays an honest record of what actually ran.
func (s *Store) RestoreCanceledTasks(ctx context.Context, executionID uuid.UUID) (int, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE tasks SET
			state            = 'PENDING',
			scheduled_at     = NULL,
			completed_at     = NULL,
			claim_token      = NULL,
			worker_id        = '',
			lease_expires_at = NULL,
			cancel_requested = FALSE,
			retryable        = TRUE,
			error            = '',
			updated_at       = now()
		WHERE execution_id = $1 AND state = 'CANCELED'`, executionID)
	if err != nil {
		return 0, translateError(err, "restore canceled tasks")
	}
	return int(tag.RowsAffected()), nil
}

// prefixColumns qualifies a column list with a table alias, so the shared
// taskColumns constant can be reused verbatim in joined queries instead of being
// duplicated with a prefix (and drifting).
func prefixColumns(alias, columns string) string {
	parts := strings.Split(columns, ",")
	qualified := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			qualified = append(qualified, alias+"."+trimmed)
		}
	}
	return strings.Join(qualified, ", ")
}

func collectTasks(rows pgx.Rows) ([]domain.Task, error) {
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

func collectUUIDs(rows pgx.Rows, what string) ([]uuid.UUID, error) {
	out := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, translateError(err, "scan "+what)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate "+what)
	}
	return out, nil
}

func scanTask(row scanner) (*domain.Task, error) {
	var (
		t          domain.Task
		state      string
		dependsOn  []string
		input      []byte
		output     []byte
		claim      *uuid.UUID
		lease      *time.Time
		scheduled  *time.Time
		created    time.Time
		updated    time.Time
		started    *time.Time
		completed  *time.Time
		deadLetter *time.Time
		initialMS  int
		maxMS      int
		reason     string
	)
	if err := row.Scan(
		&t.ID, &t.ExecutionID, &t.Name, &t.Activity, &state, &dependsOn,
		&input, &output, &t.Error, &t.Attempt, &t.MaxAttempts, &t.TimeoutSeconds,
		&t.TaskQueue, &claim, &t.WorkerID, &lease, &scheduled,
		&created, &updated, &started, &completed,
		&initialMS, &t.RetryPolicy.BackoffCoefficient, &maxMS,
		&t.RetryPolicy.JitterPercent, &t.Retryable, &t.CancelRequested, &deadLetter,
		&t.LeaseExpiryCount, &t.MaxLeaseExpiries, &reason,
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
	t.DeadLetteredAt = deadLetter
	t.RetryPolicy.InitialInterval = time.Duration(initialMS) * time.Millisecond
	t.RetryPolicy.MaxInterval = time.Duration(maxMS) * time.Millisecond
	t.LastFailureReason = reason
	return &t, nil
}
