package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// DefaultLeaseDuration bounds how long a claimed task may be held before
// Phase 2's lease sweeper is entitled to reclaim it.
const DefaultLeaseDuration = 30 * time.Second

// Notifier is the engine capability the service depends on: the ability to ask
// for a prompt scheduling pass after durable state changes. Declaring it here,
// where it is consumed, keeps the service testable without the real engine.
type Notifier interface {
	Nudge()
}

// noopNotifier is used when no engine is wired in, e.g. in an API-only process
// where a separate scheduler deployment owns the sweeping.
type noopNotifier struct{}

func (noopNotifier) Nudge() {}

// Service implements the control-plane operations exposed over the API.
//
// It owns orchestration and validation; the HTTP layer above it only translates
// wire formats, and the store below it only persists. Every method that changes
// durable state does so in a single transaction and then nudges the engine.
type Service struct {
	store    *store.Store
	notifier Notifier
	logger   *slog.Logger
	// metrics is optional; nil disables instrumentation.
	metrics *telemetry.Metrics
}

// WithMetrics returns a copy of the service that records metrics.
func (svc *Service) WithMetrics(m *telemetry.Metrics) *Service {
	clone := *svc
	clone.metrics = m
	return &clone
}

// NewService constructs a Service. A nil notifier is allowed and means state
// changes rely on the engine's polling sweep alone.
//
// There is no injectable clock here by design: every deadline the service writes
// is computed by the database, so all replicas agree on what time it is.
func NewService(s *store.Store, notifier Notifier, logger *slog.Logger) *Service {
	if notifier == nil {
		notifier = noopNotifier{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: s, notifier: notifier, logger: logger}
}

// Ping reports database reachability for readiness checks.
func (svc *Service) Ping(ctx context.Context) error { return svc.store.Ping(ctx) }

// ---------------------------------------------------------------------------
// Workflow definitions
// ---------------------------------------------------------------------------

// RegisterWorkflow registers a workflow version. Re-registering an identical
// spec is a no-op; changing an existing version is a conflict.
func (svc *Service) RegisterWorkflow(ctx context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error) {
	def, created, err := svc.store.RegisterDefinition(ctx, spec)
	if err != nil {
		return nil, false, err
	}
	if created {
		svc.logger.Info("workflow registered",
			"workflow", def.Name, "version", def.Version, "tasks", len(def.Spec.Tasks))
	}
	return def, created, nil
}

// GetWorkflow fetches a specific version, or the latest when version <= 0.
func (svc *Service) GetWorkflow(ctx context.Context, name string, version int) (*domain.WorkflowDefinition, error) {
	if version <= 0 {
		return svc.store.GetLatestDefinition(ctx, name)
	}
	return svc.store.GetDefinition(ctx, name, version)
}

// ListWorkflows returns registered workflow versions.
func (svc *Service) ListWorkflows(ctx context.Context, name string, limit int) ([]domain.WorkflowDefinition, error) {
	return svc.store.ListDefinitions(ctx, name, limit)
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------

// StartExecutionRequest describes a requested run.
type StartExecutionRequest struct {
	WorkflowName string
	// WorkflowVersion pins a version. Zero means "latest registered".
	WorkflowVersion int
	Input           json.RawMessage
	// IdempotencyKey deduplicates retried start requests. Two requests with the
	// same key and workflow name always resolve to the same execution.
	IdempotencyKey string
}

// StartExecution durably enqueues a workflow run and returns immediately.
//
// The execution is persisted in PENDING and the engine picks it up; the caller
// never waits for scheduling. Returns created=false when an idempotency key
// matched an existing run.
func (svc *Service) StartExecution(ctx context.Context, req StartExecutionRequest) (*domain.WorkflowExecution, bool, error) {
	if req.WorkflowName == "" {
		return nil, false, fmt.Errorf("%w: workflowName is required", domain.ErrValidation)
	}

	def, err := svc.GetWorkflow(ctx, req.WorkflowName, req.WorkflowVersion)
	if err != nil {
		return nil, false, err
	}

	// Capture the caller's trace context onto the row. This is the anchor every
	// later span for this workflow descends from, across processes and across
	// however long the workflow takes.
	exec, created, err := svc.store.CreateExecution(ctx, store.StartExecutionParams{
		Definition:     def,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
		Traceparent:    telemetry.TraceparentFrom(ctx),
	})
	if err != nil {
		return nil, false, err
	}

	if created {
		svc.logger.Info("execution accepted",
			"executionId", exec.ID, "workflow", def.Name, "version", def.Version,
			"traceId", telemetry.TraceIDFrom(ctx))
		svc.notifier.Nudge()
	}
	return exec, created, nil
}

// ExecutionDetail is an execution together with its task graph.
type ExecutionDetail struct {
	Execution *domain.WorkflowExecution
	Tasks     []domain.Task
}

// GetExecution returns an execution and its tasks.
func (svc *Service) GetExecution(ctx context.Context, id uuid.UUID) (*ExecutionDetail, error) {
	exec, err := svc.store.GetExecution(ctx, id)
	if err != nil {
		return nil, err
	}
	tasks, err := svc.store.ListTasks(ctx, id)
	if err != nil {
		return nil, err
	}
	return &ExecutionDetail{Execution: exec, Tasks: tasks}, nil
}

// ListExecutions returns executions newest first.
func (svc *Service) ListExecutions(ctx context.Context, f store.ExecutionFilter) ([]domain.WorkflowExecution, error) {
	if f.State != "" && !f.State.Valid() {
		return nil, fmt.Errorf("%w: unknown execution state %q", domain.ErrValidation, f.State)
	}
	return svc.store.ListExecutions(ctx, f)
}

// GetHistory returns an execution's append-only history. afterID is a cursor for
// incremental tailing; pass 0 to read from the beginning.
func (svc *Service) GetHistory(ctx context.Context, id uuid.UUID, afterID int64, limit int) ([]domain.HistoryEvent, error) {
	// Confirm the execution exists so an unknown id is a 404 rather than an
	// empty list that looks like a workflow with no history.
	if _, err := svc.store.GetExecution(ctx, id); err != nil {
		return nil, err
	}
	return svc.store.ListHistory(ctx, id, afterID, limit)
}

// CancelExecution stops an execution and cancels its outstanding tasks.
//
// Cancellation is idempotent: canceling an already-canceled execution succeeds
// and returns the existing state. Already-completed and already-failed runs are
// rejected, because reporting them as canceled would misrepresent what happened.
//
// Note this is control-plane cancellation: outstanding tasks are marked CANCELED
// and their claims invalidated, so a worker's late report is rejected as stale.
// Interrupting a task that is mid-execution inside a worker is cooperative
// cancellation and lands in Phase 2.
func (svc *Service) CancelExecution(ctx context.Context, id uuid.UUID, reason string) (*domain.WorkflowExecution, error) {
	if reason == "" {
		reason = "canceled by request"
	}

	var result *domain.WorkflowExecution
	err := svc.store.WithTx(ctx, func(tx *store.Store) error {
		if err := tx.LockExecutionForWrite(ctx, id); err != nil {
			return err
		}
		exec, err := tx.GetExecution(ctx, id)
		if err != nil {
			return err
		}

		if exec.State == domain.WorkflowCanceled {
			result = exec
			return nil
		}
		if exec.State.IsTerminal() {
			return fmt.Errorf("%w: execution %s already finished as %s",
				domain.ErrConflict, id, exec.State)
		}

		// Signal running tasks before invalidating their claims, so the history
		// records that workers were asked to stop rather than simply cut off.
		signalled, err := svc.requestTaskCancellation(ctx, tx, id)
		if err != nil {
			return err
		}

		canceled, err := tx.CancelIncompleteTasks(ctx, id)
		if err != nil {
			return err
		}
		result, err = tx.TransitionExecution(ctx, id, exec.State, domain.WorkflowCanceled, nil, reason)
		if err != nil {
			return err
		}
		_, err = tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: id,
			EventType:   domain.EventWorkflowCanceled,
			Payload: map[string]any{
				"reason":         reason,
				"tasksCanceled":  canceled,
				"tasksSignalled": signalled,
			},
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	svc.logger.Info("execution canceled", "executionId", id, "reason", reason)
	return result, nil
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// RegisterWorker records a worker and the activities it can serve.
func (svc *Service) RegisterWorker(ctx context.Context, p store.RegisterWorkerParams) (*domain.Worker, error) {
	w, err := svc.store.RegisterWorker(ctx, p)
	if err != nil {
		return nil, err
	}
	svc.logger.Info("worker registered",
		"workerId", w.ID, "worker", w.Name, "taskQueue", w.TaskQueue, "activities", len(w.Activities))
	return w, nil
}

// Heartbeat refreshes worker liveness.
func (svc *Service) Heartbeat(ctx context.Context, workerID uuid.UUID) (*domain.Worker, error) {
	return svc.store.Heartbeat(ctx, workerID)
}

// ListWorkers returns registered workers.
func (svc *Service) ListWorkers(ctx context.Context, taskQueue string, limit int) ([]domain.Worker, error) {
	return svc.store.ListWorkers(ctx, taskQueue, limit)
}

// ---------------------------------------------------------------------------
// Task dispatch
// ---------------------------------------------------------------------------

// PollRequest is a worker asking for work.
type PollRequest struct {
	WorkerID      string
	TaskQueue     string
	Activities    []string
	LeaseDuration time.Duration
}

// PollTask leases the next eligible task to a worker, or returns
// domain.ErrNotFound when the queue is empty.
//
// The claim and its TASK_STARTED history entry commit in one transaction. The
// transaction takes no execution-wide lock — the claim locks only the single task
// row it pops, via SKIP LOCKED, and history sequencing is optimistic. That is
// what allows many workers to claim tasks from the same workflow concurrently
// instead of queueing behind one another.
func (svc *Service) PollTask(ctx context.Context, req PollRequest) (*domain.Task, error) {
	if req.WorkerID == "" {
		return nil, fmt.Errorf("%w: workerId is required", domain.ErrValidation)
	}
	claim := store.ClaimParams{
		TaskQueue:     req.TaskQueue,
		Activities:    req.Activities,
		WorkerID:      req.WorkerID,
		LeaseDuration: req.LeaseDuration,
	}
	if claim.LeaseDuration <= 0 {
		claim.LeaseDuration = DefaultLeaseDuration
	}

	var claimed *domain.Task
	err := svc.store.WithTx(ctx, func(tx *store.Store) error {
		task, err := tx.ClaimTask(ctx, claim)
		if err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: task.ExecutionID,
			EventType:   domain.EventTaskStarted,
			TaskName:    task.Name,
			Payload: map[string]any{
				"taskId":         task.ID,
				"activity":       task.Activity,
				"workerId":       task.WorkerID,
				"attempt":        task.Attempt,
				"maxAttempts":    task.MaxAttempts,
				"leaseExpiresAt": task.LeaseExpiresAt,
			},
		}); err != nil {
			return err
		}
		claimed = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	// One read of the parent execution serves two purposes: the workflow name for
	// metric labels, and the trace context the worker needs to place its activity
	// span inside this workflow's trace.
	//
	// Deliberately outside the claim transaction. It is a primary-key lookup that
	// neither the claim's correctness nor its locking depends on, so keeping it out
	// leaves the transaction holding its row lock for as short a time as possible —
	// and that lock duration is what bounds claim throughput under contention.
	if exec, err := svc.store.GetExecution(ctx, claimed.ExecutionID); err == nil {
		claimed.Traceparent = exec.Traceparent
		svc.observeClaim(claimed, exec.WorkflowName)
	} else {
		// A claim must not fail because a metric label or a trace link could not be
		// resolved. The task is already durably leased at this point; losing the
		// label is a monitoring gap, losing the claim would be a correctness bug.
		svc.logger.Warn("could not resolve execution for claimed task",
			"taskId", claimed.ID, "executionId", claimed.ExecutionID, "error", err)
		svc.observeClaim(claimed, "unknown")
	}

	svc.logger.Debug("task leased",
		"taskId", claimed.ID, "task", claimed.Name, "worker", req.WorkerID, "attempt", claimed.Attempt)
	return claimed, nil
}

// observeClaim records the queue wait: how long the task sat claimable before a
// worker took it.
//
// This is the metric that answers "is the worker pool undersized", and it is only
// measurable here because scheduled_at and the claim happen in different processes
// — the worker cannot know when the task became eligible, and the engine cannot
// know when it was taken.
//
// workflow is passed in rather than looked up. It used to be resolved by a helper
// that issued its own GetExecution, which meant two extra round trips per claim
// (the helper was called twice) on the hottest path in the system.
func (svc *Service) observeClaim(task *domain.Task, workflow string) {
	if svc.metrics == nil {
		return
	}
	svc.metrics.TasksStarted.WithLabelValues(workflow, task.Activity).Inc()

	if task.ScheduledAt != nil && task.StartedAt != nil {
		// Measured against the claim's own timestamps, both written by the
		// database, so the two ends of the interval share a clock.
		if wait := task.UpdatedAt.Sub(*task.ScheduledAt); wait >= 0 {
			svc.metrics.TaskQueueWait.
				WithLabelValues(workflow, task.Activity).
				Observe(wait.Seconds())
		}
	}
}

// CompleteTask records a successful task attempt and lets the engine schedule
// whatever became ready as a result.
func (svc *Service) CompleteTask(ctx context.Context, taskID, claimToken uuid.UUID, output json.RawMessage) (*domain.Task, error) {
	task, err := svc.reportTask(ctx, taskID, claimToken, domain.TaskCompleted, output, store.FailParams{})
	if err != nil {
		if errors.Is(err, domain.ErrStaleClaim) {
			svc.observeStaleClaim("complete")
		}
		return nil, err
	}
	svc.notifier.Nudge()
	return task, nil
}

// FailTask records a failed task attempt.
//
// Whether another attempt follows is not decided here: the engine's retry pass
// reads the task's persisted policy. A worker declaring the failure
// non-retryable, however, short-circuits that budget entirely.
func (svc *Service) FailTask(ctx context.Context, taskID, claimToken uuid.UUID, p store.FailParams) (*domain.Task, error) {
	task, err := svc.reportTask(ctx, taskID, claimToken, domain.TaskFailed, nil, p)
	if err != nil {
		if errors.Is(err, domain.ErrStaleClaim) {
			svc.observeStaleClaim("fail")
		}
		return nil, err
	}
	svc.notifier.Nudge()
	return task, nil
}

// reportTask is the shared body of CompleteTask and FailTask.
//
// The task update and its history entry commit together, again without taking an
// execution-wide lock, so concurrent reports for sibling tasks do not serialize.
// Idempotency is enforced one level down by the claim-token guard in the store,
// so a duplicate report returns the already-persisted task.
func (svc *Service) reportTask(
	ctx context.Context,
	taskID, claimToken uuid.UUID,
	outcome domain.TaskState,
	output json.RawMessage,
	failure store.FailParams,
) (*domain.Task, error) {
	if claimToken == uuid.Nil {
		return nil, fmt.Errorf("%w: claimToken is required to report a task result", domain.ErrValidation)
	}

	var result *domain.Task
	err := svc.store.WithTx(ctx, func(tx *store.Store) error {
		// Read the current row first so a replay can be told apart from a first
		// report; the guarded UPDATE below is what actually enforces safety.
		existing, err := tx.GetTask(ctx, taskID)
		if err != nil {
			return err
		}

		// If the task already sat in the outcome state before this call, the
		// store will treat the write as an idempotent replay. History already
		// records the outcome, so appending again would double-count it.
		isReplay := existing.State == outcome

		var task *domain.Task
		var eventType domain.EventType

		switch outcome {
		case domain.TaskCompleted:
			task, err = tx.CompleteTask(ctx, taskID, claimToken, output)
			eventType = domain.EventTaskCompleted
		case domain.TaskFailed:
			task, err = tx.FailTask(ctx, taskID, claimToken, failure)
			eventType = domain.EventTaskFailed
		default:
			return fmt.Errorf("%w: %s is not a reportable outcome", domain.ErrValidation, outcome)
		}
		if err != nil {
			return err
		}
		if isReplay {
			result = task
			return nil
		}

		payload := map[string]any{
			"taskId":   task.ID,
			"activity": task.Activity,
			"workerId": task.WorkerID,
			"attempt":  task.Attempt,
		}
		if outcome == domain.TaskFailed {
			payload["error"] = task.Error
			payload["attemptsRemaining"] = task.MaxAttempts - task.Attempt
			payload["retryable"] = task.Retryable
			payload["failureReason"] = task.LastFailureReason
		}

		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: task.ExecutionID,
			EventType:   eventType,
			TaskName:    task.Name,
			Payload:     payload,
		}); err != nil {
			return err
		}
		result = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	svc.observeReport(ctx, result, outcome)
	svc.logger.Debug("task reported",
		"taskId", result.ID, "task", result.Name, "state", result.State)
	return result, nil
}

// workflowLabel resolves a task's workflow name for metric labels.
//
// A separate round trip rather than a SQL join on the task query: the name is
// bounded-cardinality label data needed only by metrics, so widening the hot task
// queries to carry it would be the wrong trade. Guarded by the caller's metrics
// check, so it costs nothing when metrics are off.
func (svc *Service) workflowLabel(ctx context.Context, task *domain.Task) string {
	exec, err := svc.store.GetExecution(ctx, task.ExecutionID)
	if err != nil {
		return "unknown"
	}
	return exec.WorkflowName
}

// observeReport records a completed attempt's outcome and how long it ran.
func (svc *Service) observeReport(ctx context.Context, task *domain.Task, outcome domain.TaskState) {
	if svc.metrics == nil {
		return
	}
	workflow := svc.workflowLabel(ctx, task)

	svc.metrics.TasksFinished.WithLabelValues(workflow, task.Activity, string(outcome)).Inc()
	if task.StartedAt != nil && task.CompletedAt != nil {
		if d := task.CompletedAt.Sub(*task.StartedAt); d >= 0 {
			svc.metrics.TaskDuration.
				WithLabelValues(workflow, task.Activity, string(outcome)).
				Observe(d.Seconds())
		}
	}
}

// observeStaleClaim records a report refused because the lease was gone.
func (svc *Service) observeStaleClaim(operation string) {
	if svc.metrics != nil {
		svc.metrics.StaleClaimRejections.WithLabelValues(operation).Inc()
	}
}
