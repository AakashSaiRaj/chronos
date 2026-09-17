package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
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
}

// NewService constructs a Service. A nil notifier is allowed and means state
// changes rely on the engine's polling sweep alone.
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

	exec, created, err := svc.store.CreateExecution(ctx, store.StartExecutionParams{
		Definition:     def,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return nil, false, err
	}

	if created {
		svc.logger.Info("execution accepted",
			"executionId", exec.ID, "workflow", def.Name, "version", def.Version)
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
			Payload:     map[string]any{"reason": reason, "tasksCanceled": canceled},
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

	svc.logger.Debug("task leased",
		"taskId", claimed.ID, "task", claimed.Name, "worker", req.WorkerID, "attempt", claimed.Attempt)
	return claimed, nil
}

// CompleteTask records a successful task attempt and lets the engine schedule
// whatever became ready as a result.
func (svc *Service) CompleteTask(ctx context.Context, taskID, claimToken uuid.UUID, output json.RawMessage) (*domain.Task, error) {
	task, err := svc.reportTask(ctx, taskID, claimToken, domain.TaskCompleted, output, "")
	if err != nil {
		return nil, err
	}
	svc.notifier.Nudge()
	return task, nil
}

// FailTask records a failed task attempt.
func (svc *Service) FailTask(ctx context.Context, taskID, claimToken uuid.UUID, failure string) (*domain.Task, error) {
	task, err := svc.reportTask(ctx, taskID, claimToken, domain.TaskFailed, nil, failure)
	if err != nil {
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
	failure string,
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

	svc.logger.Debug("task reported",
		"taskId", result.ID, "task", result.Name, "state", result.State)
	return result, nil
}
