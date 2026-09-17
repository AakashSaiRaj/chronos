// Package engine contains Chronos's scheduling core.
//
// The central design rule is that the engine keeps no run state in memory.
// Every decision — which task to schedule next, whether a workflow is finished,
// what a task's input is — is derived from rows in PostgreSQL on each pass. A
// replica can be killed at any instant and another replica (or the same one
// after a restart) reaches the identical conclusion from the persisted state.
// That is what makes executions recoverable rather than merely restartable.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// Config tunes the scheduling loop.
type Config struct {
	// PollInterval is how often the engine sweeps for work. The sweep is the
	// correctness mechanism: even if every in-process signal is lost, the next
	// sweep rediscovers the pending work from the database.
	PollInterval time.Duration
	// BatchSize bounds how many executions one sweep considers.
	BatchSize int
	// Jitter is injectable so retry delays can be made exactly reproducible in
	// tests. Note there is deliberately no injectable clock: every time-based
	// decision is evaluated against the database clock, in the same statement as
	// the write it guards. One clock for the whole cluster means a replica with a
	// drifted clock cannot reap a live lease or park a retry for an hour.
	Jitter func() float64
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.Jitter == nil {
		c.Jitter = rand.Float64
	}
	return c
}

// Engine advances persisted workflow executions.
type Engine struct {
	store  *store.Store
	cfg    Config
	logger *slog.Logger

	// nudge carries a hint that work may be available now. It is a latency
	// optimization layered on top of polling, never a correctness requirement:
	// a dropped nudge only means the work waits for the next sweep.
	nudge chan struct{}

	startOnce sync.Once
	doneCh    chan struct{}
}

// New constructs an Engine.
func New(s *store.Store, cfg Config, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		store:  s,
		cfg:    cfg.withDefaults(),
		logger: logger,
		nudge:  make(chan struct{}, 1),
		doneCh: make(chan struct{}),
	}
}

// Nudge asks the engine to sweep promptly. It never blocks: if a sweep is
// already pending, the signal is coalesced.
func (e *Engine) Nudge() {
	select {
	case e.nudge <- struct{}{}:
	default:
	}
}

// Run drives the scheduling loop until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	e.logger.Info("engine started",
		"pollInterval", e.cfg.PollInterval, "batchSize", e.cfg.BatchSize)

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()
	defer e.startOnce.Do(func() { close(e.doneCh) })

	for {
		// Sweep first so a freshly started engine immediately picks up whatever
		// was left behind by a previous process.
		if err := e.Sweep(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}
			// A sweep failure is almost always a transient database problem.
			// Log and keep looping rather than tearing down the process.
			e.logger.Error("engine sweep failed", "error", err)
		}

		select {
		case <-ctx.Done():
			e.logger.Info("engine stopping")
			return nil
		case <-ticker.C:
		case <-e.nudge:
		}
	}
	return nil
}

// Sweep performs one scheduling pass over all active executions. It is exported
// so tests can drive the engine deterministically instead of waiting on timers.
func (e *Engine) Sweep(ctx context.Context) error {
	ids, err := e.store.ListActiveExecutionIDs(ctx, e.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("list active executions: %w", err)
	}

	var firstErr error
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := e.ProcessExecution(ctx, id); err != nil {
			// One poisoned execution must not stall the others.
			e.logger.Error("advance execution failed", "executionId", id, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// ProcessExecution advances a single execution as far as it can go right now.
//
// The whole pass runs in one transaction that begins by locking the execution
// row with SKIP LOCKED. That gives two properties at once: the execution's
// tasks, state, and history move atomically, and concurrent engine replicas
// never work on the same execution — a replica that finds the row locked simply
// moves on instead of blocking.
func (e *Engine) ProcessExecution(ctx context.Context, id uuid.UUID) error {
	return e.store.WithTx(ctx, func(tx *store.Store) error {
		exec, err := tx.LockExecution(ctx, id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				// Either another replica holds it, or it was deleted. Both are
				// fine: this replica has nothing to do.
				return nil
			}
			return err
		}
		if exec.State.IsTerminal() {
			return nil
		}

		def, err := tx.GetDefinitionByID(ctx, exec.DefinitionID)
		if err != nil {
			return fmt.Errorf("load definition for execution %s: %w", exec.ID, err)
		}

		if exec.State == domain.WorkflowPending {
			exec, err = e.startExecution(ctx, tx, exec, def)
			if err != nil {
				return err
			}
		}
		if exec.State != domain.WorkflowRunning {
			return nil
		}
		return e.advanceExecution(ctx, tx, exec, def)
	})
}

// startExecution materializes the task graph and moves PENDING -> RUNNING.
//
// Materialization is idempotent, and the state transition is a compare-and-swap,
// so a crash anywhere in here leaves the execution safely re-processable: the
// next pass either redoes the whole step or finds it already done.
func (e *Engine) startExecution(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	def *domain.WorkflowDefinition,
) (*domain.WorkflowExecution, error) {
	inserted, err := tx.MaterializeTasks(ctx, exec, &def.Spec)
	if err != nil {
		return nil, fmt.Errorf("materialize tasks for %s: %w", exec.ID, err)
	}

	running, err := tx.TransitionExecution(ctx, exec.ID,
		domain.WorkflowPending, domain.WorkflowRunning, nil, "")
	if err != nil {
		return nil, err
	}

	if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
		ExecutionID: exec.ID,
		EventType:   domain.EventWorkflowStarted,
		Payload: map[string]any{
			"workflowName":    def.Name,
			"workflowVersion": def.Version,
			"taskQueue":       exec.TaskQueue,
			"tasksCreated":    inserted,
		},
	}); err != nil {
		return nil, err
	}

	e.logger.Info("execution started",
		"executionId", exec.ID, "workflow", def.Name, "version", def.Version, "tasks", inserted)
	return running, nil
}

// advanceExecution schedules everything that has become ready and finalizes the
// execution when the graph can make no further progress.
func (e *Engine) advanceExecution(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	def *domain.WorkflowDefinition,
) error {
	tasks, err := tx.ListTasks(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("list tasks for %s: %w", exec.ID, err)
	}
	if len(tasks) == 0 {
		// The execution is RUNNING with no tasks, which can only happen if a
		// previous materialization was rolled back. Re-materialize.
		if _, err := tx.MaterializeTasks(ctx, exec, &def.Spec); err != nil {
			return err
		}
		if tasks, err = tx.ListTasks(ctx, exec.ID); err != nil {
			return err
		}
	}

	// Retire failed attempts first: each either earns a retry or is parked in the
	// dead letter queue. Doing this before the readiness scan means a retry
	// becomes visible in the same pass that observed the failure.
	retired, err := e.processFailedTasks(ctx, tx, exec, tasks)
	if err != nil {
		return err
	}
	if retired > 0 {
		if tasks, err = tx.ListTasks(ctx, exec.ID); err != nil {
			return err
		}
	}

	// A dead-lettered task can never complete, so the run cannot either.
	for i := range tasks {
		if tasks[i].State == domain.TaskDeadLetter {
			return e.failExecution(ctx, tx, exec, &tasks[i])
		}
	}

	byName := indexTasks(tasks)
	scheduled, err := e.scheduleReadyTasks(ctx, tx, exec, def, tasks, byName)
	if err != nil {
		return err
	}

	// Re-read only if we changed anything, so the completion check below sees
	// the post-schedule truth.
	if scheduled > 0 {
		if tasks, err = tx.ListTasks(ctx, exec.ID); err != nil {
			return err
		}
		byName = indexTasks(tasks)
	}

	allDone := true
	for i := range tasks {
		if tasks[i].State != domain.TaskCompleted {
			allDone = false
			break
		}
	}
	if allDone {
		return e.completeExecution(ctx, tx, exec, def, byName)
	}
	return nil
}

func indexTasks(tasks []domain.Task) map[string]*domain.Task {
	byName := make(map[string]*domain.Task, len(tasks))
	for i := range tasks {
		byName[tasks[i].Name] = &tasks[i]
	}
	return byName
}

// processFailedTasks decides what becomes of each FAILED task: another attempt
// after a backoff delay, or the dead letter queue.
//
// The delay is written into scheduled_at rather than held as a timer, so a retry
// pending at the moment the engine dies is still pending, at the same instant,
// for whichever process takes over.
func (e *Engine) processFailedTasks(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	tasks []domain.Task,
) (int, error) {
	changed := 0
	for i := range tasks {
		t := &tasks[i]
		if t.State != domain.TaskFailed {
			continue
		}

		if !t.ShouldRetry() {
			reason := domain.FailureActivityError
			message := fmt.Sprintf("task %q exhausted its %d attempt(s): %s",
				t.Name, t.MaxAttempts, t.Error)
			if !t.Retryable {
				message = fmt.Sprintf("task %q failed permanently on attempt %d: %s",
					t.Name, t.Attempt, t.Error)
			}
			if t.LastFailureReason != "" {
				reason = domain.FailureReason(t.LastFailureReason)
			}

			if _, err := tx.DeadLetterTask(ctx, t.ID, reason, message); err != nil {
				return changed, fmt.Errorf("dead-letter task %q: %w", t.Name, err)
			}
			if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
				ExecutionID: exec.ID,
				EventType:   domain.EventTaskDeadLettered,
				TaskName:    t.Name,
				Payload: map[string]any{
					"taskId":      t.ID,
					"activity":    t.Activity,
					"attempt":     t.Attempt,
					"maxAttempts": t.MaxAttempts,
					"retryable":   t.Retryable,
					"reason":      reason,
					"error":       t.Error,
				},
			}); err != nil {
				return changed, err
			}
			e.logger.Warn("task dead-lettered",
				"executionId", exec.ID, "task", t.Name,
				"attempt", t.Attempt, "retryable", t.Retryable, "error", t.Error)
			changed++
			continue
		}

		delay := t.RetryPolicy.BackoffFor(t.Attempt, e.cfg.Jitter())

		retried, err := tx.RequeueTask(ctx, t.ID, delay, store.RequeueReason{
			From:    domain.TaskFailed,
			Failure: domain.FailureReason(t.LastFailureReason),
			Error:   t.Error,
		})
		if err != nil {
			return changed, fmt.Errorf("schedule retry for task %q: %w", t.Name, err)
		}
		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: exec.ID,
			EventType:   domain.EventTaskRetryScheduled,
			TaskName:    t.Name,
			Payload: map[string]any{
				"taskId":            t.ID,
				"activity":          t.Activity,
				"attempt":           t.Attempt,
				"maxAttempts":       t.MaxAttempts,
				"attemptsRemaining": t.MaxAttempts - t.Attempt,
				"backoffMs":         delay.Milliseconds(),
				"nextAttemptAt":     retried.ScheduledAt,
				"error":             t.Error,
			},
		}); err != nil {
			return changed, err
		}

		e.logger.Info("task retry scheduled",
			"executionId", exec.ID, "task", t.Name, "attempt", t.Attempt,
			"backoff", delay, "nextAttemptAt", retried.ScheduledAt)
		changed++
	}
	return changed, nil
}

// scheduleReadyTasks enqueues every PENDING task whose dependencies are all
// COMPLETED, persisting each task's fully resolved input at schedule time.
func (e *Engine) scheduleReadyTasks(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	def *domain.WorkflowDefinition,
	tasks []domain.Task,
	byName map[string]*domain.Task,
) (int, error) {
	scheduled := 0
	for i := range tasks {
		t := &tasks[i]
		if t.State != domain.TaskPending {
			continue
		}

		ready := true
		for _, dep := range t.DependsOn {
			upstream, ok := byName[dep]
			if !ok {
				return scheduled, fmt.Errorf(
					"%w: task %q depends on %q which is not materialized",
					domain.ErrValidation, t.Name, dep)
			}
			if upstream.State != domain.TaskCompleted {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}

		spec, ok := def.Spec.Task(t.Name)
		if !ok {
			return scheduled, fmt.Errorf("%w: task %q is absent from definition %s v%d",
				domain.ErrValidation, t.Name, def.Name, def.Version)
		}

		input, err := buildTaskInput(exec, spec, byName)
		if err != nil {
			return scheduled, fmt.Errorf("build input for task %q: %w", t.Name, err)
		}

		// No delay: a task whose dependencies are satisfied is claimable at once.
		if _, err := tx.ScheduleTask(ctx, t.ID, input, 0); err != nil {
			return scheduled, fmt.Errorf("schedule task %q: %w", t.Name, err)
		}
		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: exec.ID,
			EventType:   domain.EventTaskScheduled,
			TaskName:    t.Name,
			Payload: map[string]any{
				"taskId":    t.ID,
				"activity":  t.Activity,
				"taskQueue": t.TaskQueue,
				"attempt":   t.Attempt,
			},
		}); err != nil {
			return scheduled, err
		}

		scheduled++
		e.logger.Debug("task scheduled",
			"executionId", exec.ID, "task", t.Name, "activity", t.Activity)
	}
	return scheduled, nil
}

// completeExecution finalizes a successful run. The workflow's output is the
// output of its terminal tasks — the nodes nothing else depends on.
func (e *Engine) completeExecution(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	def *domain.WorkflowDefinition,
	byName map[string]*domain.Task,
) error {
	result := map[string]json.RawMessage{}
	for _, name := range def.Spec.TerminalTasks() {
		t, ok := byName[name]
		if !ok {
			continue
		}
		out := t.Output
		if len(out) == 0 {
			out = json.RawMessage(`null`)
		}
		result[name] = out
	}
	output, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal execution output: %w", err)
	}

	if _, err := tx.TransitionExecution(ctx, exec.ID,
		domain.WorkflowRunning, domain.WorkflowCompleted, output, ""); err != nil {
		return err
	}
	if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
		ExecutionID: exec.ID,
		EventType:   domain.EventWorkflowCompleted,
		Payload:     map[string]any{"terminalTasks": def.Spec.TerminalTasks()},
	}); err != nil {
		return err
	}

	e.logger.Info("execution completed", "executionId", exec.ID, "workflow", def.Name)
	return nil
}

// failExecution finalizes a run that cannot progress.
//
// The dead-lettered task is left in place so an operator can still inspect and
// replay it; only the tasks that will now never run are canceled, so nothing is
// left dangling in the queue.
func (e *Engine) failExecution(
	ctx context.Context,
	tx *store.Store,
	exec *domain.WorkflowExecution,
	failed *domain.Task,
) error {
	reason := fmt.Sprintf("task %q was dead-lettered after %d attempt(s) [%s]: %s",
		failed.Name, failed.Attempt, failed.LastFailureReason, failed.Error)

	canceled, err := tx.CancelIncompleteTasksExcept(ctx, exec.ID, failed.ID)
	if err != nil {
		return err
	}
	if _, err := tx.TransitionExecution(ctx, exec.ID,
		domain.WorkflowRunning, domain.WorkflowFailed, nil, reason); err != nil {
		return err
	}
	if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
		ExecutionID: exec.ID,
		EventType:   domain.EventWorkflowFailed,
		TaskName:    failed.Name,
		Payload: map[string]any{
			"reason":           reason,
			"failedTask":       failed.Name,
			"attempt":          failed.Attempt,
			"leaseExpiryCount": failed.LeaseExpiryCount,
			"failureReason":    failed.LastFailureReason,
			"tasksCanceled":    canceled,
			"taskError":        failed.Error,
			"deadLetteredTask": failed.ID,
		},
	}); err != nil {
		return err
	}

	e.logger.Warn("execution failed",
		"executionId", exec.ID, "task", failed.Name,
		"failureReason", failed.LastFailureReason, "error", failed.Error)
	return nil
}

// buildTaskInput assembles the self-contained payload a worker receives.
//
// Inputs are resolved and persisted at schedule time rather than at claim time.
// That means a task's input is fixed the moment it is enqueued, so a re-claim
// after a worker crash replays byte-identical input instead of re-deriving it
// from state that may have moved on.
func buildTaskInput(
	exec *domain.WorkflowExecution,
	spec domain.TaskSpec,
	byName map[string]*domain.Task,
) (json.RawMessage, error) {
	payload := domain.TaskInput{
		WorkflowInput: exec.Input,
		TaskInput:     spec.Input,
	}
	if len(spec.DependsOn) > 0 {
		payload.Upstream = make(map[string]json.RawMessage, len(spec.DependsOn))
		for _, dep := range spec.DependsOn {
			upstream, ok := byName[dep]
			if !ok {
				return nil, fmt.Errorf("%w: upstream task %q not found", domain.ErrValidation, dep)
			}
			out := upstream.Output
			if len(out) == 0 {
				out = json.RawMessage(`null`)
			}
			payload.Upstream[dep] = out
		}
	}
	return json.Marshal(payload)
}
