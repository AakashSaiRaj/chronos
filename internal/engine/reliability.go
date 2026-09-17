package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// DefaultReplayExtraAttempts is how much attempt budget an operator replay grants
// a dead-lettered task. One, so a replay is a deliberate single retry rather than
// a reset that quietly re-arms the whole retry policy.
const DefaultReplayExtraAttempts = 1

// LeaseHeartbeat is what a worker learns when it renews a task lease.
type LeaseHeartbeat struct {
	Task *domain.Task
	// CancelRequested tells the worker to abandon the task. Cooperative
	// cancellation: the control plane cannot stop another process, only ask.
	CancelRequested bool
	// LeaseExpiresAt is the new deadline the worker must renew before.
	LeaseExpiresAt *time.Time
}

// HeartbeatTask renews a running task's lease and relays any cancellation request.
//
// This is what allows leases to stay short while still supporting long-running
// activities. The alternative — a lease long enough for the slowest possible task
// — would leave a crashed worker's task stuck for that entire duration. Here a
// live worker keeps proving it is alive, and the moment it stops the reaper's
// window is only one heartbeat interval wide.
//
// Renewal deliberately does not extend the *task's* timeout: the lease bounds how
// long the control plane waits for news, not how long the activity may run.
func (svc *Service) HeartbeatTask(ctx context.Context, taskID, claimToken uuid.UUID, extendBy time.Duration) (*LeaseHeartbeat, error) {
	if claimToken == uuid.Nil {
		return nil, fmt.Errorf("%w: claimToken is required to renew a lease", domain.ErrValidation)
	}

	task, err := svc.store.RenewLease(ctx, taskID, claimToken, extendBy)
	if err != nil {
		return nil, err
	}

	if task.CancelRequested {
		svc.logger.Debug("relaying cancellation to worker",
			"taskId", task.ID, "task", task.Name, "workerId", task.WorkerID)
	}
	return &LeaseHeartbeat{
		Task:            task,
		CancelRequested: task.CancelRequested,
		LeaseExpiresAt:  task.LeaseExpiresAt,
	}, nil
}

// ListDeadLetterTasks returns tasks parked for operator attention.
func (svc *Service) ListDeadLetterTasks(ctx context.Context, f store.DeadLetterFilter) ([]domain.Task, error) {
	return svc.store.ListDeadLetterTasks(ctx, f)
}

// CountDeadLetterTasks returns the size of the dead letter queue, which is the
// number an operator watches.
func (svc *Service) CountDeadLetterTasks(ctx context.Context) (int, error) {
	return svc.store.CountDeadLetterTasks(ctx)
}

// ReplayTask returns a dead-lettered task to the queue and revives its workflow.
//
// This is the remediation path that makes dead-lettering more than a
// well-documented way to lose work: once the underlying cause is fixed, an
// operator can resume the run from exactly where it stopped instead of restarting
// it and redoing everything that already succeeded.
//
// It is the only thing in Chronos that moves an execution out of FAILED, and it
// requires an explicit request. The engine never does it on its own, so a run it
// has given up on stays given up on until a human decides otherwise.
func (svc *Service) ReplayTask(ctx context.Context, taskID uuid.UUID, extraAttempts int) (*domain.Task, error) {
	if extraAttempts <= 0 {
		extraAttempts = DefaultReplayExtraAttempts
	}

	var replayed *domain.Task
	err := svc.store.WithTx(ctx, func(tx *store.Store) error {
		task, err := tx.GetTask(ctx, taskID)
		if err != nil {
			return err
		}
		if task.State != domain.TaskDeadLetter {
			return fmt.Errorf("%w: task %s is %s, only a dead-lettered task can be replayed",
				domain.ErrConflict, taskID, task.State)
		}

		// Lock the execution: reviving it races with the engine's own sweep, and
		// this is the one place a FAILED execution moves backwards.
		if err := tx.LockExecutionForWrite(ctx, task.ExecutionID); err != nil {
			return err
		}
		exec, err := tx.GetExecution(ctx, task.ExecutionID)
		if err != nil {
			return err
		}
		if exec.State == domain.WorkflowCompleted || exec.State == domain.WorkflowCanceled {
			return fmt.Errorf("%w: execution %s is %s and cannot be replayed",
				domain.ErrConflict, exec.ID, exec.State)
		}

		// Requeue immediately: an operator replaying a task has just fixed
		// something and wants it tried now, not after a backoff.
		replayed, err = tx.RequeueTask(ctx, taskID, 0, store.RequeueReason{
			From:               domain.TaskDeadLetter,
			Error:              "",
			GrantExtraAttempts: extraAttempts,
		})
		if err != nil {
			return err
		}

		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: task.ExecutionID,
			EventType:   domain.EventTaskReplayed,
			TaskName:    task.Name,
			Payload: map[string]any{
				"taskId":           task.ID,
				"activity":         task.Activity,
				"previousAttempts": task.Attempt,
				"previousError":    task.Error,
				"previousReason":   task.LastFailureReason,
				"extraAttempts":    extraAttempts,
				"newMaxAttempts":   replayed.MaxAttempts,
			},
		}); err != nil {
			return err
		}

		// Revive the workflow so the engine picks the task back up. Tasks the
		// engine canceled when the run failed are restored to PENDING, otherwise
		// the replayed task would succeed into a graph that can never finish.
		if exec.State == domain.WorkflowFailed {
			restored, err := tx.RestoreCanceledTasks(ctx, exec.ID)
			if err != nil {
				return err
			}
			if _, err := tx.TransitionExecution(ctx, exec.ID,
				domain.WorkflowFailed, domain.WorkflowRunning, nil, ""); err != nil {
				return err
			}
			if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
				ExecutionID: exec.ID,
				EventType:   domain.EventWorkflowStarted,
				Payload: map[string]any{
					"resumedFromDeadLetter": task.Name,
					"tasksRestored":         restored,
				},
			}); err != nil {
				return err
			}
			svc.logger.Info("execution resumed from dead letter",
				"executionId", exec.ID, "task", task.Name, "tasksRestored", restored)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	svc.notifier.Nudge()
	svc.logger.Info("task replayed",
		"taskId", replayed.ID, "task", replayed.Name, "maxAttempts", replayed.MaxAttempts)
	return replayed, nil
}

// requestTaskCancellation asks an execution's running tasks to stop, in addition
// to the control-plane cancellation performed by CancelExecution.
func (svc *Service) requestTaskCancellation(ctx context.Context, tx *store.Store, executionID uuid.UUID) (int, error) {
	flagged, err := tx.RequestTaskCancellation(ctx, executionID)
	if err != nil {
		return 0, err
	}
	if flagged == 0 {
		return 0, nil
	}
	if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
		ExecutionID: executionID,
		EventType:   domain.EventTaskCancelRequested,
		Payload:     map[string]any{"tasksSignalled": flagged},
	}); err != nil {
		return flagged, err
	}
	return flagged, nil
}

// IsStaleClaim reports whether err means the caller's lease is gone and it should
// stop working on the task rather than retry.
func IsStaleClaim(err error) bool { return errors.Is(err, domain.ErrStaleClaim) }
