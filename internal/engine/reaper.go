package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// ReaperConfig tunes failure detection.
type ReaperConfig struct {
	// Interval is how often the reaper sweeps.
	Interval time.Duration
	// BatchSize bounds how many tasks or workers one sweep handles.
	BatchSize int
	// WorkerTimeout is how long a worker's heartbeat may lapse before it is
	// declared dead. It should be several heartbeat intervals, so one dropped
	// request cannot evict a healthy worker.
	WorkerTimeout time.Duration
	// Metrics is optional; nil disables instrumentation.
	Metrics *telemetry.Metrics
	// Jitter is injectable so requeue delays can be made reproducible in tests.
	// There is deliberately no injectable clock: every expiry check is evaluated
	// against the database clock, in the same statement as the write it guards.
	Jitter func() float64
}

func (c ReaperConfig) withDefaults() ReaperConfig {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.WorkerTimeout <= 0 {
		c.WorkerTimeout = 45 * time.Second
	}
	if c.Jitter == nil {
		c.Jitter = rand.Float64
	}
	return c
}

// ReapStats summarizes one sweep. Returned so tests can assert on what happened
// and so the loop can log something meaningful.
type ReapStats struct {
	LeasesExpired                 int
	TasksRequeued                 int
	TasksDeadLettered             int
	WorkersDeclaredDead           int
	TasksReclaimedFromDeadWorkers int
}

// Empty reports whether the sweep found nothing to do, which is the steady state.
func (s ReapStats) Empty() bool {
	return s.LeasesExpired == 0 && s.WorkersDeclaredDead == 0
}

// Reaper detects and repairs the failures a worker cannot report itself.
//
// A worker that crashes, is evicted, or wedges never sends a failure — it simply
// goes quiet. Something has to notice, and it cannot be the worker. The reaper is
// that something: it reclaims tasks whose lease lapsed and declares workers dead
// when their heartbeat stops, which together turn "a process disappeared" from a
// stuck workflow into a retry.
//
// It is deliberately a separate component from the scheduling Engine. The engine
// reacts to state that workers *reported*; the reaper reacts to the absence of
// reports. Keeping them apart means each has one trigger condition, and the
// reaper can run on its own cadence (slower, since it is a timeout detector) or
// as its own deployment.
type Reaper struct {
	store    *store.Store
	cfg      ReaperConfig
	logger   *slog.Logger
	notifier Notifier
}

// NewReaper constructs a Reaper. The notifier is nudged after work is requeued so
// the engine reschedules promptly rather than at its next tick.
func NewReaper(s *store.Store, cfg ReaperConfig, notifier Notifier, logger *slog.Logger) *Reaper {
	if notifier == nil {
		notifier = noopNotifier{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Reaper{store: s, cfg: cfg.withDefaults(), logger: logger, notifier: notifier}
}

// Run drives the reaping loop until ctx is canceled.
func (r *Reaper) Run(ctx context.Context) error {
	r.logger.Info("reaper started",
		"interval", r.cfg.Interval,
		"workerTimeout", r.cfg.WorkerTimeout,
		"batchSize", r.cfg.BatchSize)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		stats, err := r.Sweep(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			r.logger.Info("reaper stopping")
			return nil
		case err != nil:
			// Almost always a transient database problem. Log and keep looping:
			// a reaper that exits leaves failures undetected.
			r.logger.Error("reaper sweep failed", "error", err)
		case !stats.Empty():
			r.logger.Info("reaper sweep",
				"leasesExpired", stats.LeasesExpired,
				"tasksRequeued", stats.TasksRequeued,
				"tasksDeadLettered", stats.TasksDeadLettered,
				"workersDeclaredDead", stats.WorkersDeclaredDead,
				"tasksReclaimed", stats.TasksReclaimedFromDeadWorkers)
		}

		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopping")
			return nil
		case <-ticker.C:
		}
	}
}

// Sweep performs one detection pass. Exported so failure tests can drive it
// deterministically instead of waiting on timers.
func (r *Reaper) Sweep(ctx context.Context) (ReapStats, error) {
	var stats ReapStats

	started := time.Now()
	defer func() {
		if r.cfg.Metrics != nil {
			r.cfg.Metrics.ReaperSweepDuration.Observe(time.Since(started).Seconds())
		}
	}()

	// Declare dead workers first. Reclaiming their tasks immediately is faster
	// than waiting for each individual lease to lapse, and it means the workflow
	// resumes in seconds rather than after a full lease timeout.
	deadStats, err := r.detectDeadWorkers(ctx)
	stats.WorkersDeclaredDead = deadStats.WorkersDeclaredDead
	stats.TasksReclaimedFromDeadWorkers = deadStats.TasksReclaimedFromDeadWorkers
	stats.TasksRequeued += deadStats.TasksRequeued
	stats.TasksDeadLettered += deadStats.TasksDeadLettered
	if err != nil {
		return stats, err
	}

	leaseStats, err := r.reapExpiredLeases(ctx)
	stats.LeasesExpired = leaseStats.LeasesExpired
	stats.TasksRequeued += leaseStats.TasksRequeued
	stats.TasksDeadLettered += leaseStats.TasksDeadLettered
	if err != nil {
		return stats, err
	}

	if m := r.cfg.Metrics; m != nil {
		if stats.TasksRequeued > 0 {
			m.ReaperReclaims.WithLabelValues("requeued").Add(float64(stats.TasksRequeued))
		}
		if stats.TasksDeadLettered > 0 {
			m.ReaperReclaims.WithLabelValues("dead_lettered").Add(float64(stats.TasksDeadLettered))
		}
		if stats.WorkersDeclaredDead > 0 {
			m.WorkersDeclaredDead.Add(float64(stats.WorkersDeclaredDead))
		}
	}

	if stats.TasksRequeued > 0 || stats.TasksDeadLettered > 0 {
		r.notifier.Nudge()
	}
	return stats, nil
}

// reapExpiredLeases reclaims tasks whose lease lapsed without a report.
func (r *Reaper) reapExpiredLeases(ctx context.Context) (ReapStats, error) {
	var stats ReapStats

	ids, err := r.store.ListExpiredLeaseTaskIDs(ctx, r.cfg.BatchSize)
	if err != nil {
		return stats, fmt.Errorf("list expired leases: %w", err)
	}

	for _, id := range ids {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		outcome, err := r.reclaimTask(ctx, id, domain.FailureLeaseExpired)
		if err != nil {
			// One stubborn task must not stall detection for the rest.
			r.logger.Error("could not reclaim task with expired lease", "taskId", id, "error", err)
			continue
		}
		switch outcome {
		case reclaimRequeued:
			stats.LeasesExpired++
			stats.TasksRequeued++
		case reclaimDeadLettered:
			stats.LeasesExpired++
			stats.TasksDeadLettered++
		case reclaimSkipped:
			// Another reaper handled it, or the worker reported just in time.
		}
	}
	return stats, nil
}

// detectDeadWorkers marks silent workers dead and reclaims what they were holding.
func (r *Reaper) detectDeadWorkers(ctx context.Context) (ReapStats, error) {
	var stats ReapStats

	dead, err := r.store.MarkStaleWorkersDead(ctx, r.cfg.WorkerTimeout, r.cfg.BatchSize)
	if err != nil {
		return stats, fmt.Errorf("detect dead workers: %w", err)
	}
	if len(dead) == 0 {
		return stats, nil
	}
	stats.WorkersDeclaredDead = len(dead)

	names := make([]string, 0, len(dead))
	for _, w := range dead {
		names = append(names, w.Name)
		r.logger.Warn("worker declared dead",
			"worker", w.Name, "workerId", w.ID,
			"lastHeartbeatAt", w.LastHeartbeatAt,
			"threshold", r.cfg.WorkerTimeout)
	}

	held, err := r.store.ListTaskIDsHeldByWorkers(ctx, names, r.cfg.BatchSize)
	if err != nil {
		return stats, fmt.Errorf("list tasks held by dead workers: %w", err)
	}

	for _, id := range held {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		outcome, err := r.reclaimTask(ctx, id, domain.FailureWorkerDead)
		if err != nil {
			r.logger.Error("could not reclaim task from dead worker", "taskId", id, "error", err)
			continue
		}
		switch outcome {
		case reclaimRequeued:
			stats.TasksReclaimedFromDeadWorkers++
			stats.TasksRequeued++
		case reclaimDeadLettered:
			stats.TasksReclaimedFromDeadWorkers++
			stats.TasksDeadLettered++
		}
	}
	return stats, nil
}

type reclaimOutcome int

const (
	reclaimSkipped reclaimOutcome = iota
	reclaimRequeued
	reclaimDeadLettered
)

// reclaimTask takes a task away from a worker presumed lost.
//
// A lease expiry grants one extra activity attempt and is charged to the task's
// separate lease-expiry budget instead. The reasoning: the activity did not fail,
// the worker did, so consuming the user's retry budget would mean a task with
// maxAttempts=1 could never survive a worker restart. Bounding it separately still
// prevents a task that reliably kills its worker from cycling forever.
func (r *Reaper) reclaimTask(ctx context.Context, taskID uuid.UUID, cause domain.FailureReason) (reclaimOutcome, error) {
	outcome := reclaimSkipped

	// A lease expiry must be re-verified against the database clock at write
	// time. A worker reporting just as its lease lapses has to win: discarding
	// completed work would be worse than a slightly late reclaim.
	requireExpired := cause == domain.FailureLeaseExpired

	err := r.store.WithTx(ctx, func(tx *store.Store) error {
		task, err := tx.GetTask(ctx, taskID)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil
			}
			return err
		}
		// Cheap pre-check; the authoritative one is the guard on the write below.
		if task.State != domain.TaskRunning {
			return nil
		}

		// The state change is attempted before any history is written. If its
		// guard rejects the write, this transaction must leave no trace — an event
		// saying a lease expired, alongside a task that completed normally, would
		// be worse than no event at all.
		leaseExpiredEvent := store.AppendEventParams{
			ExecutionID: task.ExecutionID,
			EventType:   domain.EventTaskLeaseExpired,
			TaskName:    task.Name,
			Payload: map[string]any{
				"taskId":           task.ID,
				"activity":         task.Activity,
				"workerId":         task.WorkerID,
				"attempt":          task.Attempt,
				"cause":            cause,
				"leaseExpiresAt":   task.LeaseExpiresAt,
				"leaseExpiryCount": task.LeaseExpiryCount,
			},
		}

		if !task.CanSurviveLeaseExpiry() {
			message := fmt.Sprintf(
				"task %q lost its worker %d time(s), exceeding the limit of %d (last cause: %s)",
				task.Name, task.LeaseExpiryCount+1, task.MaxLeaseExpiries, cause)

			if _, err := tx.DeadLetterTaskGuarded(ctx, task.ID, cause, message, requireExpired); err != nil {
				if errors.Is(err, domain.ErrInvalidStateTransition) {
					return nil
				}
				return err
			}
			if _, err := tx.AppendEvent(ctx, leaseExpiredEvent); err != nil {
				return err
			}
			if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
				ExecutionID: task.ExecutionID,
				EventType:   domain.EventTaskDeadLettered,
				TaskName:    task.Name,
				Payload: map[string]any{
					"taskId":           task.ID,
					"reason":           cause,
					"leaseExpiryCount": task.LeaseExpiryCount + 1,
					"maxLeaseExpiries": task.MaxLeaseExpiries,
					"error":            message,
				},
			}); err != nil {
				return err
			}
			outcome = reclaimDeadLettered
			r.logger.Warn("task dead-lettered after repeated worker loss",
				"taskId", task.ID, "task", task.Name,
				"leaseExpiryCount", task.LeaseExpiryCount+1, "cause", cause)
			return nil
		}

		delay := task.RetryPolicy.BackoffFor(task.LeaseExpiryCount+1, r.cfg.Jitter())
		message := fmt.Sprintf("worker %q stopped reporting on attempt %d (%s)",
			task.WorkerID, task.Attempt, cause)

		requeued, err := tx.RequeueTask(ctx, task.ID, delay, store.RequeueReason{
			From:                domain.TaskRunning,
			Failure:             cause,
			Error:               message,
			CountsAsLeaseExpiry: true,
			// Give back the attempt this lost claim consumed; the loss is charged
			// to the lease-expiry budget instead.
			GrantExtraAttempts:  1,
			RequireLeaseExpired: requireExpired,
		})
		if err != nil {
			if errors.Is(err, domain.ErrInvalidStateTransition) {
				// The guard rejected the write: the worker reported in time, or
				// another reaper got there first. Both are fine outcomes.
				return nil
			}
			return err
		}

		if _, err := tx.AppendEvent(ctx, leaseExpiredEvent); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: task.ExecutionID,
			EventType:   domain.EventTaskRequeued,
			TaskName:    task.Name,
			Payload: map[string]any{
				"taskId":        task.ID,
				"activity":      task.Activity,
				"lostWorkerId":  task.WorkerID,
				"cause":         cause,
				"backoffMs":     delay.Milliseconds(),
				"nextAttemptAt": requeued.ScheduledAt,
			},
		}); err != nil {
			return err
		}

		if r.cfg.Metrics != nil {
			r.cfg.Metrics.TaskLeaseExpiries.WithLabelValues(task.Activity, string(cause)).Inc()
		}
		outcome = reclaimRequeued
		r.logger.Info("task requeued after worker loss",
			"taskId", task.ID, "task", task.Name,
			"lostWorker", task.WorkerID, "cause", cause, "nextAttemptAt", requeued.ScheduledAt)
		return nil
	})
	if err != nil {
		return reclaimSkipped, err
	}
	return outcome, nil
}
