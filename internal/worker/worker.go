package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/config"
	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// Worker polls the control plane and executes activities.
type Worker struct {
	cfg      config.Worker
	client   *client.Client
	registry *Registry
	logger   *slog.Logger

	// id is assigned by the server at registration.
	id uuid.UUID
}

// New constructs a Worker. The registry must be non-empty: a worker with no
// activities would poll forever and never be able to run anything.
func New(cfg config.Worker, c *client.Client, registry *Registry, logger *slog.Logger) (*Worker, error) {
	if c == nil {
		return nil, errors.New("worker: client is required")
	}
	if registry == nil || registry.Len() == 0 {
		return nil, errors.New("worker: at least one activity must be registered")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{cfg: cfg, client: c, registry: registry, logger: logger}, nil
}

// Run registers the worker and processes tasks until ctx is canceled.
//
// Shutdown is graceful: polling stops immediately, but in-flight activities are
// given time to finish and report. That matters because a task abandoned
// mid-flight stays leased until it expires, which delays its retry.
func (w *Worker) Run(ctx context.Context) error {
	activities := w.registry.Names()

	registered, err := w.client.RegisterWorker(ctx, w.cfg.Name, w.cfg.TaskQueue, activities)
	if err != nil {
		return fmt.Errorf("register worker %q: %w", w.cfg.Name, err)
	}
	w.id = registered.ID

	w.logger.Info("worker started",
		"workerId", w.id,
		"worker", w.cfg.Name,
		"taskQueue", w.cfg.TaskQueue,
		"concurrency", w.cfg.Concurrency,
		"activities", activities)

	var wg sync.WaitGroup

	// Heartbeats are what Phase 2's failure detector uses to notice a dead
	// worker, so they run independently of task execution.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()

	// One polling goroutine per concurrency slot. Each is an independent
	// claimant, which is exactly what SKIP LOCKED is designed for: they contend
	// on the queue without blocking one another.
	for slot := range w.cfg.Concurrency {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			w.pollLoop(ctx, slot)
		}(slot)
	}

	wg.Wait()
	w.logger.Info("worker stopped", "workerId", w.id, "worker", w.cfg.Name)
	return nil
}

// heartbeatLoop refreshes worker liveness.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(ctx, w.cfg.RequestTimeout)
			err := w.client.Heartbeat(hbCtx, w.id)
			cancel()
			if err != nil && ctx.Err() == nil {
				// A missed heartbeat is not fatal; the next one may land. Only
				// sustained failure matters, and that is the control plane's
				// call to make, not the worker's.
				w.logger.Warn("heartbeat failed", "workerId", w.id, "error", err)
			}
		}
	}
}

// pollLoop claims and executes tasks one at a time.
func (w *Worker) pollLoop(ctx context.Context, slot int) {
	logger := w.logger.With("workerId", w.id, "slot", slot)
	idleBackoff := w.cfg.PollInterval

	for {
		if ctx.Err() != nil {
			return
		}

		task, err := w.poll(ctx)
		switch {
		case err == nil:
			// Reset backoff: the queue has work, so poll again immediately
			// after finishing rather than sleeping.
			idleBackoff = w.cfg.PollInterval
			w.execute(ctx, logger, task)
			continue

		case errors.Is(err, client.ErrNoTask):
			// Idle queue is the normal case; wait before asking again.

		case ctx.Err() != nil:
			return

		default:
			logger.Warn("poll failed", "error", err)
			// Back off harder on real errors so a struggling server is not
			// hammered by the whole worker fleet.
			idleBackoff = min(idleBackoff*2, 5*time.Second)
		}

		if err := sleep(ctx, idleBackoff); err != nil {
			return
		}
	}
}

func (w *Worker) poll(ctx context.Context) (*client.TaskResponse, error) {
	pollCtx, cancel := context.WithTimeout(ctx, w.cfg.RequestTimeout)
	defer cancel()

	return w.client.PollTask(pollCtx, client.PollRequest{
		WorkerID:     w.cfg.Name,
		TaskQueue:    w.cfg.TaskQueue,
		Activities:   w.registry.Names(),
		LeaseSeconds: int(w.cfg.LeaseDuration.Seconds()),
	})
}

// execute runs a task's activity and reports the outcome.
func (w *Worker) execute(ctx context.Context, logger *slog.Logger, task *client.TaskResponse) {
	logger = logger.With(
		"taskId", task.ID, "task", task.Name,
		"activity", task.Activity, "executionId", task.ExecutionID, "attempt", task.Attempt)

	fn, err := w.registry.Lookup(task.Activity)
	if err != nil {
		// The server should not lease us an activity we did not advertise, so
		// this means a stale registration. Fail the task rather than sit on it.
		logger.Error("no implementation for leased activity", "error", err)
		w.report(ctx, logger, task, nil, err)
		return
	}

	input, err := decodeActivityInput(task)
	if err != nil {
		logger.Error("task input is malformed", "error", err)
		w.report(ctx, logger, task, nil, err)
		return
	}

	// Bound the attempt: the task's own timeout wins when it declares one.
	// Without a bound, a hung activity would hold its lease until expiry and
	// silently consume a concurrency slot.
	timeout := w.cfg.TaskTimeout
	if task.TimeoutSeconds > 0 {
		timeout = time.Duration(task.TimeoutSeconds) * time.Second
	}

	// Detach from ctx cancellation for the run itself so a shutdown signal does
	// not abort work that is about to finish; the timeout still applies.
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	started := time.Now()
	output, runErr := invoke(runCtx, fn, input)
	elapsed := time.Since(started)

	if runErr != nil {
		logger.Warn("activity failed", "durationMs", elapsed.Milliseconds(), "error", runErr)
	} else {
		logger.Info("activity completed", "durationMs", elapsed.Milliseconds())
	}
	w.report(ctx, logger, task, output, runErr)
}

// invoke calls an activity, converting a panic into an error so one bad activity
// cannot take down the whole worker process.
func invoke(ctx context.Context, fn ActivityFunc, in ActivityInput) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("activity %q panicked: %v\n%s", in.Activity, recovered, debug.Stack())
			result = nil
		}
	}()
	return fn(ctx, in)
}

// report tells the control plane how the attempt went.
//
// Reporting uses a context detached from shutdown so a result that was already
// computed is not lost to a SIGTERM; losing it would force the task to wait for
// lease expiry and be redone.
func (w *Worker) report(ctx context.Context, logger *slog.Logger, task *client.TaskResponse, output any, runErr error) {
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.RequestTimeout)
	defer cancel()

	if runErr != nil {
		if _, err := w.client.FailTask(reportCtx, task.ID, task.ClaimToken, runErr.Error()); err != nil {
			w.logReportFailure(logger, "fail", err)
		}
		return
	}

	encoded, err := encodeOutput(output)
	if err != nil {
		logger.Error("activity output is not JSON-encodable", "error", err)
		if _, failErr := w.client.FailTask(reportCtx, task.ID, task.ClaimToken, err.Error()); failErr != nil {
			w.logReportFailure(logger, "fail", failErr)
		}
		return
	}
	if _, err := w.client.CompleteTask(reportCtx, task.ID, task.ClaimToken, encoded); err != nil {
		w.logReportFailure(logger, "complete", err)
	}
}

func (w *Worker) logReportFailure(logger *slog.Logger, kind string, err error) {
	if errors.Is(err, client.ErrStaleClaim) {
		// Another worker owns the task now. Nothing to do: the engine's view is
		// authoritative and our result is correctly discarded.
		logger.Warn("dropping result for reassigned task", "reportKind", kind, "error", err)
		return
	}
	// The task stays leased until it expires, at which point Phase 2's sweeper
	// requeues it. No state is lost, the task is just delayed.
	logger.Error("could not report task outcome", "reportKind", kind, "error", err)
}

// decodeActivityInput unpacks the persisted task payload.
func decodeActivityInput(task *client.TaskResponse) (ActivityInput, error) {
	in := ActivityInput{
		TaskName:    task.Name,
		Activity:    task.Activity,
		ExecutionID: task.ExecutionID.String(),
		Attempt:     task.Attempt,
	}
	if len(task.Input) == 0 {
		return in, nil
	}

	var payload domain.TaskInput
	if err := json.Unmarshal(task.Input, &payload); err != nil {
		return in, fmt.Errorf("%w: task %q input is not a Chronos task payload: %v",
			domain.ErrValidation, task.Name, err)
	}
	in.WorkflowInput = payload.WorkflowInput
	in.TaskInput = payload.TaskInput
	in.Upstream = payload.Upstream
	return in, nil
}

func encodeOutput(output any) (json.RawMessage, error) {
	if output == nil {
		return nil, nil
	}
	if raw, ok := output.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return nil, errors.New("activity returned invalid raw JSON")
		}
		return raw, nil
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("marshal activity output: %w", err)
	}
	return encoded, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
