package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/config"
	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// Worker polls the control plane and executes activities.
type Worker struct {
	cfg      config.Worker
	client   *client.Client
	registry *Registry
	logger   *slog.Logger
	health   *health
	metrics  *telemetry.Metrics

	// busySlots counts activities executing right now. Divided by configured
	// concurrency this is worker utilization, which is the signal that says whether
	// the pool is saturated -- distinct from queue depth, which says whether there
	// is work waiting.
	busySlots atomic.Int64

	// id is assigned by the server at registration.
	id uuid.UUID
}

// WithMetrics attaches a metric set to the worker.
func (w *Worker) WithMetrics(m *telemetry.Metrics) *Worker {
	w.metrics = m
	if m != nil {
		m.SetWorkerSlots(w.cfg.Concurrency, 0)
	}
	return w
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
	// A worker is unready if it has not had a successful poll in several poll
	// intervals — long enough that an idle queue or a brief blip does not flap the
	// readiness gate.
	staleAfter := max(10*cfg.PollInterval, 30*time.Second)
	return &Worker{
		cfg:      cfg,
		client:   c,
		registry: registry,
		logger:   logger,
		health:   newHealth(staleAfter),
	}, nil
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
	w.health.markRegistered()

	w.logger.Info("worker started",
		"workerId", w.id,
		"worker", w.cfg.Name,
		"taskQueue", w.cfg.TaskQueue,
		"concurrency", w.cfg.Concurrency,
		"activities", activities)

	var wg sync.WaitGroup

	// Heartbeats are what the reaper's failure detector uses to notice a dead
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
		if w.metrics != nil {
			switch {
			case err == nil:
				w.metrics.WorkerPolls.WithLabelValues("task").Inc()
			case errors.Is(err, client.ErrNoTask):
				w.metrics.WorkerPolls.WithLabelValues("empty").Inc()
			default:
				w.metrics.WorkerPolls.WithLabelValues("error").Inc()
			}
		}
		switch {
		case err == nil:
			// Reset backoff: the queue has work, so poll again immediately
			// after finishing rather than sleeping.
			idleBackoff = w.cfg.PollInterval
			w.health.markPolled()
			w.execute(ctx, logger, task)
			continue

		case errors.Is(err, client.ErrNoTask):
			// An idle queue is the normal case and still proves the control plane
			// is reachable, so it counts as a successful poll for readiness.
			w.health.markPolled()

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

	// Rejoin the workflow's trace.
	//
	// This is the point of persisting the traceparent on the execution row. The
	// span opened here becomes a child of the request that started the workflow,
	// however long ago and in whatever process — so one trace shows the whole
	// workflow: acceptance, scheduling, and each activity attempt on whichever
	// worker ran it. Without it, each attempt would be an orphan root span and the
	// causal structure that makes a trace worth reading would be gone.
	//
	// Applied to runCtx, so the span is the parent of the activity's own work and
	// of the outbound report call. Note runCtx derives from context.WithoutCancel,
	// which strips cancellation but preserves values, so the span survives a
	// shutdown signal exactly as the activity does.
	runCtx = telemetry.ContextFromTraceparent(runCtx, task.Traceparent)
	runCtx, span := telemetry.Start(runCtx, "activity."+task.Activity,
		telemetry.ActivityAttr(task.Activity),
		telemetry.TaskAttr(task.Name),
		telemetry.ExecutionAttr(task.ExecutionID),
		telemetry.AttemptAttr(task.Attempt),
	)

	// Renew the lease while the activity runs. Without this, every task would
	// need a lease as long as the slowest possible activity, and a crashed
	// worker's task would stay stuck for that entire duration. The same loop
	// carries cancellation back from the control plane and cancels runCtx.
	renewal := w.startLeaseRenewal(runCtx, logger, task, cancel)

	// Utilization is tracked around the activity call itself, so it measures time
	// actually spent executing rather than time spent polling.
	busy := w.busySlots.Add(1)
	if w.metrics != nil {
		w.metrics.SetWorkerSlots(w.cfg.Concurrency, int(busy))
	}

	started := time.Now()
	output, runErr := invoke(runCtx, fn, input)
	elapsed := time.Since(started)

	if remaining := w.busySlots.Add(-1); w.metrics != nil {
		w.metrics.SetWorkerSlots(w.cfg.Concurrency, int(remaining))
	}

	canceled, leaseLost := renewal.stop()

	// Ended before reporting, so the span's duration is the activity's execution
	// time and not execution plus the round trip that reports it. The report gets
	// its own child span from the instrumented transport.
	telemetry.End(span, runErr)

	// Report inside the traced context so the complete/fail call is a child of this
	// activity span rather than a new root. ctx, not runCtx: report needs the
	// original cancellation semantics, and takes its own timeout.
	ctx = trace.ContextWithSpan(ctx, span)

	switch {
	case leaseLost:
		// The control plane already gave this task to someone else, or canceled
		// it. Reporting would be refused, so drop the result rather than spend a
		// round trip discovering that.
		logger.Warn("abandoning task: lease no longer held",
			"durationMs", elapsed.Milliseconds())
		return
	case canceled:
		logger.Info("task canceled by request", "durationMs", elapsed.Milliseconds())
		w.report(ctx, logger, task, nil,
			fmt.Errorf("activity canceled: cancellation requested by the control plane"))
		return
	case runErr != nil:
		logger.Warn("activity failed", "durationMs", elapsed.Milliseconds(), "error", runErr)
	default:
		logger.Info("activity completed", "durationMs", elapsed.Milliseconds())
	}
	w.report(ctx, logger, task, output, runErr)
}

// leaseRenewal tracks a background lease-renewal loop for one task attempt.
type leaseRenewal struct {
	done      chan struct{}
	stopOnce  sync.Once
	stopCh    chan struct{}
	canceled  atomic.Bool
	leaseLost atomic.Bool
}

// stop halts renewal and reports whether cancellation was requested and whether
// the lease was lost.
func (r *leaseRenewal) stop() (canceled, leaseLost bool) {
	r.stopOnce.Do(func() { close(r.stopCh) })
	<-r.done
	return r.canceled.Load(), r.leaseLost.Load()
}

// startLeaseRenewal begins renewing a task's lease until stopped.
//
// It renews at a fraction of the lease duration so a single dropped request does
// not cost the lease. cancelRun is invoked when the control plane asks for
// cancellation or the lease is lost, which is what makes cancellation actually
// interrupt a running activity rather than merely being recorded.
func (w *Worker) startLeaseRenewal(
	ctx context.Context,
	logger *slog.Logger,
	task *client.TaskResponse,
	cancelRun context.CancelFunc,
) *leaseRenewal {
	r := &leaseRenewal{done: make(chan struct{}), stopCh: make(chan struct{})}

	// Renew at a third of the lease, so two consecutive failures still leave a
	// margin before the reaper would step in.
	interval := w.cfg.LeaseDuration / 3
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}

	go func() {
		defer close(r.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			beatCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.RequestTimeout)
			beat, err := w.client.HeartbeatTask(beatCtx, task.ID, task.ClaimToken,
				int(w.cfg.LeaseDuration.Seconds()))
			cancel()

			switch {
			case err == nil && beat.CancelRequested:
				logger.Info("cancellation requested, interrupting activity")
				r.canceled.Store(true)
				cancelRun()
				return
			case err == nil:
				// Lease extended; nothing to do.
			case errors.Is(err, client.ErrStaleClaim):
				logger.Warn("lease lost while running, interrupting activity", "error", err)
				r.leaseLost.Store(true)
				cancelRun()
				return
			default:
				// A transient failure. The client already retried; keep going and
				// try again next tick rather than abandoning work that may well
				// still be validly leased.
				logger.Warn("lease renewal failed", "error", err)
			}
		}
	}()

	return r
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
		if _, err := w.client.FailTask(reportCtx, task.ID, failRequestFor(task, runErr)); err != nil {
			w.logReportFailure(logger, "fail", err)
		}
		return
	}

	encoded, err := encodeOutput(output)
	if err != nil {
		// The activity succeeded but produced something we cannot persist. That is
		// a bug in the activity, not a transient fault, so retrying is pointless.
		logger.Error("activity output is not JSON-encodable", "error", err)
		permanent := false
		if _, failErr := w.client.FailTask(reportCtx, task.ID, client.FailRequest{
			ClaimToken: task.ClaimToken,
			Error:      err.Error(),
			Retryable:  &permanent,
		}); failErr != nil {
			w.logReportFailure(logger, "fail", failErr)
		}
		return
	}
	if _, err := w.client.CompleteTask(reportCtx, task.ID, task.ClaimToken, encoded); err != nil {
		w.logReportFailure(logger, "complete", err)
	}
}

// failRequestFor classifies an activity error for the control plane.
//
// A timeout is reported as such rather than as a generic error, because "the
// activity exceeded its budget" and "the activity returned an error" call for
// different operator responses even though both are failures.
func failRequestFor(task *client.TaskResponse, runErr error) client.FailRequest {
	req := client.FailRequest{
		ClaimToken: task.ClaimToken,
		Error:      runErr.Error(),
	}
	if errors.Is(runErr, context.DeadlineExceeded) {
		req.Reason = "TIMEOUT"
	}
	// An unimplemented activity cannot be fixed by trying again on this worker
	// fleet, so it goes straight to the dead letter queue for a human to look at.
	if errors.Is(runErr, domain.ErrNotFound) {
		permanent := false
		req.Retryable = &permanent
	}
	return req
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
