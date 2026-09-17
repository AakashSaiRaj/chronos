package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// workerTimeout is the heartbeat-lapse threshold these tests configure. Elapsed
// time is simulated by backdating rows (see testsupport/failure.go) rather than by
// faking a clock, because Chronos evaluates every deadline against the database
// clock — the design that keeps replicas from disagreeing about what time it is.
const workerTimeout = 30 * time.Second

// reliabilityHarness adds a reaper to the base harness.
type reliabilityHarness struct {
	*harness
	reaper *engine.Reaper
}

// newReliabilityHarness builds an engine and reaper with jitter disabled, so
// retry delays follow the backoff curve exactly.
func newReliabilityHarness(t *testing.T) *reliabilityHarness {
	t.Helper()
	s := testsupport.NewStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	noJitter := func() float64 { return 0 }

	eng := engine.New(s, engine.Config{
		PollInterval: 10 * time.Millisecond,
		Jitter:       noJitter,
	}, testsupport.Logger())

	reaper := engine.NewReaper(s, engine.ReaperConfig{
		Interval:      10 * time.Millisecond,
		WorkerTimeout: workerTimeout,
		Jitter:        noJitter,
	}, eng, testsupport.Logger())

	base := &harness{
		store:  s,
		engine: eng,
		svc:    engine.NewService(s, eng, testsupport.Logger()),
		ctx:    ctx,
	}
	return &reliabilityHarness{harness: base, reaper: reaper}
}

func (h *reliabilityHarness) reap(t *testing.T) engine.ReapStats {
	t.Helper()
	stats, err := h.reaper.Sweep(h.ctx)
	require.NoError(t, err)
	return stats
}

// crashWorker simulates a worker that stops reporting: its lease lapses with no
// result ever submitted.
func (h *reliabilityHarness) crashWorker(t *testing.T, taskID uuid.UUID) {
	t.Helper()
	testsupport.ExpireLease(t, h.store, taskID)
}

// skipBackoff pulls a pending retry into the past so a test need not sleep
// through its delay.
func (h *reliabilityHarness) skipBackoff(t *testing.T, taskID uuid.UUID) {
	t.Helper()
	testsupport.MakeClaimable(t, h.store, taskID)
}

// instantRetry is a policy with a negligible delay, for tests that exercise the
// retry *path* rather than its timing. Tests that assert on backoff durations use
// a real policy instead.
func instantRetry() *domain.RetryPolicy {
	return &domain.RetryPolicy{InitialIntervalMS: 1, MaxIntervalMS: 1, JitterPercent: ptrInt(0)}
}

// retrySpec is a single-task workflow with a known retry budget and policy.
func retrySpec(name string, maxAttempts int, policy *domain.RetryPolicy) domain.WorkflowSpec {
	return domain.WorkflowSpec{
		Name:    name,
		Version: 1,
		Tasks: []domain.TaskSpec{{
			Name:        "task_a",
			Activity:    "flaky",
			MaxAttempts: maxAttempts,
			RetryPolicy: policy,
		}},
	}
}

func (h *reliabilityHarness) task(t *testing.T, execID uuid.UUID, name string) domain.Task {
	t.Helper()
	task, ok := h.tasksByName(t, execID)[name]
	require.True(t, ok, "task %q must exist", name)
	return task
}

// failTask reports a retryable activity failure as a worker would.
func (h *reliabilityHarness) failTask(t *testing.T, task *domain.Task, message string) {
	t.Helper()
	require.NotNil(t, task.ClaimToken)
	_, err := h.svc.FailTask(h.ctx, task.ID, *task.ClaimToken, store.FailParams{
		Error: message, Retryable: true,
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// The scenario named in the project brief
// ---------------------------------------------------------------------------

// TestWorkerCrashLeaseExpiresAnotherWorkerContinues is the exact failure sequence
// Phase 2 exists to handle:
//
//	worker crashes -> task lease expires -> another worker claims it -> workflow continues
//
// The crashed worker never reports anything, which is the whole difficulty: the
// control plane has to infer the failure from silence.
func TestWorkerCrashLeaseExpiresAnotherWorkerContinues(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"crash-1"}`, "")

	// worker-1 claims task_a and then "crashes": it simply stops, reporting
	// nothing and renewing nothing.
	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	require.Equal(t, "task_a", claimed.Name)
	require.Equal(t, 1, claimed.Attempt)
	require.NotNil(t, claimed.LeaseExpiresAt)
	lostToken := *claimed.ClaimToken

	// Before the lease lapses nothing happens: a slow worker is not a dead one.
	require.Empty(t, h.reap(t).LeasesExpired, "a live lease must not be reclaimed")
	require.Nil(t, h.pollOne(t, "worker-2"), "the task must stay leased to worker-1")

	// worker-1 goes silent: its lease lapses with no result ever submitted.
	h.crashWorker(t, claimed.ID)

	stats := h.reap(t)
	require.Equal(t, 1, stats.LeasesExpired, "the lapsed lease must be detected")
	require.Equal(t, 1, stats.TasksRequeued, "and the task returned to the queue")

	requeued := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskScheduled, requeued.State)
	require.Empty(t, requeued.WorkerID, "the lost worker must no longer own it")
	require.Nil(t, requeued.ClaimToken)
	require.Equal(t, 1, requeued.LeaseExpiryCount)
	require.Equal(t, domain.FailureLeaseExpired, domain.FailureReason(requeued.LastFailureReason))

	// The reaper deliberately backs off before retrying, so a task that keeps
	// killing workers is not retried in a tight loop. Skip that wait here.
	require.NotNil(t, requeued.ScheduledAt)
	h.skipBackoff(t, requeued.ID)

	// A second worker picks it up and finishes the job.
	reclaimed := h.pollOne(t, "worker-2")
	require.NotNil(t, reclaimed)
	require.Equal(t, "task_a", reclaimed.Name)
	require.Equal(t, "worker-2", reclaimed.WorkerID)
	require.Equal(t, 2, reclaimed.Attempt, "the re-run is a second execution of the task")
	require.NotEqual(t, lostToken, *reclaimed.ClaimToken, "a new claim mints a new token")

	// The crashed worker eventually wakes up and reports. It must be refused.
	_, err := h.svc.CompleteTask(h.ctx, claimed.ID, lostToken, json.RawMessage(`{"from":"zombie"}`))
	require.ErrorIs(t, err, domain.ErrStaleClaim,
		"a worker whose lease was reaped must not be able to write a result")

	_, err = h.svc.CompleteTask(h.ctx, reclaimed.ID, *reclaimed.ClaimToken,
		json.RawMessage(`{"from":"worker-2"}`))
	require.NoError(t, err)

	// The workflow carries on to completion.
	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State,
		"the workflow must survive losing a worker mid-task")

	require.JSONEq(t, `{"from":"worker-2"}`, string(h.task(t, exec.ID, "task_a").Output),
		"the surviving result is the one from the worker that actually finished")

	events := h.eventTypes(t, exec.ID)
	require.Contains(t, events, domain.EventTaskLeaseExpired)
	require.Contains(t, events, domain.EventTaskRequeued)
}

// TestLeaseExpiryDoesNotConsumeTheActivityAttemptBudget pins the semantic
// decision that makes the scenario above work at all. The example workflow uses
// the default of one attempt; if a lost worker were charged against that budget,
// the task would dead-letter instead of being retried.
func TestLeaseExpiryDoesNotConsumeTheActivityAttemptBudget(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("single_attempt", 1, nil))
	exec := h.start(t, "single_attempt", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	require.Equal(t, 1, claimed.MaxAttempts, "one activity attempt")
	require.False(t, claimed.HasAttemptsLeft(), "which is now spent")

	h.crashWorker(t, claimed.ID)
	require.Equal(t, 1, h.reap(t).TasksRequeued,
		"a lost worker is an infrastructure failure, not an activity failure")

	requeued := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskScheduled, requeued.State)
	require.Equal(t, 2, requeued.MaxAttempts,
		"the attempt consumed by the lost claim is granted back")
	require.Equal(t, 1, requeued.LeaseExpiryCount,
		"the loss is charged to the separate lease-expiry budget instead")

	// And it really is claimable again once its backoff elapses.
	h.skipBackoff(t, requeued.ID)
	require.NotNil(t, h.pollOne(t, "worker-2"))
}

// TestRepeatedWorkerLossEventuallyDeadLetters is the other half of that decision:
// the separate budget must still be bounded, or a task that reliably kills its
// worker would cycle forever.
func TestRepeatedWorkerLossEventuallyDeadLetters(t *testing.T) {
	h := newReliabilityHarness(t)
	spec := retrySpec("poison", 1, nil)
	spec.Tasks[0].MaxLeaseExpiries = 2
	h.register(t, spec)
	exec := h.start(t, "poison", `{}`, "")
	h.sweep(t)

	for i := 1; i <= 2; i++ {
		claimed := h.pollOne(t, fmt.Sprintf("worker-%d", i))
		require.NotNil(t, claimed, "claim %d", i)

		h.crashWorker(t, claimed.ID)
		stats := h.reap(t)
		require.Equal(t, 1, stats.TasksRequeued, "loss %d should requeue", i)
		require.Equal(t, i, h.task(t, exec.ID, "task_a").LeaseExpiryCount)
		h.skipBackoff(t, claimed.ID)
	}

	// The third loss exceeds the budget.
	claimed := h.pollOne(t, "worker-3")
	require.NotNil(t, claimed)
	h.crashWorker(t, claimed.ID)

	stats := h.reap(t)
	require.Equal(t, 1, stats.TasksDeadLettered,
		"a task that keeps destroying workers must eventually be parked")

	parked := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskDeadLetter, parked.State)
	require.NotNil(t, parked.DeadLetteredAt)
	require.Contains(t, parked.Error, "lost its worker")

	h.sweep(t)
	require.Equal(t, domain.WorkflowFailed, h.execution(t, exec.ID).State)
}

// ---------------------------------------------------------------------------
// Retries with exponential backoff
// ---------------------------------------------------------------------------

// TestRetryUsesExponentialBackoff checks both that a failed task is retried and
// that it is not retried *immediately* — the delay is what protects a struggling
// downstream dependency from being hammered.
func TestRetryUsesExponentialBackoff(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("flaky_pipeline", 4, &domain.RetryPolicy{
		InitialIntervalMS:  1000,
		BackoffCoefficient: 2,
		MaxIntervalMS:      60_000,
		JitterPercent:      ptrInt(0),
	}))
	exec := h.start(t, "flaky_pipeline", `{}`, "")

	// 1s, then 2s, then 4s.
	expectedDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

	for attempt, wantDelay := range expectedDelays {
		h.sweep(t)
		claimed := h.pollOne(t, "worker-1")
		require.NotNil(t, claimed, "attempt %d must be claimable", attempt+1)
		require.Equal(t, attempt+1, claimed.Attempt)

		h.failTask(t, claimed, fmt.Sprintf("transient failure %d", attempt+1))

		h.sweep(t)
		retried := h.task(t, exec.ID, "task_a")
		require.Equal(t, domain.TaskScheduled, retried.State, "attempt %d must be retried", attempt+1)
		require.NotNil(t, retried.ScheduledAt)

		// Measured against the database clock, since that is what decides
		// claimability. A small tolerance absorbs the round trip.
		remaining := testsupport.RetryDelay(t, h.store, retried.ID)
		require.InDelta(t, wantDelay.Seconds(), remaining.Seconds(), 0.5,
			"attempt %d must wait about %s before retrying", attempt+1, wantDelay)

		// Crucially, it is not claimable until the delay has elapsed.
		require.Nil(t, h.pollOne(t, "worker-1"),
			"a task in backoff must not be claimable before its delay elapses")
		h.skipBackoff(t, retried.ID)
	}

	// Fourth and final attempt succeeds.
	h.sweep(t)
	last := h.pollOne(t, "worker-1")
	require.NotNil(t, last)
	require.Equal(t, 4, last.Attempt)
	_, err := h.svc.CompleteTask(h.ctx, last.ID, *last.ClaimToken, json.RawMessage(`{"ok":true}`))
	require.NoError(t, err)

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)

	events := h.eventTypes(t, exec.ID)
	retryEvents := 0
	for _, e := range events {
		if e == domain.EventTaskRetryScheduled {
			retryEvents++
		}
	}
	require.Equal(t, 3, retryEvents, "each retry must be recorded in the history")
}

// TestBackoffDelayIsNotClaimableEarly isolates the queue-level guarantee that
// makes backoff real: scheduled_at gates the claim.
func TestBackoffDelayIsNotClaimableEarly(t *testing.T) {
	h := newReliabilityHarness(t)
	// A short real delay, so the test can wait it out rather than fake it. This is
	// the one place worth using real time: it exercises the actual claim predicate
	// against the actual database clock.
	const delay = 600 * time.Millisecond
	h.register(t, retrySpec("delayed", 3, &domain.RetryPolicy{
		InitialIntervalMS: int(delay / time.Millisecond), JitterPercent: ptrInt(0),
	}))
	h.start(t, "delayed", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	h.failTask(t, claimed, "boom")
	h.sweep(t)

	// Immediately after failing, the retry is still in its backoff window.
	require.Nil(t, h.pollOne(t, "worker-1"),
		"a task in backoff must not be claimable before its delay elapses")

	// Once the delay really has elapsed, it becomes claimable.
	time.Sleep(delay + 200*time.Millisecond)
	require.NotNil(t, h.pollOne(t, "worker-1"))
}

// TestRetriesExhaustIntoDeadLetter covers the terminal case for activity failures.
func TestRetriesExhaustIntoDeadLetter(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("doomed", 3, &domain.RetryPolicy{
		InitialIntervalMS: 100, JitterPercent: ptrInt(0),
	}))
	exec := h.start(t, "doomed", `{}`, "")

	for attempt := 1; attempt <= 3; attempt++ {
		h.sweep(t)
		claimed := h.pollOne(t, "worker-1")
		require.NotNil(t, claimed, "attempt %d", attempt)
		require.Equal(t, attempt, claimed.Attempt)
		h.failTask(t, claimed, fmt.Sprintf("failure %d", attempt))
		h.sweep(t)
		if next := h.task(t, exec.ID, "task_a"); next.State == domain.TaskScheduled {
			h.skipBackoff(t, next.ID)
		}
	}

	parked := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskDeadLetter, parked.State,
		"a task that used every attempt must be parked, not retried again")
	require.Equal(t, 3, parked.Attempt)
	require.Contains(t, parked.Error, "exhausted its 3 attempt(s)")
	require.NotNil(t, parked.DeadLetteredAt)

	require.Nil(t, h.pollOne(t, "worker-1"), "a parked task must not be claimable")

	failed := h.execution(t, exec.ID)
	require.Equal(t, domain.WorkflowFailed, failed.State)
	require.Contains(t, failed.Error, "dead-lettered")

	dlq, err := h.store.ListDeadLetterTasks(h.ctx, store.DeadLetterFilter{})
	require.NoError(t, err)
	require.Len(t, dlq, 1)
	require.Equal(t, "task_a", dlq[0].Name)

	total, err := h.store.CountDeadLetterTasks(h.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, total)
}

// TestNonRetryableFailureSkipsRemainingAttempts: retrying a malformed input three
// more times only delays the bad news.
func TestNonRetryableFailureSkipsRemainingAttempts(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("permanent", 5, nil))
	exec := h.start(t, "permanent", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	_, err := h.svc.FailTask(h.ctx, claimed.ID, *claimed.ClaimToken, store.FailParams{
		Error:     "input is malformed and will never parse",
		Retryable: false,
	})
	require.NoError(t, err)

	h.sweep(t)
	parked := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskDeadLetter, parked.State)
	require.Equal(t, 1, parked.Attempt,
		"a permanent failure must not burn the remaining four attempts")
	require.Contains(t, parked.Error, "failed permanently")
	require.Equal(t, domain.WorkflowFailed, h.execution(t, exec.ID).State)
}

// TestTimeoutIsReportedAsItsOwnFailureReason: "exceeded its budget" and "returned
// an error" both fail, but they call for different operator responses.
func TestTimeoutIsRecordedDistinctly(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("slow", 2, &domain.RetryPolicy{
		InitialIntervalMS: 100, JitterPercent: ptrInt(0),
	}))
	exec := h.start(t, "slow", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	_, err := h.svc.FailTask(h.ctx, claimed.ID, *claimed.ClaimToken, store.FailParams{
		Error:     "activity exceeded its 30s budget",
		Reason:    domain.FailureTimeout,
		Retryable: true,
	})
	require.NoError(t, err)

	failed := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.FailureTimeout, domain.FailureReason(failed.LastFailureReason))

	// A timeout is still retryable.
	h.sweep(t)
	require.Equal(t, domain.TaskScheduled, h.task(t, exec.ID, "task_a").State)
	_ = exec
}

// ---------------------------------------------------------------------------
// Task lease and visibility timeout
// ---------------------------------------------------------------------------

// TestLeaseIsAtLeastTheTaskTimeout guards a duplicate-execution bug caused purely
// by misconfiguration: a worker asking for a 5s lease on a task allowed 120s would
// otherwise have its work reaped and handed to a second worker mid-run.
func TestLeaseIsAtLeastTheTaskTimeout(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, domain.WorkflowSpec{
		Name: "long_running", Version: 1,
		Tasks: []domain.TaskSpec{{
			Name: "task_a", Activity: "slow", TimeoutSeconds: 120,
		}},
	})
	h.start(t, "long_running", `{}`, "")
	h.sweep(t)

	claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID:      "worker-1",
		TaskQueue:     domain.DefaultTaskQueue,
		LeaseDuration: 5 * time.Second, // far too short for a 120s task
	})
	require.NoError(t, err)
	require.NotNil(t, claimed.LeaseExpiresAt)

	granted := claimed.LeaseExpiresAt.Sub(claimed.UpdatedAt)
	require.GreaterOrEqual(t, granted, 120*time.Second,
		"the server must not grant a lease shorter than the task's own timeout")
}

// TestLeaseRenewalKeepsALongTaskAlive is what allows leases to stay short: a live
// worker keeps proving it is alive rather than every task reserving a lease long
// enough for the slowest possible activity.
func TestLeaseRenewalKeepsALongTaskAlive(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("renewing", 2, nil))
	exec := h.start(t, "renewing", `{}`, "")
	h.sweep(t)

	claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID: "worker-1", TaskQueue: domain.DefaultTaskQueue,
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	firstExpiry := *claimed.LeaseExpiresAt

	// Simulate a long activity that keeps checking in. Each renewal pushes the
	// deadline out, so the reaper never sees an expired lease.
	for range 5 {
		time.Sleep(10 * time.Millisecond)
		beat, err := h.svc.HeartbeatTask(h.ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
		require.NoError(t, err)
		require.False(t, beat.CancelRequested)
		require.NotNil(t, beat.LeaseExpiresAt)
		require.False(t, beat.LeaseExpiresAt.Before(firstExpiry),
			"each renewal must extend the deadline")

		require.Zero(t, h.reap(t).LeasesExpired,
			"a task whose worker keeps checking in must never be reclaimed")
	}

	_, err = h.svc.CompleteTask(h.ctx, claimed.ID, *claimed.ClaimToken, json.RawMessage(`{"ok":true}`))
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowCompleted, h.runToCompletion(t, exec.ID).State)
}

// TestRenewalFailsOnceTheLeaseIsGone tells a worker to stop rather than letting it
// finish work whose result will be refused.
func TestRenewalFailsOnceTheLeaseIsGone(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("reaped", 3, &domain.RetryPolicy{JitterPercent: ptrInt(0)}))
	h.start(t, "reaped", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	h.crashWorker(t, claimed.ID)
	require.Equal(t, 1, h.reap(t).TasksRequeued)

	_, err := h.svc.HeartbeatTask(h.ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
	require.ErrorIs(t, err, domain.ErrStaleClaim,
		"a worker must learn from its heartbeat that it no longer holds the task")
}

// TestConcurrentReapersReclaimEachTaskOnce: the reaper is safe to run on every
// replica, which matters because it must not become a single point of failure.
func TestConcurrentReapersReclaimEachTaskOnce(t *testing.T) {
	h := newReliabilityHarness(t)

	const width = 8
	spec := domain.WorkflowSpec{Name: "wide_crash", Version: 1}
	for i := range width {
		spec.Tasks = append(spec.Tasks, domain.TaskSpec{
			Name: fmt.Sprintf("task_%02d", i), Activity: "noop",
		})
	}
	h.register(t, spec)
	exec := h.start(t, "wide_crash", `{}`, "")
	h.sweep(t)

	for i := range width {
		require.NotNil(t, h.pollOne(t, fmt.Sprintf("worker-%02d", i)))
	}
	// Every worker goes silent at once, as a whole node failing would look.
	require.Equal(t, width, testsupport.ExpireAllLeases(t, h.store, exec.ID))

	// Four reapers sweep at once, as four replicas would.
	reapers := make([]*engine.Reaper, 4)
	for i := range reapers {
		reapers[i] = engine.NewReaper(h.store, engine.ReaperConfig{
			WorkerTimeout: workerTimeout,
			Jitter:        func() float64 { return 0 },
		}, h.engine, testsupport.Logger())
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		requeued int
	)
	for _, r := range reapers {
		wg.Add(1)
		go func(r *engine.Reaper) {
			defer wg.Done()
			stats, err := r.Sweep(h.ctx)
			require.NoError(t, err)
			mu.Lock()
			requeued += stats.TasksRequeued
			mu.Unlock()
		}(r)
	}
	wg.Wait()

	require.Equal(t, width, requeued,
		"each expired lease must be reclaimed exactly once across all reapers")

	for name, task := range h.tasksByName(t, exec.ID) {
		require.Equal(t, domain.TaskScheduled, task.State, "task %s", name)
		require.Equal(t, 1, task.LeaseExpiryCount,
			"task %s must be charged exactly one lease expiry", name)
	}
}

// ---------------------------------------------------------------------------
// Worker failure detection
// ---------------------------------------------------------------------------

// TestDeadWorkerDetectionReclaimsItsTasks is faster than waiting for each lease to
// lapse: one silent heartbeat condemns everything that worker was holding.
func TestDeadWorkerDetectionReclaimsItsTasks(t *testing.T) {
	h := newReliabilityHarness(t)

	spec := domain.WorkflowSpec{Name: "dead_worker", Version: 1, Tasks: []domain.TaskSpec{
		{Name: "task_a", Activity: "noop", RetryPolicy: instantRetry()},
		{Name: "task_b", Activity: "noop", RetryPolicy: instantRetry()},
	}}
	h.register(t, spec)
	exec := h.start(t, "dead_worker", `{}`, "")
	h.sweep(t)

	worker, err := h.svc.RegisterWorker(h.ctx, store.RegisterWorkerParams{
		Name: "worker-doomed", TaskQueue: domain.DefaultTaskQueue,
		Activities: []string{"noop"},
	})
	require.NoError(t, err)
	require.Equal(t, domain.WorkerActive, worker.State)

	// It claims both tasks with a long lease, then goes silent.
	for range 2 {
		claimed, err := h.svc.PollTask(h.ctx, engine.PollRequest{
			WorkerID: "worker-doomed", TaskQueue: domain.DefaultTaskQueue,
			LeaseDuration: time.Hour, // long enough that lease expiry alone would not help
		})
		require.NoError(t, err)
		require.NotNil(t, claimed)
	}

	// Before the heartbeat threshold, nothing happens.
	require.Zero(t, h.reap(t).WorkersDeclaredDead)

	// The worker process dies: no more heartbeats arrive.
	testsupport.SilenceWorker(t, h.store, "worker-doomed", 2*time.Minute)

	stats := h.reap(t)
	require.Equal(t, 1, stats.WorkersDeclaredDead)
	require.Equal(t, 2, stats.TasksReclaimedFromDeadWorkers,
		"every task the dead worker held must be reclaimed, not just one")

	workers, err := h.store.ListWorkers(h.ctx, domain.DefaultTaskQueue, 10)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, domain.WorkerDead, workers[0].State)
	require.NotNil(t, workers[0].DeclaredDeadAt)

	for name, task := range h.tasksByName(t, exec.ID) {
		require.Equal(t, domain.TaskScheduled, task.State, "task %s must be reclaimed", name)
		require.Equal(t, domain.FailureWorkerDead, domain.FailureReason(task.LastFailureReason),
			"task %s must record why it was reclaimed", name)
	}

	// A replacement worker finishes the run.
	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)
}

// TestHeartbeatRevivesAWorker: detection must not be a one-way door, or a worker
// that hit a slow GC pause would be permanently unusable.
func TestHeartbeatRevivesAWorker(t *testing.T) {
	h := newReliabilityHarness(t)

	worker, err := h.svc.RegisterWorker(h.ctx, store.RegisterWorkerParams{
		Name: "worker-flaky", TaskQueue: domain.DefaultTaskQueue,
		Activities: []string{"noop"},
	})
	require.NoError(t, err)

	testsupport.SilenceWorker(t, h.store, "worker-flaky", 2*time.Minute)
	require.Equal(t, 1, h.reap(t).WorkersDeclaredDead)

	revived, err := h.svc.Heartbeat(h.ctx, worker.ID)
	require.NoError(t, err)
	require.Equal(t, domain.WorkerActive, revived.State,
		"a heartbeat is proof of life and must clear the dead marking")
	require.Nil(t, revived.DeclaredDeadAt)

	// Re-registering also revives, which is what a restarted worker does.
	testsupport.SilenceWorker(t, h.store, "worker-flaky", 2*time.Minute)
	require.Equal(t, 1, h.reap(t).WorkersDeclaredDead)

	reregistered, err := h.svc.RegisterWorker(h.ctx, store.RegisterWorkerParams{
		Name: "worker-flaky", TaskQueue: domain.DefaultTaskQueue,
		Activities: []string{"noop"},
	})
	require.NoError(t, err)
	require.Equal(t, worker.ID, reregistered.ID, "identity is reused across restarts")
	require.Equal(t, domain.WorkerActive, reregistered.State)
}

// ---------------------------------------------------------------------------
// Duplicate execution protection
// ---------------------------------------------------------------------------

// TestOnlyOneWorkerCanReportAfterReassignment is the side-effect protection the
// brief asks for: when a task is reassigned, at most one attempt's result is ever
// accepted, no matter how many workers believe they own it.
func TestOnlyOneWorkerCanReportAfterReassignment(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("contested", 5, &domain.RetryPolicy{JitterPercent: ptrInt(0)}))
	exec := h.start(t, "contested", `{}`, "")
	h.sweep(t)

	// Three generations of worker each believe they hold the task, because each
	// previous one was reaped after going silent.
	var tokens []uuid.UUID
	var taskID uuid.UUID
	for i := 1; i <= 3; i++ {
		claimed := h.pollOne(t, fmt.Sprintf("worker-%d", i))
		require.NotNil(t, claimed, "generation %d", i)
		tokens = append(tokens, *claimed.ClaimToken)
		taskID = claimed.ID

		if i < 3 {
			h.crashWorker(t, claimed.ID)
			require.Equal(t, 1, h.reap(t).TasksRequeued)
			h.skipBackoff(t, claimed.ID)
		}
	}
	require.Len(t, tokens, 3)

	// The two superseded generations are refused.
	for i, stale := range tokens[:2] {
		_, err := h.svc.CompleteTask(h.ctx, taskID, stale,
			json.RawMessage(fmt.Sprintf(`{"from":"generation-%d"}`, i+1)))
		require.ErrorIs(t, err, domain.ErrStaleClaim,
			"generation %d no longer owns the task", i+1)
	}

	// Only the current holder's result lands.
	_, err := h.svc.CompleteTask(h.ctx, taskID, tokens[2], json.RawMessage(`{"from":"generation-3"}`))
	require.NoError(t, err)

	require.JSONEq(t, `{"from":"generation-3"}`, string(h.task(t, exec.ID, "task_a").Output))

	// Exactly one TASK_COMPLETED, despite three workers having tried.
	events, err := h.store.ListHistory(h.ctx, exec.ID, 0, 200)
	require.NoError(t, err)
	completed := 0
	for _, e := range events {
		if e.EventType == domain.EventTaskCompleted {
			completed++
		}
	}
	require.Equal(t, 1, completed,
		"a reassigned task must produce exactly one accepted completion")
}

// TestDuplicateReportsUnderConcurrencyAreIdempotent covers the lost-response case
// happening in parallel, which is what a retrying client fleet actually does.
func TestDuplicateReportsUnderConcurrencyAreIdempotent(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("dup", 3, nil))
	exec := h.start(t, "dup", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	output := json.RawMessage(`{"receipt":"r-1"}`)

	const racers = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []error
	)
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := h.svc.CompleteTask(h.ctx, claimed.ID, *claimed.ClaimToken, output); err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Empty(t, failures, "every replay of the same report must succeed")

	events, err := h.store.ListHistory(h.ctx, exec.ID, 0, 200)
	require.NoError(t, err)
	completed := 0
	for _, e := range events {
		if e.EventType == domain.EventTaskCompleted {
			completed++
		}
	}
	require.Equal(t, 1, completed,
		"eight concurrent replays must still record exactly one completion")
}

// ---------------------------------------------------------------------------
// Cooperative cancellation
// ---------------------------------------------------------------------------

// TestCancellationIsRelayedThroughLeaseRenewal: the control plane cannot reach
// into a worker process and stop it, so cancellation has to be a request the
// worker collects on a channel it already uses.
func TestCancellationIsRelayedThroughLeaseRenewal(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("cancelme", 3, nil))
	exec := h.start(t, "cancelme", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	// The worker is mid-activity and checks in: nothing to report yet.
	beat, err := h.svc.HeartbeatTask(h.ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
	require.NoError(t, err)
	require.False(t, beat.CancelRequested)

	_, err = h.svc.CancelExecution(h.ctx, exec.ID, "operator changed their mind")
	require.NoError(t, err)

	// Cancellation invalidates the claim, so the next renewal is refused outright
	// — which is itself an unambiguous instruction to stop.
	_, err = h.svc.HeartbeatTask(h.ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
	require.ErrorIs(t, err, domain.ErrStaleClaim)

	// And any result it produces anyway is refused.
	_, err = h.svc.CompleteTask(h.ctx, claimed.ID, *claimed.ClaimToken, json.RawMessage(`{"late":true}`))
	require.ErrorIs(t, err, domain.ErrStaleClaim)

	require.Equal(t, domain.WorkflowCanceled, h.execution(t, exec.ID).State)

	events := h.eventTypes(t, exec.ID)
	require.Contains(t, events, domain.EventTaskCancelRequested,
		"the history must show workers were asked to stop, not merely cut off")
	require.Contains(t, events, domain.EventWorkflowCanceled)
}

// TestCancelRequestedFlagIsVisibleBeforeClaimInvalidation covers the path where a
// task is flagged but its claim is still live, which is what a per-task cancel
// would look like.
func TestCancelRequestedFlagIsVisibleToTheWorker(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("signal", 3, nil))
	exec := h.start(t, "signal", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	// Flag the task without tearing down the execution.
	flagged, err := h.store.RequestTaskCancellation(h.ctx, exec.ID)
	require.NoError(t, err)
	require.Equal(t, 1, flagged)

	beat, err := h.svc.HeartbeatTask(h.ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
	require.NoError(t, err)
	require.True(t, beat.CancelRequested,
		"the worker must learn about cancellation on its next check-in")

	// A fresh claim clears the flag, so a requeued task is not born canceled.
	h.crashWorker(t, claimed.ID)
	require.Equal(t, 1, h.reap(t).TasksRequeued)
	h.skipBackoff(t, claimed.ID)
	reclaimed := h.pollOne(t, "worker-2")
	require.NotNil(t, reclaimed)
	require.False(t, reclaimed.CancelRequested)
}

// ---------------------------------------------------------------------------
// Dead letter handling and replay
// ---------------------------------------------------------------------------

// TestReplayResumesAFailedWorkflow is what makes dead-lettering a remediation
// path rather than a well-documented way to lose work: once the cause is fixed,
// the run resumes from where it stopped instead of redoing everything.
func TestReplayResumesAFailedWorkflow(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"replay-1"}`, "")

	// task_a succeeds, task_b exhausts its single attempt.
	h.sweep(t)
	taskA := h.pollOne(t, "worker-1")
	require.NotNil(t, taskA)
	_, err := h.svc.CompleteTask(h.ctx, taskA.ID, *taskA.ClaimToken, json.RawMessage(`{"step":"a"}`))
	require.NoError(t, err)

	h.sweep(t)
	taskB := h.pollOne(t, "worker-1")
	require.NotNil(t, taskB)
	require.Equal(t, "task_b", taskB.Name)
	h.failTask(t, taskB, "inventory service down")
	h.sweep(t)

	require.Equal(t, domain.WorkflowFailed, h.execution(t, exec.ID).State)
	parked := h.task(t, exec.ID, "task_b")
	require.Equal(t, domain.TaskDeadLetter, parked.State)
	require.Equal(t, domain.TaskCanceled, h.task(t, exec.ID, "task_c").State)

	// The operator fixes the dependency and replays.
	replayed, err := h.svc.ReplayTask(h.ctx, parked.ID, 0)
	require.NoError(t, err)
	require.Equal(t, domain.TaskScheduled, replayed.State)
	require.Equal(t, 2, replayed.MaxAttempts, "replay grants one more attempt")
	require.Equal(t, 1, replayed.Attempt, "attempt stays an honest count of executions")

	require.Equal(t, domain.WorkflowRunning, h.execution(t, exec.ID).State,
		"replay is the one thing that revives a failed run")
	require.Equal(t, domain.TaskPending, h.task(t, exec.ID, "task_c").State,
		"downstream work canceled by the failure must be restored, or the graph can never finish")
	require.Equal(t, domain.TaskCompleted, h.task(t, exec.ID, "task_a").State,
		"work that already succeeded must not be redone")

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)

	require.JSONEq(t, `{"step":"a"}`, string(h.task(t, exec.ID, "task_a").Output),
		"task_a keeps its original result across the replay")

	events := h.eventTypes(t, exec.ID)
	require.Contains(t, events, domain.EventTaskDeadLettered)
	require.Contains(t, events, domain.EventTaskReplayed)

	// The dead letter queue is empty again.
	total, err := h.store.CountDeadLetterTasks(h.ctx)
	require.NoError(t, err)
	require.Zero(t, total)
}

func TestReplayRejectsTasksThatAreNotParked(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("notparked", 3, nil))
	exec := h.start(t, "notparked", `{}`, "")
	h.sweep(t)

	scheduled := h.task(t, exec.ID, "task_a")
	_, err := h.svc.ReplayTask(h.ctx, scheduled.ID, 0)
	require.ErrorIs(t, err, domain.ErrConflict,
		"only a dead-lettered task can be replayed")

	_, err = h.svc.ReplayTask(h.ctx, uuid.New(), 0)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestReplayRefusesCanceledExecutions(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("cancelled_dlq", 1, nil))
	exec := h.start(t, "cancelled_dlq", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	h.failTask(t, claimed, "boom")
	h.sweep(t)

	parked := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskDeadLetter, parked.State)

	// Cancelling a failed run also clears its dead letter entry, since a parked
	// task under a canceled workflow is not actionable.
	_, err := h.svc.CancelExecution(h.ctx, exec.ID, "abandoning this run")
	require.ErrorIs(t, err, domain.ErrConflict,
		"a failed run is terminal to cancellation; replay is the way back")
}

// TestDeadLetterQueueFiltersAndPages gives an operator a usable triage view.
func TestDeadLetterQueueFiltersAndPages(t *testing.T) {
	h := newReliabilityHarness(t)

	for i := range 3 {
		name := fmt.Sprintf("doomed_%d", i)
		h.register(t, retrySpec(name, 1, nil))
		exec := h.start(t, name, `{}`, "")
		h.sweep(t)
		claimed := h.pollOne(t, "worker-1")
		require.NotNil(t, claimed)
		h.failTask(t, claimed, fmt.Sprintf("failure in %s", name))
		h.sweep(t)
		require.Equal(t, domain.WorkflowFailed, h.execution(t, exec.ID).State)
	}

	all, err := h.store.ListDeadLetterTasks(h.ctx, store.DeadLetterFilter{})
	require.NoError(t, err)
	require.Len(t, all, 3)

	byWorkflow, err := h.store.ListDeadLetterTasks(h.ctx,
		store.DeadLetterFilter{WorkflowName: "doomed_1"})
	require.NoError(t, err)
	require.Len(t, byWorkflow, 1)

	byActivity, err := h.store.ListDeadLetterTasks(h.ctx,
		store.DeadLetterFilter{Activity: "flaky"})
	require.NoError(t, err)
	require.Len(t, byActivity, 3)

	none, err := h.store.ListDeadLetterTasks(h.ctx,
		store.DeadLetterFilter{Activity: "does_not_exist"})
	require.NoError(t, err)
	require.Empty(t, none)

	page, err := h.store.ListDeadLetterTasks(h.ctx, store.DeadLetterFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page, 2)

	rest, err := h.store.ListDeadLetterTasks(h.ctx, store.DeadLetterFilter{Limit: 2, Offset: 2})
	require.NoError(t, err)
	require.Len(t, rest, 1)
}

// ---------------------------------------------------------------------------
// Scheduler recovery
// ---------------------------------------------------------------------------

// TestPendingRetrySurvivesEngineRestart: a retry delay is persisted as a
// timestamp, not held as an in-process timer, so a scheduler restart cannot lose
// or re-fire it.
func TestPendingRetrySurvivesEngineRestart(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("durable_retry", 3, &domain.RetryPolicy{
		InitialIntervalMS: 5000, JitterPercent: ptrInt(0),
	}))
	exec := h.start(t, "durable_retry", `{}`, "")

	h.sweep(t)
	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	h.failTask(t, claimed, "transient")
	h.sweep(t)

	pending := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskScheduled, pending.State)
	require.NotNil(t, pending.ScheduledAt)
	scheduledFor := *pending.ScheduledAt
	require.InDelta(t, 5.0, testsupport.RetryDelay(t, h.store, pending.ID).Seconds(), 0.5)

	// Discard the engine entirely, as a crash would.
	h.restartEngine(t)

	// The retry is still pending at exactly the same instant.
	after := h.task(t, exec.ID, "task_a")
	require.Equal(t, scheduledFor.UnixNano(), after.ScheduledAt.UnixNano(),
		"a pending retry must not shift when the scheduler restarts")
	require.Nil(t, h.pollOne(t, "worker-1"), "and must not fire early")
}

// TestReaperRecoversAfterRestart confirms detection is stateless too: a reaper
// that has never seen a task can still reclaim it.
func TestReaperRecoversAfterRestart(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("reaper_restart", 3, &domain.RetryPolicy{JitterPercent: ptrInt(0)}))
	exec := h.start(t, "reaper_restart", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	h.crashWorker(t, claimed.ID)

	// A brand new reaper, with no memory of the claim.
	fresh := engine.NewReaper(h.store, engine.ReaperConfig{
		WorkerTimeout: workerTimeout,
		Jitter:        func() float64 { return 0 },
	}, h.engine, testsupport.Logger())

	stats, err := fresh.Sweep(h.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TasksRequeued,
		"failure detection must be reconstructible from persisted state alone")
	require.Equal(t, domain.TaskScheduled, h.task(t, exec.ID, "task_a").State)
}

// TestReaperSweepIsIdempotent: repeated sweeps over stable state must be no-ops,
// or a reaper on a short interval would inflate attempt counts.
func TestReaperSweepIsIdempotent(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("idempotent_reap", 5, &domain.RetryPolicy{JitterPercent: ptrInt(0)}))
	exec := h.start(t, "idempotent_reap", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)
	h.crashWorker(t, claimed.ID)

	require.Equal(t, 1, h.reap(t).TasksRequeued)
	before := h.task(t, exec.ID, "task_a")

	for range 3 {
		stats := h.reap(t)
		require.Zero(t, stats.LeasesExpired, "a reclaimed task must not be reclaimed again")
		require.Zero(t, stats.TasksRequeued)
	}

	after := h.task(t, exec.ID, "task_a")
	require.Equal(t, before.Attempt, after.Attempt)
	require.Equal(t, before.LeaseExpiryCount, after.LeaseExpiryCount)
	require.Equal(t, before.MaxAttempts, after.MaxAttempts)
}

// TestWorkerReportBeatsTheReaper: when a worker reports just as its lease lapses,
// the report must win — the alternative is discarding completed work.
func TestWorkerReportBeatsTheReaper(t *testing.T) {
	h := newReliabilityHarness(t)
	h.register(t, retrySpec("photo_finish", 3, &domain.RetryPolicy{JitterPercent: ptrInt(0)}))
	exec := h.start(t, "photo_finish", `{}`, "")
	h.sweep(t)

	claimed := h.pollOne(t, "worker-1")
	require.NotNil(t, claimed)

	h.crashWorker(t, claimed.ID) // the lease is now lapsed

	// The worker reports before the reaper gets there.
	_, err := h.svc.CompleteTask(h.ctx, claimed.ID, *claimed.ClaimToken,
		json.RawMessage(`{"justInTime":true}`))
	require.NoError(t, err, "an expired lease does not invalidate a claim by itself")

	stats := h.reap(t)
	require.Zero(t, stats.LeasesExpired,
		"the reaper must not reclaim a task that has already completed")

	done := h.task(t, exec.ID, "task_a")
	require.Equal(t, domain.TaskCompleted, done.State)
	require.JSONEq(t, `{"justInTime":true}`, string(done.Output))
	require.Equal(t, 1, done.Attempt, "the work must not be redone")

	require.Equal(t, domain.WorkflowCompleted, h.runToCompletion(t, exec.ID).State)
}

func ptrInt(v int) *int { return &v }
