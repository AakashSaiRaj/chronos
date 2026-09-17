package store_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// reliabilityFixture materializes a single-task workflow and returns the task
// scheduled and ready to claim.
func reliabilityFixture(t *testing.T, s *store.Store, spec domain.WorkflowSpec) (*domain.WorkflowExecution, *domain.Task) {
	t.Helper()
	ctx := testCtx(t)
	spec.Normalize()

	def, created, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	require.True(t, created)

	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	scheduled, err := s.ScheduleTask(ctx, tasks[0].ID, json.RawMessage(`{}`), 0)
	require.NoError(t, err)
	return exec, scheduled
}

func singleTaskSpec(name string, maxAttempts int) domain.WorkflowSpec {
	return domain.WorkflowSpec{
		Name: name, Version: 1,
		Tasks: []domain.TaskSpec{{Name: "task_a", Activity: "noop", MaxAttempts: maxAttempts}},
	}
}

// TestMaterializeTasksPersistsEffectiveRetryPolicy: the policy is resolved once
// and stored, so a task's retry schedule is reproducible from its row and immune
// to later changes in the engine's defaults.
func TestMaterializeTasksPersistsEffectiveRetryPolicy(t *testing.T) {
	s := testsupport.NewStore(t)

	jitter := 5
	spec := domain.WorkflowSpec{
		Name: "policies", Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "noop"}, // no policy: defaults apply
			{
				Name: "task_b", Activity: "noop",
				MaxLeaseExpiries: 7,
				RetryPolicy: &domain.RetryPolicy{
					InitialIntervalMS: 250, BackoffCoefficient: 3,
					MaxIntervalMS: 5_000, JitterPercent: &jitter,
				},
			},
		},
	}
	_, _ = reliabilityFixture(t, s, spec)

	ctx := testCtx(t)
	defs, err := s.ListDefinitions(ctx, "policies", 1)
	require.NoError(t, err)
	require.Len(t, defs, 1)

	execs, err := s.ListExecutions(ctx, store.ExecutionFilter{WorkflowName: "policies"})
	require.NoError(t, err)
	require.Len(t, execs, 1)

	tasks, err := s.ListTasks(ctx, execs[0].ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	byName := map[string]domain.Task{}
	for _, task := range tasks {
		byName[task.Name] = task
	}

	defaults := domain.DefaultRetryPolicy()
	require.Equal(t, defaults, byName["task_a"].RetryPolicy,
		"a task with no declared policy stores the resolved defaults")
	require.Equal(t, domain.DefaultMaxLeaseExpiries, byName["task_a"].MaxLeaseExpiries)
	require.True(t, byName["task_a"].Retryable, "tasks start retryable")

	custom := byName["task_b"].RetryPolicy
	require.Equal(t, 250*time.Millisecond, custom.InitialInterval)
	require.Equal(t, 3.0, custom.BackoffCoefficient)
	require.Equal(t, 5*time.Second, custom.MaxInterval)
	require.Equal(t, 5, custom.JitterPercent)
	require.Equal(t, 7, byName["task_b"].MaxLeaseExpiries)
}

// TestClaimLeaseCoversTheTaskTimeout guards against a duplicate execution caused
// purely by a worker asking for too short a lease.
func TestClaimLeaseCoversTheTaskTimeout(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := domain.WorkflowSpec{
		Name: "slow_task", Version: 1,
		Tasks: []domain.TaskSpec{{Name: "task_a", Activity: "slow", TimeoutSeconds: 300}},
	}
	reliabilityFixture(t, s, spec)

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		WorkerID:      "worker-1",
		LeaseDuration: time.Second, // wildly shorter than the task's own budget
	})
	require.NoError(t, err)
	require.NotNil(t, claimed.LeaseExpiresAt)

	granted := claimed.LeaseExpiresAt.Sub(claimed.UpdatedAt)
	require.GreaterOrEqual(t, granted, 300*time.Second,
		"the lease must cover the task's timeout no matter what the worker requested")
}

func TestRenewLeaseExtendsAndReportsCancellation(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	exec, _ := reliabilityFixture(t, s, singleTaskSpec("renew", 3))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		WorkerID: "worker-1", LeaseDuration: 10 * time.Second,
	})
	require.NoError(t, err)
	firstExpiry := *claimed.LeaseExpiresAt

	time.Sleep(20 * time.Millisecond)
	renewed, err := s.RenewLease(ctx, claimed.ID, *claimed.ClaimToken, 60*time.Second)
	require.NoError(t, err)
	require.True(t, renewed.LeaseExpiresAt.After(firstExpiry), "renewal must push the deadline out")
	require.False(t, renewed.CancelRequested)
	require.Equal(t, claimed.Attempt, renewed.Attempt, "renewal must not consume an attempt")

	// Cancellation is relayed on the next renewal.
	flagged, err := s.RequestTaskCancellation(ctx, exec.ID)
	require.NoError(t, err)
	require.Equal(t, 1, flagged)

	beat, err := s.RenewLease(ctx, claimed.ID, *claimed.ClaimToken, 60*time.Second)
	require.NoError(t, err)
	require.True(t, beat.CancelRequested)

	// A wrong token cannot renew someone else's lease.
	_, err = s.RenewLease(ctx, claimed.ID, uuid.New(), 60*time.Second)
	require.ErrorIs(t, err, domain.ErrStaleClaim)
}

func TestRenewLeaseFailsOnceRequeued(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("requeued", 3))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)

	_, err = s.RequeueTask(ctx, claimed.ID, 0, store.RequeueReason{
		From: domain.TaskRunning, Failure: domain.FailureLeaseExpired,
		Error: "worker lost", CountsAsLeaseExpiry: true, GrantExtraAttempts: 1,
	})
	require.NoError(t, err)

	_, err = s.RenewLease(ctx, claimed.ID, *claimed.ClaimToken, 30*time.Second)
	require.ErrorIs(t, err, domain.ErrStaleClaim,
		"a worker must discover through renewal that its task was taken away")
}

// TestRequeueTaskClearsOwnershipAndPreservesInput: a re-run must replay
// byte-identical input, and the previous holder must be locked out.
func TestRequeueTaskClearsOwnershipAndPreservesInput(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := singleTaskSpec("requeue_input", 3)
	def, _, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)

	original := json.RawMessage(`{"workflowInput":{"orderId":"A-1"}}`)
	_, err = s.ScheduleTask(ctx, tasks[0].ID, original, 0)
	require.NoError(t, err)

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)
	staleToken := *claimed.ClaimToken

	requeued, err := s.RequeueTask(ctx, claimed.ID, 0, store.RequeueReason{
		From: domain.TaskRunning, Failure: domain.FailureWorkerDead,
		Error: "worker vanished", CountsAsLeaseExpiry: true, GrantExtraAttempts: 1,
	})
	require.NoError(t, err)

	require.Equal(t, domain.TaskScheduled, requeued.State)
	require.JSONEq(t, string(original), string(requeued.Input),
		"a requeue must preserve the resolved input so the re-run is identical")
	require.Nil(t, requeued.ClaimToken, "ownership must be released")
	require.Empty(t, requeued.WorkerID)
	require.Nil(t, requeued.LeaseExpiresAt)
	require.Equal(t, 1, requeued.LeaseExpiryCount)
	require.Equal(t, 4, requeued.MaxAttempts, "the lost attempt is granted back")
	require.Equal(t, domain.FailureWorkerDead, domain.FailureReason(requeued.LastFailureReason))

	// The previous holder is locked out.
	_, err = s.CompleteTask(ctx, claimed.ID, staleToken, json.RawMessage(`{"stale":true}`))
	require.ErrorIs(t, err, domain.ErrStaleClaim)
}

// TestRequeueGuardRejectsUnexpiredLease is what lets a worker's just-in-time
// report beat the reaper: the expiry is re-checked against the database clock in
// the same statement as the write.
func TestRequeueGuardRejectsUnexpiredLease(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("guarded", 3))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		WorkerID: "worker-1", LeaseDuration: time.Hour,
	})
	require.NoError(t, err)

	_, err = s.RequeueTask(ctx, claimed.ID, 0, store.RequeueReason{
		From:                domain.TaskRunning,
		Failure:             domain.FailureLeaseExpired,
		RequireLeaseExpired: true,
	})
	require.ErrorIs(t, err, domain.ErrInvalidStateTransition,
		"a live lease must not be reclaimable")

	// Once the lease really has lapsed, the same call succeeds.
	testsupport.ExpireLease(t, s, claimed.ID)
	requeued, err := s.RequeueTask(ctx, claimed.ID, 0, store.RequeueReason{
		From:                domain.TaskRunning,
		Failure:             domain.FailureLeaseExpired,
		RequireLeaseExpired: true,
		GrantExtraAttempts:  1,
	})
	require.NoError(t, err)
	require.Equal(t, domain.TaskScheduled, requeued.State)
}

func TestRequeueRespectsTheDelay(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("delayed_requeue", 3))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)

	requeued, err := s.RequeueTask(ctx, claimed.ID, 30*time.Second, store.RequeueReason{
		From: domain.TaskRunning, GrantExtraAttempts: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, requeued.ScheduledAt)

	remaining := testsupport.RetryDelay(t, s, requeued.ID)
	require.InDelta(t, 30.0, remaining.Seconds(), 2.0,
		"the delay must be applied against the database clock")

	// And it is genuinely not claimable yet.
	_, err = s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-2"})
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestDeadLetterAndReplay(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("dlq", 1))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)
	_, err = s.FailTask(ctx, claimed.ID, *claimed.ClaimToken, store.FailParams{
		Error: "permanent problem", Retryable: false,
	})
	require.NoError(t, err)

	parked, err := s.DeadLetterTask(ctx, claimed.ID, domain.FailureActivityError, "gave up")
	require.NoError(t, err)
	require.Equal(t, domain.TaskDeadLetter, parked.State)
	require.NotNil(t, parked.DeadLetteredAt)
	require.Nil(t, parked.ClaimToken)
	require.Equal(t, "gave up", parked.Error)

	// A parked task is not claimable.
	_, err = s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-2"})
	require.ErrorIs(t, err, domain.ErrNotFound)

	count, err := s.CountDeadLetterTasks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Replay returns it to the queue with a fresh attempt budget.
	replayed, err := s.RequeueTask(ctx, parked.ID, 0, store.RequeueReason{
		From: domain.TaskDeadLetter, GrantExtraAttempts: 2,
	})
	require.NoError(t, err)
	require.Equal(t, domain.TaskScheduled, replayed.State)
	require.Nil(t, replayed.DeadLetteredAt, "the dead-letter marking must be cleared")
	require.Equal(t, 3, replayed.MaxAttempts)
	require.Equal(t, 1, replayed.Attempt, "attempt stays an honest lifetime count")
	require.True(t, replayed.Retryable, "replay clears a permanent-failure marking")

	count, err = s.CountDeadLetterTasks(ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// And it can be claimed again.
	reclaimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-2"})
	require.NoError(t, err)
	require.Equal(t, 2, reclaimed.Attempt)
}

// TestDeadLetterConsistencyIsEnforcedBySchema: the invariant lives in the
// database, so no code path can leave state and timestamp disagreeing.
func TestDeadLetterConsistencyIsEnforcedBySchema(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("consistency", 1))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)

	_, err = s.Pool().Exec(ctx,
		`UPDATE tasks SET dead_lettered_at = now() WHERE id = $1`, claimed.ID)
	require.Error(t, err, "a non-parked task must not be allowed a dead-letter timestamp")

	_, err = s.Pool().Exec(ctx,
		`UPDATE tasks SET state = 'DEAD_LETTER' WHERE id = $1`, claimed.ID)
	require.Error(t, err, "a parked task must record when it was parked")
}

func TestFailTaskRecordsRetryabilityAndReason(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	reliabilityFixture(t, s, singleTaskSpec("reasons", 5))

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)

	failed, err := s.FailTask(ctx, claimed.ID, *claimed.ClaimToken, store.FailParams{
		Error: "timed out after 30s", Reason: domain.FailureTimeout, Retryable: true,
	})
	require.NoError(t, err)
	require.Equal(t, domain.TaskFailed, failed.State)
	require.Equal(t, domain.FailureTimeout, domain.FailureReason(failed.LastFailureReason))
	require.True(t, failed.Retryable)
	require.True(t, failed.ShouldRetry(), "attempts remain and the failure is transient")

	// A permanent failure is recorded as such, and short-circuits the budget.
	_, err = s.RequeueTask(ctx, claimed.ID, 0, store.RequeueReason{From: domain.TaskFailed})
	require.NoError(t, err)
	reclaimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)

	permanent, err := s.FailTask(ctx, reclaimed.ID, *reclaimed.ClaimToken, store.FailParams{
		Error: "input will never parse", Retryable: false,
	})
	require.NoError(t, err)
	require.False(t, permanent.Retryable)
	require.True(t, permanent.HasAttemptsLeft(), "attempts remain")
	require.False(t, permanent.ShouldRetry(), "but a permanent failure must not use them")
}

func TestListExpiredLeaseTaskIDs(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := domain.WorkflowSpec{Name: "expiries", Version: 1, Tasks: []domain.TaskSpec{
		{Name: "task_a", Activity: "noop"},
		{Name: "task_b", Activity: "noop"},
		{Name: "task_c", Activity: "noop"},
	}}
	spec.Normalize()
	def, _, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	for _, task := range tasks {
		_, err := s.ScheduleTask(ctx, task.ID, json.RawMessage(`{}`), 0)
		require.NoError(t, err)
	}

	claimed := make([]*domain.Task, 0, 3)
	for i := range 3 {
		c, err := s.ClaimTask(ctx, store.ClaimParams{
			WorkerID: "worker-1", LeaseDuration: time.Hour,
		})
		require.NoError(t, err, "claim %d", i)
		claimed = append(claimed, c)
	}

	expired, err := s.ListExpiredLeaseTaskIDs(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, expired, "live leases must not be listed")

	testsupport.ExpireLease(t, s, claimed[0].ID)
	testsupport.ExpireLease(t, s, claimed[2].ID)

	expired, err = s.ListExpiredLeaseTaskIDs(ctx, 100)
	require.NoError(t, err)
	require.Len(t, expired, 2)
	require.ElementsMatch(t, []uuid.UUID{claimed[0].ID, claimed[2].ID}, expired)

	// A completed task is no longer a candidate even if its lease had lapsed.
	_, err = s.CompleteTask(ctx, claimed[0].ID, *claimed[0].ClaimToken, json.RawMessage(`{}`))
	require.NoError(t, err)
	expired, err = s.ListExpiredLeaseTaskIDs(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{claimed[2].ID}, expired)
}

func TestMarkStaleWorkersDead(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	for _, name := range []string{"worker-alive", "worker-silent"} {
		_, err := s.RegisterWorker(ctx, store.RegisterWorkerParams{
			Name: name, TaskQueue: domain.DefaultTaskQueue, Activities: []string{"noop"},
		})
		require.NoError(t, err)
	}

	dead, err := s.MarkStaleWorkersDead(ctx, 30*time.Second, 100)
	require.NoError(t, err)
	require.Empty(t, dead, "freshly registered workers must not be declared dead")

	testsupport.SilenceWorker(t, s, "worker-silent", 2*time.Minute)

	dead, err = s.MarkStaleWorkersDead(ctx, 30*time.Second, 100)
	require.NoError(t, err)
	require.Len(t, dead, 1)
	require.Equal(t, "worker-silent", dead[0].Name)
	require.Equal(t, domain.WorkerDead, dead[0].State)
	require.NotNil(t, dead[0].DeclaredDeadAt)

	// Detection is idempotent: an already-dead worker is not re-detected.
	dead, err = s.MarkStaleWorkersDead(ctx, 30*time.Second, 100)
	require.NoError(t, err)
	require.Empty(t, dead)

	// The healthy worker is untouched.
	workers, err := s.ListWorkers(ctx, domain.DefaultTaskQueue, 10)
	require.NoError(t, err)
	byName := map[string]domain.Worker{}
	for _, w := range workers {
		byName[w.Name] = w
	}
	require.Equal(t, domain.WorkerActive, byName["worker-alive"].State)
	require.Nil(t, byName["worker-alive"].DeclaredDeadAt)
}

// TestConcurrentStaleDetectionMarksEachWorkerOnce lets the reaper run on every
// replica without inflating counts or double-reclaiming.
func TestConcurrentStaleDetectionMarksEachWorkerOnce(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	const workers = 6
	for i := range workers {
		name := "worker-" + string(rune('a'+i))
		_, err := s.RegisterWorker(ctx, store.RegisterWorkerParams{
			Name: name, TaskQueue: domain.DefaultTaskQueue,
		})
		require.NoError(t, err)
		testsupport.SilenceWorker(t, s, name, 2*time.Minute)
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dead, err := s.MarkStaleWorkersDead(ctx, 30*time.Second, 100)
			require.NoError(t, err)
			mu.Lock()
			total += len(dead)
			mu.Unlock()
		}()
	}
	wg.Wait()

	require.Equal(t, workers, total,
		"each stale worker must be declared dead exactly once across all reapers")
}

func TestListTaskIDsHeldByWorkers(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := domain.WorkflowSpec{Name: "held", Version: 1, Tasks: []domain.TaskSpec{
		{Name: "task_a", Activity: "noop"},
		{Name: "task_b", Activity: "noop"},
	}}
	spec.Normalize()
	def, _, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	for _, task := range tasks {
		_, err := s.ScheduleTask(ctx, task.ID, json.RawMessage(`{}`), 0)
		require.NoError(t, err)
	}

	first, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-doomed"})
	require.NoError(t, err)
	second, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-fine"})
	require.NoError(t, err)

	held, err := s.ListTaskIDsHeldByWorkers(ctx, []string{"worker-doomed"}, 100)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{first.ID}, held)

	both, err := s.ListTaskIDsHeldByWorkers(ctx, []string{"worker-doomed", "worker-fine"}, 100)
	require.NoError(t, err)
	require.Len(t, both, 2)
	require.ElementsMatch(t, []uuid.UUID{first.ID, second.ID}, both)

	none, err := s.ListTaskIDsHeldByWorkers(ctx, nil, 100)
	require.NoError(t, err)
	require.Empty(t, none)

	// Only RUNNING tasks count.
	_, err = s.CompleteTask(ctx, first.ID, *first.ClaimToken, json.RawMessage(`{}`))
	require.NoError(t, err)
	held, err = s.ListTaskIDsHeldByWorkers(ctx, []string{"worker-doomed"}, 100)
	require.NoError(t, err)
	require.Empty(t, held)
}

func TestRestoreCanceledTasks(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := domain.WorkflowSpec{Name: "restore", Version: 1, Tasks: []domain.TaskSpec{
		{Name: "task_a", Activity: "noop"},
		{Name: "task_b", Activity: "noop", DependsOn: []string{"task_a"}},
	}}
	spec.Normalize()
	def, _, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	canceled, err := s.CancelIncompleteTasks(ctx, exec.ID)
	require.NoError(t, err)
	require.Equal(t, 2, canceled)

	restored, err := s.RestoreCanceledTasks(ctx, exec.ID)
	require.NoError(t, err)
	require.Equal(t, 2, restored)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	for _, task := range tasks {
		require.Equal(t, domain.TaskPending, task.State, "task %s", task.Name)
		require.Nil(t, task.ScheduledAt)
		require.Nil(t, task.CompletedAt)
		require.Empty(t, task.Error)
	}
}

// TestCancelIncompleteTasksExceptSparesTheParkedTask keeps a dead-lettered task
// visible in the queue an operator triages, while cleaning up its siblings.
func TestCancelIncompleteTasksExceptSparesTheParkedTask(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	spec := domain.WorkflowSpec{Name: "spare", Version: 1, Tasks: []domain.TaskSpec{
		{Name: "task_a", Activity: "noop"},
		{Name: "task_b", Activity: "noop"},
	}}
	spec.Normalize()
	def, _, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	_, err = s.ScheduleTask(ctx, tasks[0].ID, json.RawMessage(`{}`), 0)
	require.NoError(t, err)
	claimed, err := s.ClaimTask(ctx, store.ClaimParams{WorkerID: "worker-1"})
	require.NoError(t, err)
	_, err = s.FailTask(ctx, claimed.ID, *claimed.ClaimToken,
		store.FailParams{Error: "boom", Retryable: false})
	require.NoError(t, err)
	parked, err := s.DeadLetterTask(ctx, claimed.ID, domain.FailureActivityError, "gave up")
	require.NoError(t, err)

	canceled, err := s.CancelIncompleteTasksExcept(ctx, exec.ID, parked.ID)
	require.NoError(t, err)
	require.Equal(t, 1, canceled, "only the sibling is canceled")

	after, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	byName := map[string]domain.Task{}
	for _, task := range after {
		byName[task.Name] = task
	}
	require.Equal(t, domain.TaskDeadLetter, byName["task_a"].State,
		"the parked task must stay parked so it remains replayable")
	require.Equal(t, domain.TaskCanceled, byName["task_b"].State)

	count, err := s.CountDeadLetterTasks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
