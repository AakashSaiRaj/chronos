package store_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// TestCollectQueueStatsCountsOnlyClaimableWork is the important property of the
// backlog gauge: it must count what a worker could actually claim right now, not
// every row in the SCHEDULED state. Those differ whenever a retry is waiting out
// its backoff, and conflating them would make a healthy system with a few pending
// retries look like a system with a growing queue.
func TestCollectQueueStatsCountsOnlyClaimableWork(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	def := mustRegister(t, s, linearSpec())
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	// One claimable immediately, one deferred well into the future.
	_, err = s.ScheduleTask(ctx, tasks[0].ID, json.RawMessage(`{}`), 0)
	require.NoError(t, err)
	_, err = s.ScheduleTask(ctx, tasks[1].ID, json.RawMessage(`{}`), time.Hour)
	require.NoError(t, err)

	stats, err := s.CollectQueueStats(ctx)
	require.NoError(t, err)

	claimable := stats.ClaimableByActivity[store.QueueActivityKey{
		TaskQueue: tasks[0].TaskQueue,
		Activity:  tasks[0].Activity,
	}]
	assert.Equal(t, 1, claimable, "only the immediately-eligible task is claimable")

	// The deferred one is visible, but as backoff rather than as backlog.
	assert.Equal(t, 1, stats.BackoffByQueue[tasks[1].TaskQueue])
	assert.NotContains(t, stats.ClaimableByActivity, store.QueueActivityKey{
		TaskQueue: tasks[1].TaskQueue,
		Activity:  tasks[1].Activity,
	})

	// Coarse totals include the still-PENDING third task.
	assert.Equal(t, 2, stats.TasksByState[domain.TaskScheduled])
	assert.Equal(t, 1, stats.TasksByState[domain.TaskPending])
	assert.Equal(t, 1, stats.ExecutionsByState[exec.State])
}

func TestCollectQueueStatsTracksRunningAndOldestClaimableAge(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	def := mustRegister(t, s, linearSpec())
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)

	for _, task := range tasks[:2] {
		_, err = s.ScheduleTask(ctx, task.ID, json.RawMessage(`{}`), 0)
		require.NoError(t, err)
	}

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		TaskQueue:     tasks[0].TaskQueue,
		WorkerID:      "worker-observability",
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)

	// Age is measured from scheduled_at against the database clock, so backdate the
	// remaining claimable task rather than sleeping.
	remaining := tasks[0].ID
	if claimed.ID == remaining {
		remaining = tasks[1].ID
	}
	backdateScheduledAt(t, s, remaining, 90*time.Second)

	stats, err := s.CollectQueueStats(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, stats.RunningByQueue[claimed.TaskQueue])

	// Age, not depth, is what separates a big queue draining fast from a stuck one.
	age := stats.OldestClaimableAge[claimed.TaskQueue]
	assert.Greater(t, age, 60*time.Second, "oldest claimable age should reflect the backdated task")
}

func TestCollectQueueStatsReportsDeadLetterAndWorkers(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	_, err := s.RegisterWorker(ctx, store.RegisterWorkerParams{
		Name:       "worker-alive",
		TaskQueue:  "default",
		Activities: []string{"charge_payment"},
	})
	require.NoError(t, err)

	stats, err := s.CollectQueueStats(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, stats.WorkersByQueueState[store.QueueStateKey{
		TaskQueue: "default",
		State:     string(domain.WorkerActive),
	}])
	assert.Zero(t, stats.DeadLetterDepth)
}

func TestCollectQueueStatsOnAnEmptyDatabaseReturnsEmptyMaps(t *testing.T) {
	s := testsupport.NewStore(t)

	stats, err := s.CollectQueueStats(testCtx(t))
	require.NoError(t, err)

	// Maps must be non-nil so the observer can range over them without a nil check,
	// and empty so no stale gauge is published for a system with no work.
	require.NotNil(t, stats.ClaimableByActivity)
	require.NotNil(t, stats.BackoffByQueue)
	require.NotNil(t, stats.OldestClaimableAge)
	require.NotNil(t, stats.WorkersByQueueState)
	assert.Empty(t, stats.ClaimableByActivity)
	assert.Zero(t, stats.DeadLetterDepth)
}

func TestPoolStatsReportsTheCeiling(t *testing.T) {
	s := testsupport.NewStore(t)

	stats := s.PoolStats()

	// Pool size per replica times the replica count is a hard ceiling against the
	// database's max_connections, so the maximum has to be exported, not just usage.
	assert.Positive(t, stats.Max)
	assert.LessOrEqual(t, stats.Acquired, stats.Max)
}

// backdateScheduledAt pushes a task's scheduled_at into the past, simulating a
// task that has been waiting to be claimed. Uses the database clock for the same
// reason the rest of the suite does: the application's clock is not authoritative.
func backdateScheduledAt(t *testing.T, s *store.Store, taskID interface{ String() string }, by time.Duration) {
	t.Helper()
	ctx := testCtx(t)
	tag, err := s.Pool().Exec(ctx, `
		UPDATE tasks
		SET scheduled_at = now() - ($2::bigint * interval '1 microsecond')
		WHERE id = $1 AND state = 'SCHEDULED'`, taskID.String(), by.Microseconds())
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}
