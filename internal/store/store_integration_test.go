package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// linearSpec is the Task A -> Task B -> Task C example from the project brief.
func linearSpec() domain.WorkflowSpec {
	spec := domain.WorkflowSpec{
		Name:    "order_pipeline",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "charge_payment"},
			{Name: "task_b", Activity: "reserve_inventory", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "send_receipt", DependsOn: []string{"task_b"}},
		},
	}
	spec.Normalize()
	return spec
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustRegister(t *testing.T, s *store.Store, spec domain.WorkflowSpec) *domain.WorkflowDefinition {
	t.Helper()
	ctx := testCtx(t)
	def, created, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	require.True(t, created)
	return def
}

func TestRegisterDefinitionIsIdempotent(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	spec := linearSpec()

	first, created, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	require.True(t, created, "first registration must create the row")
	require.NotEmpty(t, first.SpecHash)
	require.Len(t, first.Spec.Tasks, 3, "spec must round-trip through JSONB")
	require.Equal(t, domain.DefaultTaskQueue, first.TaskQueue)

	// Re-registering the identical spec is a no-op that returns the same row.
	second, created, err := s.RegisterDefinition(ctx, spec)
	require.NoError(t, err)
	require.False(t, created, "identical re-registration must not create a new row")
	require.Equal(t, first.ID, second.ID)

	// Declaration order is not semantic, so a reordered spec is still identical.
	reordered := linearSpec()
	reordered.Tasks[0], reordered.Tasks[2] = reordered.Tasks[2], reordered.Tasks[0]
	third, created, err := s.RegisterDefinition(ctx, reordered)
	require.NoError(t, err)
	require.False(t, created, "reordering tasks must not change the spec hash")
	require.Equal(t, first.ID, third.ID)
}

func TestRegisterDefinitionRejectsMutatedVersion(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	mustRegister(t, s, linearSpec())

	mutated := linearSpec()
	mutated.Tasks[0].Activity = "charge_payment_v2"

	_, _, err := s.RegisterDefinition(ctx, mutated)
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrConflict,
		"changing a registered version must conflict so persisted history stays replayable")
}

func TestCreateExecutionDeduplicatesOnIdempotencyKey(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())

	params := store.StartExecutionParams{
		Definition:     def,
		Input:          json.RawMessage(`{"orderId":"A-1"}`),
		IdempotencyKey: "order-A-1",
	}

	first, created, err := s.CreateExecution(ctx, params)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, domain.WorkflowPending, first.State)
	require.JSONEq(t, `{"orderId":"A-1"}`, string(first.Input))

	second, created, err := s.CreateExecution(ctx, params)
	require.NoError(t, err)
	require.False(t, created, "replayed start must not create a second execution")
	require.Equal(t, first.ID, second.ID)

	// Without a key every request is a distinct run.
	noKey := params
	noKey.IdempotencyKey = ""
	third, created, err := s.CreateExecution(ctx, noKey)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEqual(t, first.ID, third.ID)
}

// TestCreateExecutionDeduplicatesUnderConcurrency proves the dedup guarantee
// holds when racing requests hit the database at the same moment, which is the
// case an application-level "check then insert" would get wrong.
func TestCreateExecutionDeduplicatesUnderConcurrency(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())

	const racers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]struct{}{}
		creates int
		errs    []error
	)
	start := make(chan struct{})

	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			exec, created, err := s.CreateExecution(ctx, store.StartExecutionParams{
				Definition:     def,
				Input:          json.RawMessage(`{"orderId":"race"}`),
				IdempotencyKey: "race-key",
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[exec.ID] = struct{}{}
			if created {
				creates++
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Empty(t, errs, "concurrent identical starts must all succeed")
	require.Len(t, ids, 1, "all racers must converge on one execution")
	require.Equal(t, 1, creates, "exactly one racer may create the execution")
}

func TestMaterializeTasksIsIdempotent(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())

	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)

	inserted, err := s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	require.Equal(t, 3, inserted)

	// A crash-and-retry of materialization must not duplicate tasks.
	inserted, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	require.Zero(t, inserted, "re-materialization must insert nothing")

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.Equal(t, domain.TaskPending, task.State)
		require.Nil(t, task.ClaimToken)
	}
	// depends_on must survive the TEXT[] round-trip.
	require.Equal(t, []string{"task_a"}, tasks[1].DependsOn)
	require.Empty(t, tasks[0].DependsOn)
}

// claimableTask materializes the example workflow and enqueues task_a.
func claimableTask(t *testing.T, s *store.Store) (*domain.WorkflowExecution, *domain.Task) {
	t.Helper()
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())

	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)

	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)

	scheduled, err := s.ScheduleTask(ctx, tasks[0].ID, json.RawMessage(`{"amount":42}`), time.Now())
	require.NoError(t, err)
	require.Equal(t, domain.TaskScheduled, scheduled.State)
	return exec, scheduled
}

func TestClaimTaskLeasesAndCompletes(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	_, scheduled := claimableTask(t, s)

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		TaskQueue:     domain.DefaultTaskQueue,
		Activities:    []string{"charge_payment"},
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, scheduled.ID, claimed.ID)
	require.Equal(t, domain.TaskRunning, claimed.State)
	require.Equal(t, 1, claimed.Attempt, "claiming consumes an attempt")
	require.NotNil(t, claimed.ClaimToken, "claim must mint a token")
	require.NotNil(t, claimed.LeaseExpiresAt)
	require.WithinDuration(t, time.Now().Add(30*time.Second), *claimed.LeaseExpiresAt, 5*time.Second)
	require.JSONEq(t, `{"amount":42}`, string(claimed.Input))

	// Queue is now empty for that activity.
	_, err = s.ClaimTask(ctx, store.ClaimParams{
		TaskQueue:  domain.DefaultTaskQueue,
		Activities: []string{"charge_payment"},
		WorkerID:   "worker-2",
	})
	require.ErrorIs(t, err, domain.ErrNotFound, "a claimed task must not be claimable again")

	done, err := s.CompleteTask(ctx, claimed.ID, *claimed.ClaimToken, json.RawMessage(`{"receipt":"r-1"}`))
	require.NoError(t, err)
	require.Equal(t, domain.TaskCompleted, done.State)
	require.JSONEq(t, `{"receipt":"r-1"}`, string(done.Output))
	require.Nil(t, done.LeaseExpiresAt)
}

func TestCompleteTaskIsIdempotentAndRejectsStaleClaims(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	_, scheduled := claimableTask(t, s)

	claimed, err := s.ClaimTask(ctx, store.ClaimParams{
		TaskQueue: domain.DefaultTaskQueue, WorkerID: "worker-1",
	})
	require.NoError(t, err)
	require.Equal(t, scheduled.ID, claimed.ID)

	output := json.RawMessage(`{"receipt":"r-1"}`)
	first, err := s.CompleteTask(ctx, claimed.ID, *claimed.ClaimToken, output)
	require.NoError(t, err)

	// A retried report of the same attempt is an idempotent replay, not an error:
	// a worker whose response was lost in flight must be able to retry safely.
	second, err := s.CompleteTask(ctx, claimed.ID, *claimed.ClaimToken, output)
	require.NoError(t, err)
	require.Equal(t, first.State, second.State)
	require.Equal(t, first.CompletedAt.UnixNano(), second.CompletedAt.UnixNano(),
		"replay must not move the completion timestamp")

	// A worker holding a superseded lease must not be able to write a result.
	_, err = s.CompleteTask(ctx, claimed.ID, uuid.New(), output)
	require.ErrorIs(t, err, domain.ErrStaleClaim)

	// Reporting a different outcome for the same attempt is a conflict.
	_, err = s.FailTask(ctx, claimed.ID, *claimed.ClaimToken, "boom")
	require.ErrorIs(t, err, domain.ErrConflict)
}

// TestClaimTaskUnderConcurrencyHandsOutDistinctTasks is the core queue
// guarantee: N workers polling at once get N different tasks, never the same one.
func TestClaimTaskUnderConcurrencyHandsOutDistinctTasks(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	// A fan-out workflow so several tasks are simultaneously claimable.
	spec := domain.WorkflowSpec{Name: "fanout", Version: 1}
	const taskCount = 12
	for i := range taskCount {
		spec.Tasks = append(spec.Tasks, domain.TaskSpec{
			Name:     "task_" + string(rune('a'+i)),
			Activity: "noop",
		})
	}
	spec.Normalize()
	def := mustRegister(t, s, spec)

	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)
	_, err = s.MaterializeTasks(ctx, exec, &def.Spec)
	require.NoError(t, err)
	tasks, err := s.ListTasks(ctx, exec.ID)
	require.NoError(t, err)
	for _, task := range tasks {
		_, err := s.ScheduleTask(ctx, task.ID, json.RawMessage(`{}`), time.Now())
		require.NoError(t, err)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = map[uuid.UUID]string{}
		dupes   []uuid.UUID
	)
	start := make(chan struct{})

	// More claimants than tasks, so some must legitimately come back empty.
	for w := range taskCount + 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			task, err := s.ClaimTask(ctx, store.ClaimParams{
				TaskQueue: domain.DefaultTaskQueue,
				WorkerID:  "worker-" + string(rune('A'+worker)),
			})
			if errors.Is(err, domain.ErrNotFound) {
				return
			}
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()
			if _, seen := claimed[task.ID]; seen {
				dupes = append(dupes, task.ID)
			}
			claimed[task.ID] = task.WorkerID
		}(w)
	}
	close(start)
	wg.Wait()

	require.Empty(t, dupes, "SKIP LOCKED must never hand the same task to two workers")
	require.Len(t, claimed, taskCount, "every scheduled task should be claimed exactly once")
}

func TestHistoryIsAppendOnlyAndOrdered(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)

	events := []domain.EventType{
		domain.EventWorkflowStarted,
		domain.EventTaskScheduled,
		domain.EventTaskCompleted,
		domain.EventWorkflowCompleted,
	}
	for _, et := range events {
		_, err := s.AppendEvent(ctx, store.AppendEventParams{
			ExecutionID: exec.ID,
			EventType:   et,
			Payload:     map[string]string{"event": string(et)},
		})
		require.NoError(t, err)
	}

	history, err := s.ListHistory(ctx, exec.ID, 0, 100)
	require.NoError(t, err)
	require.Len(t, history, len(events))

	var prevID int64
	for i, e := range history {
		require.Equal(t, events[i], e.EventType, "events must read back in append order")
		require.Greater(t, e.ID, prevID, "ids must be strictly increasing within an execution")
		require.JSONEq(t, fmt.Sprintf(`{"event":%q}`, events[i]), string(e.Payload))
		prevID = e.ID
	}

	// Cursor paging: pass back the last id seen to read only what is new.
	tail, err := s.ListHistory(ctx, exec.ID, history[1].ID, 100)
	require.NoError(t, err)
	require.Len(t, tail, 2)
	require.Equal(t, domain.EventTaskCompleted, tail[0].EventType)

	// A cursor at the end yields nothing, which is how a tailing client idles.
	empty, err := s.ListHistory(ctx, exec.ID, history[len(history)-1].ID, 100)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// TestConcurrentHistoryAppendsDoNotContend is the property that motivated
// dropping the dense per-execution counter: many writers appending to one
// execution must all succeed without serializing or retrying.
func TestConcurrentHistoryAppendsDoNotContend(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)

	const writers = 24
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	start := make(chan struct{})

	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := s.AppendEvent(ctx, store.AppendEventParams{
				ExecutionID: exec.ID,
				EventType:   domain.EventTaskStarted,
				TaskName:    fmt.Sprintf("task_%02d", i),
			})
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	require.Empty(t, errs, "concurrent appends must not conflict")

	history, err := s.ListHistory(ctx, exec.ID, 0, 1000)
	require.NoError(t, err)
	require.Len(t, history, writers, "every append must be durable")

	seen := map[int64]struct{}{}
	for _, e := range history {
		require.NotContains(t, seen, e.ID, "event ids must be unique")
		seen[e.ID] = struct{}{}
	}
}

func TestTransitionExecutionIsCompareAndSwap(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	def := mustRegister(t, s, linearSpec())
	exec, _, err := s.CreateExecution(ctx, store.StartExecutionParams{Definition: def})
	require.NoError(t, err)

	running, err := s.TransitionExecution(ctx, exec.ID, domain.WorkflowPending, domain.WorkflowRunning, nil, "")
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowRunning, running.State)
	require.NotNil(t, running.StartedAt)

	// Replaying the same transition must fail rather than clobber the row.
	_, err = s.TransitionExecution(ctx, exec.ID, domain.WorkflowPending, domain.WorkflowRunning, nil, "")
	require.ErrorIs(t, err, domain.ErrInvalidStateTransition)

	done, err := s.TransitionExecution(ctx, exec.ID, domain.WorkflowRunning, domain.WorkflowCompleted,
		json.RawMessage(`{"ok":true}`), "")
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowCompleted, done.State)
	require.NotNil(t, done.CompletedAt)
	require.JSONEq(t, `{"ok":true}`, string(done.Output))

	// Terminal states are terminal.
	_, err = s.TransitionExecution(ctx, exec.ID, domain.WorkflowCompleted, domain.WorkflowRunning, nil, "")
	require.ErrorIs(t, err, domain.ErrInvalidStateTransition)
}

func TestRegisterWorkerUpsertsByName(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)

	first, err := s.RegisterWorker(ctx, store.RegisterWorkerParams{
		Name:       "worker-1",
		TaskQueue:  domain.DefaultTaskQueue,
		Activities: []string{"charge_payment"},
	})
	require.NoError(t, err)
	require.Equal(t, domain.WorkerActive, first.State)
	require.Equal(t, []string{"charge_payment"}, first.Activities)

	// A restarted worker reuses its identity instead of leaking a new row.
	second, err := s.RegisterWorker(ctx, store.RegisterWorkerParams{
		Name:       "worker-1",
		TaskQueue:  domain.DefaultTaskQueue,
		Activities: []string{"charge_payment", "send_receipt"},
	})
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.Len(t, second.Activities, 2)

	workers, err := s.ListWorkers(ctx, domain.DefaultTaskQueue, 10)
	require.NoError(t, err)
	require.Len(t, workers, 1)

	beat, err := s.Heartbeat(ctx, first.ID)
	require.NoError(t, err)
	require.False(t, beat.LastHeartbeatAt.Before(first.LastHeartbeatAt))
}

func TestWithTxRollsBackOnError(t *testing.T) {
	s := testsupport.NewStore(t)
	ctx := testCtx(t)
	sentinel := errors.New("intentional failure")

	err := s.WithTx(ctx, func(tx *store.Store) error {
		spec := linearSpec()
		if _, _, err := tx.RegisterDefinition(ctx, spec); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	defs, err := s.ListDefinitions(ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, defs, "a failed transaction must leave no trace")
}
