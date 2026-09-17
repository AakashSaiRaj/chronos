package engine_test

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
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// harness bundles the pieces a test drives: durable state, a scheduler, and the
// control-plane service a worker would call.
type harness struct {
	store  *store.Store
	engine *engine.Engine
	svc    *engine.Service
	ctx    context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	s := testsupport.NewStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	eng := engine.New(s, engine.Config{PollInterval: 10 * time.Millisecond}, testsupport.Logger())
	return &harness{
		store:  s,
		engine: eng,
		svc:    engine.NewService(s, eng, testsupport.Logger()),
		ctx:    ctx,
	}
}

// restartEngine returns a brand-new Engine and Service over the same database,
// standing in for a process restart. Nothing is carried over in memory, so
// anything the new instance can do it derived purely from persisted rows.
func (h *harness) restartEngine(t *testing.T) {
	t.Helper()
	eng := engine.New(h.store, engine.Config{PollInterval: 10 * time.Millisecond}, testsupport.Logger())
	h.engine = eng
	h.svc = engine.NewService(h.store, eng, testsupport.Logger())
}

// linearSpec is the Task A -> Task B -> Task C example from the brief.
func linearSpec() domain.WorkflowSpec {
	return domain.WorkflowSpec{
		Name:        "order_pipeline",
		Version:     1,
		Description: "charge, reserve, then notify",
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "charge_payment"},
			{Name: "task_b", Activity: "reserve_inventory", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "send_receipt", DependsOn: []string{"task_b"}},
		},
	}
}

func (h *harness) register(t *testing.T, spec domain.WorkflowSpec) *domain.WorkflowDefinition {
	t.Helper()
	def, created, err := h.svc.RegisterWorkflow(h.ctx, spec)
	require.NoError(t, err)
	require.True(t, created)
	return def
}

func (h *harness) start(t *testing.T, workflow string, input string, key string) *domain.WorkflowExecution {
	t.Helper()
	exec, _, err := h.svc.StartExecution(h.ctx, engine.StartExecutionRequest{
		WorkflowName:   workflow,
		Input:          json.RawMessage(input),
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	return exec
}

func (h *harness) sweep(t *testing.T) {
	t.Helper()
	require.NoError(t, h.engine.Sweep(h.ctx))
}

func (h *harness) execution(t *testing.T, id uuid.UUID) *domain.WorkflowExecution {
	t.Helper()
	exec, err := h.store.GetExecution(h.ctx, id)
	require.NoError(t, err)
	return exec
}

func (h *harness) tasksByName(t *testing.T, id uuid.UUID) map[string]domain.Task {
	t.Helper()
	tasks, err := h.store.ListTasks(h.ctx, id)
	require.NoError(t, err)

	out := make(map[string]domain.Task, len(tasks))
	for _, task := range tasks {
		out[task.Name] = task
	}
	return out
}

func (h *harness) eventTypes(t *testing.T, id uuid.UUID) []domain.EventType {
	t.Helper()
	events, err := h.store.ListHistory(h.ctx, id, 0, 1000)
	require.NoError(t, err)

	out := make([]domain.EventType, 0, len(events))
	var prevID int64
	for _, e := range events {
		require.Greater(t, e.ID, prevID, "history must read back in increasing id order")
		prevID = e.ID
		out = append(out, e.EventType)
	}
	return out
}

// pollOne claims a single task as a worker would, returning nil when idle.
func (h *harness) pollOne(t *testing.T, workerID string) *domain.Task {
	t.Helper()
	task, err := h.svc.PollTask(h.ctx, engine.PollRequest{
		WorkerID:      workerID,
		TaskQueue:     domain.DefaultTaskQueue,
		LeaseDuration: 30 * time.Second,
	})
	if errors.Is(err, domain.ErrNotFound) {
		return nil
	}
	require.NoError(t, err)
	return task
}

// runToCompletion alternates engine sweeps with worker polls until the execution
// reaches a terminal state. Each poll/complete pair mimics one worker turn.
func (h *harness) runToCompletion(t *testing.T, execID uuid.UUID) *domain.WorkflowExecution {
	t.Helper()
	const maxTurns = 50

	for turn := range maxTurns {
		h.sweep(t)

		exec := h.execution(t, execID)
		if exec.State.IsTerminal() {
			return exec
		}

		task := h.pollOne(t, "worker-1")
		if task == nil {
			// Nothing claimable yet; the next sweep should schedule more.
			continue
		}
		require.NotNil(t, task.ClaimToken)

		output := fmt.Sprintf(`{"ranTask":%q,"activity":%q,"attempt":%d}`,
			task.Name, task.Activity, task.Attempt)
		_, err := h.svc.CompleteTask(h.ctx, task.ID, *task.ClaimToken, json.RawMessage(output))
		require.NoError(t, err, "turn %d completing %s", turn, task.Name)
	}

	t.Fatalf("execution %s did not reach a terminal state within %d turns", execID, maxTurns)
	return nil
}

// TestLinearWorkflowRunsToCompletion is the Phase 1 deliverable in one test:
// a persisted workflow whose tasks are executed in dependency order by a worker.
func TestLinearWorkflowRunsToCompletion(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-1","amount":42}`, "order-A-1")

	require.Equal(t, domain.WorkflowPending, exec.State,
		"a started execution is durable before it is scheduled")

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)
	require.Empty(t, final.Error)
	require.NotNil(t, final.StartedAt)
	require.NotNil(t, final.CompletedAt)

	tasks := h.tasksByName(t, exec.ID)
	require.Len(t, tasks, 3)
	for name, task := range tasks {
		require.Equal(t, domain.TaskCompleted, task.State, "task %s", name)
		require.Equal(t, 1, task.Attempt, "task %s should succeed on its first attempt", name)
	}

	// Dependency order must hold in wall-clock terms, not just in the graph.
	require.True(t, tasks["task_a"].CompletedAt.Before(*tasks["task_b"].StartedAt),
		"task_b must not start before task_a completes")
	require.True(t, tasks["task_b"].CompletedAt.Before(*tasks["task_c"].StartedAt),
		"task_c must not start before task_b completes")

	// The workflow's output is its terminal task's output.
	require.JSONEq(t,
		`{"task_c":{"ranTask":"task_c","activity":"send_receipt","attempt":1}}`,
		string(final.Output))

	require.Equal(t, []domain.EventType{
		domain.EventWorkflowStarted,
		domain.EventTaskScheduled, domain.EventTaskStarted, domain.EventTaskCompleted,
		domain.EventTaskScheduled, domain.EventTaskStarted, domain.EventTaskCompleted,
		domain.EventTaskScheduled, domain.EventTaskStarted, domain.EventTaskCompleted,
		domain.EventWorkflowCompleted,
	}, h.eventTypes(t, exec.ID), "history must record the full causal chain")
}

// TestTaskInputPropagation checks that a task receives the workflow input plus
// its upstream dependency's output, resolved and persisted at schedule time.
func TestTaskInputPropagation(t *testing.T) {
	h := newHarness(t)
	spec := linearSpec()
	spec.Tasks[1].Input = json.RawMessage(`{"warehouse":"eu-1"}`)
	h.register(t, spec)

	h.start(t, "order_pipeline", `{"orderId":"A-2","amount":7}`, "")

	// Run task_a and capture what task_b is then handed.
	h.sweep(t)
	taskA := h.pollOne(t, "worker-1")
	require.NotNil(t, taskA)
	require.Equal(t, "task_a", taskA.Name)

	var inputA domain.TaskInput
	require.NoError(t, json.Unmarshal(taskA.Input, &inputA))
	require.JSONEq(t, `{"orderId":"A-2","amount":7}`, string(inputA.WorkflowInput))
	require.Empty(t, inputA.Upstream, "a root task has no upstream outputs")

	_, err := h.svc.CompleteTask(h.ctx, taskA.ID, *taskA.ClaimToken, json.RawMessage(`{"chargeId":"chg_1"}`))
	require.NoError(t, err)

	h.sweep(t)
	taskB := h.pollOne(t, "worker-1")
	require.NotNil(t, taskB)
	require.Equal(t, "task_b", taskB.Name)

	var inputB domain.TaskInput
	require.NoError(t, json.Unmarshal(taskB.Input, &inputB))
	require.JSONEq(t, `{"orderId":"A-2","amount":7}`, string(inputB.WorkflowInput),
		"workflow input must reach every task")
	require.JSONEq(t, `{"warehouse":"eu-1"}`, string(inputB.TaskInput),
		"static task input must be delivered")
	require.JSONEq(t, `{"chargeId":"chg_1"}`, string(inputB.Upstream["task_a"]),
		"the upstream task's output must be delivered under its task name")
}

// TestExecutionRecoversFromPersistedStateAfterEngineRestart is the core Phase 1
// requirement: a workflow must be recoverable from persisted state rather than
// relying on in-memory state.
func TestExecutionRecoversFromPersistedStateAfterEngineRestart(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-3"}`, "")

	// Get the workflow half-way: task_a done, task_b scheduled and claimed.
	h.sweep(t)
	taskA := h.pollOne(t, "worker-1")
	require.NotNil(t, taskA)
	require.Equal(t, "task_a", taskA.Name)
	_, err := h.svc.CompleteTask(h.ctx, taskA.ID, *taskA.ClaimToken, json.RawMessage(`{"step":"a"}`))
	require.NoError(t, err)
	h.sweep(t)

	midTasks := h.tasksByName(t, exec.ID)
	require.Equal(t, domain.TaskCompleted, midTasks["task_a"].State)
	require.Equal(t, domain.TaskScheduled, midTasks["task_b"].State)
	require.Equal(t, domain.TaskPending, midTasks["task_c"].State)

	// Throw the engine away entirely. The replacement has never seen this
	// execution and holds no in-memory state about it.
	h.restartEngine(t)

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State,
		"a fresh engine must finish a workflow it never started")

	tasks := h.tasksByName(t, exec.ID)
	for name, task := range tasks {
		require.Equal(t, domain.TaskCompleted, task.State, "task %s", name)
	}
	// task_a keeps its original result: recovery resumes, it does not restart.
	require.JSONEq(t, `{"step":"a"}`, string(tasks["task_a"].Output),
		"already-completed work must not be redone after a restart")
	require.Equal(t, 1, tasks["task_a"].Attempt)
}

// TestPendingExecutionSurvivesEngineOutage covers the case where the engine dies
// before it ever touches a newly accepted execution.
func TestPendingExecutionSurvivesEngineOutage(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())

	// Accept the work but never sweep: the engine "was down" the whole time.
	exec := h.start(t, "order_pipeline", `{"orderId":"A-4"}`, "")
	require.Equal(t, domain.WorkflowPending, h.execution(t, exec.ID).State)
	require.Empty(t, h.tasksByName(t, exec.ID), "no tasks exist until the engine starts it")

	h.restartEngine(t)

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State,
		"work accepted during an engine outage must still run once an engine returns")
}

// TestExhaustedTaskIsDeadLetteredAndFailsWorkflow pins the failure semantics:
// with the default single attempt, one unrecoverable failure parks the task in
// the dead letter queue and ends the run. The parked task is deliberately left
// in DEAD_LETTER rather than canceled, so it stays visible and replayable.
func TestExhaustedTaskIsDeadLetteredAndFailsWorkflow(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-5"}`, "")

	h.sweep(t)
	taskA := h.pollOne(t, "worker-1")
	require.NotNil(t, taskA)
	_, err := h.svc.CompleteTask(h.ctx, taskA.ID, *taskA.ClaimToken, json.RawMessage(`{"step":"a"}`))
	require.NoError(t, err)

	h.sweep(t)
	taskB := h.pollOne(t, "worker-1")
	require.NotNil(t, taskB)
	require.Equal(t, "task_b", taskB.Name)
	_, err = h.svc.FailTask(h.ctx, taskB.ID, *taskB.ClaimToken, store.FailParams{
		Error: "inventory service unavailable", Retryable: true,
	})
	require.NoError(t, err)

	h.sweep(t)

	final := h.execution(t, exec.ID)
	require.Equal(t, domain.WorkflowFailed, final.State)
	require.Contains(t, final.Error, "task_b")
	require.Contains(t, final.Error, "inventory service unavailable")
	require.NotNil(t, final.CompletedAt)

	tasks := h.tasksByName(t, exec.ID)
	require.Equal(t, domain.TaskCompleted, tasks["task_a"].State, "completed work is preserved")
	require.Equal(t, domain.TaskDeadLetter, tasks["task_b"].State,
		"the exhausted task is parked for operator attention, not silently canceled")
	require.NotNil(t, tasks["task_b"].DeadLetteredAt)
	require.Equal(t, domain.TaskCanceled, tasks["task_c"].State,
		"downstream work must not be left dangling in the queue")

	// Nothing is claimable after the workflow fails.
	require.Nil(t, h.pollOne(t, "worker-1"))

	events := h.eventTypes(t, exec.ID)
	require.Contains(t, events, domain.EventTaskFailed)
	require.Contains(t, events, domain.EventTaskDeadLettered)
	require.Equal(t, domain.EventWorkflowFailed, events[len(events)-1])

	// The parked task is visible in the dead letter queue.
	parked, err := h.store.ListDeadLetterTasks(h.ctx, store.DeadLetterFilter{})
	require.NoError(t, err)
	require.Len(t, parked, 1)
	require.Equal(t, "task_b", parked[0].Name)
	require.Contains(t, parked[0].Error, "inventory service unavailable")
}

func TestCancelExecutionStopsScheduling(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-6"}`, "")

	h.sweep(t)
	require.Equal(t, domain.TaskScheduled, h.tasksByName(t, exec.ID)["task_a"].State)

	canceled, err := h.svc.CancelExecution(h.ctx, exec.ID, "operator stopped the run")
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowCanceled, canceled.State)
	require.Equal(t, "operator stopped the run", canceled.Error)

	tasks := h.tasksByName(t, exec.ID)
	for name, task := range tasks {
		require.Equal(t, domain.TaskCanceled, task.State, "task %s", name)
	}

	// The queue must be empty and further sweeps must be no-ops.
	require.Nil(t, h.pollOne(t, "worker-1"))
	h.sweep(t)
	require.Equal(t, domain.WorkflowCanceled, h.execution(t, exec.ID).State)

	// Cancellation is idempotent.
	again, err := h.svc.CancelExecution(h.ctx, exec.ID, "second attempt")
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowCanceled, again.State)
}

func TestCancelCompletedExecutionIsRejected(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-7"}`, "")

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)

	_, err := h.svc.CancelExecution(h.ctx, exec.ID, "too late")
	require.ErrorIs(t, err, domain.ErrConflict,
		"canceling a finished run must not misrepresent what happened")
}

// TestCanceledTaskRejectsLateWorkerReport shows that invalidating claim tokens on
// cancel is what stops a slow worker from resurrecting a canceled execution.
func TestCanceledTaskRejectsLateWorkerReport(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-8"}`, "")

	h.sweep(t)
	task := h.pollOne(t, "worker-1")
	require.NotNil(t, task)
	token := *task.ClaimToken

	_, err := h.svc.CancelExecution(h.ctx, exec.ID, "canceled while task was running")
	require.NoError(t, err)

	// The worker finishes its work and reports, unaware of the cancellation.
	_, err = h.svc.CompleteTask(h.ctx, task.ID, token, json.RawMessage(`{"step":"a"}`))
	require.ErrorIs(t, err, domain.ErrStaleClaim)

	require.Equal(t, domain.WorkflowCanceled, h.execution(t, exec.ID).State)
}

// TestDuplicateTaskReportIsIdempotent covers the lost-response case: a worker
// that never saw its success acknowledged retries, and must not double-count.
func TestDuplicateTaskReportIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-9"}`, "")

	h.sweep(t)
	task := h.pollOne(t, "worker-1")
	require.NotNil(t, task)

	output := json.RawMessage(`{"step":"a"}`)
	first, err := h.svc.CompleteTask(h.ctx, task.ID, *task.ClaimToken, output)
	require.NoError(t, err)

	second, err := h.svc.CompleteTask(h.ctx, task.ID, *task.ClaimToken, output)
	require.NoError(t, err, "a replayed report must succeed rather than error")
	require.Equal(t, first.CompletedAt.UnixNano(), second.CompletedAt.UnixNano())

	// Exactly one TASK_COMPLETED event for task_a, not two.
	events, err := h.store.ListHistory(h.ctx, exec.ID, 0, 1000)
	require.NoError(t, err)
	completed := 0
	for _, e := range events {
		if e.EventType == domain.EventTaskCompleted && e.TaskName == "task_a" {
			completed++
		}
	}
	require.Equal(t, 1, completed, "a replayed report must not append a second history event")
}

// TestStartExecutionIsIdempotent checks the API-level dedup guarantee.
func TestStartExecutionIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())

	req := engine.StartExecutionRequest{
		WorkflowName:   "order_pipeline",
		Input:          json.RawMessage(`{"orderId":"A-10"}`),
		IdempotencyKey: "order-A-10",
	}

	first, created, err := h.svc.StartExecution(h.ctx, req)
	require.NoError(t, err)
	require.True(t, created)

	second, created, err := h.svc.StartExecution(h.ctx, req)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.ID, second.ID)

	// Replaying after the run finished still resolves to the same execution.
	final := h.runToCompletion(t, first.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)

	third, created, err := h.svc.StartExecution(h.ctx, req)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.ID, third.ID)
	require.Equal(t, domain.WorkflowCompleted, third.State)
}

// TestDiamondWorkflowSchedulesParallelBranches proves the scheduler dispatches
// independent branches concurrently and waits for a join.
func TestDiamondWorkflowSchedulesParallelBranches(t *testing.T) {
	h := newHarness(t)
	h.register(t, domain.WorkflowSpec{
		Name:    "diamond",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "fetch", Activity: "noop"},
			{Name: "left", Activity: "noop", DependsOn: []string{"fetch"}},
			{Name: "right", Activity: "noop", DependsOn: []string{"fetch"}},
			{Name: "merge", Activity: "noop", DependsOn: []string{"left", "right"}},
		},
	})
	exec := h.start(t, "diamond", `{}`, "")

	h.sweep(t)
	root := h.pollOne(t, "worker-1")
	require.NotNil(t, root)
	require.Equal(t, "fetch", root.Name)
	_, err := h.svc.CompleteTask(h.ctx, root.ID, *root.ClaimToken, json.RawMessage(`{"ok":true}`))
	require.NoError(t, err)

	h.sweep(t)

	// Both branches become claimable at once, by different workers.
	branch1 := h.pollOne(t, "worker-1")
	branch2 := h.pollOne(t, "worker-2")
	require.NotNil(t, branch1)
	require.NotNil(t, branch2, "independent branches must be schedulable in parallel")
	require.NotEqual(t, branch1.ID, branch2.ID)
	require.ElementsMatch(t, []string{"left", "right"}, []string{branch1.Name, branch2.Name})

	// The join must not be claimable until both branches finish.
	require.Nil(t, h.pollOne(t, "worker-3"), "the join task must wait for both branches")

	_, err = h.svc.CompleteTask(h.ctx, branch1.ID, *branch1.ClaimToken, json.RawMessage(`{"b":1}`))
	require.NoError(t, err)
	h.sweep(t)
	require.Nil(t, h.pollOne(t, "worker-3"), "one finished branch is not enough to release the join")

	_, err = h.svc.CompleteTask(h.ctx, branch2.ID, *branch2.ClaimToken, json.RawMessage(`{"b":2}`))
	require.NoError(t, err)

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)
	require.Equal(t, domain.TaskCompleted, h.tasksByName(t, exec.ID)["merge"].State)
}

// TestConcurrentEnginesDoNotDoubleSchedule proves several scheduler replicas can
// sweep the same database safely: per-execution row locks make the work exclusive.
func TestConcurrentEnginesDoNotDoubleSchedule(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())

	execIDs := make([]uuid.UUID, 0, 5)
	for i := range 5 {
		exec := h.start(t, "order_pipeline", `{"orderId":"batch"}`, fmt.Sprintf("batch-%d", i))
		execIDs = append(execIDs, exec.ID)
	}

	// Four independent engines sweep at once, as four scheduler pods would.
	engines := make([]*engine.Engine, 4)
	for i := range engines {
		engines[i] = engine.New(h.store, engine.Config{}, testsupport.Logger())
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(engines))
	for _, eng := range engines {
		wg.Add(1)
		go func(e *engine.Engine) {
			defer wg.Done()
			if err := e.Sweep(h.ctx); err != nil {
				errs <- err
			}
		}(eng)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "concurrent sweeps must not conflict")
	}

	// Each execution must have exactly one scheduled root task and exactly one
	// WORKFLOW_STARTED event, even though four engines raced to start it.
	for _, id := range execIDs {
		tasks := h.tasksByName(t, id)
		require.Len(t, tasks, 3, "execution %s must have its tasks materialized once", id)
		require.Equal(t, domain.TaskScheduled, tasks["task_a"].State)

		started := 0
		scheduled := 0
		events, err := h.store.ListHistory(h.ctx, id, 0, 100)
		require.NoError(t, err)
		for _, e := range events {
			switch e.EventType {
			case domain.EventWorkflowStarted:
				started++
			case domain.EventTaskScheduled:
				scheduled++
			}
		}
		require.Equal(t, 1, started, "execution %s must be started exactly once", id)
		require.Equal(t, 1, scheduled, "execution %s must schedule task_a exactly once", id)
	}
}

// TestConcurrentWorkersNeverShareATask exercises the queue under real parallel
// polling against a wide fan-out graph.
func TestConcurrentWorkersNeverShareATask(t *testing.T) {
	h := newHarness(t)

	const width = 16
	spec := domain.WorkflowSpec{Name: "wide", Version: 1}
	for i := range width {
		spec.Tasks = append(spec.Tasks, domain.TaskSpec{
			Name:     fmt.Sprintf("task_%02d", i),
			Activity: "noop",
		})
	}
	h.register(t, spec)
	exec := h.start(t, "wide", `{}`, "")
	h.sweep(t)

	var (
		mu     sync.Mutex
		owners = map[uuid.UUID]string{}
		dupes  []uuid.UUID
		wg     sync.WaitGroup
	)

	// More workers than tasks, all polling simultaneously.
	for w := range width + 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%02d", w)

			task, err := h.svc.PollTask(h.ctx, engine.PollRequest{
				WorkerID:      workerID,
				TaskQueue:     domain.DefaultTaskQueue,
				LeaseDuration: 30 * time.Second,
			})
			if errors.Is(err, domain.ErrNotFound) {
				return
			}
			if err != nil {
				mu.Lock()
				dupes = append(dupes, uuid.Nil)
				mu.Unlock()
				t.Errorf("worker %s poll failed: %v", workerID, err)
				return
			}

			mu.Lock()
			if _, seen := owners[task.ID]; seen {
				dupes = append(dupes, task.ID)
			}
			owners[task.ID] = workerID
			mu.Unlock()

			_, err = h.svc.CompleteTask(h.ctx, task.ID, *task.ClaimToken, json.RawMessage(`{"ok":true}`))
			if err != nil {
				t.Errorf("worker %s complete failed: %v", workerID, err)
			}
		}(w)
	}
	wg.Wait()

	require.Empty(t, dupes, "no task may be leased to two workers at once")
	require.Len(t, owners, width, "every scheduled task must be claimed exactly once")

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)
}

// TestEngineIgnoresTerminalExecutions confirms sweeps are cheap no-ops once a
// run is finished, so completed history does not create ongoing work.
func TestEngineIgnoresTerminalExecutions(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())
	exec := h.start(t, "order_pipeline", `{"orderId":"A-11"}`, "")

	final := h.runToCompletion(t, exec.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)

	before := h.eventTypes(t, exec.ID)
	for range 3 {
		h.sweep(t)
	}
	require.Equal(t, before, h.eventTypes(t, exec.ID),
		"sweeping a finished execution must not append history")

	// A finished execution must no longer appear in the engine's work list.
	ids, err := h.store.ListActiveExecutionIDs(h.ctx, 100)
	require.NoError(t, err)
	require.NotContains(t, ids, exec.ID)
}

func TestStartExecutionRejectsUnknownWorkflow(t *testing.T) {
	h := newHarness(t)

	_, _, err := h.svc.StartExecution(h.ctx, engine.StartExecutionRequest{WorkflowName: "ghost"})
	require.ErrorIs(t, err, domain.ErrNotFound)

	_, _, err = h.svc.StartExecution(h.ctx, engine.StartExecutionRequest{})
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestStartExecutionPinsVersion(t *testing.T) {
	h := newHarness(t)
	h.register(t, linearSpec())

	v2 := linearSpec()
	v2.Version = 2
	v2.Tasks = append(v2.Tasks, domain.TaskSpec{
		Name: "task_d", Activity: "noop", DependsOn: []string{"task_c"},
	})
	h.register(t, v2)

	// Omitting the version uses the latest.
	latest := h.start(t, "order_pipeline", `{}`, "")
	require.Equal(t, 2, latest.WorkflowVersion)

	// Pinning selects the older definition, which must still be runnable.
	pinned, _, err := h.svc.StartExecution(h.ctx, engine.StartExecutionRequest{
		WorkflowName:    "order_pipeline",
		WorkflowVersion: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, pinned.WorkflowVersion)

	h.sweep(t)
	require.Len(t, h.tasksByName(t, pinned.ID), 3, "v1 must run its own three-task graph")
	require.Len(t, h.tasksByName(t, latest.ID), 4, "v2 must run its own four-task graph")

	final := h.runToCompletion(t, pinned.ID)
	require.Equal(t, domain.WorkflowCompleted, final.State)
}

func TestGetHistoryRejectsUnknownExecution(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.GetHistory(h.ctx, uuid.New(), 0, 100)
	require.ErrorIs(t, err, domain.ErrNotFound,
		"an unknown execution must 404 rather than return an empty history")
}
