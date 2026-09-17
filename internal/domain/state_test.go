package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

func TestWorkflowStateMachine(t *testing.T) {
	legal := []struct{ from, to domain.WorkflowState }{
		{domain.WorkflowPending, domain.WorkflowRunning},
		{domain.WorkflowPending, domain.WorkflowCanceled},
		{domain.WorkflowPending, domain.WorkflowFailed},
		{domain.WorkflowRunning, domain.WorkflowCompleted},
		{domain.WorkflowRunning, domain.WorkflowFailed},
		{domain.WorkflowRunning, domain.WorkflowCanceled},
		// Operator replay of a dead-lettered task revives a failed run. This is
		// the only way out of FAILED, and only an explicit request makes it.
		{domain.WorkflowFailed, domain.WorkflowRunning},
	}
	for _, tc := range legal {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			require.NoError(t, domain.TransitionWorkflow(tc.from, tc.to))
		})
	}

	illegal := []struct{ from, to domain.WorkflowState }{
		// COMPLETED and CANCELED are absolutely terminal.
		{domain.WorkflowCompleted, domain.WorkflowRunning},
		{domain.WorkflowCompleted, domain.WorkflowFailed},
		{domain.WorkflowFailed, domain.WorkflowCompleted},
		{domain.WorkflowCanceled, domain.WorkflowRunning},
		{domain.WorkflowCanceled, domain.WorkflowCompleted},
		// A run cannot skip straight from PENDING to COMPLETED without ever
		// having been scheduled.
		{domain.WorkflowPending, domain.WorkflowCompleted},
		// Going backwards is never valid.
		{domain.WorkflowRunning, domain.WorkflowPending},
	}
	for _, tc := range illegal {
		t.Run("reject "+string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			err := domain.TransitionWorkflow(tc.from, tc.to)
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrInvalidStateTransition)
		})
	}

	t.Run("self transition is rejected", func(t *testing.T) {
		err := domain.TransitionWorkflow(domain.WorkflowRunning, domain.WorkflowRunning)
		require.ErrorIs(t, err, domain.ErrInvalidStateTransition)
	})

	t.Run("unknown states are rejected", func(t *testing.T) {
		require.ErrorIs(t,
			domain.TransitionWorkflow(domain.WorkflowState("BOGUS"), domain.WorkflowRunning),
			domain.ErrInvalidStateTransition)
		require.ErrorIs(t,
			domain.TransitionWorkflow(domain.WorkflowRunning, domain.WorkflowState("BOGUS")),
			domain.ErrInvalidStateTransition)
	})
}

func TestWorkflowTerminalStates(t *testing.T) {
	// FAILED counts as terminal to the engine even though operator replay can
	// revive it: the engine must never resume a run it gave up on by itself.
	terminal := []domain.WorkflowState{
		domain.WorkflowCompleted, domain.WorkflowFailed, domain.WorkflowCanceled,
	}
	for _, s := range terminal {
		require.True(t, s.IsTerminal(), "%s must be terminal", s)
	}
	require.True(t, domain.WorkflowFailed.CanTransitionTo(domain.WorkflowRunning),
		"a failed run must still be revivable by an explicit replay")

	nonTerminal := []domain.WorkflowState{domain.WorkflowPending, domain.WorkflowRunning}
	for _, s := range nonTerminal {
		require.False(t, s.IsTerminal(), "%s must not be terminal", s)
	}

	require.False(t, domain.WorkflowState("BOGUS").IsTerminal(),
		"an unknown state must not be reported as terminal")
	require.False(t, domain.WorkflowState("BOGUS").Valid())
}

func TestTaskStateMachine(t *testing.T) {
	legal := []struct{ from, to domain.TaskState }{
		{domain.TaskPending, domain.TaskScheduled},
		{domain.TaskPending, domain.TaskCanceled},
		{domain.TaskScheduled, domain.TaskRunning},
		{domain.TaskScheduled, domain.TaskCanceled},
		{domain.TaskRunning, domain.TaskCompleted},
		{domain.TaskRunning, domain.TaskFailed},
		{domain.TaskRunning, domain.TaskCanceled},
		// Lease expiry requeues a running task.
		{domain.TaskRunning, domain.TaskScheduled},
		// A running task whose worker was lost too many times is parked.
		{domain.TaskRunning, domain.TaskDeadLetter},
		// A retry policy revives a failed attempt.
		{domain.TaskFailed, domain.TaskScheduled},
		// An exhausted or permanently failed attempt is parked.
		{domain.TaskFailed, domain.TaskDeadLetter},
		// Operator replay returns a parked task to the queue.
		{domain.TaskDeadLetter, domain.TaskScheduled},
		{domain.TaskDeadLetter, domain.TaskCanceled},
		// Reviving a failed run restores the tasks it had canceled.
		{domain.TaskCanceled, domain.TaskPending},
	}
	for _, tc := range legal {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			require.NoError(t, domain.TransitionTask(tc.from, tc.to))
		})
	}

	illegal := []struct{ from, to domain.TaskState }{
		{domain.TaskPending, domain.TaskRunning},
		{domain.TaskPending, domain.TaskCompleted},
		{domain.TaskScheduled, domain.TaskCompleted},
		{domain.TaskCompleted, domain.TaskRunning},
		{domain.TaskCompleted, domain.TaskScheduled},
		{domain.TaskCompleted, domain.TaskFailed},
		{domain.TaskFailed, domain.TaskCompleted},
		// A canceled task is restored to PENDING, never straight back onto the
		// queue: the engine must re-check its dependencies first.
		{domain.TaskCanceled, domain.TaskScheduled},
		{domain.TaskCanceled, domain.TaskRunning},
		// A parked task cannot skip the queue into RUNNING.
		{domain.TaskDeadLetter, domain.TaskRunning},
		{domain.TaskDeadLetter, domain.TaskCompleted},
	}
	for _, tc := range illegal {
		t.Run("reject "+string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			err := domain.TransitionTask(tc.from, tc.to)
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrInvalidStateTransition)
		})
	}
}

func TestTaskTerminalStates(t *testing.T) {
	require.True(t, domain.TaskCompleted.IsTerminal())
	require.True(t, domain.TaskCanceled.IsTerminal())
	// DEAD_LETTER is terminal to the engine: that is what stops a hopeless task
	// from retrying forever. Replay is an operator action, not an engine one.
	require.True(t, domain.TaskDeadLetter.IsTerminal(),
		"the engine must not act on a dead-lettered task by itself")

	// FAILED is deliberately not terminal: the retry pass may revive it, so the
	// state machine must keep that door open.
	require.False(t, domain.TaskFailed.IsTerminal(),
		"FAILED must stay revivable so a retry policy can reschedule it")
	require.True(t, domain.TaskFailed.IsRetryable())
	require.False(t, domain.TaskPending.IsTerminal())
	require.False(t, domain.TaskScheduled.IsTerminal())
	require.False(t, domain.TaskRunning.IsTerminal())

	for _, s := range []domain.TaskState{
		domain.TaskPending, domain.TaskScheduled, domain.TaskRunning,
		domain.TaskCompleted, domain.TaskCanceled, domain.TaskDeadLetter,
	} {
		require.False(t, s.IsRetryable(), "%s must not be a retry candidate", s)
	}
}

func TestTaskHasAttemptsLeft(t *testing.T) {
	require.True(t, (&domain.Task{Attempt: 0, MaxAttempts: 1}).HasAttemptsLeft(),
		"a task that has never run has attempts left")
	require.False(t, (&domain.Task{Attempt: 1, MaxAttempts: 1}).HasAttemptsLeft(),
		"a single-attempt task is exhausted after one try")
	require.True(t, (&domain.Task{Attempt: 1, MaxAttempts: 3}).HasAttemptsLeft())
	require.False(t, (&domain.Task{Attempt: 3, MaxAttempts: 3}).HasAttemptsLeft())
}

// TestEveryStateIsReachableAndAccountedFor guards against adding a state to one
// map and forgetting the other, which would make transitions silently invalid.
func TestEveryStateIsAccountedFor(t *testing.T) {
	workflowStates := []domain.WorkflowState{
		domain.WorkflowPending, domain.WorkflowRunning,
		domain.WorkflowCompleted, domain.WorkflowFailed, domain.WorkflowCanceled,
	}
	require.Len(t, domain.ValidWorkflowTransitions, len(workflowStates))
	for _, s := range workflowStates {
		require.True(t, s.Valid(), "%s must appear in ValidWorkflowTransitions", s)
	}

	taskStates := []domain.TaskState{
		domain.TaskPending, domain.TaskScheduled, domain.TaskRunning,
		domain.TaskCompleted, domain.TaskFailed, domain.TaskDeadLetter, domain.TaskCanceled,
	}
	require.Len(t, domain.ValidTaskTransitions, len(taskStates))
	for _, s := range taskStates {
		require.True(t, s.Valid(), "%s must appear in ValidTaskTransitions", s)
	}
}
