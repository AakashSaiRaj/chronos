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
	}
	for _, tc := range legal {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			require.NoError(t, domain.TransitionWorkflow(tc.from, tc.to))
		})
	}

	illegal := []struct{ from, to domain.WorkflowState }{
		// Terminal states are terminal: nothing may resurrect a finished run.
		{domain.WorkflowCompleted, domain.WorkflowRunning},
		{domain.WorkflowCompleted, domain.WorkflowFailed},
		{domain.WorkflowFailed, domain.WorkflowRunning},
		{domain.WorkflowFailed, domain.WorkflowCompleted},
		{domain.WorkflowCanceled, domain.WorkflowRunning},
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
	terminal := []domain.WorkflowState{
		domain.WorkflowCompleted, domain.WorkflowFailed, domain.WorkflowCanceled,
	}
	for _, s := range terminal {
		require.True(t, s.IsTerminal(), "%s must be terminal", s)
	}

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
		// Lease expiry requeues a running task (Phase 2).
		{domain.TaskRunning, domain.TaskScheduled},
		// A retry policy revives a failed attempt (Phase 2).
		{domain.TaskFailed, domain.TaskScheduled},
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
		{domain.TaskCanceled, domain.TaskScheduled},
		{domain.TaskFailed, domain.TaskCompleted},
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

	// FAILED is deliberately not terminal: a retry policy may revive it, so the
	// state machine must keep that door open.
	require.False(t, domain.TaskFailed.IsTerminal(),
		"FAILED must stay revivable so a retry policy can reschedule it")
	require.False(t, domain.TaskPending.IsTerminal())
	require.False(t, domain.TaskScheduled.IsTerminal())
	require.False(t, domain.TaskRunning.IsTerminal())
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
		domain.TaskCompleted, domain.TaskFailed, domain.TaskCanceled,
	}
	require.Len(t, domain.ValidTaskTransitions, len(taskStates))
	for _, s := range taskStates {
		require.True(t, s.Valid(), "%s must appear in ValidTaskTransitions", s)
	}
}
