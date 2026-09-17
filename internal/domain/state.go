package domain

import "fmt"

// WorkflowState is the persisted lifecycle state of a single workflow execution.
//
// The state machine is deliberately small and explicit. Every transition the
// engine performs is validated against ValidWorkflowTransitions before it is
// written, so an illegal transition is a programming error that surfaces
// immediately rather than silently corrupting durable state.
type WorkflowState string

const (
	// WorkflowPending means the execution row is durably persisted but the
	// engine has not yet materialized its tasks.
	WorkflowPending WorkflowState = "PENDING"
	// WorkflowRunning means tasks have been materialized and are being
	// scheduled and executed.
	WorkflowRunning WorkflowState = "RUNNING"
	// WorkflowCompleted means every task reached COMPLETED.
	WorkflowCompleted WorkflowState = "COMPLETED"
	// WorkflowFailed means at least one task exhausted its attempts.
	WorkflowFailed WorkflowState = "FAILED"
	// WorkflowCanceled means an operator or client canceled the execution.
	WorkflowCanceled WorkflowState = "CANCELED"
)

// ValidWorkflowTransitions is the complete set of legal workflow transitions.
//
// FAILED -> RUNNING exists solely for operator-initiated replay of a
// dead-lettered task. The engine never makes that move on its own: a run it has
// given up on stays given up on unless a human asks for it back. COMPLETED and
// CANCELED remain absolutely terminal.
var ValidWorkflowTransitions = map[WorkflowState][]WorkflowState{
	WorkflowPending:   {WorkflowRunning, WorkflowCanceled, WorkflowFailed},
	WorkflowRunning:   {WorkflowCompleted, WorkflowFailed, WorkflowCanceled},
	WorkflowFailed:    {WorkflowRunning},
	WorkflowCompleted: {},
	WorkflowCanceled:  {},
}

// terminalWorkflowStates is the set the engine treats as "do not touch". FAILED
// is included even though replay can revive it, because only an explicit
// operator action may do so.
var terminalWorkflowStates = map[WorkflowState]struct{}{
	WorkflowCompleted: {},
	WorkflowFailed:    {},
	WorkflowCanceled:  {},
}

// IsTerminal reports whether the engine should consider the execution finished.
func (s WorkflowState) IsTerminal() bool {
	_, ok := terminalWorkflowStates[s]
	return ok
}

// Valid reports whether s is a known workflow state.
func (s WorkflowState) Valid() bool {
	_, ok := ValidWorkflowTransitions[s]
	return ok
}

// CanTransitionTo reports whether s -> next is legal.
func (s WorkflowState) CanTransitionTo(next WorkflowState) bool {
	for _, allowed := range ValidWorkflowTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// TransitionWorkflow validates a workflow state transition.
func TransitionWorkflow(from, to WorkflowState) error {
	if !from.Valid() {
		return fmt.Errorf("%w: unknown workflow state %q", ErrInvalidStateTransition, from)
	}
	if !to.Valid() {
		return fmt.Errorf("%w: unknown workflow state %q", ErrInvalidStateTransition, to)
	}
	if from == to {
		return fmt.Errorf("%w: workflow already in state %q", ErrInvalidStateTransition, from)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%w: workflow cannot move %s -> %s", ErrInvalidStateTransition, from, to)
	}
	return nil
}

// TaskState is the persisted lifecycle state of a single task within an execution.
type TaskState string

const (
	// TaskPending means the task exists durably but its dependencies are not
	// yet satisfied, so it has not been enqueued.
	TaskPending TaskState = "PENDING"
	// TaskScheduled means the task is enqueued and claimable by a worker.
	TaskScheduled TaskState = "SCHEDULED"
	// TaskRunning means a worker holds a lease on the task and is executing it.
	TaskRunning TaskState = "RUNNING"
	// TaskCompleted means a worker reported success.
	TaskCompleted TaskState = "COMPLETED"
	// TaskFailed means an attempt failed. It is not terminal: a retry policy
	// with attempts remaining will revive it.
	TaskFailed TaskState = "FAILED"
	// TaskDeadLetter means every attempt was used up (or the failure was declared
	// non-retryable) and the task has been parked for operator attention. It is
	// terminal to the engine but can be replayed by an explicit request.
	TaskDeadLetter TaskState = "DEAD_LETTER"
	// TaskCanceled means the task was canceled before finishing.
	TaskCanceled TaskState = "CANCELED"
)

// ValidTaskTransitions is the complete set of legal task transitions.
//
// Two Phase 2 paths converge on SCHEDULED. RUNNING -> SCHEDULED is a requeue,
// used when a lease expires or the worker holding it is declared dead.
// FAILED -> SCHEDULED is a retry, used when attempts remain. DEAD_LETTER ->
// SCHEDULED is an operator replay.
// CANCELED -> PENDING exists for the same reason as WorkflowFailed -> RUNNING:
// operator replay. A task is canceled as a *consequence* of its run stopping, not
// as a judgement about the task itself, so reviving it under an explicitly
// revived run is coherent. A canceled workflow, by contrast, stays canceled.
var ValidTaskTransitions = map[TaskState][]TaskState{
	TaskPending:    {TaskScheduled, TaskCanceled},
	TaskScheduled:  {TaskRunning, TaskCanceled},
	TaskRunning:    {TaskCompleted, TaskFailed, TaskScheduled, TaskDeadLetter, TaskCanceled},
	TaskFailed:     {TaskScheduled, TaskDeadLetter, TaskCanceled},
	TaskDeadLetter: {TaskScheduled, TaskCanceled},
	TaskCanceled:   {TaskPending},
	TaskCompleted:  {},
}

// Valid reports whether s is a known task state.
func (s TaskState) Valid() bool {
	_, ok := ValidTaskTransitions[s]
	return ok
}

// IsTerminal reports whether the engine should consider the task finished.
//
// DEAD_LETTER counts as terminal here: the engine will not act on it again, and
// only an explicit replay can move it. That is what stops a dead-lettered task
// from being retried forever while still leaving a remediation path open.
func (s TaskState) IsTerminal() bool {
	return s == TaskCompleted || s == TaskCanceled || s == TaskDeadLetter
}

// IsRetryable reports whether a task in this state is a candidate for the
// engine's retry pass.
func (s TaskState) IsRetryable() bool { return s == TaskFailed }

// CanTransitionTo reports whether s -> next is legal.
func (s TaskState) CanTransitionTo(next TaskState) bool {
	for _, allowed := range ValidTaskTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// TransitionTask validates a task state transition.
func TransitionTask(from, to TaskState) error {
	if !from.Valid() {
		return fmt.Errorf("%w: unknown task state %q", ErrInvalidStateTransition, from)
	}
	if !to.Valid() {
		return fmt.Errorf("%w: unknown task state %q", ErrInvalidStateTransition, to)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%w: task cannot move %s -> %s", ErrInvalidStateTransition, from, to)
	}
	return nil
}
