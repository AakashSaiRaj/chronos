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
var ValidWorkflowTransitions = map[WorkflowState][]WorkflowState{
	WorkflowPending:   {WorkflowRunning, WorkflowCanceled, WorkflowFailed},
	WorkflowRunning:   {WorkflowCompleted, WorkflowFailed, WorkflowCanceled},
	WorkflowCompleted: {},
	WorkflowFailed:    {},
	WorkflowCanceled:  {},
}

// IsTerminal reports whether the state admits no further transitions.
func (s WorkflowState) IsTerminal() bool {
	return len(ValidWorkflowTransitions[s]) == 0 && s.Valid()
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
	// TaskFailed means the task exhausted its attempts.
	TaskFailed TaskState = "FAILED"
	// TaskCanceled means the task was canceled before finishing.
	TaskCanceled TaskState = "CANCELED"
)

// ValidTaskTransitions is the complete set of legal task transitions.
//
// SCHEDULED -> SCHEDULED and RUNNING -> SCHEDULED are intentionally absent as
// self-transitions; requeue-after-lease-expiry and retry-with-backoff are
// Phase 2 concerns and are modeled there as RUNNING -> SCHEDULED via an
// explicit requeue operation. FAILED -> SCHEDULED is already permitted so a
// retry policy can revive a failed attempt without a schema change.
var ValidTaskTransitions = map[TaskState][]TaskState{
	TaskPending:   {TaskScheduled, TaskCanceled},
	TaskScheduled: {TaskRunning, TaskCanceled},
	TaskRunning:   {TaskCompleted, TaskFailed, TaskScheduled, TaskCanceled},
	TaskFailed:    {TaskScheduled},
	TaskCompleted: {},
	TaskCanceled:  {},
}

// Valid reports whether s is a known task state.
func (s TaskState) Valid() bool {
	_, ok := ValidTaskTransitions[s]
	return ok
}

// IsTerminal reports whether the task can never transition again.
func (s TaskState) IsTerminal() bool {
	return s == TaskCompleted || s == TaskCanceled
}

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
