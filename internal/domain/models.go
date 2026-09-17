package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// WorkflowDefinition is a persisted, immutable workflow version.
type WorkflowDefinition struct {
	ID          uuid.UUID    `json:"id"`
	Name        string       `json:"name"`
	Version     int          `json:"version"`
	Description string       `json:"description"`
	TaskQueue   string       `json:"taskQueue"`
	Spec        WorkflowSpec `json:"spec"`
	SpecHash    string       `json:"specHash"`
	CreatedAt   time.Time    `json:"createdAt"`
}

// WorkflowExecution is a single durable run of a WorkflowDefinition.
//
// Every field the engine needs to decide what to do next lives here or in the
// execution's tasks. The engine holds no in-memory run state, so any replica
// can pick up any execution after a restart.
type WorkflowExecution struct {
	ID              uuid.UUID       `json:"id"`
	DefinitionID    uuid.UUID       `json:"definitionId"`
	WorkflowName    string          `json:"workflowName"`
	WorkflowVersion int             `json:"workflowVersion"`
	State           WorkflowState   `json:"state"`
	TaskQueue       string          `json:"taskQueue"`
	Input           json.RawMessage `json:"input"`
	Output          json.RawMessage `json:"output,omitempty"`
	Error           string          `json:"error,omitempty"`
	IdempotencyKey  string          `json:"idempotencyKey,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	StartedAt       *time.Time      `json:"startedAt,omitempty"`
	CompletedAt     *time.Time      `json:"completedAt,omitempty"`
}

// Task is a single durable unit of work belonging to an execution.
type Task struct {
	ID             uuid.UUID       `json:"id"`
	ExecutionID    uuid.UUID       `json:"executionId"`
	Name           string          `json:"name"`
	Activity       string          `json:"activity"`
	State          TaskState       `json:"state"`
	DependsOn      []string        `json:"dependsOn,omitempty"`
	Input          json.RawMessage `json:"input"`
	Output         json.RawMessage `json:"output,omitempty"`
	Error          string          `json:"error,omitempty"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"maxAttempts"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	TaskQueue      string          `json:"taskQueue"`
	// ClaimToken is minted on every successful claim. A worker must present it
	// to report a result, which is what stops a superseded worker from writing
	// a result for a task that has since been reassigned.
	ClaimToken     *uuid.UUID `json:"-"`
	WorkerID       string     `json:"workerId,omitempty"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	ScheduledAt    *time.Time `json:"scheduledAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	StartedAt      *time.Time `json:"startedAt,omitempty"`
	CompletedAt    *time.Time `json:"completedAt,omitempty"`
}

// HasAttemptsLeft reports whether the task may be retried.
func (t *Task) HasAttemptsLeft() bool {
	return t.Attempt < t.MaxAttempts
}

// EventType enumerates append-only execution history events.
type EventType string

const (
	EventWorkflowStarted   EventType = "WORKFLOW_STARTED"
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
	EventWorkflowCanceled  EventType = "WORKFLOW_CANCELED"

	EventTaskCreated   EventType = "TASK_CREATED"
	EventTaskScheduled EventType = "TASK_SCHEDULED"
	EventTaskStarted   EventType = "TASK_STARTED"
	EventTaskCompleted EventType = "TASK_COMPLETED"
	EventTaskFailed    EventType = "TASK_FAILED"
	EventTaskCanceled  EventType = "TASK_CANCELED"
	EventTaskRequeued  EventType = "TASK_REQUEUED"
)

// HistoryEvent is one immutable entry in an execution's history: the audit trail
// of how an execution reached its current state.
//
// ID doubles as the ordering key and the tailing cursor. It is monotonic within
// an execution but not contiguous, because it is allocated by a database
// sequence — the tradeoff that keeps appends contention-free.
type HistoryEvent struct {
	ID          int64           `json:"id"`
	ExecutionID uuid.UUID       `json:"executionId"`
	EventType   EventType       `json:"eventType"`
	TaskName    string          `json:"taskName,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
}

// WorkerState is the lifecycle state of a registered worker.
type WorkerState string

const (
	WorkerActive WorkerState = "ACTIVE"
	// WorkerDead is set by Phase 2 failure detection when heartbeats lapse.
	WorkerDead WorkerState = "DEAD"
)

// Worker is a registered task executor.
type Worker struct {
	ID              uuid.UUID   `json:"id"`
	Name            string      `json:"name"`
	TaskQueue       string      `json:"taskQueue"`
	Activities      []string    `json:"activities"`
	State           WorkerState `json:"state"`
	RegisteredAt    time.Time   `json:"registeredAt"`
	LastHeartbeatAt time.Time   `json:"lastHeartbeatAt"`
}

// TaskInput is the self-contained payload handed to a worker. It is computed
// and persisted at schedule time so a worker never needs to read upstream state
// itself, and so a replayed claim always sees byte-identical input.
type TaskInput struct {
	// WorkflowInput is the input the execution was started with.
	WorkflowInput json.RawMessage `json:"workflowInput,omitempty"`
	// TaskInput is the static input declared on the task spec.
	TaskInput json.RawMessage `json:"taskInput,omitempty"`
	// Upstream maps each direct dependency's task name to its output.
	Upstream map[string]json.RawMessage `json:"upstream,omitempty"`
}
