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

	// RetryPolicy is the *effective* policy resolved at materialization time and
	// stored on the row, so a task in flight keeps the policy it started with
	// even if the engine's defaults change underneath it.
	RetryPolicy ResolvedRetryPolicy `json:"retryPolicy"`
	// Retryable is false when a worker declared its failure permanent, which
	// sends the task straight to the dead letter queue with attempts to spare.
	Retryable bool `json:"retryable"`
	// CancelRequested is the cooperative-cancellation flag a worker observes when
	// it renews its lease.
	CancelRequested bool `json:"cancelRequested,omitempty"`
	// DeadLetteredAt records when the task was parked for operator attention.
	DeadLetteredAt *time.Time `json:"deadLetteredAt,omitempty"`
	// LeaseExpiryCount counts how many times a lease on this task lapsed. A
	// non-zero value is the signal that work was redone because a worker
	// vanished, not because the activity itself failed.
	LeaseExpiryCount int `json:"leaseExpiryCount,omitempty"`
	// MaxLeaseExpiries bounds LeaseExpiryCount. It is a separate budget from
	// MaxAttempts on purpose; see the migration for the reasoning.
	MaxLeaseExpiries int `json:"maxLeaseExpiries"`
	// LastFailureReason distinguishes an activity error from a lease expiry or a
	// timeout, which the plain error string cannot.
	LastFailureReason string `json:"lastFailureReason,omitempty"`
}

// HasAttemptsLeft reports whether the task has attempt budget remaining.
func (t *Task) HasAttemptsLeft() bool {
	return t.Attempt < t.MaxAttempts
}

// ShouldRetry reports whether a failed task is eligible for another attempt.
// A failure the worker declared permanent short-circuits the remaining budget,
// because retrying something that cannot succeed only delays the bad news.
func (t *Task) ShouldRetry() bool {
	return t.Retryable && t.HasAttemptsLeft()
}

// CanSurviveLeaseExpiry reports whether the task may be requeued after its lease
// lapsed. This consults the infrastructure budget, not the activity budget: a
// worker being killed is not the activity failing.
func (t *Task) CanSurviveLeaseExpiry() bool {
	return t.LeaseExpiryCount < t.MaxLeaseExpiries
}

// LeaseExpired reports whether the task's lease has lapsed as of now.
func (t *Task) LeaseExpired(now time.Time) bool {
	return t.State == TaskRunning && t.LeaseExpiresAt != nil && t.LeaseExpiresAt.Before(now)
}

// FailureReason categorizes why an attempt did not succeed.
type FailureReason string

const (
	// FailureActivityError means the activity itself returned an error.
	FailureActivityError FailureReason = "ACTIVITY_ERROR"
	// FailureTimeout means the attempt exceeded its per-attempt budget.
	FailureTimeout FailureReason = "TIMEOUT"
	// FailureLeaseExpired means the lease lapsed without a report, so the worker
	// is presumed lost.
	FailureLeaseExpired FailureReason = "LEASE_EXPIRED"
	// FailureWorkerDead means heartbeat detection declared the holder dead.
	FailureWorkerDead FailureReason = "WORKER_DEAD"
)

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

	// Phase 2 reliability events. Together with the task rows they explain every
	// reason a task ran more than once, which is the question an operator asks
	// first when a workflow misbehaves.
	EventTaskRetryScheduled  EventType = "TASK_RETRY_SCHEDULED"
	EventTaskRequeued        EventType = "TASK_REQUEUED"
	EventTaskLeaseExpired    EventType = "TASK_LEASE_EXPIRED"
	EventTaskLeaseRenewed    EventType = "TASK_LEASE_RENEWED"
	EventTaskDeadLettered    EventType = "TASK_DEAD_LETTERED"
	EventTaskReplayed        EventType = "TASK_REPLAYED"
	EventTaskCancelRequested EventType = "TASK_CANCEL_REQUESTED"
	EventWorkerDeclaredDead  EventType = "WORKER_DECLARED_DEAD"
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
	// DeclaredDeadAt is set by heartbeat-based failure detection. It is cleared
	// if the worker comes back, since re-registering or heartbeating is itself
	// proof of life.
	DeclaredDeadAt *time.Time `json:"declaredDeadAt,omitempty"`
}

// IsStale reports whether the worker's heartbeat has lapsed by more than
// threshold as of now.
func (w *Worker) IsStale(now time.Time, threshold time.Duration) bool {
	return w.State == WorkerActive && now.Sub(w.LastHeartbeatAt) > threshold
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
