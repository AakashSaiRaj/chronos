package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// ---------------------------------------------------------------------------
// Workflow definitions
// ---------------------------------------------------------------------------

// RegisterWorkflowRequest is the body of POST /v1/workflows.
type RegisterWorkflowRequest struct {
	Name        string            `json:"name"`
	Version     int               `json:"version"`
	Description string            `json:"description,omitempty"`
	TaskQueue   string            `json:"taskQueue,omitempty"`
	Tasks       []TaskSpecRequest `json:"tasks"`
}

// TaskSpecRequest is one node of the submitted task graph.
type TaskSpecRequest struct {
	Name           string          `json:"name"`
	Activity       string          `json:"activity"`
	DependsOn      []string        `json:"dependsOn,omitempty"`
	Input          json.RawMessage `json:"input,omitempty"`
	MaxAttempts    int             `json:"maxAttempts,omitempty"`
	TimeoutSeconds int             `json:"timeoutSeconds,omitempty"`
	// RetryPolicy controls the delay between attempts. Omit for the defaults.
	RetryPolicy *domain.RetryPolicy `json:"retryPolicy,omitempty"`
	// MaxLeaseExpiries bounds how many times this task may be requeued because
	// the worker holding it vanished, as distinct from the activity failing.
	MaxLeaseExpiries int `json:"maxLeaseExpiries,omitempty"`
}

// toDomain converts the wire shape into the domain spec.
func (r RegisterWorkflowRequest) toDomain() domain.WorkflowSpec {
	spec := domain.WorkflowSpec{
		Name:        r.Name,
		Version:     r.Version,
		Description: r.Description,
		TaskQueue:   r.TaskQueue,
		Tasks:       make([]domain.TaskSpec, 0, len(r.Tasks)),
	}
	for _, t := range r.Tasks {
		spec.Tasks = append(spec.Tasks, domain.TaskSpec{
			Name:             t.Name,
			Activity:         t.Activity,
			DependsOn:        t.DependsOn,
			Input:            t.Input,
			MaxAttempts:      t.MaxAttempts,
			TimeoutSeconds:   t.TimeoutSeconds,
			RetryPolicy:      t.RetryPolicy,
			MaxLeaseExpiries: t.MaxLeaseExpiries,
		})
	}
	return spec
}

// WorkflowResponse is a registered workflow version.
type WorkflowResponse struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	Version     int               `json:"version"`
	Description string            `json:"description,omitempty"`
	TaskQueue   string            `json:"taskQueue"`
	Tasks       []domain.TaskSpec `json:"tasks"`
	SpecHash    string            `json:"specHash"`
	CreatedAt   time.Time         `json:"createdAt"`
}

func newWorkflowResponse(d *domain.WorkflowDefinition) WorkflowResponse {
	return WorkflowResponse{
		ID:          d.ID,
		Name:        d.Name,
		Version:     d.Version,
		Description: d.Description,
		TaskQueue:   d.TaskQueue,
		Tasks:       d.Spec.Tasks,
		SpecHash:    d.SpecHash,
		CreatedAt:   d.CreatedAt,
	}
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------

// StartExecutionRequest is the body of POST /v1/executions.
type StartExecutionRequest struct {
	WorkflowName string `json:"workflowName"`
	// WorkflowVersion pins a version; omit or use 0 for the latest registered.
	WorkflowVersion int             `json:"workflowVersion,omitempty"`
	Input           json.RawMessage `json:"input,omitempty"`
	// IdempotencyKey may also be supplied via the Idempotency-Key header, which
	// takes precedence when both are present.
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// ExecutionResponse is a workflow execution, optionally with its tasks.
type ExecutionResponse struct {
	ID              uuid.UUID            `json:"id"`
	WorkflowName    string               `json:"workflowName"`
	WorkflowVersion int                  `json:"workflowVersion"`
	State           domain.WorkflowState `json:"state"`
	TaskQueue       string               `json:"taskQueue"`
	Input           json.RawMessage      `json:"input"`
	Output          json.RawMessage      `json:"output,omitempty"`
	Error           string               `json:"error,omitempty"`
	IdempotencyKey  string               `json:"idempotencyKey,omitempty"`
	CreatedAt       time.Time            `json:"createdAt"`
	UpdatedAt       time.Time            `json:"updatedAt"`
	StartedAt       *time.Time           `json:"startedAt,omitempty"`
	CompletedAt     *time.Time           `json:"completedAt,omitempty"`
	Tasks           []TaskResponse       `json:"tasks,omitempty"`
}

func newExecutionResponse(e *domain.WorkflowExecution, tasks []domain.Task) ExecutionResponse {
	resp := ExecutionResponse{
		ID:              e.ID,
		WorkflowName:    e.WorkflowName,
		WorkflowVersion: e.WorkflowVersion,
		State:           e.State,
		TaskQueue:       e.TaskQueue,
		Input:           e.Input,
		Output:          e.Output,
		Error:           e.Error,
		IdempotencyKey:  e.IdempotencyKey,
		CreatedAt:       e.CreatedAt,
		UpdatedAt:       e.UpdatedAt,
		StartedAt:       e.StartedAt,
		CompletedAt:     e.CompletedAt,
	}
	if len(tasks) > 0 {
		resp.Tasks = make([]TaskResponse, 0, len(tasks))
		for i := range tasks {
			resp.Tasks = append(resp.Tasks, newTaskResponse(&tasks[i], false))
		}
	}
	return resp
}

// CancelExecutionRequest is the body of POST /v1/executions/{id}/cancel.
type CancelExecutionRequest struct {
	Reason string `json:"reason,omitempty"`
}

// HistoryResponse is an execution's append-only history.
type HistoryResponse struct {
	ExecutionID uuid.UUID             `json:"executionId"`
	Events      []domain.HistoryEvent `json:"events"`
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

// TaskResponse describes a task. ClaimToken is only populated on the poll
// response, where the worker needs it to report a result.
type TaskResponse struct {
	ID          uuid.UUID        `json:"id"`
	ExecutionID uuid.UUID        `json:"executionId"`
	Name        string           `json:"name"`
	Activity    string           `json:"activity"`
	State       domain.TaskState `json:"state"`
	DependsOn   []string         `json:"dependsOn,omitempty"`
	Input       json.RawMessage  `json:"input"`
	Output      json.RawMessage  `json:"output,omitempty"`
	Error       string           `json:"error,omitempty"`
	Attempt     int              `json:"attempt"`
	MaxAttempts int              `json:"maxAttempts"`
	// TimeoutSeconds is the per-attempt budget the worker should honour.
	TimeoutSeconds int        `json:"timeoutSeconds,omitempty"`
	TaskQueue      string     `json:"taskQueue"`
	WorkerID       string     `json:"workerId,omitempty"`
	ClaimToken     string     `json:"claimToken,omitempty"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	ScheduledAt    *time.Time `json:"scheduledAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	StartedAt      *time.Time `json:"startedAt,omitempty"`
	CompletedAt    *time.Time `json:"completedAt,omitempty"`

	// Reliability fields.
	Retryable         bool       `json:"retryable"`
	CancelRequested   bool       `json:"cancelRequested,omitempty"`
	DeadLetteredAt    *time.Time `json:"deadLetteredAt,omitempty"`
	LeaseExpiryCount  int        `json:"leaseExpiryCount,omitempty"`
	MaxLeaseExpiries  int        `json:"maxLeaseExpiries,omitempty"`
	LastFailureReason string     `json:"lastFailureReason,omitempty"`
}

func newTaskResponse(t *domain.Task, includeClaimToken bool) TaskResponse {
	resp := TaskResponse{
		ID:             t.ID,
		ExecutionID:    t.ExecutionID,
		Name:           t.Name,
		Activity:       t.Activity,
		State:          t.State,
		DependsOn:      t.DependsOn,
		Input:          t.Input,
		Output:         t.Output,
		Error:          t.Error,
		Attempt:        t.Attempt,
		MaxAttempts:    t.MaxAttempts,
		TimeoutSeconds: t.TimeoutSeconds,
		TaskQueue:      t.TaskQueue,
		WorkerID:       t.WorkerID,
		LeaseExpiresAt: t.LeaseExpiresAt,
		ScheduledAt:    t.ScheduledAt,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
		StartedAt:      t.StartedAt,
		CompletedAt:    t.CompletedAt,

		Retryable:         t.Retryable,
		CancelRequested:   t.CancelRequested,
		DeadLetteredAt:    t.DeadLetteredAt,
		LeaseExpiryCount:  t.LeaseExpiryCount,
		MaxLeaseExpiries:  t.MaxLeaseExpiries,
		LastFailureReason: t.LastFailureReason,
	}
	if includeClaimToken && t.ClaimToken != nil {
		resp.ClaimToken = t.ClaimToken.String()
	}
	return resp
}

// PollTaskRequest is the body of POST /v1/tasks/poll.
type PollTaskRequest struct {
	WorkerID   string   `json:"workerId"`
	TaskQueue  string   `json:"taskQueue,omitempty"`
	Activities []string `json:"activities,omitempty"`
	// LeaseSeconds is how long the worker asks to hold the task.
	LeaseSeconds int `json:"leaseSeconds,omitempty"`
}

// CompleteTaskRequest is the body of POST /v1/tasks/{id}/complete.
type CompleteTaskRequest struct {
	ClaimToken string          `json:"claimToken"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// FailTaskRequest is the body of POST /v1/tasks/{id}/fail.
type FailTaskRequest struct {
	ClaimToken string `json:"claimToken"`
	Error      string `json:"error"`
	// Retryable, when explicitly false, sends the task straight to the dead
	// letter queue instead of burning its remaining attempts on a failure the
	// worker already knows is permanent. A pointer so that omitting it means
	// "retryable" rather than "permanent", which is the safer default.
	Retryable *bool `json:"retryable,omitempty"`
	// Reason categorizes the failure, e.g. TIMEOUT. Defaults to ACTIVITY_ERROR.
	Reason string `json:"reason,omitempty"`
}

// HeartbeatTaskRequest is the body of POST /v1/tasks/{id}/heartbeat.
type HeartbeatTaskRequest struct {
	ClaimToken string `json:"claimToken"`
	// LeaseSeconds is how much further the worker asks to hold the task.
	LeaseSeconds int `json:"leaseSeconds,omitempty"`
}

// HeartbeatTaskResponse tells a worker whether to keep going.
type HeartbeatTaskResponse struct {
	TaskID uuid.UUID `json:"taskId"`
	// CancelRequested is the cooperative-cancellation signal. A worker that sees
	// it should abandon the task; its result will be rejected regardless.
	CancelRequested bool       `json:"cancelRequested"`
	LeaseExpiresAt  *time.Time `json:"leaseExpiresAt,omitempty"`
	Attempt         int        `json:"attempt"`
}

// ReplayTaskRequest is the body of POST /v1/tasks/{id}/replay.
type ReplayTaskRequest struct {
	// ExtraAttempts is how much attempt budget the replay grants. Defaults to 1.
	ExtraAttempts int `json:"extraAttempts,omitempty"`
}

// DeadLetterResponse is the operator's view of the dead letter queue.
type DeadLetterResponse struct {
	Items []DeadLetterItem `json:"items"`
	Count int              `json:"count"`
	// Total is the size of the whole queue, not just this page, so an operator
	// can see the backlog without paging through it.
	Total int `json:"total"`
}

// DeadLetterItem is a parked task plus the context needed to triage it.
type DeadLetterItem struct {
	Task TaskResponse `json:"task"`
	// FailureReason distinguishes an activity error from a lost worker, which the
	// error string alone does not.
	FailureReason  string     `json:"failureReason,omitempty"`
	DeadLetteredAt *time.Time `json:"deadLetteredAt,omitempty"`
	// LeaseExpiryCount being non-zero means work was redone because workers
	// vanished, not because the activity kept failing.
	LeaseExpiryCount int `json:"leaseExpiryCount"`
}

func newDeadLetterItem(t *domain.Task) DeadLetterItem {
	return DeadLetterItem{
		Task:             newTaskResponse(t, false),
		FailureReason:    t.LastFailureReason,
		DeadLetteredAt:   t.DeadLetteredAt,
		LeaseExpiryCount: t.LeaseExpiryCount,
	}
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// RegisterWorkerRequest is the body of POST /v1/workers.
type RegisterWorkerRequest struct {
	Name       string   `json:"name"`
	TaskQueue  string   `json:"taskQueue,omitempty"`
	Activities []string `json:"activities,omitempty"`
}

// ---------------------------------------------------------------------------
// Envelopes
// ---------------------------------------------------------------------------

// listResponse wraps collections so the payload can grow (paging cursors,
// totals) without breaking clients that already parse it.
type listResponse[T any] struct {
	Items []T `json:"items"`
	Count int `json:"count"`
}

func newListResponse[T any](items []T) listResponse[T] {
	if items == nil {
		items = []T{}
	}
	return listResponse[T]{Items: items, Count: len(items)}
}

// healthResponse is returned by /healthz and /readyz.
type healthResponse struct {
	Status   string `json:"status"`
	Database string `json:"database,omitempty"`
	Detail   string `json:"detail,omitempty"`
}
