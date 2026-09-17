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
			Name:           t.Name,
			Activity:       t.Activity,
			DependsOn:      t.DependsOn,
			Input:          t.Input,
			MaxAttempts:    t.MaxAttempts,
			TimeoutSeconds: t.TimeoutSeconds,
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
