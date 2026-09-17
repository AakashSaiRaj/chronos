package api

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// Service is the control-plane behaviour the HTTP layer needs.
//
// It is declared here, at the point of use, rather than exported from the engine
// package. That keeps the API testable against a stub and documents exactly which
// engine operations the transport depends on. *engine.Service satisfies it.
type Service interface {
	Ping(ctx context.Context) error

	RegisterWorkflow(ctx context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error)
	GetWorkflow(ctx context.Context, name string, version int) (*domain.WorkflowDefinition, error)
	ListWorkflows(ctx context.Context, name string, limit int) ([]domain.WorkflowDefinition, error)

	StartExecution(ctx context.Context, req engine.StartExecutionRequest) (*domain.WorkflowExecution, bool, error)
	GetExecution(ctx context.Context, id uuid.UUID) (*engine.ExecutionDetail, error)
	ListExecutions(ctx context.Context, f store.ExecutionFilter) ([]domain.WorkflowExecution, error)
	GetHistory(ctx context.Context, id uuid.UUID, afterID int64, limit int) ([]domain.HistoryEvent, error)
	CancelExecution(ctx context.Context, id uuid.UUID, reason string) (*domain.WorkflowExecution, error)

	RegisterWorker(ctx context.Context, p store.RegisterWorkerParams) (*domain.Worker, error)
	Heartbeat(ctx context.Context, workerID uuid.UUID) (*domain.Worker, error)
	ListWorkers(ctx context.Context, taskQueue string, limit int) ([]domain.Worker, error)

	PollTask(ctx context.Context, req engine.PollRequest) (*domain.Task, error)
	CompleteTask(ctx context.Context, taskID, claimToken uuid.UUID, output json.RawMessage) (*domain.Task, error)
	FailTask(ctx context.Context, taskID, claimToken uuid.UUID, failure string) (*domain.Task, error)
}

// Compile-time proof that the engine implementation satisfies the transport's
// contract; a drift in either direction fails the build rather than a test.
var _ Service = (*engine.Service)(nil)
