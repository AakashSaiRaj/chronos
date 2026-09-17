package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

const executionColumns = `id, definition_id, workflow_name, workflow_version, state, task_queue,
	input, output, error, idempotency_key, created_at, updated_at, started_at, completed_at`

// StartExecutionParams describes a requested workflow run.
type StartExecutionParams struct {
	Definition     *domain.WorkflowDefinition
	Input          json.RawMessage
	IdempotencyKey string
}

// CreateExecution durably records a new execution in PENDING.
//
// When an idempotency key is supplied, a partial unique index on
// (workflow_name, idempotency_key) makes the insert the arbiter of uniqueness:
// two concurrent identical requests race on the index and the loser reads back
// the winner's row. That is why dedup is correct even across replicas, where an
// application-level "check then insert" would not be.
func (s *Store) CreateExecution(ctx context.Context, p StartExecutionParams) (exec *domain.WorkflowExecution, created bool, err error) {
	if p.Definition == nil {
		return nil, false, fmt.Errorf("%w: definition is required", domain.ErrValidation)
	}
	input := p.Input
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if !json.Valid(input) {
		return nil, false, fmt.Errorf("%w: execution input is not valid JSON", domain.ErrValidation)
	}

	var keyArg any
	if p.IdempotencyKey != "" {
		keyArg = p.IdempotencyKey
	}

	row := s.db.QueryRow(ctx, `
		INSERT INTO workflow_executions
			(id, definition_id, workflow_name, workflow_version, state, task_queue, input, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (workflow_name, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING `+executionColumns,
		uuid.New(), p.Definition.ID, p.Definition.Name, p.Definition.Version,
		domain.WorkflowPending, p.Definition.TaskQueue, input, keyArg)

	inserted, err := scanExecution(row)
	if err == nil {
		return inserted, true, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, false, translateError(err, "create workflow execution")
	}
	if p.IdempotencyKey == "" {
		// No dedup key means DO NOTHING cannot have fired; a missing RETURNING
		// row here would be a real fault rather than a replay.
		return nil, false, fmt.Errorf("create workflow execution: insert returned no row")
	}

	existing, err := s.GetExecutionByIdempotencyKey(ctx, p.Definition.Name, p.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

// GetExecution fetches an execution by ID.
func (s *Store) GetExecution(ctx context.Context, id uuid.UUID) (*domain.WorkflowExecution, error) {
	row := s.db.QueryRow(ctx, `SELECT `+executionColumns+` FROM workflow_executions WHERE id = $1`, id)
	exec, err := scanExecution(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get execution %s", id))
	}
	return exec, nil
}

// GetExecutionByIdempotencyKey resolves a previously deduplicated start request.
func (s *Store) GetExecutionByIdempotencyKey(ctx context.Context, workflowName, key string) (*domain.WorkflowExecution, error) {
	row := s.db.QueryRow(ctx,
		`SELECT `+executionColumns+` FROM workflow_executions
		 WHERE workflow_name = $1 AND idempotency_key = $2`, workflowName, key)
	exec, err := scanExecution(row)
	if err != nil {
		return nil, translateError(err, "get execution by idempotency key")
	}
	return exec, nil
}

// LockExecution takes a row-level lock on an execution for the duration of the
// enclosing transaction, skipping the row if another replica already holds it.
//
// This is what lets several engine replicas run the same scheduling loop
// concurrently: each execution is processed by exactly one replica at a time,
// and contention degrades into "someone else has it, move on" rather than
// blocking. Returns domain.ErrNotFound when the row is locked elsewhere.
func (s *Store) LockExecution(ctx context.Context, id uuid.UUID) (*domain.WorkflowExecution, error) {
	if _, inTx := s.db.(pgx.Tx); !inTx {
		return nil, errors.New("LockExecution must be called inside a transaction")
	}
	row := s.db.QueryRow(ctx,
		`SELECT `+executionColumns+` FROM workflow_executions WHERE id = $1 FOR UPDATE SKIP LOCKED`, id)
	exec, err := scanExecution(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("lock execution %s", id))
	}
	return exec, nil
}

// ListActiveExecutionIDs returns IDs of executions the engine may still need to
// act on. It takes no locks so it stays cheap; per-execution locking happens in
// LockExecution once a replica decides to work on one.
func (s *Store) ListActiveExecutionIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT id FROM workflow_executions
		WHERE state IN ('PENDING', 'RUNNING')
		ORDER BY created_at ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, translateError(err, "list active executions")
	}
	defer rows.Close()

	out := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, translateError(err, "scan active execution id")
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate active executions")
	}
	return out, nil
}

// ExecutionFilter narrows an execution listing.
type ExecutionFilter struct {
	WorkflowName string
	State        domain.WorkflowState
	Limit        int
	Offset       int
}

// ListExecutions returns executions newest first.
func (s *Store) ListExecutions(ctx context.Context, f ExecutionFilter) ([]domain.WorkflowExecution, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+executionColumns+`
		FROM workflow_executions
		WHERE ($1 = '' OR workflow_name = $1)
		  AND ($2 = '' OR state = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`,
		f.WorkflowName, string(f.State), f.Limit, f.Offset)
	if err != nil {
		return nil, translateError(err, "list executions")
	}
	defer rows.Close()

	out := []domain.WorkflowExecution{}
	for rows.Next() {
		exec, err := scanExecution(rows)
		if err != nil {
			return nil, translateError(err, "scan execution")
		}
		out = append(out, *exec)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate executions")
	}
	return out, nil
}

// TransitionExecution performs a guarded workflow state change.
//
// The expected current state is part of the WHERE clause, making the update a
// compare-and-swap. If another replica moved the execution first, zero rows
// match and the caller learns it lost the race instead of clobbering the write.
func (s *Store) TransitionExecution(
	ctx context.Context,
	id uuid.UUID,
	from, to domain.WorkflowState,
	output json.RawMessage,
	failure string,
) (*domain.WorkflowExecution, error) {
	if err := domain.TransitionWorkflow(from, to); err != nil {
		return nil, err
	}

	var outputArg any
	if len(output) > 0 {
		if !json.Valid(output) {
			return nil, fmt.Errorf("%w: execution output is not valid JSON", domain.ErrValidation)
		}
		outputArg = []byte(output)
	}

	row := s.db.QueryRow(ctx, `
		UPDATE workflow_executions SET
			state        = $3,
			output       = COALESCE($4, output),
			error        = $5,
			updated_at   = now(),
			started_at   = CASE WHEN $3 = 'RUNNING' THEN COALESCE(started_at, now()) ELSE started_at END,
			completed_at = CASE WHEN $3 IN ('COMPLETED', 'FAILED', 'CANCELED') THEN now() ELSE completed_at END
		WHERE id = $1 AND state = $2
		RETURNING `+executionColumns,
		id, from, to, outputArg, failure)

	exec, err := scanExecution(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("%w: execution %s is no longer in state %s",
				domain.ErrInvalidStateTransition, id, from)
		}
		return nil, translateError(err, fmt.Sprintf("transition execution %s to %s", id, to))
	}
	return exec, nil
}

func scanExecution(row scanner) (*domain.WorkflowExecution, error) {
	var (
		exec      domain.WorkflowExecution
		input     []byte
		output    []byte
		idemKey   *string
		created   time.Time
		updated   time.Time
		started   *time.Time
		completed *time.Time
		state     string
	)
	if err := row.Scan(
		&exec.ID, &exec.DefinitionID, &exec.WorkflowName, &exec.WorkflowVersion,
		&state, &exec.TaskQueue, &input, &output, &exec.Error, &idemKey,
		&created, &updated, &started, &completed,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, err
	}

	exec.State = domain.WorkflowState(state)
	exec.Input = json.RawMessage(input)
	if len(output) > 0 {
		exec.Output = json.RawMessage(output)
	}
	if idemKey != nil {
		exec.IdempotencyKey = *idemKey
	}
	exec.CreatedAt = created
	exec.UpdatedAt = updated
	exec.StartedAt = started
	exec.CompletedAt = completed
	return &exec, nil
}
