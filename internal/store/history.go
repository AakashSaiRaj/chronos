package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// AppendEventParams describes one history entry.
type AppendEventParams struct {
	ExecutionID uuid.UUID
	EventType   domain.EventType
	TaskName    string
	Payload     any
}

// LockExecutionForWrite takes a blocking row lock on an execution.
//
// Only operations that must be serialized against the engine's scheduling pass
// need this — cancellation is the one in Phase 1, because it has to stop tasks
// the engine may simultaneously be trying to schedule. The high-volume worker
// paths (claim a task, report a result) deliberately do not take it, so tasks in
// the same workflow stay independently claimable.
func (s *Store) LockExecutionForWrite(ctx context.Context, executionID uuid.UUID) error {
	if _, inTx := s.db.(pgx.Tx); !inTx {
		return errors.New("LockExecutionForWrite must be called inside a transaction")
	}
	var one int
	err := s.db.QueryRow(ctx,
		`SELECT 1 FROM workflow_executions WHERE id = $1 FOR UPDATE`, executionID).Scan(&one)
	if err != nil {
		return translateError(err, fmt.Sprintf("lock execution %s for write", executionID))
	}
	return nil
}

// AppendEvent appends to an execution's immutable history.
//
// This is a pure append: the id is allocated by the sequence, so concurrent
// appends never contend and never need retrying. Events for one execution are
// read back in id order, which is monotonic in commit order.
func (s *Store) AppendEvent(ctx context.Context, p AppendEventParams) (*domain.HistoryEvent, error) {
	payload := json.RawMessage(`{}`)
	if p.Payload != nil {
		encoded, err := json.Marshal(p.Payload)
		if err != nil {
			return nil, fmt.Errorf("marshal history payload: %w", err)
		}
		payload = encoded
	}

	event := domain.HistoryEvent{
		ExecutionID: p.ExecutionID,
		EventType:   p.EventType,
		TaskName:    p.TaskName,
		Payload:     payload,
	}
	err := s.db.QueryRow(ctx, `
		INSERT INTO history_events (execution_id, event_type, task_name, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at`,
		p.ExecutionID, string(p.EventType), p.TaskName, []byte(payload),
	).Scan(&event.ID, &event.CreatedAt)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("append %s event", p.EventType))
	}
	return &event, nil
}

// ListHistory returns an execution's events in order. afterID is a cursor for
// tailing a long-running execution incrementally; pass 0 to read from the start.
func (s *Store) ListHistory(ctx context.Context, executionID uuid.UUID, afterID int64, limit int) ([]domain.HistoryEvent, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, execution_id, event_type, task_name, payload, created_at
		FROM history_events
		WHERE execution_id = $1 AND id > $2
		ORDER BY id ASC
		LIMIT $3`, executionID, afterID, limit)
	if err != nil {
		return nil, translateError(err, "list history events")
	}
	defer rows.Close()

	out := []domain.HistoryEvent{}
	for rows.Next() {
		var (
			e         domain.HistoryEvent
			eventType string
			payload   []byte
		)
		if err := rows.Scan(&e.ID, &e.ExecutionID, &eventType,
			&e.TaskName, &payload, &e.CreatedAt); err != nil {
			return nil, translateError(err, "scan history event")
		}
		e.EventType = domain.EventType(eventType)
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate history events")
	}
	return out, nil
}
