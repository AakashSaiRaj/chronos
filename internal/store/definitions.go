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

const definitionColumns = `id, name, version, description, task_queue, spec, spec_hash, created_at`

// RegisterDefinition durably registers a workflow version.
//
// Registration is idempotent: re-submitting a byte-identical spec returns the
// existing row with created=false. Submitting a *different* spec for a version
// that already exists is a conflict, because executions already reference that
// version and silently changing it would make persisted history unreplayable.
func (s *Store) RegisterDefinition(ctx context.Context, spec domain.WorkflowSpec) (def *domain.WorkflowDefinition, created bool, err error) {
	spec.Normalize()
	if err := spec.Validate(); err != nil {
		return nil, false, err
	}
	hash, err := spec.Hash()
	if err != nil {
		return nil, false, fmt.Errorf("hash spec: %w", err)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, false, fmt.Errorf("marshal spec: %w", err)
	}

	row := s.db.QueryRow(ctx, `
		INSERT INTO workflow_definitions (id, name, version, description, task_queue, spec, spec_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (name, version) DO NOTHING
		RETURNING `+definitionColumns,
		uuid.New(), spec.Name, spec.Version, spec.Description, spec.TaskQueue, specJSON, hash)

	inserted, err := scanDefinition(row)
	if err == nil {
		return inserted, true, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, false, translateError(err, "register workflow definition")
	}

	// The version already exists. Decide between idempotent replay and conflict.
	existing, err := s.GetDefinition(ctx, spec.Name, spec.Version)
	if err != nil {
		return nil, false, err
	}
	if existing.SpecHash != hash {
		return nil, false, fmt.Errorf(
			"%w: workflow %s version %d already exists with a different definition; "+
				"workflow versions are immutable, register a new version instead",
			domain.ErrConflict, spec.Name, spec.Version)
	}
	return existing, false, nil
}

// GetDefinition fetches an exact workflow version.
func (s *Store) GetDefinition(ctx context.Context, name string, version int) (*domain.WorkflowDefinition, error) {
	row := s.db.QueryRow(ctx,
		`SELECT `+definitionColumns+` FROM workflow_definitions WHERE name = $1 AND version = $2`,
		name, version)
	def, err := scanDefinition(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get workflow %s version %d", name, version))
	}
	return def, nil
}

// GetDefinitionByID fetches a workflow version by its primary key.
func (s *Store) GetDefinitionByID(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error) {
	row := s.db.QueryRow(ctx,
		`SELECT `+definitionColumns+` FROM workflow_definitions WHERE id = $1`, id)
	def, err := scanDefinition(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get workflow definition %s", id))
	}
	return def, nil
}

// GetLatestDefinition fetches the highest registered version of a workflow.
func (s *Store) GetLatestDefinition(ctx context.Context, name string) (*domain.WorkflowDefinition, error) {
	row := s.db.QueryRow(ctx,
		`SELECT `+definitionColumns+` FROM workflow_definitions
		 WHERE name = $1 ORDER BY version DESC LIMIT 1`, name)
	def, err := scanDefinition(row)
	if err != nil {
		return nil, translateError(err, fmt.Sprintf("get latest version of workflow %s", name))
	}
	return def, nil
}

// ListDefinitions returns registered workflow versions, newest first. When name
// is non-empty only that workflow's versions are returned.
func (s *Store) ListDefinitions(ctx context.Context, name string, limit int) ([]domain.WorkflowDefinition, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+definitionColumns+`
		FROM workflow_definitions
		WHERE ($1 = '' OR name = $1)
		ORDER BY name ASC, version DESC
		LIMIT $2`, name, limit)
	if err != nil {
		return nil, translateError(err, "list workflow definitions")
	}
	defer rows.Close()

	out := []domain.WorkflowDefinition{}
	for rows.Next() {
		def, err := scanDefinition(rows)
		if err != nil {
			return nil, translateError(err, "scan workflow definition")
		}
		out = append(out, *def)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate workflow definitions")
	}
	return out, nil
}

// scanner abstracts pgx.Row and pgx.Rows so one scan helper serves both.
type scanner interface {
	Scan(dest ...any) error
}

func scanDefinition(row scanner) (*domain.WorkflowDefinition, error) {
	var (
		def      domain.WorkflowDefinition
		specJSON []byte
		created  time.Time
	)
	if err := row.Scan(
		&def.ID, &def.Name, &def.Version, &def.Description,
		&def.TaskQueue, &specJSON, &def.SpecHash, &created,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, err
	}
	if err := json.Unmarshal(specJSON, &def.Spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec for %s v%d: %w", def.Name, def.Version, err)
	}
	def.CreatedAt = created
	return &def, nil
}
