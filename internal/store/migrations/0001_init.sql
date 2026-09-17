-- Chronos Phase 1 schema.
--
-- Design notes:
--  * Durable state is the single source of truth. No engine or worker keeps
--    run state in memory, so any replica can resume any execution after a
--    restart purely by reading these tables.
--  * The tasks table doubles as the task queue. Claiming uses
--    SELECT ... FOR UPDATE SKIP LOCKED, which gives concurrent workers
--    non-blocking, at-most-one-claim semantics without a separate broker.
--  * Uniqueness constraints, not application checks, enforce idempotency.

CREATE TABLE workflow_definitions (
    id          UUID        PRIMARY KEY,
    name        TEXT        NOT NULL,
    version     INTEGER     NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    task_queue  TEXT        NOT NULL,
    spec        JSONB       NOT NULL,
    -- Fingerprint of the canonical spec. Re-registering an identical
    -- (name, version, spec_hash) is a no-op; a different hash for the same
    -- (name, version) is rejected, which makes versions immutable.
    spec_hash   TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_definitions_name_version_key UNIQUE (name, version),
    CONSTRAINT workflow_definitions_version_positive CHECK (version >= 1)
);

CREATE INDEX workflow_definitions_name_idx ON workflow_definitions (name, version DESC);

CREATE TABLE workflow_executions (
    id               UUID        PRIMARY KEY,
    definition_id    UUID        NOT NULL REFERENCES workflow_definitions (id),
    workflow_name    TEXT        NOT NULL,
    workflow_version INTEGER     NOT NULL,
    state            TEXT        NOT NULL,
    task_queue       TEXT        NOT NULL,
    input            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    output           JSONB,
    error            TEXT        NOT NULL DEFAULT '',
    -- Caller-supplied dedup key for start-execution. Scoped per workflow name
    -- so two different workflows may reuse the same key.
    idempotency_key  TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    CONSTRAINT workflow_executions_state_check
        CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELED'))
);

CREATE UNIQUE INDEX workflow_executions_idempotency_key_idx
    ON workflow_executions (workflow_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Partial index over live executions: this is the engine's hot scan path, and
-- it stays small even when the completed-execution history grows large.
CREATE INDEX workflow_executions_active_idx
    ON workflow_executions (state, created_at)
    WHERE state IN ('PENDING', 'RUNNING');

CREATE INDEX workflow_executions_listing_idx
    ON workflow_executions (created_at DESC);

CREATE TABLE tasks (
    id               UUID        PRIMARY KEY,
    execution_id     UUID        NOT NULL REFERENCES workflow_executions (id) ON DELETE CASCADE,
    name             TEXT        NOT NULL,
    activity         TEXT        NOT NULL,
    state            TEXT        NOT NULL,
    depends_on       TEXT[]      NOT NULL DEFAULT '{}',
    input            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    output           JSONB,
    error            TEXT        NOT NULL DEFAULT '',
    attempt          INTEGER     NOT NULL DEFAULT 0,
    max_attempts     INTEGER     NOT NULL DEFAULT 1,
    timeout_seconds  INTEGER     NOT NULL DEFAULT 0,
    task_queue       TEXT        NOT NULL,
    -- Minted on each claim; a worker must echo it back to report a result.
    claim_token      UUID,
    worker_id        TEXT        NOT NULL DEFAULT '',
    lease_expires_at TIMESTAMPTZ,
    scheduled_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    -- Makes task materialization idempotent: the engine can retry
    -- materialization after a crash mid-transaction without duplicating tasks.
    CONSTRAINT tasks_execution_name_key UNIQUE (execution_id, name),
    CONSTRAINT tasks_state_check
        CHECK (state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELED')),
    CONSTRAINT tasks_attempt_bounds CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT tasks_max_attempts_positive CHECK (max_attempts >= 1)
);

-- The queue index. Claim ordering is (scheduled_at, created_at) within a
-- (task_queue, activity) pair; the partial predicate keeps the index tight.
CREATE INDEX tasks_claimable_idx
    ON tasks (task_queue, activity, scheduled_at, created_at)
    WHERE state = 'SCHEDULED';

CREATE INDEX tasks_execution_idx ON tasks (execution_id, name);

-- Supports Phase 2 lease-expiry sweeps without another migration.
CREATE INDEX tasks_lease_idx
    ON tasks (lease_expires_at)
    WHERE state = 'RUNNING';

-- Append-only audit trail of how an execution reached its current state.
--
-- Ordering comes from the BIGSERIAL id, which is monotonic (though not
-- contiguous) within an execution. A dense per-execution counter was considered
-- and rejected: assigning it requires reading the current maximum, which turns
-- every append into a read-modify-write that concurrent writers must serialize
-- on. That would have made claiming two tasks of the same workflow mutually
-- blocking. Ordering is all the audit trail needs, and a plain append gives it
-- with no coordination at all.
--
-- Note the engine never reads history to make decisions; authoritative state
-- lives in workflow_executions and tasks. History exists for operators.
CREATE TABLE history_events (
    id           BIGSERIAL   PRIMARY KEY,
    execution_id UUID        NOT NULL REFERENCES workflow_executions (id) ON DELETE CASCADE,
    event_type   TEXT        NOT NULL,
    task_name    TEXT        NOT NULL DEFAULT '',
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports both "read an execution's history" and cursor-based tailing.
CREATE INDEX history_events_execution_idx ON history_events (execution_id, id);

CREATE TABLE workers (
    id                UUID        PRIMARY KEY,
    name              TEXT        NOT NULL,
    task_queue        TEXT        NOT NULL,
    activities        TEXT[]      NOT NULL DEFAULT '{}',
    state             TEXT        NOT NULL DEFAULT 'ACTIVE',
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Re-registration by the same logical worker name is an upsert, so a
    -- restarted worker reuses its identity instead of leaking rows.
    CONSTRAINT workers_name_key UNIQUE (name),
    CONSTRAINT workers_state_check CHECK (state IN ('ACTIVE', 'DEAD'))
);

CREATE INDEX workers_heartbeat_idx ON workers (task_queue, last_heartbeat_at)
    WHERE state = 'ACTIVE';
