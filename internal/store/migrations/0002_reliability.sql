-- Chronos Phase 2: reliability and distributed execution.
--
-- Adds the durable state behind retries, timeouts, dead-lettering, lease
-- reclamation, and cooperative cancellation. Everything here is additive with a
-- default, so an existing Phase 1 database migrates without downtime and
-- in-flight tasks keep working under the defaults.

-- ---------------------------------------------------------------------------
-- Effective retry policy, stored per task
-- ---------------------------------------------------------------------------
-- Resolved from the workflow spec at materialization time rather than read from
-- the spec on each retry. A task therefore keeps the policy it started with even
-- if the engine's defaults change mid-flight, which makes its retry schedule
-- reproducible from the row alone.
ALTER TABLE tasks
    ADD COLUMN retry_initial_interval_ms INTEGER          NOT NULL DEFAULT 1000,
    ADD COLUMN retry_backoff_coefficient DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    ADD COLUMN retry_max_interval_ms     INTEGER          NOT NULL DEFAULT 60000,
    ADD COLUMN retry_jitter_percent      INTEGER          NOT NULL DEFAULT 20,

    -- Set false when a worker declares a failure permanent (a validation error,
    -- say). Such a task dead-letters immediately instead of burning its
    -- remaining attempts on an outcome that cannot change.
    ADD COLUMN retryable                 BOOLEAN          NOT NULL DEFAULT TRUE,

    -- Cooperative cancellation. The engine sets this; the worker observes it on
    -- its next lease renewal and cancels the activity's context. Interrupting a
    -- worker cannot be done by fiat, so cancellation is a request, not an order.
    ADD COLUMN cancel_requested          BOOLEAN          NOT NULL DEFAULT FALSE,

    ADD COLUMN dead_lettered_at          TIMESTAMPTZ,

    -- Counts leases that lapsed on this task. A non-zero value is how an
    -- operator tells "this work was redone because a worker vanished" apart from
    -- "the activity itself kept failing" — the plain error string cannot.
    ADD COLUMN lease_expiry_count        INTEGER          NOT NULL DEFAULT 0,

    -- Infrastructure failures get their own budget, separate from max_attempts.
    --
    -- max_attempts is the *activity's* budget: how many times the user is willing
    -- for their code to be tried. A worker being killed is not the activity
    -- failing, so charging a crash against that budget would mean a task with
    -- maxAttempts=1 could never survive a worker restart — the exact scenario
    -- this phase exists to handle. Conversely, without any cap, a task that
    -- reliably kills its worker would be requeued forever.
    --
    -- So a lease expiry grants one extra activity attempt (keeping attempt an
    -- honest count of executions actually started) and is charged here instead.
    ADD COLUMN max_lease_expiries        INTEGER          NOT NULL DEFAULT 3,
    ADD COLUMN last_failure_reason       TEXT             NOT NULL DEFAULT '',

    ADD CONSTRAINT tasks_retry_bounds CHECK (
        retry_initial_interval_ms >= 0
        AND retry_max_interval_ms >= 0
        AND retry_backoff_coefficient >= 1
        AND retry_jitter_percent BETWEEN 0 AND 100
    ),
    ADD CONSTRAINT tasks_lease_expiry_bounds CHECK (
        lease_expiry_count >= 0 AND max_lease_expiries >= 0
    );

-- ---------------------------------------------------------------------------
-- DEAD_LETTER task state
-- ---------------------------------------------------------------------------
-- Terminal to the engine, but reachable again through an explicit operator
-- replay. That combination is the point: a task cannot retry forever, and it
-- also is not silently discarded.
ALTER TABLE tasks DROP CONSTRAINT tasks_state_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_state_check CHECK (
    state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD_LETTER', 'CANCELED')
);

-- A dead-lettered task must record when it was parked, and only a dead-lettered
-- task may carry that timestamp. Encoding the invariant here means no code path
-- can leave the two disagreeing.
ALTER TABLE tasks ADD CONSTRAINT tasks_dead_letter_consistency CHECK (
    (state = 'DEAD_LETTER') = (dead_lettered_at IS NOT NULL)
);

-- The operator's dead letter queue view: small, and ordered the way it is read.
CREATE INDEX tasks_dead_letter_idx
    ON tasks (dead_lettered_at DESC)
    WHERE state = 'DEAD_LETTER';

-- Retry candidates for the engine's retry pass. Kept separate from the
-- claimable index because the predicate differs.
CREATE INDEX tasks_retry_candidates_idx
    ON tasks (execution_id)
    WHERE state = 'FAILED';

-- ---------------------------------------------------------------------------
-- Worker failure detection
-- ---------------------------------------------------------------------------
-- The Phase 1 index leads with task_queue, which cannot serve a global "which
-- active workers have stopped heartbeating" scan. This one can.
CREATE INDEX workers_stale_detection_idx
    ON workers (last_heartbeat_at)
    WHERE state = 'ACTIVE';

-- Records why a worker was declared dead, for the same reason tasks record their
-- failure reason: the aggregate state alone does not explain how it got there.
ALTER TABLE workers
    ADD COLUMN declared_dead_at TIMESTAMPTZ,
    ADD CONSTRAINT workers_dead_consistency CHECK (
        (state = 'DEAD') = (declared_dead_at IS NOT NULL)
    );
