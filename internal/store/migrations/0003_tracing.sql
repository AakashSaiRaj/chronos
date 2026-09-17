-- Chronos Phase 4: carry trace context through the durable queue.
--
-- This is the one piece of distributed tracing that cannot be solved by HTTP
-- header propagation. A workflow's spans are produced by different processes at
-- different times: the API accepts the execution, a scheduler materializes and
-- enqueues its tasks seconds later, a worker claims one minutes after that, and a
-- reaper may requeue it minutes after *that*. There is no live call chain to
-- propagate a header along.
--
-- Storing the W3C traceparent on the execution row makes the workflow itself the
-- trace: every later span, in whatever process, is created as a child of the
-- context recorded here. Without it a single workflow appears as a scatter of
-- unrelated single-span traces, which is the least useful possible outcome of
-- having instrumented anything.
--
-- Nullable, because executions created before this migration have no trace and
-- because tracing can be switched off entirely.
ALTER TABLE workflow_executions
    ADD COLUMN traceparent TEXT NOT NULL DEFAULT '';

-- A W3C traceparent is 55 characters: "00-" + 32 hex trace id + "-" + 16 hex
-- span id + "-" + 2 hex flags. Bounding it stops a malformed or hostile value
-- from being stored, and the empty string remains valid for "untraced".
ALTER TABLE workflow_executions
    ADD CONSTRAINT workflow_executions_traceparent_format CHECK (
        traceparent = '' OR traceparent ~ '^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'
    );

-- Finding an execution from a trace ID is the query an operator runs when a trace
-- looks wrong and they want the durable state behind it. The index covers the
-- trace-id substring rather than the whole header, since that is what a tracing
-- backend shows.
CREATE INDEX workflow_executions_trace_idx
    ON workflow_executions (substring(traceparent from 4 for 32))
    WHERE traceparent <> '';
