package store

import (
	"context"
	"time"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// QueueStats is a point-in-time picture of the backlog, aggregated in the
// database rather than by scanning rows into the application.
//
// Everything here is derived from the same tables the engine reads, so the
// dashboard and the scheduler can never disagree about the state of the queue.
type QueueStats struct {
	// ClaimableByActivity counts tasks a worker could claim right now, keyed by
	// (task queue, activity). This is the primary backlog signal.
	ClaimableByActivity map[QueueActivityKey]int
	// BackoffByQueue counts tasks scheduled for the future, waiting out a retry
	// delay. Separated from the claimable count because a large backoff queue is
	// benign while a large claimable queue is not.
	BackoffByQueue map[string]int
	// RunningByQueue counts tasks currently leased to a worker.
	RunningByQueue map[string]int
	// OldestClaimableAge is how long the longest-waiting claimable task has been
	// waiting, per queue. This distinguishes a big queue that is draining fast
	// from a small one that is stuck — a distinction depth alone cannot make.
	OldestClaimableAge map[string]time.Duration
	// TasksByState and ExecutionsByState are the coarse totals.
	TasksByState      map[domain.TaskState]int
	ExecutionsByState map[domain.WorkflowState]int
	// DeadLetterDepth is the operator's backlog.
	DeadLetterDepth int
	// WorkersByQueueState counts registered workers.
	WorkersByQueueState map[QueueStateKey]int
}

// QueueActivityKey identifies a (task queue, activity) pair.
type QueueActivityKey struct {
	TaskQueue string
	Activity  string
}

// QueueStateKey identifies a (task queue, state) pair.
type QueueStateKey struct {
	TaskQueue string
	State     string
}

// CollectQueueStats gathers every observability gauge in one round trip per
// aggregate.
//
// Deliberately a handful of GROUP BY queries against the partial indexes the
// engine already relies on, rather than one giant UNION: each is independently
// cheap and independently explainable in a slow-query log. They are also read
// outside a transaction, so a scrape can never block a claim.
func (s *Store) CollectQueueStats(ctx context.Context) (*QueueStats, error) {
	stats := &QueueStats{
		ClaimableByActivity: map[QueueActivityKey]int{},
		BackoffByQueue:      map[string]int{},
		RunningByQueue:      map[string]int{},
		OldestClaimableAge:  map[string]time.Duration{},
		TasksByState:        map[domain.TaskState]int{},
		ExecutionsByState:   map[domain.WorkflowState]int{},
		WorkersByQueueState: map[QueueStateKey]int{},
	}

	// Claimable now: the same predicate ClaimTask uses, so the gauge measures
	// exactly what a worker would find. Evaluated against the database clock for
	// the same reason every other deadline is.
	rows, err := s.db.Query(ctx, `
		SELECT task_queue, activity, count(*)
		FROM tasks
		WHERE state = 'SCHEDULED'
		  AND (scheduled_at IS NULL OR scheduled_at <= now())
		  AND attempt < max_attempts
		GROUP BY task_queue, activity`)
	if err != nil {
		return nil, translateError(err, "collect claimable queue depth")
	}
	for rows.Next() {
		var key QueueActivityKey
		var n int
		if err := rows.Scan(&key.TaskQueue, &key.Activity, &n); err != nil {
			rows.Close()
			return nil, translateError(err, "scan claimable queue depth")
		}
		stats.ClaimableByActivity[key] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate claimable queue depth")
	}

	// Waiting out a backoff, plus the age of the oldest claimable task. Combining
	// them keeps the number of round trips down without mixing unrelated predicates.
	rows, err = s.db.Query(ctx, `
		SELECT task_queue,
		       count(*) FILTER (
		         WHERE state = 'SCHEDULED' AND scheduled_at > now()
		       ) AS backoff,
		       count(*) FILTER (WHERE state = 'RUNNING') AS running,
		       COALESCE(EXTRACT(EPOCH FROM (now() - min(scheduled_at) FILTER (
		         WHERE state = 'SCHEDULED'
		           AND (scheduled_at IS NULL OR scheduled_at <= now())
		       ))), 0) AS oldest_claimable_seconds
		FROM tasks
		WHERE state IN ('SCHEDULED', 'RUNNING')
		GROUP BY task_queue`)
	if err != nil {
		return nil, translateError(err, "collect queue backlog")
	}
	for rows.Next() {
		var (
			queue           string
			backoff         int
			running         int
			oldestClaimable float64
		)
		if err := rows.Scan(&queue, &backoff, &running, &oldestClaimable); err != nil {
			rows.Close()
			return nil, translateError(err, "scan queue backlog")
		}
		stats.BackoffByQueue[queue] = backoff
		stats.RunningByQueue[queue] = running
		if oldestClaimable > 0 {
			stats.OldestClaimableAge[queue] = time.Duration(oldestClaimable * float64(time.Second))
		} else {
			stats.OldestClaimableAge[queue] = 0
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate queue backlog")
	}

	rows, err = s.db.Query(ctx, `SELECT state, count(*) FROM tasks GROUP BY state`)
	if err != nil {
		return nil, translateError(err, "collect task states")
	}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return nil, translateError(err, "scan task states")
		}
		stats.TasksByState[domain.TaskState(state)] = n
		if domain.TaskState(state) == domain.TaskDeadLetter {
			stats.DeadLetterDepth = n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate task states")
	}

	rows, err = s.db.Query(ctx,
		`SELECT state, count(*) FROM workflow_executions GROUP BY state`)
	if err != nil {
		return nil, translateError(err, "collect execution states")
	}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return nil, translateError(err, "scan execution states")
		}
		stats.ExecutionsByState[domain.WorkflowState(state)] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate execution states")
	}

	rows, err = s.db.Query(ctx,
		`SELECT task_queue, state, count(*) FROM workers GROUP BY task_queue, state`)
	if err != nil {
		return nil, translateError(err, "collect worker counts")
	}
	for rows.Next() {
		var key QueueStateKey
		var n int
		if err := rows.Scan(&key.TaskQueue, &key.State, &n); err != nil {
			rows.Close()
			return nil, translateError(err, "scan worker counts")
		}
		stats.WorkersByQueueState[key] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err, "iterate worker counts")
	}

	return stats, nil
}

// PoolStats reports connection pool saturation.
//
// Worth exporting because Chronos's pool size per replica multiplied by the
// replica count is a hard ceiling: a scale-up event can exhaust the database's
// max_connections long before it exhausts its CPU, and this is the gauge that
// shows it coming.
type PoolStats struct {
	Acquired        int32
	Idle            int32
	Total           int32
	Max             int32
	AcquireCount    int64
	AcquireDuration time.Duration
	EmptyAcquires   int64
	Canceled        int64
}

// PoolStats snapshots the pgx pool.
func (s *Store) PoolStats() PoolStats {
	if s.pool == nil {
		return PoolStats{}
	}
	st := s.pool.Stat()
	return PoolStats{
		Acquired:        st.AcquiredConns(),
		Idle:            st.IdleConns(),
		Total:           st.TotalConns(),
		Max:             st.MaxConns(),
		AcquireCount:    st.AcquireCount(),
		AcquireDuration: st.AcquireDuration(),
		// EmptyAcquireCount rising means callers are waiting for a connection,
		// which shows up as latency long before it shows up as an error.
		EmptyAcquires: st.EmptyAcquireCount(),
		Canceled:      st.CanceledAcquireCount(),
	}
}
