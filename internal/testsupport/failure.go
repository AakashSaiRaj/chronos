package testsupport

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/store"
)

// Time-based failure detection in Chronos is evaluated against the *database*
// clock, so that every replica agrees on what time it is and a drifted app-server
// clock cannot reap a live lease. That is the right production design, but it
// means a test cannot fake time by injecting a clock.
//
// These helpers simulate elapsed time the only way that stays faithful to the
// production code path: by backdating the timestamps the real queries read. A
// test that wants "this worker went silent two minutes ago" writes exactly that,
// and the reaper then runs its real logic against a real database clock.

// ExpireLease backdates a task's lease so the reaper will treat it as lapsed.
// This stands in for a worker that stopped reporting.
func ExpireLease(t testing.TB, s *store.Store, taskID uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := s.Pool().Exec(ctx, `
		UPDATE tasks SET lease_expires_at = now() - interval '1 hour'
		WHERE id = $1 AND state = 'RUNNING'`, taskID)
	if err != nil {
		t.Fatalf("expire lease on task %s: %v", taskID, err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatalf("expire lease on task %s: task is not RUNNING", taskID)
	}
}

// ExpireAllLeases backdates every running task's lease for an execution.
func ExpireAllLeases(t testing.TB, s *store.Store, executionID uuid.UUID) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := s.Pool().Exec(ctx, `
		UPDATE tasks SET lease_expires_at = now() - interval '1 hour'
		WHERE execution_id = $1 AND state = 'RUNNING'`, executionID)
	if err != nil {
		t.Fatalf("expire leases for execution %s: %v", executionID, err)
	}
	return int(tag.RowsAffected())
}

// SilenceWorker backdates a worker's heartbeat by the given duration, standing in
// for a worker process that died or was evicted.
func SilenceWorker(t testing.TB, s *store.Store, workerName string, silentFor time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := s.Pool().Exec(ctx, `
		UPDATE workers
		SET last_heartbeat_at = now() - ($2::bigint * interval '1 microsecond')
		WHERE name = $1`, workerName, silentFor.Microseconds())
	if err != nil {
		t.Fatalf("silence worker %s: %v", workerName, err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatalf("silence worker %s: no such worker", workerName)
	}
}

// MakeClaimable pulls a task's scheduled_at into the past, so a test can skip a
// retry backoff delay without sleeping through it.
func MakeClaimable(t testing.TB, s *store.Store, taskID uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := s.Pool().Exec(ctx, `
		UPDATE tasks SET scheduled_at = now() - interval '1 second'
		WHERE id = $1 AND state = 'SCHEDULED'`, taskID)
	if err != nil {
		t.Fatalf("make task %s claimable: %v", taskID, err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatalf("make task %s claimable: task is not SCHEDULED", taskID)
	}
}

// RetryDelay reports how far in the future a task is scheduled, relative to the
// database clock. Used to assert backoff without depending on the test process's
// own clock agreeing with the database's.
func RetryDelay(t testing.TB, s *store.Store, taskID uuid.UUID) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var micros *int64
	err := s.Pool().QueryRow(ctx, `
		SELECT (EXTRACT(EPOCH FROM (scheduled_at - now())) * 1000000)::bigint
		FROM tasks WHERE id = $1`, taskID).Scan(&micros)
	if err != nil {
		t.Fatalf("read retry delay for task %s: %v", taskID, err)
	}
	if micros == nil {
		t.Fatalf("task %s has no scheduled_at", taskID)
	}
	return time.Duration(*micros) * time.Microsecond
}
