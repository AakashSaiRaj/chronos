package domain_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

func ptr[T any](v T) *T { return &v }

func TestDefaultRetryPolicy(t *testing.T) {
	p := domain.DefaultRetryPolicy()
	require.Equal(t, time.Second, p.InitialInterval)
	require.Equal(t, 2.0, p.BackoffCoefficient)
	require.Equal(t, time.Minute, p.MaxInterval)
	require.Equal(t, 20, p.JitterPercent)
}

// TestNilPolicyResolvesToDefaults is what keeps Phase 1 workflows working: an
// absent retry policy is not an error, it is the default.
func TestNilPolicyResolvesToDefaults(t *testing.T) {
	var p *domain.RetryPolicy
	require.Equal(t, domain.DefaultRetryPolicy(), p.Resolve())
	require.NoError(t, p.Validate())
}

func TestResolveFillsOnlyUnsetFields(t *testing.T) {
	p := &domain.RetryPolicy{InitialIntervalMS: 250}
	resolved := p.Resolve()

	require.Equal(t, 250*time.Millisecond, resolved.InitialInterval)
	require.Equal(t, 2.0, resolved.BackoffCoefficient, "unset fields keep the default")
	require.Equal(t, time.Minute, resolved.MaxInterval)
	require.Equal(t, 20, resolved.JitterPercent)
}

// TestZeroJitterIsDistinguishableFromUnset is the reason JitterPercent is a
// pointer: deterministic retries are a legitimate choice, and "0" has to mean
// something different from "not configured".
func TestZeroJitterIsDistinguishableFromUnset(t *testing.T) {
	unset := (&domain.RetryPolicy{InitialIntervalMS: 100}).Resolve()
	require.Equal(t, 20, unset.JitterPercent, "an unset jitter takes the default")

	explicit := (&domain.RetryPolicy{InitialIntervalMS: 100, JitterPercent: ptr(0)}).Resolve()
	require.Zero(t, explicit.JitterPercent, "an explicit zero must disable jitter")
}

func TestBaseBackoffFollowsTheCurve(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaxInterval:        time.Minute,
	}

	// attempt is the number of attempts already made, so the first failure
	// (attempt=1) waits the initial interval.
	expected := map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 16 * time.Second,
		6: 32 * time.Second,
		7: time.Minute, // capped
		8: time.Minute,
	}
	for attempt, want := range expected {
		require.Equal(t, want, p.BaseBackoff(attempt), "attempt %d", attempt)
	}
}

func TestBaseBackoffIsCapped(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaxInterval:        10 * time.Second,
	}
	require.Equal(t, 8*time.Second, p.BaseBackoff(4))
	require.Equal(t, 10*time.Second, p.BaseBackoff(5), "growth must stop at the cap")
	require.Equal(t, 10*time.Second, p.BaseBackoff(50))
}

// TestBaseBackoffSurvivesLargeAttemptCounts guards the overflow the cap exists
// to prevent: coefficient^attempt exceeds int64 nanoseconds long before it
// exceeds float64, so capping has to happen before the conversion.
func TestBaseBackoffSurvivesLargeAttemptCounts(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 10,
		MaxInterval:        time.Hour,
	}
	for _, attempt := range []int{1, 10, 100, 1_000, math.MaxInt32} {
		got := p.BaseBackoff(attempt)
		require.GreaterOrEqual(t, got, time.Duration(0), "attempt %d must not go negative", attempt)
		require.LessOrEqual(t, got, time.Hour, "attempt %d must respect the cap", attempt)
	}
}

func TestBaseBackoffHandlesDegenerateInput(t *testing.T) {
	fixed := domain.ResolvedRetryPolicy{
		InitialInterval:    2 * time.Second,
		BackoffCoefficient: 1, // no growth
		MaxInterval:        time.Minute,
	}
	require.Equal(t, 2*time.Second, fixed.BaseBackoff(1))
	require.Equal(t, 2*time.Second, fixed.BaseBackoff(10),
		"a coefficient of 1 must give a fixed delay")

	// attempt below 1 is clamped rather than producing a negative exponent.
	require.Equal(t, 2*time.Second, fixed.BaseBackoff(0))
	require.Equal(t, 2*time.Second, fixed.BaseBackoff(-5))

	// A coefficient below 1 would shrink the delay; it is clamped to 1.
	shrinking := domain.ResolvedRetryPolicy{
		InitialInterval: time.Second, BackoffCoefficient: 0.5, MaxInterval: time.Minute,
	}
	require.Equal(t, time.Second, shrinking.BaseBackoff(5),
		"backoff must never shrink below the initial interval")

	zero := domain.ResolvedRetryPolicy{InitialInterval: 0, BackoffCoefficient: 2}
	require.Zero(t, zero.BaseBackoff(3), "a zero initial interval means retry immediately")
}

// TestJitterIsAdditiveOnly matters because a retry that fires *earlier* than the
// backoff curve intends would defeat the purpose of backing off at all.
func TestJitterIsAdditiveOnly(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaxInterval:        time.Minute,
		JitterPercent:      50,
	}
	base := p.BaseBackoff(3) // 4s

	for _, fraction := range []float64{0, 0.25, 0.5, 0.75, 0.999} {
		got := p.BackoffFor(3, fraction)
		require.GreaterOrEqual(t, got, base, "jitter must never pull a retry earlier")
		require.LessOrEqual(t, got, base+base/2, "jitter must stay within JitterPercent")
	}

	require.Equal(t, base, p.BackoffFor(3, 0), "zero jitter yields the base delay")

	// The documented input range is [0,1), matching rand.Float64. A fraction of
	// exactly 1 is clamped just below, so the result approaches base+spread
	// without ever exceeding it.
	full := p.BackoffFor(3, 1.0)
	require.LessOrEqual(t, full, base+base/2)
	require.Greater(t, full, base+base/2-time.Millisecond,
		"the top of the range should reach essentially the full spread")
}

func TestJitterFractionIsClamped(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval: time.Second, BackoffCoefficient: 1,
		MaxInterval: time.Minute, JitterPercent: 100,
	}
	base := time.Second

	require.Equal(t, base, p.BackoffFor(1, -5), "a negative fraction is clamped to zero")
	require.LessOrEqual(t, p.BackoffFor(1, 99), 2*base, "an oversized fraction is clamped")
}

func TestNoJitterWhenDisabled(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval: time.Second, BackoffCoefficient: 2,
		MaxInterval: time.Minute, JitterPercent: 0,
	}
	for _, fraction := range []float64{0, 0.5, 0.99} {
		require.Equal(t, p.BaseBackoff(2), p.BackoffFor(2, fraction),
			"a zero jitter percent must make retries exactly reproducible")
	}
}

func TestNextRetryAt(t *testing.T) {
	p := domain.ResolvedRetryPolicy{
		InitialInterval: time.Second, BackoffCoefficient: 2,
		MaxInterval: time.Minute, JitterPercent: 0,
	}
	from := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	require.Equal(t, from.Add(time.Second), p.NextRetryAt(from, 1, 0))
	require.Equal(t, from.Add(4*time.Second), p.NextRetryAt(from, 3, 0))
}

func TestRetryPolicyValidation(t *testing.T) {
	valid := []*domain.RetryPolicy{
		nil,
		{},
		{InitialIntervalMS: 100},
		{InitialIntervalMS: 100, MaxIntervalMS: 100},
		{BackoffCoefficient: 1},
		{BackoffCoefficient: 10},
		{JitterPercent: ptr(0)},
		{JitterPercent: ptr(100)},
	}
	for i, p := range valid {
		require.NoError(t, p.Validate(), "case %d must be valid", i)
	}

	invalid := map[string]*domain.RetryPolicy{
		"negative initial":          {InitialIntervalMS: -1},
		"negative max":              {MaxIntervalMS: -1},
		"coefficient below one":     {BackoffCoefficient: 0.9},
		"coefficient above ceiling": {BackoffCoefficient: 100},
		"max below initial":         {InitialIntervalMS: 5000, MaxIntervalMS: 1000},
		"jitter negative":           {JitterPercent: ptr(-1)},
		"jitter above 100":          {JitterPercent: ptr(101)},
		"initial beyond ceiling":    {InitialIntervalMS: 48 * 60 * 60 * 1000},
	}
	for name, p := range invalid {
		t.Run(name, func(t *testing.T) {
			err := p.Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrValidation)
		})
	}
}

// TestRetryPolicyJSONRoundTrip pins the wire contract, including that an absent
// policy stays absent rather than serializing as an empty object.
func TestRetryPolicyJSONRoundTrip(t *testing.T) {
	spec := domain.TaskSpec{Name: "task_a", Activity: "noop"}
	encoded, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "retryPolicy",
		"an unset policy must not appear on the wire, or every Phase 1 spec hash changes")

	spec.RetryPolicy = &domain.RetryPolicy{
		InitialIntervalMS: 500, BackoffCoefficient: 3,
		MaxIntervalMS: 30_000, JitterPercent: ptr(10),
	}
	encoded, err = json.Marshal(spec)
	require.NoError(t, err)

	var decoded domain.TaskSpec
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NotNil(t, decoded.RetryPolicy)
	require.Equal(t, 500, decoded.RetryPolicy.InitialIntervalMS)
	require.Equal(t, 3.0, decoded.RetryPolicy.BackoffCoefficient)
	require.Equal(t, 30_000, decoded.RetryPolicy.MaxIntervalMS)
	require.NotNil(t, decoded.RetryPolicy.JitterPercent)
	require.Equal(t, 10, *decoded.RetryPolicy.JitterPercent)
}

// TestAddingRetrySupportDidNotChangeSpecHashes is the compatibility guarantee.
// If this fails, every workflow registered before Phase 2 starts returning 409.
func TestAddingRetrySupportDidNotChangeSpecHashes(t *testing.T) {
	withoutPolicy := domain.WorkflowSpec{
		Name:    "order_pipeline",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "charge_payment"},
			{Name: "task_b", Activity: "reserve_inventory", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "send_receipt", DependsOn: []string{"task_b"}},
		},
	}
	withoutPolicy.Normalize()
	require.NoError(t, withoutPolicy.Validate())

	hash, err := withoutPolicy.Hash()
	require.NoError(t, err)

	// A golden value, pinned deliberately. Every reliability field added in
	// Phase 2 is optional and omitempty, so the canonical form of a workflow that
	// sets none of them is byte-identical to what Phase 1 produced. If this
	// literal ever needs updating, every already-registered workflow will start
	// returning 409 on re-registration — so changing it is a breaking change, not
	// a test fix.
	require.Equal(t,
		"4803206be7e1883017beb028b7ce9c6a60fbaf7c582de097532b46a99e9bfad6",
		hash,
		"the canonical spec hash of a Phase 1 workflow must not change")
}

// TestExplicitRetryPolicyChangesTheHash is the flip side: a policy is part of the
// definition, so setting one really is a different workflow version.
func TestExplicitRetryPolicyChangesTheHash(t *testing.T) {
	base := domain.WorkflowSpec{
		Name: "w", Version: 1,
		Tasks: []domain.TaskSpec{{Name: "task_a", Activity: "noop"}},
	}
	base.Normalize()
	baseHash, err := base.Hash()
	require.NoError(t, err)

	withPolicy := domain.WorkflowSpec{
		Name: "w", Version: 1,
		Tasks: []domain.TaskSpec{{
			Name: "task_a", Activity: "noop",
			RetryPolicy: &domain.RetryPolicy{InitialIntervalMS: 500},
		}},
	}
	withPolicy.Normalize()
	withHash, err := withPolicy.Hash()
	require.NoError(t, err)
	require.NotEqual(t, baseHash, withHash)

	withLeaseBudget := domain.WorkflowSpec{
		Name: "w", Version: 1,
		Tasks: []domain.TaskSpec{{Name: "task_a", Activity: "noop", MaxLeaseExpiries: 5}},
	}
	withLeaseBudget.Normalize()
	leaseHash, err := withLeaseBudget.Hash()
	require.NoError(t, err)
	require.NotEqual(t, baseHash, leaseHash)
}

func TestTaskRetryHelpers(t *testing.T) {
	// A permanent failure short-circuits the remaining attempt budget.
	permanent := &domain.Task{Attempt: 1, MaxAttempts: 5, Retryable: false}
	require.True(t, permanent.HasAttemptsLeft())
	require.False(t, permanent.ShouldRetry(),
		"a failure the worker declared permanent must not be retried")

	transient := &domain.Task{Attempt: 1, MaxAttempts: 5, Retryable: true}
	require.True(t, transient.ShouldRetry())

	exhausted := &domain.Task{Attempt: 5, MaxAttempts: 5, Retryable: true}
	require.False(t, exhausted.ShouldRetry())

	// The lease-expiry budget is independent of the attempt budget.
	fresh := &domain.Task{Attempt: 1, MaxAttempts: 1, LeaseExpiryCount: 0, MaxLeaseExpiries: 3}
	require.False(t, fresh.HasAttemptsLeft(), "the activity budget is spent")
	require.True(t, fresh.CanSurviveLeaseExpiry(),
		"a worker crash must not be charged to the activity's attempt budget")

	worn := &domain.Task{LeaseExpiryCount: 3, MaxLeaseExpiries: 3}
	require.False(t, worn.CanSurviveLeaseExpiry(),
		"a task that keeps losing its worker must eventually be parked")
}

func TestTaskLeaseExpired(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	future := now.Add(time.Minute)

	require.True(t, (&domain.Task{State: domain.TaskRunning, LeaseExpiresAt: &past}).LeaseExpired(now))
	require.False(t, (&domain.Task{State: domain.TaskRunning, LeaseExpiresAt: &future}).LeaseExpired(now))
	require.False(t, (&domain.Task{State: domain.TaskRunning}).LeaseExpired(now),
		"a task with no lease cannot have an expired one")
	require.False(t, (&domain.Task{State: domain.TaskScheduled, LeaseExpiresAt: &past}).LeaseExpired(now),
		"only a running task holds a lease")
}

func TestWorkerIsStale(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	fresh := &domain.Worker{State: domain.WorkerActive, LastHeartbeatAt: now.Add(-5 * time.Second)}
	require.False(t, fresh.IsStale(now, 30*time.Second))

	silent := &domain.Worker{State: domain.WorkerActive, LastHeartbeatAt: now.Add(-time.Minute)}
	require.True(t, silent.IsStale(now, 30*time.Second))

	alreadyDead := &domain.Worker{State: domain.WorkerDead, LastHeartbeatAt: now.Add(-time.Hour)}
	require.False(t, alreadyDead.IsStale(now, 30*time.Second),
		"a worker already marked dead must not be re-detected")
}
