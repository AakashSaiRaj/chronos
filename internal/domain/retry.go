package domain

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Retry policy defaults, applied when a task spec does not state its own.
const (
	DefaultRetryInitialInterval    = time.Second
	DefaultRetryBackoffCoefficient = 2.0
	DefaultRetryMaxInterval        = time.Minute
	// DefaultRetryJitterPercent spreads retries so a batch of tasks that failed
	// together does not stampede the same downstream dependency in lockstep.
	DefaultRetryJitterPercent = 20
)

// Bounds on a retry policy. These are guardrails against configuration that
// would either hammer a failing dependency or park a task for hours.
const (
	maxRetryIntervalCeiling = 24 * time.Hour
	maxBackoffCoefficient   = 10.0
	maxJitterPercent        = 100
)

// RetryPolicy controls the delay between attempts of a single task.
//
// It covers only *timing*; how many attempts a task gets is TaskSpec.MaxAttempts.
// Splitting them keeps the common case ("retry 3 times with the default
// backoff") to a single field.
//
// A nil policy on a task spec means "use the defaults", which is why adding
// retries in Phase 2 did not change the spec hash of any workflow registered in
// Phase 1: an absent policy stays absent in the canonical form. The *effective*
// policy is resolved and persisted per task at materialization time, so a task
// already in flight keeps the policy it started with even if the defaults change.
type RetryPolicy struct {
	// InitialIntervalMS is the delay before the second attempt.
	InitialIntervalMS int `json:"initialIntervalMs,omitempty"`
	// BackoffCoefficient multiplies the interval after each failure. 1.0 gives a
	// fixed delay; 2.0 doubles.
	BackoffCoefficient float64 `json:"backoffCoefficient,omitempty"`
	// MaxIntervalMS caps the computed delay.
	MaxIntervalMS int `json:"maxIntervalMs,omitempty"`
	// JitterPercent adds up to this percentage of randomness on top of the
	// computed delay. Only ever additive, so a retry never fires earlier than
	// the backoff curve intends.
	//
	// A pointer because zero is a meaningful setting (deterministic retries) and
	// has to be distinguishable from "not configured".
	JitterPercent *int `json:"jitterPercent,omitempty"`
}

// ResolvedRetryPolicy is a RetryPolicy with every field populated. It is what
// gets persisted per task and what the backoff calculation operates on.
type ResolvedRetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
	JitterPercent      int
}

// DefaultRetryPolicy returns the effective policy applied when a spec states none.
func DefaultRetryPolicy() ResolvedRetryPolicy {
	return ResolvedRetryPolicy{
		InitialInterval:    DefaultRetryInitialInterval,
		BackoffCoefficient: DefaultRetryBackoffCoefficient,
		MaxInterval:        DefaultRetryMaxInterval,
		JitterPercent:      DefaultRetryJitterPercent,
	}
}

// Resolve fills any unset field from the defaults. A nil receiver resolves to
// the full default policy.
func (p *RetryPolicy) Resolve() ResolvedRetryPolicy {
	out := DefaultRetryPolicy()
	if p == nil {
		return out
	}
	if p.InitialIntervalMS > 0 {
		out.InitialInterval = time.Duration(p.InitialIntervalMS) * time.Millisecond
	}
	if p.BackoffCoefficient > 0 {
		out.BackoffCoefficient = p.BackoffCoefficient
	}
	if p.MaxIntervalMS > 0 {
		out.MaxInterval = time.Duration(p.MaxIntervalMS) * time.Millisecond
	}
	if p.JitterPercent != nil {
		out.JitterPercent = *p.JitterPercent
	}
	return out
}

// Validate checks a caller-supplied policy.
func (p *RetryPolicy) Validate() error {
	if p == nil {
		return nil
	}
	var problems []string

	if p.InitialIntervalMS < 0 {
		problems = append(problems, "initialIntervalMs must be >= 0")
	}
	if time.Duration(p.InitialIntervalMS)*time.Millisecond > maxRetryIntervalCeiling {
		problems = append(problems, fmt.Sprintf("initialIntervalMs must be <= %d", maxRetryIntervalCeiling/time.Millisecond))
	}
	if p.BackoffCoefficient < 0 || (p.BackoffCoefficient > 0 && p.BackoffCoefficient < 1) {
		problems = append(problems, "backoffCoefficient must be >= 1 (1 gives a fixed delay)")
	}
	if p.BackoffCoefficient > maxBackoffCoefficient {
		problems = append(problems, fmt.Sprintf("backoffCoefficient must be <= %g", maxBackoffCoefficient))
	}
	if p.MaxIntervalMS < 0 {
		problems = append(problems, "maxIntervalMs must be >= 0")
	}
	if time.Duration(p.MaxIntervalMS)*time.Millisecond > maxRetryIntervalCeiling {
		problems = append(problems, fmt.Sprintf("maxIntervalMs must be <= %d", maxRetryIntervalCeiling/time.Millisecond))
	}
	if p.MaxIntervalMS > 0 && p.InitialIntervalMS > p.MaxIntervalMS {
		problems = append(problems, "maxIntervalMs must be >= initialIntervalMs")
	}
	if p.JitterPercent != nil && (*p.JitterPercent < 0 || *p.JitterPercent > maxJitterPercent) {
		problems = append(problems, fmt.Sprintf("jitterPercent must be in [0,%d]", maxJitterPercent))
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: retry policy: %s", ErrValidation, strings.Join(problems, "; "))
	}
	return nil
}

// BaseBackoff returns the un-jittered delay before the retry that follows the
// given attempt. attempt is the number of attempts already made, so attempt=1
// (the first failure) yields the initial interval.
//
// It is pure and deterministic, which is what makes the backoff curve testable
// independently of the randomness layered on top.
func (p ResolvedRetryPolicy) BaseBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if p.InitialInterval <= 0 {
		return 0
	}
	coefficient := p.BackoffCoefficient
	if coefficient < 1 {
		coefficient = 1
	}

	// Compute in float milliseconds and cap *before* converting to a Duration:
	// coefficient^attempt overflows int64 nanoseconds long before it overflows
	// float64, so capping first is what keeps a large attempt count safe.
	initialMS := float64(p.InitialInterval / time.Millisecond)
	maxMS := float64(p.MaxInterval / time.Millisecond)
	delayMS := initialMS * math.Pow(coefficient, float64(attempt-1))

	if math.IsInf(delayMS, 0) || math.IsNaN(delayMS) || (maxMS > 0 && delayMS > maxMS) {
		delayMS = maxMS
	}
	if delayMS < 0 {
		delayMS = 0
	}
	return time.Duration(delayMS) * time.Millisecond
}

// BackoffFor applies jitter to BaseBackoff. jitterFraction must be in [0,1) and
// is supplied by the caller rather than drawn internally, so the engine can use
// a real random source while tests pin an exact value.
func (p ResolvedRetryPolicy) BackoffFor(attempt int, jitterFraction float64) time.Duration {
	base := p.BaseBackoff(attempt)
	if p.JitterPercent <= 0 || base <= 0 {
		return base
	}
	if jitterFraction < 0 {
		jitterFraction = 0
	}
	if jitterFraction >= 1 {
		jitterFraction = math.Nextafter(1, 0)
	}

	spread := float64(base) * float64(p.JitterPercent) / 100.0
	return base + time.Duration(spread*jitterFraction)
}

// NextRetryAt returns the absolute time a retry should become claimable.
func (p ResolvedRetryPolicy) NextRetryAt(from time.Time, attempt int, jitterFraction float64) time.Time {
	return from.Add(p.BackoffFor(attempt, jitterFraction))
}
