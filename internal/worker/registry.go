// Package worker executes tasks handed out by the Chronos control plane.
//
// A worker is intentionally dumb: it claims a task, runs the registered activity
// for it, and reports the outcome. It holds no workflow state and makes no
// scheduling decisions, so workers are freely replaceable and horizontally
// scalable — killing one only means its lease expires and another claims the task.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// ActivityFunc is a unit of work. It receives the task's persisted input and
// returns a JSON-encodable result.
//
// Implementations should be idempotent. Chronos guarantees a task is leased to
// one worker at a time, but a worker that crashes after performing a side effect
// and before reporting will have that task re-executed, so at-least-once is the
// honest contract for the activity body itself.
type ActivityFunc func(ctx context.Context, in ActivityInput) (any, error)

// ActivityInput is what an activity is given.
type ActivityInput struct {
	// TaskName is the task's name within its workflow.
	TaskName string
	// Activity is the registered activity name.
	Activity string
	// ExecutionID identifies the workflow run.
	ExecutionID string
	// Attempt is 1 on the first try.
	Attempt int
	// WorkflowInput is the input the execution was started with.
	WorkflowInput json.RawMessage
	// TaskInput is the static input declared on the task spec.
	TaskInput json.RawMessage
	// Upstream maps each direct dependency's task name to its output.
	Upstream map[string]json.RawMessage
}

// UnmarshalWorkflowInput decodes the workflow input into dst.
func (in ActivityInput) UnmarshalWorkflowInput(dst any) error {
	if len(in.WorkflowInput) == 0 {
		return nil
	}
	if err := json.Unmarshal(in.WorkflowInput, dst); err != nil {
		return fmt.Errorf("decode workflow input for task %q: %w", in.TaskName, err)
	}
	return nil
}

// UnmarshalUpstream decodes a named dependency's output into dst.
func (in ActivityInput) UnmarshalUpstream(taskName string, dst any) error {
	raw, ok := in.Upstream[taskName]
	if !ok {
		return fmt.Errorf("task %q has no upstream output named %q", in.TaskName, taskName)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode upstream %q for task %q: %w", taskName, in.TaskName, err)
	}
	return nil
}

// Registry maps activity names to implementations.
//
// The registry is also what the worker advertises at registration time, so the
// control plane only ever leases a task to a worker that can actually run it.
type Registry struct {
	mu         sync.RWMutex
	activities map[string]ActivityFunc
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{activities: map[string]ActivityFunc{}}
}

// Register adds an activity. Registering the same name twice is a programming
// error and panics at startup, which is far better than silently shadowing an
// implementation and executing the wrong side effect in production.
func (r *Registry) Register(name string, fn ActivityFunc) *Registry {
	if name == "" {
		panic("worker: activity name must not be empty")
	}
	if fn == nil {
		panic("worker: activity " + name + " has a nil implementation")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.activities[name]; exists {
		panic("worker: activity " + name + " is already registered")
	}
	r.activities[name] = fn
	return r
}

// Lookup returns the implementation for an activity.
func (r *Registry) Lookup(name string) (ActivityFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.activities[name]
	if !ok {
		return nil, fmt.Errorf("%w: no activity registered for %q", domain.ErrNotFound, name)
	}
	return fn, nil
}

// Names returns the registered activity names in sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.activities))
	for name := range r.activities {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Len reports how many activities are registered.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.activities)
}
