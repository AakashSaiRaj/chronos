package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// DefaultTaskQueue is used when a definition does not name a queue.
const DefaultTaskQueue = "default"

// DefaultMaxLeaseExpiries is how many times a task may be requeued because the
// worker holding it vanished. Three tolerates a rolling restart or a couple of
// unlucky evictions without letting a task that reliably kills its worker cycle
// forever.
const DefaultMaxLeaseExpiries = 3

// maxAttemptsCeiling bounds a task's configured attempts, so a typo cannot turn
// a failing task into an unbounded retry loop against a downstream dependency.
const maxAttemptsCeiling = 100

// maxTaskTimeoutSeconds bounds a single attempt at 24h. A task claiming a longer
// budget almost certainly means a misconfiguration, and it would hold a lease
// (and a worker slot) for that entire time.
const maxTaskTimeoutSeconds = 24 * 60 * 60

var nameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]{0,127}$`)

// TaskSpec is one node in a workflow's task graph.
type TaskSpec struct {
	// Name uniquely identifies the task inside its workflow.
	Name string `json:"name"`
	// Activity is the logical unit of work a worker dispatches on. Multiple
	// tasks may share an activity.
	Activity string `json:"activity"`
	// DependsOn lists task names that must reach COMPLETED before this task
	// becomes eligible for scheduling.
	DependsOn []string `json:"dependsOn,omitempty"`
	// Input is a static payload merged into the task's runtime input.
	Input json.RawMessage `json:"input,omitempty"`
	// MaxAttempts is the total attempt budget for the task. Zero means 1.
	MaxAttempts int `json:"maxAttempts,omitempty"`
	// TimeoutSeconds bounds a single attempt. Zero means the engine default.
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
	// RetryPolicy controls the delay between attempts. Nil means the engine
	// defaults, and stays nil in the canonical form so that adding retry support
	// did not change the spec hash of any already-registered workflow.
	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`
	// MaxLeaseExpiries bounds how many times this task may be requeued because
	// the worker holding it vanished, as opposed to because the activity failed.
	// Zero means the engine default. See RetryPolicy for why an unset field must
	// stay unset in the canonical form.
	MaxLeaseExpiries int `json:"maxLeaseExpiries,omitempty"`
}

// WorkflowSpec is a versioned, immutable workflow definition: a DAG of tasks.
type WorkflowSpec struct {
	Name        string     `json:"name"`
	Version     int        `json:"version"`
	Description string     `json:"description,omitempty"`
	TaskQueue   string     `json:"taskQueue,omitempty"`
	Tasks       []TaskSpec `json:"tasks"`
}

// Normalize fills in defaults so a spec has one canonical representation.
// It is called before validation and before hashing.
func (w *WorkflowSpec) Normalize() {
	w.Name = strings.TrimSpace(w.Name)
	w.TaskQueue = strings.TrimSpace(w.TaskQueue)
	if w.TaskQueue == "" {
		w.TaskQueue = DefaultTaskQueue
	}
	for i := range w.Tasks {
		t := &w.Tasks[i]
		t.Name = strings.TrimSpace(t.Name)
		t.Activity = strings.TrimSpace(t.Activity)
		if t.MaxAttempts <= 0 {
			t.MaxAttempts = 1
		}
		if len(t.DependsOn) == 0 {
			t.DependsOn = nil
		}
	}
}

// Validate enforces every structural invariant the engine relies on. The engine
// assumes a validated spec, so this is the single gate: a spec that passes here
// can always be materialized and scheduled to completion.
func (w *WorkflowSpec) Validate() error {
	var problems []string

	if !nameRe.MatchString(w.Name) {
		problems = append(problems, fmt.Sprintf("workflow name %q must match %s", w.Name, nameRe))
	}
	if w.Version < 1 {
		problems = append(problems, fmt.Sprintf("workflow version must be >= 1, got %d", w.Version))
	}
	if !nameRe.MatchString(w.TaskQueue) {
		problems = append(problems, fmt.Sprintf("task queue %q must match %s", w.TaskQueue, nameRe))
	}
	if len(w.Tasks) == 0 {
		problems = append(problems, "workflow must declare at least one task")
	}

	seen := make(map[string]struct{}, len(w.Tasks))
	for _, t := range w.Tasks {
		if !nameRe.MatchString(t.Name) {
			problems = append(problems, fmt.Sprintf("task name %q must match %s", t.Name, nameRe))
			continue
		}
		if _, dup := seen[t.Name]; dup {
			problems = append(problems, fmt.Sprintf("duplicate task name %q", t.Name))
			continue
		}
		seen[t.Name] = struct{}{}
	}

	for _, t := range w.Tasks {
		if !nameRe.MatchString(t.Activity) {
			problems = append(problems, fmt.Sprintf("task %q: activity %q must match %s", t.Name, t.Activity, nameRe))
		}
		if t.MaxAttempts < 1 || t.MaxAttempts > maxAttemptsCeiling {
			problems = append(problems, fmt.Sprintf("task %q: maxAttempts must be in [1,%d], got %d", t.Name, maxAttemptsCeiling, t.MaxAttempts))
		}
		if t.TimeoutSeconds < 0 || t.TimeoutSeconds > maxTaskTimeoutSeconds {
			problems = append(problems, fmt.Sprintf(
				"task %q: timeoutSeconds must be in [0,%d]", t.Name, maxTaskTimeoutSeconds))
		}
		if len(t.Input) > 0 && !json.Valid(t.Input) {
			problems = append(problems, fmt.Sprintf("task %q: input is not valid JSON", t.Name))
		}
		if err := t.RetryPolicy.Validate(); err != nil {
			problems = append(problems, fmt.Sprintf("task %q: %s", t.Name, err))
		}
		if t.MaxLeaseExpiries < 0 || t.MaxLeaseExpiries > maxAttemptsCeiling {
			problems = append(problems, fmt.Sprintf(
				"task %q: maxLeaseExpiries must be in [0,%d]", t.Name, maxAttemptsCeiling))
		}

		depSeen := make(map[string]struct{}, len(t.DependsOn))
		for _, dep := range t.DependsOn {
			if dep == t.Name {
				problems = append(problems, fmt.Sprintf("task %q depends on itself", t.Name))
				continue
			}
			if _, ok := seen[dep]; !ok {
				problems = append(problems, fmt.Sprintf("task %q depends on unknown task %q", t.Name, dep))
				continue
			}
			if _, dup := depSeen[dep]; dup {
				problems = append(problems, fmt.Sprintf("task %q lists duplicate dependency %q", t.Name, dep))
				continue
			}
			depSeen[dep] = struct{}{}
		}
	}

	// Only look for cycles once names and dependencies are known-good,
	// otherwise the error would be noise on top of the real problem.
	if len(problems) == 0 {
		if cycle := w.findCycle(); len(cycle) > 0 {
			problems = append(problems, fmt.Sprintf("task graph contains a cycle: %s", strings.Join(cycle, " -> ")))
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("%w: %s", ErrValidation, strings.Join(problems, "; "))
	}
	return nil
}

// findCycle returns a cycle path if the graph is cyclic, else nil. It is an
// iterative DFS with a colour marking so deep graphs cannot blow the stack.
func (w *WorkflowSpec) findCycle() []string {
	const (
		white = 0 // unvisited
		grey  = 1 // on the current DFS path
		black = 2 // fully explored
	)
	colour := make(map[string]int, len(w.Tasks))
	deps := make(map[string][]string, len(w.Tasks))
	order := make([]string, 0, len(w.Tasks))
	for _, t := range w.Tasks {
		deps[t.Name] = t.DependsOn
		colour[t.Name] = white
		order = append(order, t.Name)
	}

	type frame struct {
		node string
		next int
	}

	for _, root := range order {
		if colour[root] != white {
			continue
		}
		stack := []frame{{node: root}}
		colour[root] = grey
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.next < len(deps[top.node]) {
				dep := deps[top.node][top.next]
				top.next++
				switch colour[dep] {
				case white:
					colour[dep] = grey
					stack = append(stack, frame{node: dep})
				case grey:
					// Found a back edge: unwind the grey path to report it.
					path := []string{}
					for _, f := range stack {
						if colour[f.node] == grey {
							path = append(path, f.node)
						}
					}
					return append(path, dep)
				}
				continue
			}
			colour[top.node] = black
			stack = stack[:len(stack)-1]
		}
	}
	return nil
}

// TopologicalOrder returns task names in a dependency-respecting order. It is
// used for deterministic task materialization and assumes Validate passed.
func (w *WorkflowSpec) TopologicalOrder() ([]string, error) {
	indegree := make(map[string]int, len(w.Tasks))
	dependents := make(map[string][]string, len(w.Tasks))
	for _, t := range w.Tasks {
		indegree[t.Name] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.Name)
		}
	}

	// Seed with dependency-free tasks in declaration order, then always pick
	// the lexicographically smallest ready task so the order is deterministic
	// across runs and processes.
	ready := make([]string, 0, len(w.Tasks))
	for _, t := range w.Tasks {
		if indegree[t.Name] == 0 {
			ready = append(ready, t.Name)
		}
	}
	sort.Strings(ready)

	out := make([]string, 0, len(w.Tasks))
	for len(ready) > 0 {
		node := ready[0]
		ready = ready[1:]
		out = append(out, node)

		promoted := false
		for _, dep := range dependents[node] {
			indegree[dep]--
			if indegree[dep] == 0 {
				ready = append(ready, dep)
				promoted = true
			}
		}
		if promoted {
			sort.Strings(ready)
		}
	}

	if len(out) != len(w.Tasks) {
		return nil, fmt.Errorf("%w: task graph contains a cycle", ErrValidation)
	}
	return out, nil
}

// Task returns the spec for a named task.
func (w *WorkflowSpec) Task(name string) (TaskSpec, bool) {
	for _, t := range w.Tasks {
		if t.Name == name {
			return t, true
		}
	}
	return TaskSpec{}, false
}

// TerminalTasks returns tasks that nothing depends on. Their outputs form the
// workflow's result.
func (w *WorkflowSpec) TerminalTasks() []string {
	hasDependent := make(map[string]bool, len(w.Tasks))
	for _, t := range w.Tasks {
		for _, dep := range t.DependsOn {
			hasDependent[dep] = true
		}
	}
	out := make([]string, 0, len(w.Tasks))
	for _, t := range w.Tasks {
		if !hasDependent[t.Name] {
			out = append(out, t.Name)
		}
	}
	sort.Strings(out)
	return out
}

// Hash is a stable fingerprint of the semantic content of a spec. Registering
// the same (name, version) twice is idempotent when the hashes agree and a
// conflict when they differ, which is what makes versions immutable.
func (w *WorkflowSpec) Hash() (string, error) {
	// Canonicalize: sort tasks and their dependencies so that declaration
	// order does not change the fingerprint.
	canon := WorkflowSpec{
		Name:        w.Name,
		Version:     w.Version,
		Description: w.Description,
		TaskQueue:   w.TaskQueue,
		Tasks:       make([]TaskSpec, len(w.Tasks)),
	}
	copy(canon.Tasks, w.Tasks)
	for i := range canon.Tasks {
		deps := make([]string, len(canon.Tasks[i].DependsOn))
		copy(deps, canon.Tasks[i].DependsOn)
		sort.Strings(deps)
		canon.Tasks[i].DependsOn = deps
		if len(canon.Tasks[i].Input) > 0 {
			compacted, err := canonicalJSON(canon.Tasks[i].Input)
			if err != nil {
				return "", fmt.Errorf("task %q: %w", canon.Tasks[i].Name, err)
			}
			canon.Tasks[i].Input = compacted
		}
	}
	sort.Slice(canon.Tasks, func(i, j int) bool { return canon.Tasks[i].Name < canon.Tasks[j].Name })

	raw, err := json.Marshal(canon)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalJSON re-encodes JSON so semantically equal payloads that differ only
// in key order or whitespace produce identical bytes.
func canonicalJSON(in json.RawMessage) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(in, &v); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrValidation, err)
	}
	// encoding/json marshals map keys in sorted order, which gives us the
	// canonical form for free.
	out, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return out, nil
}
