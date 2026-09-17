package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// linear builds the Task A -> Task B -> Task C example from the brief.
func linear() domain.WorkflowSpec {
	return domain.WorkflowSpec{
		Name:    "order_pipeline",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "charge_payment"},
			{Name: "task_b", Activity: "reserve_inventory", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "send_receipt", DependsOn: []string{"task_b"}},
		},
	}
}

func TestNormalizeAppliesDefaults(t *testing.T) {
	spec := domain.WorkflowSpec{
		Name:    "  padded  ",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: " task_a ", Activity: " noop ", DependsOn: []string{}},
		},
	}
	spec.Normalize()

	require.Equal(t, "padded", spec.Name, "names must be trimmed")
	require.Equal(t, domain.DefaultTaskQueue, spec.TaskQueue, "a missing queue must default")
	require.Equal(t, "task_a", spec.Tasks[0].Name)
	require.Equal(t, "noop", spec.Tasks[0].Activity)
	require.Equal(t, 1, spec.Tasks[0].MaxAttempts, "maxAttempts must default to a single attempt")
	require.Nil(t, spec.Tasks[0].DependsOn, "an empty dependency list must normalize to nil")
}

func TestValidateAcceptsWellFormedSpec(t *testing.T) {
	spec := linear()
	spec.Normalize()
	require.NoError(t, spec.Validate())
}

func TestValidateRejectsMalformedSpecs(t *testing.T) {
	tests := map[string]struct {
		mutate    func(*domain.WorkflowSpec)
		expectMsg string
	}{
		"empty name": {
			mutate:    func(s *domain.WorkflowSpec) { s.Name = "" },
			expectMsg: "workflow name",
		},
		"name starting with a digit": {
			mutate:    func(s *domain.WorkflowSpec) { s.Name = "1bad" },
			expectMsg: "workflow name",
		},
		"zero version": {
			mutate:    func(s *domain.WorkflowSpec) { s.Version = 0 },
			expectMsg: "version must be >= 1",
		},
		"no tasks": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks = nil },
			expectMsg: "at least one task",
		},
		"duplicate task names": {
			mutate: func(s *domain.WorkflowSpec) {
				s.Tasks[1].Name = "task_a"
				s.Tasks[2].DependsOn = []string{"task_a"}
			},
			expectMsg: "duplicate task name",
		},
		"dependency on unknown task": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[1].DependsOn = []string{"ghost"} },
			expectMsg: `depends on unknown task "ghost"`,
		},
		"self dependency": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].DependsOn = []string{"task_a"} },
			expectMsg: "depends on itself",
		},
		"duplicate dependency": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[1].DependsOn = []string{"task_a", "task_a"} },
			expectMsg: "duplicate dependency",
		},
		"empty activity": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].Activity = "" },
			expectMsg: "activity",
		},
		"maxAttempts above ceiling": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].MaxAttempts = 10_000 },
			expectMsg: "maxAttempts must be in",
		},
		"negative timeout": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].TimeoutSeconds = -1 },
			expectMsg: "timeoutSeconds must be in",
		},
		"timeout beyond the ceiling": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].TimeoutSeconds = 90_000 },
			expectMsg: "timeoutSeconds must be in",
		},
		"negative maxLeaseExpiries": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].MaxLeaseExpiries = -1 },
			expectMsg: "maxLeaseExpiries must be in",
		},
		"invalid retry backoff coefficient": {
			mutate: func(s *domain.WorkflowSpec) {
				s.Tasks[0].RetryPolicy = &domain.RetryPolicy{BackoffCoefficient: 0.5}
			},
			expectMsg: "backoffCoefficient must be >= 1",
		},
		"retry max interval below initial": {
			mutate: func(s *domain.WorkflowSpec) {
				s.Tasks[0].RetryPolicy = &domain.RetryPolicy{
					InitialIntervalMS: 10_000, MaxIntervalMS: 1_000,
				}
			},
			expectMsg: "maxIntervalMs must be >= initialIntervalMs",
		},
		"jitter out of range": {
			mutate: func(s *domain.WorkflowSpec) {
				jitter := 250
				s.Tasks[0].RetryPolicy = &domain.RetryPolicy{JitterPercent: &jitter}
			},
			expectMsg: "jitterPercent must be in",
		},
		"invalid static input": {
			mutate:    func(s *domain.WorkflowSpec) { s.Tasks[0].Input = json.RawMessage(`{not json`) },
			expectMsg: "input is not valid JSON",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			spec := linear()
			tc.mutate(&spec)
			spec.Normalize()

			err := spec.Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrValidation)
			require.Contains(t, err.Error(), tc.expectMsg)
		})
	}
}

// TestValidateDetectsCycles is the invariant that keeps the engine from
// deadlocking: a cyclic graph would leave tasks permanently unschedulable.
func TestValidateDetectsCycles(t *testing.T) {
	tests := map[string][]domain.TaskSpec{
		"two-node cycle": {
			{Name: "task_a", Activity: "noop", DependsOn: []string{"task_b"}},
			{Name: "task_b", Activity: "noop", DependsOn: []string{"task_a"}},
		},
		"three-node cycle": {
			{Name: "task_a", Activity: "noop", DependsOn: []string{"task_c"}},
			{Name: "task_b", Activity: "noop", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "noop", DependsOn: []string{"task_b"}},
		},
		"cycle reachable from an acyclic root": {
			{Name: "root", Activity: "noop"},
			{Name: "task_a", Activity: "noop", DependsOn: []string{"root", "task_c"}},
			{Name: "task_b", Activity: "noop", DependsOn: []string{"task_a"}},
			{Name: "task_c", Activity: "noop", DependsOn: []string{"task_b"}},
		},
	}

	for name, tasks := range tests {
		t.Run(name, func(t *testing.T) {
			spec := domain.WorkflowSpec{Name: "cyclic", Version: 1, Tasks: tasks}
			spec.Normalize()

			err := spec.Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrValidation)
			require.Contains(t, err.Error(), "cycle")
		})
	}
}

func TestValidateAcceptsDiamondGraph(t *testing.T) {
	// A shared node with two dependents that reconverge is a DAG, not a cycle.
	spec := domain.WorkflowSpec{
		Name:    "diamond",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "fetch", Activity: "noop"},
			{Name: "left", Activity: "noop", DependsOn: []string{"fetch"}},
			{Name: "right", Activity: "noop", DependsOn: []string{"fetch"}},
			{Name: "merge", Activity: "noop", DependsOn: []string{"left", "right"}},
		},
	}
	spec.Normalize()
	require.NoError(t, spec.Validate())

	order, err := spec.TopologicalOrder()
	require.NoError(t, err)
	require.Len(t, order, 4)
	requireBefore(t, order, "fetch", "left")
	requireBefore(t, order, "fetch", "right")
	requireBefore(t, order, "left", "merge")
	requireBefore(t, order, "right", "merge")
}

func TestTopologicalOrderIsDependencyRespectingAndDeterministic(t *testing.T) {
	spec := linear()
	spec.Normalize()

	first, err := spec.TopologicalOrder()
	require.NoError(t, err)
	require.Equal(t, []string{"task_a", "task_b", "task_c"}, first)

	// Reordering the declaration must not change the resulting order, so task
	// materialization is stable across processes.
	shuffled := linear()
	shuffled.Tasks[0], shuffled.Tasks[2] = shuffled.Tasks[2], shuffled.Tasks[0]
	shuffled.Normalize()

	second, err := shuffled.TopologicalOrder()
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestTerminalTasks(t *testing.T) {
	spec := linear()
	spec.Normalize()
	require.Equal(t, []string{"task_c"}, spec.TerminalTasks(),
		"only the last task of a chain is terminal")

	fanOut := domain.WorkflowSpec{
		Name:    "fanout",
		Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "root", Activity: "noop"},
			{Name: "leaf_b", Activity: "noop", DependsOn: []string{"root"}},
			{Name: "leaf_a", Activity: "noop", DependsOn: []string{"root"}},
		},
	}
	fanOut.Normalize()
	require.Equal(t, []string{"leaf_a", "leaf_b"}, fanOut.TerminalTasks(),
		"terminal tasks must be sorted for deterministic output")
}

// TestHashIsCanonical pins the behaviour that makes registration idempotent:
// semantically identical specs must hash identically, and any real change must
// change the hash.
func TestHashIsCanonical(t *testing.T) {
	base := linear()
	base.Normalize()
	baseHash, err := base.Hash()
	require.NoError(t, err)
	require.NotEmpty(t, baseHash)

	t.Run("task order is not semantic", func(t *testing.T) {
		reordered := linear()
		reordered.Tasks[0], reordered.Tasks[2] = reordered.Tasks[2], reordered.Tasks[0]
		reordered.Normalize()

		got, err := reordered.Hash()
		require.NoError(t, err)
		require.Equal(t, baseHash, got)
	})

	t.Run("dependency order is not semantic", func(t *testing.T) {
		a := domain.WorkflowSpec{Name: "w", Version: 1, Tasks: []domain.TaskSpec{
			{Name: "x", Activity: "noop"},
			{Name: "y", Activity: "noop"},
			{Name: "z", Activity: "noop", DependsOn: []string{"x", "y"}},
		}}
		b := domain.WorkflowSpec{Name: "w", Version: 1, Tasks: []domain.TaskSpec{
			{Name: "x", Activity: "noop"},
			{Name: "y", Activity: "noop"},
			{Name: "z", Activity: "noop", DependsOn: []string{"y", "x"}},
		}}
		a.Normalize()
		b.Normalize()

		aHash, err := a.Hash()
		require.NoError(t, err)
		bHash, err := b.Hash()
		require.NoError(t, err)
		require.Equal(t, aHash, bHash)
	})

	t.Run("json key order and whitespace are not semantic", func(t *testing.T) {
		a := linear()
		a.Tasks[0].Input = json.RawMessage(`{"b":2,"a":1}`)
		a.Normalize()
		b := linear()
		b.Tasks[0].Input = json.RawMessage("{\n  \"a\": 1,\n  \"b\": 2\n}")
		b.Normalize()

		aHash, err := a.Hash()
		require.NoError(t, err)
		bHash, err := b.Hash()
		require.NoError(t, err)
		require.Equal(t, aHash, bHash)
	})

	t.Run("real changes change the hash", func(t *testing.T) {
		for name, mutate := range map[string]func(*domain.WorkflowSpec){
			"activity":    func(s *domain.WorkflowSpec) { s.Tasks[0].Activity = "other" },
			"dependency":  func(s *domain.WorkflowSpec) { s.Tasks[2].DependsOn = []string{"task_a"} },
			"maxAttempts": func(s *domain.WorkflowSpec) { s.Tasks[0].MaxAttempts = 3 },
			"taskQueue":   func(s *domain.WorkflowSpec) { s.TaskQueue = "other-queue" },
			"description": func(s *domain.WorkflowSpec) { s.Description = "changed" },
			"staticInput": func(s *domain.WorkflowSpec) { s.Tasks[0].Input = json.RawMessage(`{"a":1}`) },
		} {
			t.Run(name, func(t *testing.T) {
				mutated := linear()
				mutate(&mutated)
				mutated.Normalize()

				got, err := mutated.Hash()
				require.NoError(t, err)
				require.NotEqual(t, baseHash, got, "%s must affect the spec hash", name)
			})
		}
	})
}

func TestTaskLookup(t *testing.T) {
	spec := linear()
	spec.Normalize()

	task, ok := spec.Task("task_b")
	require.True(t, ok)
	require.Equal(t, "reserve_inventory", task.Activity)

	_, ok = spec.Task("missing")
	require.False(t, ok)
}

// TestValidateReportsAllProblemsAtOnce keeps the API usable: a client fixing a
// bad spec should not have to submit it repeatedly to discover each error.
func TestValidateReportsAllProblemsAtOnce(t *testing.T) {
	spec := domain.WorkflowSpec{
		Name:    "1bad",
		Version: 0,
		Tasks: []domain.TaskSpec{
			{Name: "ok", Activity: ""},
			{Name: "ok", Activity: "noop"},
		},
	}
	spec.Normalize()

	err := spec.Validate()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "workflow name")
	require.Contains(t, msg, "version must be >= 1")
	require.Contains(t, msg, "duplicate task name")
	require.GreaterOrEqual(t, strings.Count(msg, ";"), 2, "problems must be reported together")
}

func requireBefore(t *testing.T, order []string, earlier, later string) {
	t.Helper()
	idx := map[string]int{}
	for i, name := range order {
		idx[name] = i
	}
	require.Less(t, idx[earlier], idx[later], "%s must be ordered before %s", earlier, later)
}
