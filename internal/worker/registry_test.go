package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/worker"
)

func noop(context.Context, worker.ActivityInput) (any, error) { return nil, nil }

func TestRegistryRegisterAndLookup(t *testing.T) {
	r := worker.NewRegistry()
	require.Zero(t, r.Len())
	require.Empty(t, r.Names())

	r.Register("charge_payment", noop).Register("send_receipt", noop)

	require.Equal(t, 2, r.Len())
	require.Equal(t, []string{"charge_payment", "send_receipt"}, r.Names(),
		"names must be sorted so worker registration is deterministic")

	fn, err := r.Lookup("charge_payment")
	require.NoError(t, err)
	require.NotNil(t, fn)
}

// TestRegistryLookupMissingActivity: the control plane should never lease an
// unadvertised activity, so this path indicates a stale registration and must be
// reported rather than silently ignored.
func TestRegistryLookupMissingActivity(t *testing.T) {
	r := worker.NewRegistry().Register("noop", noop)

	_, err := r.Lookup("does_not_exist")
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Contains(t, err.Error(), "does_not_exist")
}

// TestRegistryRejectsDuplicateRegistration fails loudly at startup rather than
// silently shadowing an implementation and running the wrong side effect.
func TestRegistryRejectsDuplicateRegistration(t *testing.T) {
	r := worker.NewRegistry().Register("noop", noop)
	require.PanicsWithValue(t, "worker: activity noop is already registered", func() {
		r.Register("noop", noop)
	})
}

func TestRegistryRejectsInvalidRegistration(t *testing.T) {
	require.Panics(t, func() { worker.NewRegistry().Register("", noop) },
		"an empty activity name is a programming error")
	require.Panics(t, func() { worker.NewRegistry().Register("x", nil) },
		"a nil implementation is a programming error")
}

// TestRegistryIsSafeForConcurrentReads matters because every polling goroutine
// calls Names and Lookup on the shared registry.
func TestRegistryIsSafeForConcurrentReads(t *testing.T) {
	r := worker.NewRegistry().Register("a", noop).Register("b", noop)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Len(t, r.Names(), 2)
			_, err := r.Lookup("a")
			require.NoError(t, err)
			require.Equal(t, 2, r.Len())
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Activity input helpers
// ---------------------------------------------------------------------------

func TestUnmarshalWorkflowInput(t *testing.T) {
	in := worker.ActivityInput{
		TaskName:      "task_a",
		WorkflowInput: json.RawMessage(`{"orderId":"A-1","amount":42.5}`),
	}

	var order struct {
		OrderID string  `json:"orderId"`
		Amount  float64 `json:"amount"`
	}
	require.NoError(t, in.UnmarshalWorkflowInput(&order))
	require.Equal(t, "A-1", order.OrderID)
	require.InDelta(t, 42.5, order.Amount, 1e-9)
}

func TestUnmarshalWorkflowInputHandlesAbsentInput(t *testing.T) {
	in := worker.ActivityInput{TaskName: "task_a"}

	var dst map[string]any
	require.NoError(t, in.UnmarshalWorkflowInput(&dst),
		"a workflow started without input must not be an error")
	require.Nil(t, dst)
}

func TestUnmarshalWorkflowInputReportsBadJSON(t *testing.T) {
	in := worker.ActivityInput{
		TaskName:      "task_a",
		WorkflowInput: json.RawMessage(`{"orderId":`),
	}

	var dst map[string]any
	err := in.UnmarshalWorkflowInput(&dst)
	require.Error(t, err)
	require.Contains(t, err.Error(), "task_a", "the error must identify the task")
}

func TestUnmarshalUpstream(t *testing.T) {
	in := worker.ActivityInput{
		TaskName: "task_b",
		Upstream: map[string]json.RawMessage{
			"task_a": json.RawMessage(`{"chargeId":"chg_1"}`),
		},
	}

	var payment struct {
		ChargeID string `json:"chargeId"`
	}
	require.NoError(t, in.UnmarshalUpstream("task_a", &payment))
	require.Equal(t, "chg_1", payment.ChargeID)
}

// TestUnmarshalUpstreamRejectsUnknownDependency catches a workflow-contract bug:
// reading a dependency the task never declared.
func TestUnmarshalUpstreamRejectsUnknownDependency(t *testing.T) {
	in := worker.ActivityInput{
		TaskName: "task_b",
		Upstream: map[string]json.RawMessage{"task_a": json.RawMessage(`{}`)},
	}

	var dst map[string]any
	err := in.UnmarshalUpstream("task_z", &dst)
	require.Error(t, err)
	require.Contains(t, err.Error(), "task_z")
	require.Contains(t, err.Error(), "task_b")
}

func TestUnmarshalUpstreamReportsBadJSON(t *testing.T) {
	in := worker.ActivityInput{
		TaskName: "task_b",
		Upstream: map[string]json.RawMessage{"task_a": json.RawMessage(`{oops`)},
	}

	var dst map[string]any
	require.Error(t, in.UnmarshalUpstream("task_a", &dst))
}

// TestActivityInputRoundTripsTaskPayload confirms the worker-side view matches
// exactly what the engine persists at schedule time.
func TestActivityInputRoundTripsTaskPayload(t *testing.T) {
	persisted := domain.TaskInput{
		WorkflowInput: json.RawMessage(`{"orderId":"A-1"}`),
		TaskInput:     json.RawMessage(`{"warehouse":"eu-1"}`),
		Upstream: map[string]json.RawMessage{
			"task_a": json.RawMessage(`{"chargeId":"chg_1"}`),
		},
	}
	encoded, err := json.Marshal(persisted)
	require.NoError(t, err)

	var decoded domain.TaskInput
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	in := worker.ActivityInput{
		TaskName:      "task_b",
		WorkflowInput: decoded.WorkflowInput,
		TaskInput:     decoded.TaskInput,
		Upstream:      decoded.Upstream,
	}

	var order struct {
		OrderID string `json:"orderId"`
	}
	require.NoError(t, in.UnmarshalWorkflowInput(&order))
	require.Equal(t, "A-1", order.OrderID)

	var upstream struct {
		ChargeID string `json:"chargeId"`
	}
	require.NoError(t, in.UnmarshalUpstream("task_a", &upstream))
	require.Equal(t, "chg_1", upstream.ChargeID)
	require.JSONEq(t, `{"warehouse":"eu-1"}`, string(in.TaskInput))
}

var errActivity = errors.New("activity failed")

func TestActivityFuncSignature(t *testing.T) {
	// A compile-time-ish check that the documented shape is usable, including
	// returning a plain struct as output.
	var fn worker.ActivityFunc = func(_ context.Context, in worker.ActivityInput) (any, error) {
		if in.TaskName == "boom" {
			return nil, errActivity
		}
		return map[string]any{"task": in.TaskName}, nil
	}

	out, err := fn(context.Background(), worker.ActivityInput{TaskName: "task_a"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"task": "task_a"}, out)

	_, err = fn(context.Background(), worker.ActivityInput{TaskName: "boom"})
	require.ErrorIs(t, err, errActivity)
}
