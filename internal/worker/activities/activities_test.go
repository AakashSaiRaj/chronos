package activities_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/worker"
	"github.com/AakashSaiRaj/chronos/internal/worker/activities"
)

func TestRegisterInstallsTheExampleSet(t *testing.T) {
	r := activities.Register(worker.NewRegistry())

	require.Equal(t, []string{
		"always_fail", "charge_payment", "noop", "reserve_inventory", "send_receipt", "sleep",
	}, r.Names())
}

func orderInput(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(activities.Order{
		OrderID: "A-1", Customer: "buyer@example.com",
		Amount: 42.5, Currency: "EUR", SKU: "sku-9", Quantity: 2,
	})
	require.NoError(t, err)
	return raw
}

func TestChargePayment(t *testing.T) {
	out, err := activities.ChargePayment(context.Background(), worker.ActivityInput{
		TaskName: "task_a", Activity: "charge_payment", Attempt: 1,
		WorkflowInput: orderInput(t),
	})
	require.NoError(t, err)

	result, ok := out.(activities.PaymentResult)
	require.True(t, ok)
	require.NotEmpty(t, result.ChargeID)
	require.InDelta(t, 42.5, result.Amount, 1e-9)
	require.Equal(t, "EUR", result.Currency)
	require.False(t, result.ChargedAt.IsZero())
}

// TestChargePaymentIsDeterministic is the property that makes the activity safe
// to re-run: a retried attempt must not produce a second distinct charge.
func TestChargePaymentIsDeterministic(t *testing.T) {
	in := worker.ActivityInput{TaskName: "task_a", WorkflowInput: orderInput(t)}

	first, err := activities.ChargePayment(context.Background(), in)
	require.NoError(t, err)
	in.Attempt = 2
	second, err := activities.ChargePayment(context.Background(), in)
	require.NoError(t, err)

	require.Equal(t,
		first.(activities.PaymentResult).ChargeID,
		second.(activities.PaymentResult).ChargeID,
		"re-running the same task must reuse the same charge id")
}

func TestChargePaymentValidatesInput(t *testing.T) {
	tests := map[string]string{
		"missing orderId": `{"amount":10}`,
		"zero amount":     `{"orderId":"A-1","amount":0}`,
		"negative amount": `{"orderId":"A-1","amount":-5}`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := activities.ChargePayment(context.Background(), worker.ActivityInput{
				TaskName: "task_a", WorkflowInput: json.RawMessage(input),
			})
			require.Error(t, err)
		})
	}
}

func TestChargePaymentDefaultsCurrency(t *testing.T) {
	out, err := activities.ChargePayment(context.Background(), worker.ActivityInput{
		TaskName:      "task_a",
		WorkflowInput: json.RawMessage(`{"orderId":"A-1","amount":5}`),
	})
	require.NoError(t, err)
	require.Equal(t, "USD", out.(activities.PaymentResult).Currency)
}

func TestReserveInventoryReadsUpstreamOutput(t *testing.T) {
	out, err := activities.ReserveInventory(context.Background(), worker.ActivityInput{
		TaskName:      "task_b",
		WorkflowInput: orderInput(t),
		Upstream: map[string]json.RawMessage{
			"task_a": json.RawMessage(`{"chargeId":"chg_abc","amount":42.5,"currency":"EUR"}`),
		},
	})
	require.NoError(t, err)

	result := out.(activities.ReservationResult)
	require.NotEmpty(t, result.ReservationID)
	require.Equal(t, "sku-9", result.SKU)
	require.Equal(t, 2, result.Quantity)
	require.Equal(t, "chg_abc", result.ChargeID,
		"the upstream charge must be threaded through the pipeline")
}

func TestReserveInventoryRequiresUpstream(t *testing.T) {
	// No upstream at all: the workflow contract was violated.
	_, err := activities.ReserveInventory(context.Background(), worker.ActivityInput{
		TaskName: "task_b", WorkflowInput: orderInput(t),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "task_a")

	// Upstream present but empty: a real but unusable result.
	_, err = activities.ReserveInventory(context.Background(), worker.ActivityInput{
		TaskName:      "task_b",
		WorkflowInput: orderInput(t),
		Upstream:      map[string]json.RawMessage{"task_a": json.RawMessage(`{}`)},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "chargeId")
}

func TestSendReceiptProducesWorkflowResult(t *testing.T) {
	out, err := activities.SendReceipt(context.Background(), worker.ActivityInput{
		TaskName:      "task_c",
		WorkflowInput: orderInput(t),
		Upstream: map[string]json.RawMessage{
			"task_b": json.RawMessage(`{"reservationId":"rsv_1","sku":"sku-9","quantity":2,"chargeId":"chg_abc"}`),
		},
	})
	require.NoError(t, err)

	result := out.(activities.ReceiptResult)
	require.NotEmpty(t, result.ReceiptID)
	require.Equal(t, "buyer@example.com", result.SentTo)
	require.Equal(t, "chg_abc", result.ChargeID)
	require.Equal(t, "rsv_1", result.ReservationID)
	require.Contains(t, result.Summary, "A-1")
	require.Contains(t, result.Summary, "sku-9")
}

func TestNoop(t *testing.T) {
	out, err := activities.Noop(context.Background(), worker.ActivityInput{TaskName: "t", Attempt: 3})
	require.NoError(t, err)

	result := out.(map[string]any)
	require.Equal(t, "t", result["task"])
	require.Equal(t, 3, result["attempt"])
	require.Equal(t, true, result["ok"])
}

func TestSleepHonoursStaticInput(t *testing.T) {
	started := time.Now()
	out, err := activities.Sleep(context.Background(), worker.ActivityInput{
		TaskName:  "t",
		TaskInput: json.RawMessage(`{"duration":"60ms"}`),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 55*time.Millisecond)
	require.Equal(t, "60ms", out.(map[string]any)["sleptFor"])
}

func TestSleepRejectsBadDuration(t *testing.T) {
	_, err := activities.Sleep(context.Background(), worker.ActivityInput{
		TaskName:  "t",
		TaskInput: json.RawMessage(`{"duration":"not-a-duration"}`),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-a-duration")
}

// TestActivitiesRespectCancellation is what lets a worker honour a task timeout
// instead of holding its lease until expiry.
func TestActivitiesRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := activities.Sleep(ctx, worker.ActivityInput{
		TaskName: "t", TaskInput: json.RawMessage(`{"duration":"10s"}`),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	_, err = activities.ChargePayment(ctx, worker.ActivityInput{
		TaskName:      "task_a",
		WorkflowInput: json.RawMessage(`{"orderId":"A-1","amount":5}`),
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestAlwaysFail(t *testing.T) {
	_, err := activities.AlwaysFail(context.Background(), worker.ActivityInput{
		TaskName: "task_x", Attempt: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "task_x")
	require.Contains(t, err.Error(), "attempt 2")

	_, err = activities.AlwaysFail(context.Background(), worker.ActivityInput{
		TaskName:  "task_x",
		TaskInput: json.RawMessage(`{"message":"simulated outage"}`),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated outage")
}

// TestActivityOutputsAreJSONEncodable: the worker marshals whatever an activity
// returns, so an unencodable result would fail the task at report time.
func TestActivityOutputsAreJSONEncodable(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		run  func() (any, error)
	}{
		{"charge_payment", func() (any, error) {
			return activities.ChargePayment(ctx, worker.ActivityInput{
				TaskName: "task_a", WorkflowInput: orderInput(t)})
		}},
		{"reserve_inventory", func() (any, error) {
			return activities.ReserveInventory(ctx, worker.ActivityInput{
				TaskName: "task_b", WorkflowInput: orderInput(t),
				Upstream: map[string]json.RawMessage{"task_a": json.RawMessage(`{"chargeId":"c"}`)}})
		}},
		{"send_receipt", func() (any, error) {
			return activities.SendReceipt(ctx, worker.ActivityInput{
				TaskName: "task_c", WorkflowInput: orderInput(t),
				Upstream: map[string]json.RawMessage{"task_b": json.RawMessage(`{"reservationId":"r","chargeId":"c"}`)}})
		}},
		{"noop", func() (any, error) {
			return activities.Noop(ctx, worker.ActivityInput{TaskName: "t"})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tc.run()
			require.NoError(t, err)

			encoded, err := json.Marshal(out)
			require.NoError(t, err, "activity output must be JSON-encodable")
			require.True(t, json.Valid(encoded))
		})
	}
}
