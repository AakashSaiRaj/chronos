// Package activities provides the example activity set the demo workflow uses.
//
// These stand in for real side effects (a payment call, an inventory
// reservation, an email). They are written the way production activities should
// be: they read their input explicitly, they derive results deterministically
// from that input, and they are safe to run more than once for the same task.
package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AakashSaiRaj/chronos/internal/worker"
)

// Register installs the example activities into a registry.
func Register(r *worker.Registry) *worker.Registry {
	return r.
		Register("charge_payment", ChargePayment).
		Register("reserve_inventory", ReserveInventory).
		Register("send_receipt", SendReceipt).
		Register("noop", Noop).
		Register("sleep", Sleep).
		Register("always_fail", AlwaysFail)
}

// Order is the workflow input the example pipeline expects.
type Order struct {
	OrderID  string  `json:"orderId"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
}

// PaymentResult is charge_payment's output.
type PaymentResult struct {
	// ChargeID is derived from the order rather than random, so a re-run of the
	// same task produces the same identifier instead of double-charging.
	ChargeID  string    `json:"chargeId"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	ChargedAt time.Time `json:"chargedAt"`
}

// ChargePayment is Task A of the example pipeline.
func ChargePayment(ctx context.Context, in worker.ActivityInput) (any, error) {
	var order Order
	if err := in.UnmarshalWorkflowInput(&order); err != nil {
		return nil, err
	}
	if order.OrderID == "" {
		return nil, errors.New("charge_payment: orderId is required")
	}
	if order.Amount <= 0 {
		return nil, fmt.Errorf("charge_payment: amount must be positive, got %v", order.Amount)
	}
	currency := order.Currency
	if currency == "" {
		currency = "USD"
	}

	if err := simulateWork(ctx, 40*time.Millisecond); err != nil {
		return nil, err
	}

	return PaymentResult{
		ChargeID:  "chg_" + deterministicID(order.OrderID, "charge"),
		Amount:    order.Amount,
		Currency:  currency,
		ChargedAt: time.Now().UTC(),
	}, nil
}

// ReservationResult is reserve_inventory's output.
type ReservationResult struct {
	ReservationID string `json:"reservationId"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	// ChargeID echoes the upstream charge, demonstrating that a task can read
	// its dependency's output.
	ChargeID string `json:"chargeId"`
}

// ReserveInventory is Task B; it depends on Task A's output.
func ReserveInventory(ctx context.Context, in worker.ActivityInput) (any, error) {
	var order Order
	if err := in.UnmarshalWorkflowInput(&order); err != nil {
		return nil, err
	}

	// The upstream task name is part of the workflow contract, so read it by
	// name rather than assuming a position.
	var payment PaymentResult
	if err := in.UnmarshalUpstream("task_a", &payment); err != nil {
		return nil, err
	}
	if payment.ChargeID == "" {
		return nil, errors.New("reserve_inventory: upstream task_a produced no chargeId")
	}

	quantity := order.Quantity
	if quantity <= 0 {
		quantity = 1
	}
	sku := order.SKU
	if sku == "" {
		sku = "sku-unknown"
	}

	if err := simulateWork(ctx, 30*time.Millisecond); err != nil {
		return nil, err
	}

	return ReservationResult{
		ReservationID: "rsv_" + deterministicID(order.OrderID, "reserve"),
		SKU:           sku,
		Quantity:      quantity,
		ChargeID:      payment.ChargeID,
	}, nil
}

// ReceiptResult is send_receipt's output and the example workflow's result.
type ReceiptResult struct {
	ReceiptID     string `json:"receiptId"`
	SentTo        string `json:"sentTo"`
	ChargeID      string `json:"chargeId"`
	ReservationID string `json:"reservationId"`
	Summary       string `json:"summary"`
}

// SendReceipt is Task C; it depends on Task B.
func SendReceipt(ctx context.Context, in worker.ActivityInput) (any, error) {
	var order Order
	if err := in.UnmarshalWorkflowInput(&order); err != nil {
		return nil, err
	}
	var reservation ReservationResult
	if err := in.UnmarshalUpstream("task_b", &reservation); err != nil {
		return nil, err
	}

	recipient := order.Customer
	if recipient == "" {
		recipient = "unknown@example.com"
	}

	if err := simulateWork(ctx, 20*time.Millisecond); err != nil {
		return nil, err
	}

	return ReceiptResult{
		ReceiptID:     "rcpt_" + deterministicID(order.OrderID, "receipt"),
		SentTo:        recipient,
		ChargeID:      reservation.ChargeID,
		ReservationID: reservation.ReservationID,
		Summary: fmt.Sprintf("order %s: %d x %s charged via %s",
			order.OrderID, reservation.Quantity, reservation.SKU, reservation.ChargeID),
	}, nil
}

// Noop succeeds immediately. Useful for fan-out and throughput testing.
func Noop(_ context.Context, in worker.ActivityInput) (any, error) {
	return map[string]any{"task": in.TaskName, "attempt": in.Attempt, "ok": true}, nil
}

// Sleep pauses for the duration named in the task's static input, e.g.
// {"duration": "250ms"}. It exercises timeout and lease behaviour.
func Sleep(ctx context.Context, in worker.ActivityInput) (any, error) {
	spec := struct {
		Duration string `json:"duration"`
	}{}
	if len(in.TaskInput) > 0 {
		if err := unmarshalTaskInput(in, &spec); err != nil {
			return nil, err
		}
	}
	d := 100 * time.Millisecond
	if strings.TrimSpace(spec.Duration) != "" {
		parsed, err := time.ParseDuration(spec.Duration)
		if err != nil {
			return nil, fmt.Errorf("sleep: duration %q is not a valid Go duration: %w", spec.Duration, err)
		}
		d = parsed
	}

	if err := simulateWork(ctx, d); err != nil {
		return nil, err
	}
	return map[string]any{"sleptFor": d.String()}, nil
}

// AlwaysFail returns an error every time. It exists so the failure path — task
// FAILED propagating to workflow FAILED — can be exercised end to end.
func AlwaysFail(_ context.Context, in worker.ActivityInput) (any, error) {
	spec := struct {
		Message string `json:"message"`
	}{}
	if len(in.TaskInput) > 0 {
		if err := unmarshalTaskInput(in, &spec); err != nil {
			return nil, err
		}
	}
	if spec.Message == "" {
		spec.Message = "always_fail activity invoked"
	}
	return nil, fmt.Errorf("%s (task %q attempt %d)", spec.Message, in.TaskName, in.Attempt)
}

// simulateWork stands in for real I/O while staying responsive to cancellation.
func simulateWork(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("activity canceled after %s: %w", d, ctx.Err())
	case <-timer.C:
		return nil
	}
}

// deterministicID derives a stable identifier from the order and a purpose, so
// re-running a task yields the same ID instead of a fresh side effect.
func deterministicID(orderID, purpose string) string {
	sum := sha256.Sum256([]byte(purpose + ":" + orderID))
	return hex.EncodeToString(sum[:])[:16]
}

func unmarshalTaskInput(in worker.ActivityInput, dst any) error {
	if len(in.TaskInput) == 0 {
		return nil
	}
	if err := json.Unmarshal(in.TaskInput, dst); err != nil {
		return fmt.Errorf("decode static input for task %q: %w", in.TaskName, err)
	}
	return nil
}
