package kernel_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// This file doubles as the kernel's usage example: a minimal Order
// aggregate following every convention — typed ID, value objects instead
// of primitives, DomainError sentinels, and an explicit Apply switch.

// --- value objects (ADR-0011: parse, don't validate) ---

type Money struct {
	cents    int64
	currency string
}

func NewMoney(cents int64, currency string) (Money, error) {
	if cents < 0 {
		return Money{}, kernel.NewDomainError("negative_amount", "amount must not be negative")
	}
	if len(currency) != 3 {
		return Money{}, kernel.NewDomainError("invalid_currency", "currency must be a 3-letter code")
	}
	return Money{cents: cents, currency: currency}, nil
}

func (m Money) IsZero() bool { return m == Money{} }

// --- the aggregate ---

type Order struct {
	kernel.Root

	id        OrderID
	total     Money
	cancelled bool
}

type OrderID = kernel.ID[Order]

var (
	ErrEmptyOrder       = kernel.NewDomainError("empty_order", "an order must have a total")
	ErrAlreadyCancelled = kernel.NewDomainError("already_cancelled", "the order is already cancelled")
)

type OrderPlaced struct {
	ID    OrderID
	Total Money
}

func (OrderPlaced) EventName() string { return "ordering.order_placed" }

type OrderCancelled struct {
	ID OrderID
}

func (OrderCancelled) EventName() string { return "ordering.order_cancelled" }

func PlaceOrder(id OrderID, total Money) (*Order, error) {
	if total.IsZero() {
		return nil, ErrEmptyOrder
	}
	o := &Order{}
	if err := kernel.Raise(o, OrderPlaced{ID: id, Total: total}); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Order) Cancel() error {
	if o.cancelled {
		return ErrAlreadyCancelled
	}
	return kernel.Raise(o, OrderCancelled{ID: o.id})
}

func (o *Order) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case OrderPlaced:
		o.id = ev.ID
		o.total = ev.Total
	case OrderCancelled:
		o.cancelled = true
	default:
		return fmt.Errorf("order: unknown event %q", e.EventName())
	}
	return nil
}

// --- tests ---

func mustMoney(t *testing.T, cents int64) Money {
	t.Helper()
	m, err := NewMoney(cents, "USD")
	if err != nil {
		t.Fatalf("NewMoney: %v", err)
	}
	return m
}

func TestRaiseRecordsAndApplies(t *testing.T) {
	id := kernel.NewID[Order]()
	o, err := PlaceOrder(id, mustMoney(t, 5000))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	if o.id != id {
		t.Errorf("state not applied: id = %v, want %v", o.id, id)
	}
	if got := o.Version(); got != 0 {
		t.Errorf("Version() = %d, want 0 (nothing committed yet)", got)
	}
	events := o.UncommittedEvents()
	if len(events) != 1 {
		t.Fatalf("UncommittedEvents() len = %d, want 1", len(events))
	}
	if events[0].EventName() != "ordering.order_placed" {
		t.Errorf("event name = %q", events[0].EventName())
	}
}

func TestDecideFirstDomainError(t *testing.T) {
	o, err := PlaceOrder(kernel.NewID[Order](), mustMoney(t, 100))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if err := o.Cancel(); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	err = o.Cancel()
	if !errors.Is(err, ErrAlreadyCancelled) {
		t.Fatalf("second Cancel = %v, want ErrAlreadyCancelled", err)
	}
	if got := len(o.UncommittedEvents()); got != 2 {
		t.Errorf("rejected command must record nothing: %d events, want 2", got)
	}
}

func TestFailedApplyRecordsNothing(t *testing.T) {
	o, err := PlaceOrder(kernel.NewID[Order](), mustMoney(t, 100))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	before := len(o.UncommittedEvents())

	err = kernel.Raise(o, unknownEvent{})
	if err == nil {
		t.Fatal("Raise(unknown) = nil, want error")
	}
	if got := len(o.UncommittedEvents()); got != before {
		t.Errorf("failed Apply must not record: %d events, want %d", got, before)
	}
}

type unknownEvent struct{}

func (unknownEvent) EventName() string { return "test.unknown" }

func TestTakeUncommittedDrainsAndAdvancesVersion(t *testing.T) {
	o, err := PlaceOrder(kernel.NewID[Order](), mustMoney(t, 100))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if err := o.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	events := kernel.TakeUncommitted(o)
	if len(events) != 2 {
		t.Fatalf("TakeUncommitted len = %d, want 2", len(events))
	}
	if got := o.Version(); got != 2 {
		t.Errorf("Version() after take = %d, want 2", got)
	}
	if got := len(o.UncommittedEvents()); got != 0 {
		t.Errorf("pending after take = %d, want 0", got)
	}
}

func TestRehydrateRebuildsState(t *testing.T) {
	id := kernel.NewID[Order]()
	total := mustMoney(t, 4200)

	o := &Order{}
	err := kernel.Rehydrate(o,
		OrderPlaced{ID: id, Total: total},
		OrderCancelled{ID: id},
	)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	if o.id != id || o.total != total || !o.cancelled {
		t.Errorf("state not rebuilt: %+v", o)
	}
	if got := o.Version(); got != 2 {
		t.Errorf("Version() = %d, want 2", got)
	}
	if got := len(o.UncommittedEvents()); got != 0 {
		t.Errorf("rehydrate must leave no uncommitted events, got %d", got)
	}
}

func TestRehydrateThenRaise(t *testing.T) {
	id := kernel.NewID[Order]()
	o := &Order{}
	if err := kernel.Rehydrate(o, OrderPlaced{ID: id, Total: mustMoney(t, 100)}); err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	if err := o.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := o.Version(); got != 1 {
		t.Errorf("Version() = %d, want 1 (raise must not advance version)", got)
	}
	if got := len(o.UncommittedEvents()); got != 1 {
		t.Fatalf("uncommitted = %d, want 1", got)
	}

	events := kernel.TakeUncommitted(o)
	if len(events) != 1 || o.Version() != 2 {
		t.Errorf("after take: %d events, version %d; want 1 and 2", len(events), o.Version())
	}
}

func TestRehydrateFailsOnCorruptHistory(t *testing.T) {
	o := &Order{}
	err := kernel.Rehydrate(o, unknownEvent{})
	if err == nil {
		t.Fatal("Rehydrate(unknown) = nil, want error")
	}
	if got := o.Version(); got != 0 {
		t.Errorf("failed rehydrate must not advance version, got %d", got)
	}
}
