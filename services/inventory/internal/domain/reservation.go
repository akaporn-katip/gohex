// Package domain is inventory's domain model: the Reservation aggregate.
// It imports only the kernel (ADR-0009).
package domain

import (
	"fmt"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// AvailableStock is the demo business rule: reservations above this
// quantity are rejected.
const AvailableStock = 10

var ErrInvalidQuantity = kernel.NewDomainError("invalid_quantity", "quantity must be positive")

// Quantity is the amount value object of this context (ADR-0011).
type Quantity struct {
	n int
}

func NewQuantity(n int) (Quantity, error) {
	if n <= 0 {
		return Quantity{}, ErrInvalidQuantity
	}
	return Quantity{n: n}, nil
}

func (q Quantity) Int() int { return q.n }

// OrderRef identifies the order this reservation serves.
type OrderRef string

// --- events ---

type StockReserved struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

func (StockReserved) EventName() string { return "inventory.stock_reserved" }

type StockRejected struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (StockRejected) EventName() string { return "inventory.stock_rejected" }

// Category is the aggregate's stream category.
const Category = "reservation"

// Reservation is the aggregate: one reservation attempt per order.
type Reservation struct {
	kernel.Root

	order   OrderRef
	decided bool
}

func NewReservation() *Reservation { return &Reservation{} }

// Decided reports whether this reservation was already decided.
func (r *Reservation) Decided() bool { return r.decided }

// Reserve decides the reservation. Both outcomes are facts (ADR-0012):
// a rejection is recorded so the saga can compensate on it.
func Reserve(order OrderRef, qty Quantity) (*Reservation, error) {
	r := NewReservation()
	if qty.Int() > AvailableStock {
		if err := kernel.Raise(r, StockRejected{OrderID: string(order), Reason: "insufficient stock"}); err != nil {
			return nil, err
		}
		return r, nil
	}
	if err := kernel.Raise(r, StockReserved{OrderID: string(order), Qty: qty.Int()}); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reservation) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case StockReserved:
		r.order = OrderRef(ev.OrderID)
		r.decided = true
	case StockRejected:
		r.order = OrderRef(ev.OrderID)
		r.decided = true
	default:
		return fmt.Errorf("reservation: unknown event %q", e.EventName())
	}
	return nil
}
