// Package domain is billing's domain model: the Payment aggregate and
// its events. It imports only the kernel (ADR-0009).
package domain

import (
	"fmt"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// CaptureLimitCents is the demo business rule: captures above this
// amount are declined.
const CaptureLimitCents = 99_999

// Errors.
var (
	ErrNotCaptured     = kernel.NewDomainError("not_captured", "the payment was never captured")
	ErrAlreadyRefunded = kernel.NewDomainError("already_refunded", "the payment is already refunded")
	ErrInvalidAmount   = kernel.NewDomainError("invalid_amount", "amount must be positive")
)

// Amount is the money value object of this context (ADR-0011).
type Amount struct {
	cents int64
}

func NewAmount(cents int64) (Amount, error) {
	if cents <= 0 {
		return Amount{}, ErrInvalidAmount
	}
	return Amount{cents: cents}, nil
}

func (a Amount) Cents() int64 { return a.cents }

// OrderRef identifies the order this payment settles — a reference into
// the ordering context, opaque here.
type OrderRef string

// --- events ---

type PaymentCaptured struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (PaymentCaptured) EventName() string { return "billing.payment_captured" }

type PaymentFailed struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (PaymentFailed) EventName() string { return "billing.payment_failed" }

type PaymentRefunded struct {
	OrderID string `json:"order_id"`
}

func (PaymentRefunded) EventName() string { return "billing.payment_refunded" }

// Category is the aggregate's stream category.
const Category = "payment"

type status int

const (
	statusNone status = iota
	statusCaptured
	statusFailed
	statusRefunded
)

// Payment is the aggregate: one payment attempt per order.
type Payment struct {
	kernel.Root

	order  OrderRef
	status status
}

func NewPayment() *Payment { return &Payment{} }

// Capture decides the payment. Both outcomes are facts, not errors: a
// declined capture is recorded as PaymentFailed so the saga can
// compensate on it (ADR-0012).
func Capture(order OrderRef, amount Amount) (*Payment, error) {
	p := NewPayment()
	if amount.Cents() > CaptureLimitCents {
		if err := kernel.Raise(p, PaymentFailed{OrderID: string(order), Reason: "amount exceeds capture limit"}); err != nil {
			return nil, err
		}
		return p, nil
	}
	if err := kernel.Raise(p, PaymentCaptured{OrderID: string(order), Cents: amount.Cents()}); err != nil {
		return nil, err
	}
	return p, nil
}

// Refund compensates a captured payment.
func (p *Payment) Refund() error {
	switch p.status {
	case statusRefunded:
		return ErrAlreadyRefunded
	case statusCaptured:
		return kernel.Raise(p, PaymentRefunded{OrderID: string(p.order)})
	default:
		return ErrNotCaptured
	}
}

func (p *Payment) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case PaymentCaptured:
		p.order = OrderRef(ev.OrderID)
		p.status = statusCaptured
	case PaymentFailed:
		p.order = OrderRef(ev.OrderID)
		p.status = statusFailed
	case PaymentRefunded:
		p.status = statusRefunded
	default:
		return fmt.Errorf("payment: unknown event %q", e.EventName())
	}
	return nil
}
