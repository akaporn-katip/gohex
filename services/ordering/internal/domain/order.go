// Package domain is ordering's domain model: the Order aggregate and its
// value objects. It imports only the kernel (ADR-0009); every field is a
// value object, never a bare primitive (ADR-0011).
package domain

import (
	"encoding/json"
	"fmt"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// Errors.
var (
	ErrEmptyOrder      = kernel.NewDomainError("empty_order", "an order needs a positive total")
	ErrInvalidAmount   = kernel.NewDomainError("invalid_amount", "amount must be positive")
	ErrInvalidCurrency = kernel.NewDomainError("invalid_currency", "currency must be a 3-letter code")
	ErrInvalidQuantity = kernel.NewDomainError("invalid_quantity", "quantity must be positive")
)

// Customer is the marker type for customer identifiers; customers are
// owned by another (unmodeled) context and referenced by ID only.
type Customer struct{}

type (
	OrderID    = kernel.ID[Order]
	CustomerID = kernel.ID[Customer]
)

// Money is an amount in a currency. Immutable; the zero value is
// invalid; construct only via NewMoney (parse, don't validate).
type Money struct {
	cents    int64
	currency string
}

func NewMoney(cents int64, currency string) (Money, error) {
	if cents <= 0 {
		return Money{}, ErrInvalidAmount
	}
	if len(currency) != 3 {
		return Money{}, ErrInvalidCurrency
	}
	return Money{cents: cents, currency: currency}, nil
}

func (m Money) Cents() int64     { return m.cents }
func (m Money) Currency() string { return m.currency }
func (m Money) IsZero() bool     { return m == Money{} }

// moneyJSON is Money's stable serialized shape. Changing it is an event
// schema change and needs an upcaster (ADR-0010).
type moneyJSON struct {
	Cents    int64  `json:"cents"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if m.IsZero() {
		return nil, fmt.Errorf("%w: marshaling zero Money", ErrInvalidAmount)
	}
	return json.Marshal(moneyJSON{Cents: m.cents, Currency: m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var raw moneyJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewMoney(raw.Cents, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

// Quantity is a count of ordered units. The zero value is invalid.
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

func (q Quantity) MarshalJSON() ([]byte, error) {
	if q.n <= 0 {
		return nil, fmt.Errorf("%w: marshaling zero Quantity", ErrInvalidQuantity)
	}
	return json.Marshal(q.n)
}

func (q *Quantity) UnmarshalJSON(data []byte) error {
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	parsed, err := NewQuantity(n)
	if err != nil {
		return err
	}
	*q = parsed
	return nil
}

// --- events ---

type OrderPlaced struct {
	ID       OrderID    `json:"id"`
	Customer CustomerID `json:"customer"`
	Total    Money      `json:"total"`
	Qty      Quantity   `json:"qty"`
}

func (OrderPlaced) EventName() string { return "ordering.order_placed" }

// Category is the aggregate's stream category.
const Category = "order"

// Order is the aggregate. Its lifecycle beyond placement (payment,
// stock, shipment) is coordinated by the fulfillment saga and reflected
// in the order_summary read model — the aggregate itself only owns the
// facts it can decide alone.
type Order struct {
	kernel.Root

	id       OrderID
	customer CustomerID
	total    Money
	qty      Quantity
}

func NewOrder() *Order { return &Order{} }

// Place creates an order.
func Place(id OrderID, customer CustomerID, total Money, qty Quantity) (*Order, error) {
	if total.IsZero() {
		return nil, ErrEmptyOrder
	}
	o := NewOrder()
	if err := kernel.Raise(o, OrderPlaced{ID: id, Customer: customer, Total: total, Qty: qty}); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Order) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case OrderPlaced:
		o.id = ev.ID
		o.customer = ev.Customer
		o.total = ev.Total
		o.qty = ev.Qty
	default:
		return fmt.Errorf("order: unknown event %q", e.EventName())
	}
	return nil
}
