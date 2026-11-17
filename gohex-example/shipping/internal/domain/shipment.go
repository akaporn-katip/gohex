// Package domain is shipping's domain model: the Shipment aggregate.
// It imports only the kernel (ADR-0009).
package domain

import (
	"fmt"

	"github.com/akaporn-katip/gohex/kernel"
)

// OrderRef identifies the order this shipment fulfills.
type OrderRef string

// --- events ---

type ShipmentDispatched struct {
	OrderID string `json:"order_id"`
	Express bool   `json:"express"`
}

func (ShipmentDispatched) EventName() string { return "shipping.shipment_dispatched" }

// Category is the aggregate's stream category.
const Category = "shipment"

// Shipment is the aggregate: one shipment per order. The demo warehouse
// is infinitely efficient — creation dispatches immediately.
type Shipment struct {
	kernel.Root

	order      OrderRef
	express    bool
	dispatched bool
}

func NewShipment() *Shipment { return &Shipment{} }

// Dispatched reports whether the shipment already went out.
func (s *Shipment) Dispatched() bool { return s.dispatched }

// Express reports whether the order was placed under the express
// shipping decision the ordering edge made.
func (s *Shipment) Express() bool { return s.express }

// Create dispatches a shipment for the order.
func Create(order OrderRef, express bool) (*Shipment, error) {
	s := NewShipment()
	if err := kernel.Raise(s, ShipmentDispatched{OrderID: string(order), Express: express}); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Shipment) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case ShipmentDispatched:
		s.order = OrderRef(ev.OrderID)
		s.express = ev.Express
		s.dispatched = true
	default:
		return fmt.Errorf("shipment: unknown event %q", e.EventName())
	}
	return nil
}
