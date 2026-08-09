// Package app is ordering's application layer: the place-order command,
// the order_summary projection, the fulfillment saga, and the service's
// registrations.
package app

import (
	"context"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/libs/saga"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/ordering/internal/domain"
	"github.com/akaporn-katip/gohex/services/ordering/internal/ports"
)

// RegisterEvents registers ordering's domain events and the saga stream
// events (this service hosts the fulfillment saga).
func RegisterEvents(r *eventstore.Registry) {
	eventstore.Register[domain.OrderPlaced](r)
	saga.RegisterEvents(r)
}

// RegisterTranslators maps ordering's public facts (ADR-0004) and wires
// saga command routing onto the relay.
func RegisterTranslators(r *relay.Relay) {
	relay.Translate(r, func(e domain.OrderPlaced) (broker.IntegrationEvent, bool) {
		return contracts.OrderPlacedV1{
			OrderID:    e.ID.String(),
			CustomerID: e.Customer.String(),
			Cents:      e.Total.Cents(),
			Currency:   e.Total.Currency(),
			Qty:        e.Qty.Int(),
		}, true
	})
	saga.RegisterCommandRouting(r)
}

// PlaceOrder is the service's own command, dispatched locally by the
// HTTP adapter.
type PlaceOrder struct {
	ID       domain.OrderID
	Customer domain.CustomerID
	Total    domain.Money
	Qty      domain.Quantity
}

func (PlaceOrder) CommandName() string { return "ordering.place_order" }

// GetOrder is the read-side query for one order summary.
type GetOrder struct {
	OrderID string
}

func (GetOrder) QueryName() string { return "ordering.get_order" }

// Handlers holds the application's dependencies.
type Handlers struct {
	orders    *eventstore.Repository[*domain.Order]
	summaries ports.SummaryStore
}

func New(store eventstore.Store, registry *eventstore.Registry, summaries ports.SummaryStore) *Handlers {
	return &Handlers{
		orders: eventstore.NewRepository(store, registry, domain.Category,
			func() *domain.Order { return domain.NewOrder() }),
		summaries: summaries,
	}
}

// Register wires the handlers onto the buses.
func (h *Handlers) Register(bus *cqrs.Bus, queries *cqrs.QueryBus) {
	cqrs.Handle(bus, h.PlaceOrder)
	cqrs.HandleQuery(queries, h.GetOrder)
}

func (h *Handlers) PlaceOrder(ctx context.Context, cmd PlaceOrder) error {
	o, err := domain.Place(cmd.ID, cmd.Customer, cmd.Total, cmd.Qty)
	if err != nil {
		return err
	}
	return h.orders.Save(ctx, cmd.ID, o)
}

func (h *Handlers) GetOrder(ctx context.Context, q GetOrder) (ports.OrderSummary, error) {
	return h.summaries.Get(ctx, q.OrderID)
}
