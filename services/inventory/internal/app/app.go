// Package app is inventory's application layer.
package app

import (
	"context"
	"errors"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/inventory/internal/domain"
)

func RegisterEvents(r *eventstore.Registry) {
	eventstore.Register[domain.StockReserved](r)
	eventstore.Register[domain.StockRejected](r)
}

func RegisterCommands(r *cqrs.Registry) {
	cqrs.RegisterCommand[contracts.ReserveStock](r)
}

func RegisterTranslators(r *relay.Relay) {
	relay.Translate(r, func(e domain.StockReserved) (broker.IntegrationEvent, bool) {
		return contracts.StockReservedV1{OrderID: e.OrderID, Qty: e.Qty}, true
	})
	relay.Translate(r, func(e domain.StockRejected) (broker.IntegrationEvent, bool) {
		return contracts.StockRejectedV1{OrderID: e.OrderID, Reason: e.Reason}, true
	})
}

type Handlers struct {
	reservations *eventstore.Repository[*domain.Reservation]
}

func New(store eventstore.Store, registry *eventstore.Registry) *Handlers {
	return &Handlers{
		reservations: eventstore.NewRepository(store, registry, domain.Category,
			func() *domain.Reservation { return domain.NewReservation() }),
	}
}

func (h *Handlers) Register(bus *cqrs.Bus) {
	cqrs.Handle(bus, h.ReserveStock)
}

// ReserveStock decides a reservation; replays are no-ops.
func (h *Handlers) ReserveStock(ctx context.Context, cmd contracts.ReserveStock) error {
	_, err := h.reservations.Load(ctx, eventstore.StringID(cmd.OrderID))
	if err == nil {
		return nil // already decided
	}
	if !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	qty, err := domain.NewQuantity(cmd.Qty)
	if err != nil {
		return err
	}
	r, err := domain.Reserve(domain.OrderRef(cmd.OrderID), qty)
	if err != nil {
		return err
	}
	return h.reservations.Save(ctx, eventstore.StringID(cmd.OrderID), r)
}
