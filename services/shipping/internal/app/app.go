// Package app is shipping's application layer.
package app

import (
	"context"
	"errors"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/shipping/internal/domain"
)

func RegisterEvents(r *eventstore.Registry) {
	eventstore.Register[domain.ShipmentDispatched](r)
}

func RegisterCommands(r *cqrs.Registry) {
	cqrs.RegisterCommand[contracts.CreateShipment](r)
}

func RegisterTranslators(r *relay.Relay) {
	relay.Translate(r, func(e domain.ShipmentDispatched) (broker.IntegrationEvent, bool) {
		return contracts.ShipmentDispatchedV1{OrderID: e.OrderID}, true
	})
}

type Handlers struct {
	shipments *eventstore.Repository[*domain.Shipment]
}

func New(store eventstore.Store, registry *eventstore.Registry) *Handlers {
	return &Handlers{
		shipments: eventstore.NewRepository(store, registry, domain.Category,
			func() *domain.Shipment { return domain.NewShipment() }),
	}
}

func (h *Handlers) Register(bus *cqrs.Bus) {
	cqrs.Handle(bus, h.CreateShipment)
}

// CreateShipment dispatches a shipment; replays are no-ops.
func (h *Handlers) CreateShipment(ctx context.Context, cmd contracts.CreateShipment) error {
	_, err := h.shipments.Load(ctx, eventstore.StringID(cmd.OrderID))
	if err == nil {
		return nil // already dispatched
	}
	if !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	s, err := domain.Create(domain.OrderRef(cmd.OrderID))
	if err != nil {
		return err
	}
	return h.shipments.Save(ctx, eventstore.StringID(cmd.OrderID), s)
}
