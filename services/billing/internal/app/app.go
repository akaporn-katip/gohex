// Package app is billing's application layer: command handlers bridging
// the wire contracts to the domain, plus the service's registrations.
package app

import (
	"context"
	"errors"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/services/billing/internal/domain"
	"github.com/akaporn-katip/gohex/services/contracts"
)

// RegisterEvents registers billing's domain events with the store
// registry.
func RegisterEvents(r *eventstore.Registry) {
	eventstore.Register[domain.PaymentCaptured](r)
	eventstore.Register[domain.PaymentFailed](r)
	eventstore.Register[domain.PaymentRefunded](r)
}

// RegisterCommands registers the wire commands this service consumes.
func RegisterCommands(r *cqrs.Registry) {
	cqrs.RegisterCommand[contracts.CapturePayment](r)
	cqrs.RegisterCommand[contracts.RefundPayment](r)
}

// RegisterTranslators maps domain events to the published contracts
// (ADR-0004). Every billing fact is public — the saga depends on them.
func RegisterTranslators(r *relay.Relay) {
	relay.Translate(r, func(e domain.PaymentCaptured) (broker.IntegrationEvent, bool) {
		return contracts.PaymentCapturedV1{OrderID: e.OrderID, Cents: e.Cents}, true
	})
	relay.Translate(r, func(e domain.PaymentFailed) (broker.IntegrationEvent, bool) {
		return contracts.PaymentFailedV1{OrderID: e.OrderID, Reason: e.Reason}, true
	})
	relay.Translate(r, func(e domain.PaymentRefunded) (broker.IntegrationEvent, bool) {
		return contracts.PaymentRefundedV1{OrderID: e.OrderID}, true
	})
}

// Handlers holds the application's dependencies.
type Handlers struct {
	payments *eventstore.Repository[*domain.Payment]
}

func New(store eventstore.Store, registry *eventstore.Registry) *Handlers {
	return &Handlers{
		payments: eventstore.NewRepository(store, registry, domain.Category,
			func() *domain.Payment { return domain.NewPayment() }),
	}
}

// Register wires the handlers onto the command bus.
func (h *Handlers) Register(bus *cqrs.Bus) {
	cqrs.Handle(bus, h.CapturePayment)
	cqrs.Handle(bus, h.RefundPayment)
}

// CapturePayment decides a payment. Replays (consumer crash between
// execute and mark) are safe: an already-decided payment is a no-op.
func (h *Handlers) CapturePayment(ctx context.Context, cmd contracts.CapturePayment) error {
	_, err := h.payments.Load(ctx, eventstore.StringID(cmd.OrderID))
	if err == nil {
		return nil // already decided
	}
	if !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	amount, err := domain.NewAmount(cmd.Cents)
	if err != nil {
		return err
	}
	p, err := domain.Capture(domain.OrderRef(cmd.OrderID), amount)
	if err != nil {
		return err
	}
	return h.payments.Save(ctx, eventstore.StringID(cmd.OrderID), p)
}

// RefundPayment compensates a captured payment.
func (h *Handlers) RefundPayment(ctx context.Context, cmd contracts.RefundPayment) error {
	p, err := h.payments.Load(ctx, eventstore.StringID(cmd.OrderID))
	if err != nil {
		return err // ErrNotFound is a DomainError: definitive rejection
	}
	if err := p.Refund(); err != nil {
		if errors.Is(err, domain.ErrAlreadyRefunded) {
			return nil // replay-safe
		}
		return err
	}
	return h.payments.Save(ctx, eventstore.StringID(cmd.OrderID), p)
}
