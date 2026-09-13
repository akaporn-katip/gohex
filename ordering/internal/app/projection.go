package app

import (
	"context"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/projection"
	"github.com/akaporn-katip/gohex-example/contracts"
	"github.com/akaporn-katip/gohex-example/ordering/internal/domain"
	"github.com/akaporn-katip/gohex-example/ordering/internal/ports"
)

// ForeignTopics are the topics whose integration events feed the
// order_summary projection through the inbox (ADR-0006).
var ForeignTopics = []string{
	contracts.TopicBillingEvents,
	contracts.TopicInventoryEvents,
	contracts.TopicShippingEvents,
}

// NewOrderSummaryProjection builds the order_summary read model from
// both sources: ordering's own OrderPlaced plus the billing, inventory,
// and shipping facts. All handlers are idempotent and commutative —
// SetStatus only ever raises the status rank, so cross-source ordering
// doesn't matter (ADR-0006).
// Terminal milestones the customer is told about. The projection only
// QUEUES them (queueing is a write to its own read side); the Notifier
// worker acts on them later.
const (
	ReasonShipped       = "shipped"
	ReasonPaymentFailed = "payment_failed"
)

func NewOrderSummaryProjection(store ports.SummaryStore, queue ports.NotificationQueue) *projection.Projection {
	p := projection.New("order_summary")

	// enqueue captures the ORIGIN trace context of the fact that created
	// the work — msg.Metadata is the envelope the relay published, whose
	// traceparent goes all the way back to the HTTP request. The worker
	// replays it as a link, not as a parent (ADR-0015).
	enqueue := func(ctx context.Context, orderID, reason string, msg broker.Message) error {
		return queue.Enqueue(ctx, orderID, reason, msg.Metadata)
	}

	projection.On(p, func(ctx context.Context, e domain.OrderPlaced, m projection.Meta) error {
		return store.UpsertPlaced(ctx, ports.OrderSummary{
			OrderID:    e.ID.String(),
			CustomerID: e.Customer.String(),
			Cents:      e.Total.Cents(),
			Currency:   e.Total.Currency(),
			Qty:        e.Qty.Int(),
			Status:     "placed",
			PlacedAt:   m.OccurredAt,
		})
	})

	projection.OnIntegration(p, func(ctx context.Context, e contracts.PaymentCapturedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "paid", ports.RankPaid)
	})
	projection.OnIntegration(p, func(ctx context.Context, e contracts.PaymentFailedV1, msg broker.Message) error {
		if err := store.SetStatus(ctx, e.OrderID, "payment_failed", ports.RankPaymentFailed); err != nil {
			return err
		}
		return enqueue(ctx, e.OrderID, ReasonPaymentFailed, msg)
	})
	projection.OnIntegration(p, func(ctx context.Context, e contracts.StockReservedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "reserved", ports.RankReserved)
	})
	projection.OnIntegration(p, func(ctx context.Context, e contracts.StockRejectedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "rejected", ports.RankRejected)
	})
	projection.OnIntegration(p, func(ctx context.Context, e contracts.PaymentRefundedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "refunded", ports.RankRefunded)
	})
	projection.OnIntegration(p, func(ctx context.Context, e contracts.ShipmentDispatchedV1, msg broker.Message) error {
		if err := store.SetStatus(ctx, e.OrderID, "shipped", ports.RankShipped); err != nil {
			return err
		}
		return enqueue(ctx, e.OrderID, ReasonShipped, msg)
	})
	return p
}
