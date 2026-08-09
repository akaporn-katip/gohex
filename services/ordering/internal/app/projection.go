package app

import (
	"context"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/projection"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/ordering/internal/domain"
	"github.com/akaporn-katip/gohex/services/ordering/internal/ports"
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
func NewOrderSummaryProjection(store ports.SummaryStore) *projection.Projection {
	p := projection.New("order_summary")

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
	projection.OnIntegration(p, func(ctx context.Context, e contracts.PaymentFailedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "payment_failed", ports.RankPaymentFailed)
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
	projection.OnIntegration(p, func(ctx context.Context, e contracts.ShipmentDispatchedV1, _ broker.Message) error {
		return store.SetStatus(ctx, e.OrderID, "shipped", ports.RankShipped)
	})
	return p
}
