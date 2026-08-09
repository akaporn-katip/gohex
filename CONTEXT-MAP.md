# Context Map

## Contexts

- [gohex Framework](./CONTEXT.md) — the framework's own language: aggregates, events, relay, sagas, projections
- [Ordering](./services/ordering/CONTEXT.md) — receives and tracks customer orders
- [Billing](./services/billing/CONTEXT.md) — captures and refunds payments
- [Inventory](./services/inventory/CONTEXT.md) — tracks stock levels and reservations
- [Shipping](./services/shipping/CONTEXT.md) — creates and dispatches shipments

## Relationships

All inter-context communication is asynchronous via the broker (ADR-0008): integration events out, commands in. No context reads another's database or calls it synchronously.

- **Ordering → all**: emits `OrderPlacedV1`; the order-fulfillment saga (hosted in Ordering) drives the flow
- **Saga → Billing**: `CapturePayment` / `RefundPayment` commands; Billing answers with `PaymentCapturedV1` / `PaymentFailedV1` / `PaymentRefundedV1`
- **Saga → Inventory**: `ReserveStock` / `ReleaseStock` commands; Inventory answers with `StockReservedV1` / `StockRejectedV1`
- **Saga → Shipping**: `CreateShipment` command; Shipping answers with `ShipmentDispatchedV1`
- **Billing → Ordering (read side)**: Ordering's `order_summary` projection consumes `PaymentCapturedV1` to show payment status
