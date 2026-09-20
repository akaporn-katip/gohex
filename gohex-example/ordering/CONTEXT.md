# Ordering

Receives and tracks customer orders; hosts the order-fulfillment saga
and the order_summary read model.

## Language

**Order**:
A customer's request to buy a quantity of goods for a total price. The aggregate owns only placement; the rest of the lifecycle is coordinated facts.
_Avoid_: Purchase, basket, cart

**Order Summary**:
The read model answering "where is my order?" — one row per order whose status only ever moves forward in rank.
_Avoid_: Order view, order status table

**Fulfillment**:
The saga-coordinated journey of a placed order: capture payment, reserve stock, ship — or compensate and stop.
_Avoid_: Order processing, workflow

**Customer**:
The buyer, owned by an unmodeled external context and referenced by ID only.

**Customer Notification**:
Telling the customer about a milestone (`shipped`, `payment_failed`). The projection only queues it on the `pending_notification` worklist; the Notifier worker sends it on a later tick and records it on the order.
_Avoid_: Alert, email (the channel is not modeled)

**Notifier**:
Ordering's polling worker: every tick it claims a batch of pending notifications and dispatches one command per row, under a trace linked back to the request that queued it (gohex ADR-0015).
_Avoid_: Job runner, cron, dispatcher
