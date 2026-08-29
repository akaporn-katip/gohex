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
