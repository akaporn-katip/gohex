# Billing

Captures and refunds payments for orders.

## Language

**Payment**:
One settlement attempt for one order — captured, failed, or later refunded. Both capture outcomes are facts, not errors.
_Avoid_: Transaction, charge

**Capture**:
Taking the customer's money for an order.
_Avoid_: Charge, collect

**Refund**:
Returning a captured payment — billing's compensation when fulfillment falls through downstream.
_Avoid_: Chargeback (that's a dispute, not a compensation)
