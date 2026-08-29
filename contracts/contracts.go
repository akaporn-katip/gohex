// Package contracts is the example system's public contract catalog:
// every versioned integration event and command that crosses a service
// boundary (ADR-0004, ADR-0008), plus the topic names they travel on.
//
// Contracts are deliberately boring: flat structs of primitives with
// stable JSON shapes. Once published, a version never changes — evolution
// means adding OrderPlacedV2 alongside V1. This module depends on
// nothing; the framework's IntegrationEvent and Command interfaces are
// satisfied structurally.
package contracts

// Topics.
const (
	TopicOrderingEvents  = "ordering.events"
	TopicBillingEvents   = "billing.events"
	TopicInventoryEvents = "inventory.events"
	TopicShippingEvents  = "shipping.events"

	TopicBillingCommands   = "billing.commands"
	TopicInventoryCommands = "inventory.commands"
	TopicShippingCommands  = "shipping.commands"
)

// --- ordering: integration events ---

type OrderPlacedV1 struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Cents      int64  `json:"cents"`
	Currency   string `json:"currency"`
	Qty        int    `json:"qty"`
}

func (OrderPlacedV1) EventName() string    { return "ordering.order_placed" }
func (OrderPlacedV1) ContractVersion() int { return 1 }

// --- billing: integration events ---

type PaymentCapturedV1 struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (PaymentCapturedV1) EventName() string    { return "billing.payment_captured" }
func (PaymentCapturedV1) ContractVersion() int { return 1 }

type PaymentFailedV1 struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (PaymentFailedV1) EventName() string    { return "billing.payment_failed" }
func (PaymentFailedV1) ContractVersion() int { return 1 }

type PaymentRefundedV1 struct {
	OrderID string `json:"order_id"`
}

func (PaymentRefundedV1) EventName() string    { return "billing.payment_refunded" }
func (PaymentRefundedV1) ContractVersion() int { return 1 }

// --- inventory: integration events ---

type StockReservedV1 struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

func (StockReservedV1) EventName() string    { return "inventory.stock_reserved" }
func (StockReservedV1) ContractVersion() int { return 1 }

type StockRejectedV1 struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (StockRejectedV1) EventName() string    { return "inventory.stock_rejected" }
func (StockRejectedV1) ContractVersion() int { return 1 }

// --- shipping: integration events ---

type ShipmentDispatchedV1 struct {
	OrderID string `json:"order_id"`
}

func (ShipmentDispatchedV1) EventName() string    { return "shipping.shipment_dispatched" }
func (ShipmentDispatchedV1) ContractVersion() int { return 1 }

// --- commands ---

type CapturePayment struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (CapturePayment) CommandName() string { return "billing.capture_payment" }

type RefundPayment struct {
	OrderID string `json:"order_id"`
}

func (RefundPayment) CommandName() string { return "billing.refund_payment" }

type ReserveStock struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

func (ReserveStock) CommandName() string { return "inventory.reserve_stock" }

type CreateShipment struct {
	OrderID string `json:"order_id"`
}

func (CreateShipment) CommandName() string { return "shipping.create_shipment" }
