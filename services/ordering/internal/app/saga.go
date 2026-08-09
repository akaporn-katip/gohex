package app

import (
	"github.com/akaporn-katip/gohex/libs/saga"
	"github.com/akaporn-katip/gohex/services/contracts"
)

// Fulfillment is the saga's state: what the flow still needs to
// remember between steps. Handlers are pure decision functions
// (ADR-0007) — the whole workflow reads top to bottom in this file.
type Fulfillment struct {
	Cents int64
	Qty   int
}

// SagaTopics are the integration-event topics the fulfillment saga
// listens on.
var SagaTopics = []string{
	contracts.TopicOrderingEvents,
	contracts.TopicBillingEvents,
	contracts.TopicInventoryEvents,
	contracts.TopicShippingEvents,
}

// NewFulfillmentSaga defines the order-fulfillment workflow:
//
//	placed -> capture payment -> reserve stock -> ship
//	         payment failed -> end
//	         stock rejected -> refund payment (compensation) -> end
func NewFulfillmentSaga() *saga.Definition[Fulfillment] {
	d := saga.New[Fulfillment]("order_fulfillment")

	saga.OnEvent(d,
		func(e contracts.OrderPlacedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.OrderPlacedV1) (saga.Decisions, error) {
			s.Cents = e.Cents
			s.Qty = e.Qty
			return saga.Send(saga.OutCommand{
				Topic: contracts.TopicBillingCommands,
				Cmd:   contracts.CapturePayment{OrderID: e.OrderID, Cents: e.Cents},
			}), nil
		})

	saga.OnEvent(d,
		func(e contracts.PaymentCapturedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.PaymentCapturedV1) (saga.Decisions, error) {
			return saga.Send(saga.OutCommand{
				Topic: contracts.TopicInventoryCommands,
				Cmd:   contracts.ReserveStock{OrderID: e.OrderID, Qty: s.Qty},
			}), nil
		})

	saga.OnEvent(d,
		func(e contracts.PaymentFailedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.PaymentFailedV1) (saga.Decisions, error) {
			return saga.End(), nil // nothing to compensate: nothing happened yet
		})

	saga.OnEvent(d,
		func(e contracts.StockReservedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.StockReservedV1) (saga.Decisions, error) {
			return saga.Send(saga.OutCommand{
				Topic: contracts.TopicShippingCommands,
				Cmd:   contracts.CreateShipment{OrderID: e.OrderID},
			}), nil
		})

	saga.OnEvent(d,
		func(e contracts.StockRejectedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.StockRejectedV1) (saga.Decisions, error) {
			// Compensation: the payment was captured but stock fell
			// through — undo the money side, then finish.
			return saga.Decisions{
				Commands: []saga.OutCommand{{
					Topic: contracts.TopicBillingCommands,
					Cmd:   contracts.RefundPayment{OrderID: e.OrderID},
				}},
				End: true,
			}, nil
		})

	saga.OnEvent(d,
		func(e contracts.ShipmentDispatchedV1) string { return e.OrderID },
		func(s *Fulfillment, e contracts.ShipmentDispatchedV1) (saga.Decisions, error) {
			return saga.End(), nil
		})

	return d
}
