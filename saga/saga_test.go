package saga_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/relay"
	"github.com/akaporn-katip/gohex/saga"
)

// --- integration events the saga reacts to ---

type orderPlacedV1 struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (orderPlacedV1) EventName() string    { return "ordering.order_placed" }
func (orderPlacedV1) ContractVersion() int { return 1 }

type paymentCapturedV1 struct {
	OrderID string `json:"order_id"`
}

func (paymentCapturedV1) EventName() string    { return "billing.payment_captured" }
func (paymentCapturedV1) ContractVersion() int { return 1 }

type stockRejectedV1 struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (stockRejectedV1) EventName() string    { return "inventory.stock_rejected" }
func (stockRejectedV1) ContractVersion() int { return 1 }

// --- commands the saga sends ---

type capturePayment struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (capturePayment) CommandName() string { return "billing.capture_payment" }

type reserveStock struct {
	OrderID string `json:"order_id"`
}

func (reserveStock) CommandName() string { return "inventory.reserve_stock" }

type refundPayment struct {
	OrderID string `json:"order_id"`
}

func (refundPayment) CommandName() string { return "billing.refund_payment" }

// --- the saga: place -> capture -> reserve; stock rejection compensates ---

type fulfillment struct {
	Step  string
	Cents int64
}

func newDefinition() *saga.Definition[fulfillment] {
	d := saga.New[fulfillment]("order_fulfillment")
	saga.OnEvent(d,
		func(e orderPlacedV1) string { return e.OrderID },
		func(s *fulfillment, e orderPlacedV1) (saga.Decisions, error) {
			s.Step = "awaiting_payment"
			s.Cents = e.Cents
			return saga.Send(saga.OutCommand{
				Topic: "billing.commands",
				Cmd:   capturePayment{OrderID: e.OrderID, Cents: e.Cents},
			}), nil
		})
	saga.OnEvent(d,
		func(e paymentCapturedV1) string { return e.OrderID },
		func(s *fulfillment, e paymentCapturedV1) (saga.Decisions, error) {
			s.Step = "awaiting_stock"
			return saga.Send(saga.OutCommand{
				Topic: "inventory.commands",
				Cmd:   reserveStock{OrderID: e.OrderID},
			}), nil
		})
	saga.OnEvent(d,
		func(e stockRejectedV1) string { return e.OrderID },
		func(s *fulfillment, e stockRejectedV1) (saga.Decisions, error) {
			// Compensation is just another decision.
			s.Step = "compensated"
			return saga.Decisions{
				Commands: []saga.OutCommand{{
					Topic: "billing.commands",
					Cmd:   refundPayment{OrderID: e.OrderID},
				}},
				End: true,
			}, nil
		})
	return d
}

// --- fixture: saga runner + relay wired over memory adapters ---

type fixture struct {
	t      *testing.T
	def    *saga.Definition[fulfillment]
	store  *eventstore.MemoryStore
	broker *broker.MemoryBroker
	runner *saga.Runner[fulfillment]
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	registry := eventstore.NewRegistry()
	saga.RegisterEvents(registry)

	f := &fixture{
		t:      t,
		def:    newDefinition(),
		store:  eventstore.NewMemoryStore(),
		broker: broker.NewMemoryBroker(),
	}
	f.runner = saga.NewRunner(f.def, f.store, registry)

	// The service's relay: routes saga commands to their topics.
	rly := relay.New(f.store, registry, f.broker, eventstore.NewMemoryCheckpointStore(), relay.Config{
		Name: "ordering.relay", Topic: "ordering.events",
		PollInterval: 2 * time.Millisecond, PublishBackoff: 2 * time.Millisecond,
	})
	saga.RegisterCommandRouting(rly)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = rly.Run(ctx) }()
	go func() {
		_ = f.runner.Run(ctx, f.broker, "ordering.order_fulfillment",
			"ordering.events.self", "billing.events", "inventory.events")
	}()
	return f
}

// deliver publishes an integration event the saga listens on.
func (f *fixture) deliver(topic, id string, e broker.IntegrationEvent) {
	f.t.Helper()
	payload, err := json.Marshal(e)
	if err != nil {
		f.t.Fatal(err)
	}
	msg := broker.Message{
		ID: id, Key: "42", Type: e.EventName(), Version: e.ContractVersion(),
		OccurredAt: time.Now().UTC(), Payload: payload,
		Metadata: map[string]string{"traceparent": "00-abc-def-01"},
	}
	if err := f.broker.Publish(context.Background(), topic, msg); err != nil {
		f.t.Fatal(err)
	}
}

var collectorSeq atomic.Int64

// commands collects the first n command messages on a topic, subscribing
// with a fresh group each call (so it always reads from the beginning).
func (f *fixture) commands(topic string, n int) []broker.Message {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := fmt.Sprintf("collector-%d", collectorSeq.Add(1))
	var mu sync.Mutex
	var msgs []broker.Message
	go func() {
		_ = f.broker.Subscribe(ctx, topic, group, func(_ context.Context, m broker.Message) error {
			mu.Lock()
			defer mu.Unlock()
			msgs = append(msgs, m)
			return nil
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if len(msgs) >= n {
			out := append([]broker.Message(nil), msgs...)
			mu.Unlock()
			return out
		}
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	f.t.Fatalf("timed out: %d of %d commands on %s", len(msgs), n, topic)
	return nil
}

func (f *fixture) streamLen(id string) int {
	recs, err := f.store.Load(context.Background(),
		eventstore.StreamID{Category: "saga.order_fulfillment", ID: id}, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(recs)
}

func TestSagaDecidesAndSendsAtomically(t *testing.T) {
	f := newFixture(t)
	f.deliver("ordering.events.self", "order/42#1", orderPlacedV1{OrderID: "42", Cents: 5000})

	got := f.commands("billing.commands", 1)[0]
	if got.Type != "billing.capture_payment" || got.Version != 1 {
		t.Errorf("Type/Version = %s/%d", got.Type, got.Version)
	}
	if got.Key != "42" {
		t.Errorf("Key = %q, want saga correlation id", got.Key)
	}
	if got.ID != "saga.order_fulfillment/42#2" {
		t.Errorf("ID = %q, want deterministic stream-position id", got.ID)
	}
	if got.Metadata["traceparent"] != "00-abc-def-01" {
		t.Errorf("Metadata = %v (trace context must flow event -> command)", got.Metadata)
	}
	var cmd capturePayment
	if err := json.Unmarshal(got.Payload, &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.OrderID != "42" || cmd.Cents != 5000 {
		t.Errorf("payload = %+v", cmd)
	}
}

func TestSagaAdvancesAcrossRestarts(t *testing.T) {
	f := newFixture(t)
	f.deliver("ordering.events.self", "order/42#1", orderPlacedV1{OrderID: "42", Cents: 5000})
	f.commands("billing.commands", 1)

	// The next event is handled by a rehydrated instance: state (Cents)
	// must survive via replay of the recorded envelopes.
	f.deliver("billing.events", "payment/9#1", paymentCapturedV1{OrderID: "42"})
	got := f.commands("inventory.commands", 1)[0]
	if got.Type != "inventory.reserve_stock" {
		t.Errorf("Type = %s", got.Type)
	}
}

func TestSagaCompensatesAndEnds(t *testing.T) {
	f := newFixture(t)
	f.deliver("ordering.events.self", "order/42#1", orderPlacedV1{OrderID: "42", Cents: 5000})
	f.commands("billing.commands", 1)
	f.deliver("billing.events", "payment/9#1", paymentCapturedV1{OrderID: "42"})
	f.commands("inventory.commands", 1)

	f.deliver("inventory.events", "stock/7#1", stockRejectedV1{OrderID: "42", Reason: "out of stock"})
	refund := f.commands("billing.commands", 2)[1]
	if refund.Type != "billing.refund_payment" {
		t.Fatalf("compensation = %s, want billing.refund_payment", refund.Type)
	}

	// Ended: further events are ignored, nothing more is appended.
	before := f.streamLen("42")
	f.deliver("billing.events", "payment/9#2", paymentCapturedV1{OrderID: "42"})
	time.Sleep(30 * time.Millisecond)
	if after := f.streamLen("42"); after != before {
		t.Errorf("ended saga appended %d events", after-before)
	}
}

func TestSagaDeduplicatesRedeliveries(t *testing.T) {
	f := newFixture(t)
	f.deliver("ordering.events.self", "order/42#1", orderPlacedV1{OrderID: "42", Cents: 5000})
	f.commands("billing.commands", 1)
	before := f.streamLen("42")

	f.deliver("ordering.events.self", "order/42#1", orderPlacedV1{OrderID: "42", Cents: 5000})
	time.Sleep(30 * time.Millisecond)
	if after := f.streamLen("42"); after != before {
		t.Errorf("redelivery appended %d events (must dedup by envelope ID)", after-before)
	}
}

func TestSagaIgnoresUnrelatedEvents(t *testing.T) {
	f := newFixture(t)
	// An event type the saga has no handler for, on a topic it consumes.
	f.deliver("billing.events", "payment/1#1", paymentRefundedV1{OrderID: "42"})
	time.Sleep(30 * time.Millisecond)
	if n := f.streamLen("42"); n != 0 {
		t.Errorf("unrelated event started a saga: %d events", n)
	}
}

type paymentRefundedV1 struct {
	OrderID string `json:"order_id"`
}

func (paymentRefundedV1) EventName() string    { return "billing.payment_refunded" }
func (paymentRefundedV1) ContractVersion() int { return 1 }
