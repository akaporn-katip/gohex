package relay_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/kernel"
	"github.com/akaporn-katip/gohex/libs/relay"
)

// Domain events (private).

type orderPlaced struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
	// Internal detail that must never leak to the contract.
	PricingRules string `json:"pricing_rules"`
}

func (orderPlaced) EventName() string { return "ordering.order_placed" }

type orderAudited struct {
	OrderID string `json:"order_id"`
}

func (orderAudited) EventName() string { return "ordering.order_audited" }

// Integration event (public contract).

type orderPlacedV1 struct {
	OrderID    string `json:"order_id"`
	TotalCents int64  `json:"total_cents"`
}

func (orderPlacedV1) EventName() string    { return "ordering.order_placed" }
func (orderPlacedV1) ContractVersion() int { return 1 }

type fixture struct {
	store  *eventstore.MemoryStore
	broker *broker.MemoryBroker
	cps    *eventstore.MemoryCheckpointStore
	relay  *relay.Relay
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	registry := eventstore.NewRegistry()
	eventstore.Register[orderPlaced](registry)
	eventstore.Register[orderAudited](registry)

	f := &fixture{
		store:  eventstore.NewMemoryStore(),
		broker: broker.NewMemoryBroker(),
		cps:    eventstore.NewMemoryCheckpointStore(),
	}
	f.relay = relay.New(f.store, registry, f.broker, f.cps, relay.Config{
		Name:         "ordering.relay",
		Topic:        "ordering.events",
		PollInterval: 2 * time.Millisecond,
	})
	relay.Translate(f.relay, func(e orderPlaced) (broker.IntegrationEvent, bool) {
		return orderPlacedV1{OrderID: e.OrderID, TotalCents: e.Cents}, true
	})
	// orderAudited has no translator: private.
	return f
}

func (f *fixture) append(t *testing.T, stream eventstore.StreamID, expected int64, metadata map[string]string, events ...kernel.DomainEvent) {
	t.Helper()
	registry := eventstore.NewRegistry()
	eventstore.Register[orderPlaced](registry)
	eventstore.Register[orderAudited](registry)
	data := make([]eventstore.EventData, len(events))
	for i, e := range events {
		var err error
		if data[i], err = registry.Encode(e); err != nil {
			t.Fatal(err)
		}
		data[i].Metadata = metadata
	}
	if err := f.store.Append(context.Background(), stream, expected, data); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.relay.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("relay.Run = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("relay did not stop")
		}
	})
	return cancel
}

var collectGroup atomic.Int64

// collect subscribes with a fresh group (seeing the topic from the
// beginning) and returns once n messages arrived.
func (f *fixture) collect(t *testing.T, n int) []broker.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := fmt.Sprintf("test-%s-%d", t.Name(), collectGroup.Add(1))
	var mu sync.Mutex
	var msgs []broker.Message
	go func() {
		_ = f.broker.Subscribe(ctx, "ordering.events", group, func(_ context.Context, m broker.Message) error {
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
	t.Fatalf("timed out: got %d of %d messages", len(msgs), n)
	return nil
}

func TestRelayTranslatesAndPublishes(t *testing.T) {
	f := newFixture(t)
	stream := eventstore.StreamID{Category: "order", ID: "42"}
	f.append(t, stream, 0, map[string]string{"traceparent": "00-abc-def-01"},
		orderPlaced{OrderID: "42", Cents: 5000, PricingRules: "secret"})
	f.run(t)

	got := f.collect(t, 1)[0]
	if got.ID != "order/42#1" {
		t.Errorf("ID = %q, want order/42#1", got.ID)
	}
	if got.Key != "42" {
		t.Errorf("Key = %q, want 42", got.Key)
	}
	if got.Type != "ordering.order_placed" || got.Version != 1 {
		t.Errorf("Type/Version = %s/%d", got.Type, got.Version)
	}
	if got.Metadata["traceparent"] != "00-abc-def-01" {
		t.Errorf("Metadata = %v (trace context must propagate)", got.Metadata)
	}
	want := `{"order_id":"42","total_cents":5000}`
	if string(got.Payload) != want {
		t.Errorf("Payload = %s, want %s (internal fields must not leak)", got.Payload, want)
	}
	if got.OccurredAt.IsZero() {
		t.Error("OccurredAt is zero")
	}
}

func TestRelaySkipsPrivateEventsButAdvances(t *testing.T) {
	f := newFixture(t)
	stream := eventstore.StreamID{Category: "order", ID: "42"}
	f.append(t, stream, 0, nil,
		orderAudited{OrderID: "42"}, // private
		orderPlaced{OrderID: "42", Cents: 100},
		orderAudited{OrderID: "42"}, // private
	)
	f.run(t)

	got := f.collect(t, 1)
	if len(got) != 1 || got[0].ID != "order/42#2" {
		t.Fatalf("got %+v, want only order/42#2", got)
	}
	// Checkpoint must cover the trailing private event too.
	waitFor(t, func() bool {
		seq, _ := f.cps.Get(context.Background(), "ordering.relay")
		return seq == 3
	}, "checkpoint = 3")
}

func TestRelayResumesFromCheckpointWithoutDuplicates(t *testing.T) {
	f := newFixture(t)
	stream := eventstore.StreamID{Category: "order", ID: "42"}
	f.append(t, stream, 0, nil, orderPlaced{OrderID: "42", Cents: 1})

	cancel := f.run(t)
	f.collect(t, 1)
	waitFor(t, func() bool {
		seq, _ := f.cps.Get(context.Background(), "ordering.relay")
		return seq == 1
	}, "first checkpoint")
	cancel()

	// While stopped, more events arrive; a restarted relay publishes only those.
	f.append(t, stream, 1, nil, orderPlaced{OrderID: "42", Cents: 2})
	f.run(t)

	msgs := f.collect(t, 2) // group is fresh, so it sees the full topic: exactly 2, no dupes
	if msgs[0].ID != "order/42#1" || msgs[1].ID != "order/42#2" {
		t.Fatalf("IDs = %s, %s", msgs[0].ID, msgs[1].ID)
	}
	time.Sleep(20 * time.Millisecond) // would surface a duplicate republish
	if extra := f.collect(t, 2); len(extra) != 2 {
		t.Fatalf("duplicates published: %d messages", len(extra))
	}
}

// flakyPublisher fails N times before delegating.
type flakyPublisher struct {
	mu       sync.Mutex
	failures int
	attempts int
	delegate broker.Publisher
}

func (p *flakyPublisher) Publish(ctx context.Context, topic string, msgs ...broker.Message) error {
	p.mu.Lock()
	p.attempts++
	fail := p.attempts <= p.failures
	p.mu.Unlock()
	if fail {
		return errors.New("broker down")
	}
	return p.delegate.Publish(ctx, topic, msgs...)
}

func TestRelayRetriesPublishWithoutAdvancing(t *testing.T) {
	registry := eventstore.NewRegistry()
	eventstore.Register[orderPlaced](registry)

	store := eventstore.NewMemoryStore()
	memBroker := broker.NewMemoryBroker()
	cps := eventstore.NewMemoryCheckpointStore()
	flaky := &flakyPublisher{failures: 2, delegate: memBroker}

	r := relay.New(store, registry, flaky, cps, relay.Config{
		Name: "ordering.relay", Topic: "ordering.events",
		PollInterval: 2 * time.Millisecond, PublishBackoff: 2 * time.Millisecond,
	})
	relay.Translate(r, func(e orderPlaced) (broker.IntegrationEvent, bool) {
		return orderPlacedV1{OrderID: e.OrderID, TotalCents: e.Cents}, true
	})

	data, err := registry.Encode(orderPlaced{OrderID: "42", Cents: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), eventstore.StreamID{Category: "order", ID: "42"}, 0,
		[]eventstore.EventData{data}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// The message arrives despite two failures, exactly once.
	f := &fixture{broker: memBroker}
	got := f.collect(t, 1)
	if got[0].ID != "order/42#1" {
		t.Fatalf("ID = %s", got[0].ID)
	}
	waitFor(t, func() bool {
		seq, _ := cps.Get(context.Background(), "ordering.relay")
		return seq == 1
	}, "checkpoint after retry")
	if flaky.attempts != 3 {
		t.Errorf("attempts = %d, want 3", flaky.attempts)
	}
}

func TestTranslateDuplicatePanics(t *testing.T) {
	f := newFixture(t)
	defer func() {
		if recover() == nil {
			t.Error("duplicate Translate did not panic")
		}
	}()
	relay.Translate(f.relay, func(e orderPlaced) (broker.IntegrationEvent, bool) {
		return orderPlacedV1{}, true
	})
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
