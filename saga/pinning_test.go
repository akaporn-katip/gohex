package saga_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/featureflag"
	"github.com/akaporn-katip/gohex/o11y"
	"github.com/akaporn-katip/gohex/relay"
	"github.com/akaporn-katip/gohex/saga"
)

// TestPinnedFlagReachesTheNextServicesCommand: a decision taken at the
// HTTP edge has to survive the hop the saga makes on its behalf, or
// ADR-0021 is a promise the framework cannot keep.
type consumerSpanKey struct{}

func TestPinnedFlagReachesTheNextServicesCommand(t *testing.T) {
	// Every real service consumes under o11y.Subscriber, so the runner's
	// context always carries a live span and the extractor always has
	// something to say. That is the case this test exists for: with the
	// old "context or envelope, never both" rule, the envelope — and the
	// pinned flag on it — was dropped exactly when telemetry was wired.
	// Extractors are process-global and cannot be removed, so this one
	// only speaks for contexts this test owns.
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		if ctx.Value(consumerSpanKey{}) == nil {
			return nil
		}
		md := o11y.Inject(ctx, map[string]string{})
		if len(md) == 0 {
			md = map[string]string{"traceparent": "00-consumerspan-0001-01"}
		}
		return md
	})
	featureflag.Install()

	registry := eventstore.NewRegistry()
	saga.RegisterEvents(registry)
	store := eventstore.NewMemoryStore()
	bkr := broker.NewMemoryBroker()
	rly := relay.New(store, registry, bkr, eventstore.NewMemoryCheckpointStore(), relay.Config{
		Name: "p.relay", Topic: "p.events", PollInterval: 2 * time.Millisecond, PublishBackoff: 2 * time.Millisecond,
	})
	saga.RegisterCommandRouting(rly)

	d := saga.New[shipped]("propagation")
	saga.OnEvent(d,
		func(e orderPlacedV1) string { return e.OrderID },
		func(s *shipped, e orderPlacedV1) (saga.Decisions, error) {
			return saga.Send(saga.OutCommand{Topic: "billing.commands", Cmd: capturePayment{OrderID: e.OrderID}}), nil
		})
	runner := saga.NewRunner(d, store, registry)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), consumerSpanKey{}, true))
	defer cancel()
	go func() { _ = rly.Run(ctx) }()
	go func() { _ = runner.Run(ctx, bkr, "p.group", "ordering.events.self") }()

	payload, _ := json.Marshal(orderPlacedV1{OrderID: "42", Cents: 1})
	_ = bkr.Publish(ctx, "ordering.events.self", broker.Message{
		ID: "order/42#1", Key: "42", Type: "ordering.order_placed", Version: 1,
		OccurredAt: time.Now().UTC(), Payload: payload,
		Metadata: map[string]string{"traceparent": "00-aaaa-bbbb-01", featureflag.FlagsKey: "fake-omise"},
	})

	got := make(chan map[string]string, 1)
	go func() {
		_ = bkr.Subscribe(ctx, "billing.commands", "watch", func(_ context.Context, m broker.Message) error {
			select {
			case got <- m.Metadata:
			default:
			}
			return nil
		})
	}()
	select {
	case md := <-got:
		if !featureflag.PinnedIn(md, "fake-omise") {
			t.Errorf("pinned flag lost between the saga's trigger and its command: %v", md)
		}
		if md["traceparent"] != "00-consumerspan-0001-01" {
			t.Errorf("traceparent = %q, want the saga's own consumer span to win", md["traceparent"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no command")
	}
}
