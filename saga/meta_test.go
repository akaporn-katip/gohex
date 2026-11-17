package saga_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/relay"
	"github.com/akaporn-katip/gohex/saga"
)

// shipped is the saga under test: it reacts to one event and lets the
// envelope's metadata — a flag pinned at the edge — choose the command.
type shipped struct{ Express bool }

func newFlaggedDefinition() *saga.Definition[shipped] {
	d := saga.New[shipped]("flagged_shipping")
	saga.OnEventWithMeta(d,
		func(e orderPlacedV1) string { return e.OrderID },
		func(s *shipped, e orderPlacedV1, m saga.Meta) (saga.Decisions, error) {
			s.Express = m.Metadata["gohex.flags"] == "express-shipping"
			topic := "shipping.commands"
			if s.Express {
				topic = "shipping.commands.express"
			}
			return saga.Send(saga.OutCommand{
				Topic: topic,
				Cmd:   capturePayment{OrderID: e.OrderID, Cents: e.Cents},
			}), nil
		})
	return d
}

// runFlagged starts the flagged saga over memory adapters and delivers
// one event carrying meta, returning the store so the stream can be
// replayed.
func runFlagged(t *testing.T, meta map[string]string) (*eventstore.MemoryStore, *broker.MemoryBroker) {
	t.Helper()
	registry := eventstore.NewRegistry()
	saga.RegisterEvents(registry)
	store := eventstore.NewMemoryStore()
	bkr := broker.NewMemoryBroker()

	rly := relay.New(store, registry, bkr, eventstore.NewMemoryCheckpointStore(), relay.Config{
		Name: "flagged.relay", Topic: "flagged.events",
		PollInterval: 2 * time.Millisecond, PublishBackoff: 2 * time.Millisecond,
	})
	saga.RegisterCommandRouting(rly)
	runner := saga.NewRunner(newFlaggedDefinition(), store, registry)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = rly.Run(ctx) }()
	go func() { _ = runner.Run(ctx, bkr, "flagged.group", "ordering.events.self") }()

	payload, err := json.Marshal(orderPlacedV1{OrderID: "42", Cents: 5000})
	if err != nil {
		t.Fatal(err)
	}
	err = bkr.Publish(ctx, "ordering.events.self", broker.Message{
		ID: "order/42#1", Key: "42", Type: "ordering.order_placed", Version: 1,
		OccurredAt: time.Now().UTC(), Payload: payload, Metadata: meta,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, bkr
}

// waitForCommand reports the topic the saga routed its command to.
func waitForCommand(t *testing.T, bkr *broker.MemoryBroker, topic string) bool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan struct{}, 1)
	go func() {
		_ = bkr.Subscribe(ctx, topic, "watch-"+topic, func(_ context.Context, _ broker.Message) error {
			select {
			case seen <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	select {
	case <-seen:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// TestSagaReadsPinnedMetadata is the point of OnEventWithMeta: a flag
// decided at the edge reaches a handler that must not re-evaluate it.
func TestSagaReadsPinnedMetadata(t *testing.T) {
	_, bkr := runFlagged(t, map[string]string{"gohex.flags": "express-shipping"})
	if !waitForCommand(t, bkr, "shipping.commands.express") {
		t.Error("pinned flag did not reach the handler: command took the normal route")
	}
}

// TestSagaWithoutPinnedMetadata proves the safe default: no flags in the
// envelope means off, not "ask someone".
func TestSagaWithoutPinnedMetadata(t *testing.T) {
	_, bkr := runFlagged(t, map[string]string{"traceparent": "00-abc-def-01"})
	if !waitForCommand(t, bkr, "shipping.commands") {
		t.Error("absent flag should route the normal way")
	}
}

// TestSagaMetadataSurvivesReplay is why reading Meta keeps a handler
// pure: the envelope is stored verbatim, so a rehydrated instance sees
// the metadata of the original delivery, not of today.
func TestSagaMetadataSurvivesReplay(t *testing.T) {
	store, _ := runFlagged(t, map[string]string{"gohex.flags": "express-shipping"})
	registry := eventstore.NewRegistry()
	saga.RegisterEvents(registry)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := store.Load(context.Background(),
			eventstore.StreamID{Category: "saga.flagged_shipping", ID: "42"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) > 0 {
			if got := string(recs[0].Payload); !strings.Contains(got, "express-shipping") {
				t.Errorf("stored envelope = %s, want the pinned flag recorded verbatim", got)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("saga stream stayed empty")
}
