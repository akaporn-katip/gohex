package projection_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/projection"
)

// observations records what an Observer saw, for the runner tests.
type observations struct {
	mu    sync.Mutex
	items []projection.Item
	errs  []error
	// marked counts contexts the observer decorated that actually
	// reached the handler.
	marked int
}

type observeKey struct{}

func (o *observations) observer() projection.Observer {
	return func(ctx context.Context, item projection.Item) (context.Context, func(error)) {
		o.mu.Lock()
		o.items = append(o.items, item)
		o.mu.Unlock()
		return context.WithValue(ctx, observeKey{}, o), func(err error) {
			o.mu.Lock()
			defer o.mu.Unlock()
			o.errs = append(o.errs, err)
		}
	}
}

func (o *observations) snapshot() ([]projection.Item, []error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projection.Item(nil), o.items...), append([]error(nil), o.errs...)
}

// seen reports whether the observer's context reached a handler.
func (o *observations) seen(ctx context.Context) {
	if ctx.Value(observeKey{}) == o {
		o.mu.Lock()
		o.marked++
		o.mu.Unlock()
	}
}

func TestCatchUpInvokesObserver(t *testing.T) {
	obs := &observations{}
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()

	p := projection.New("observed")
	projection.On(p, func(ctx context.Context, _ orderPlaced, _ projection.Meta) error {
		obs.seen(ctx)
		return nil
	})

	// Metadata must reach the observer: it is the whole point (the
	// stored trace context lives there).
	registry := newRegistry()
	data, err := registry.Encode(orderPlaced{OrderID: "42", Cents: 1})
	if err != nil {
		t.Fatal(err)
	}
	data.Metadata = map[string]string{"traceparent": "00-abc-def-01"}
	if _, err := store.Append(context.Background(), eventstore.StreamID{Category: "order", ID: "42"}, 0,
		[]eventstore.EventData{data}); err != nil {
		t.Fatal(err)
	}

	cfg := fastCfg
	cfg.Observe = obs.observer()
	run(t, "CatchUp", projection.NewCatchUp(p, store, registry, cps, cfg).Run)

	waitFor(t, "observation", func() bool { items, _ := obs.snapshot(); return len(items) == 1 })
	items, errs := obs.snapshot()
	got := items[0]
	if got.Projection != "observed" || got.Source != projection.SourceStore {
		t.Errorf("item = %+v, want projection observed from the store", got)
	}
	if got.Name != "ordering.order_placed" || got.ID != "order/42#1" {
		t.Errorf("item identity = %q %q, want ordering.order_placed order/42#1", got.Name, got.ID)
	}
	if got.Metadata["traceparent"] != "00-abc-def-01" {
		t.Errorf("item metadata = %v, want the stored trace context", got.Metadata)
	}
	if len(errs) != 1 || errs[0] != nil {
		t.Errorf("completion errors = %v, want one nil", errs)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.marked != 1 {
		t.Error("handler did not run under the observer's context")
	}
}

func TestCatchUpObserverSeesHandlerError(t *testing.T) {
	obs := &observations{}
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()
	boom := errors.New("read model db down")

	p := projection.New("failing_observed")
	projection.On(p, func(context.Context, orderPlaced, projection.Meta) error { return boom })
	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 1})

	cfg := fastCfg
	cfg.Observe = obs.observer()
	catchup := projection.NewCatchUp(p, store, newRegistry(), cps, cfg)
	if err := catchup.Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want %v", err, boom)
	}
	_, errs := obs.snapshot()
	if len(errs) != 1 || !errors.Is(errs[0], boom) {
		t.Errorf("completion errors = %v, want the handler error", errs)
	}
}

func TestInboxReaderInvokesObserver(t *testing.T) {
	obs := &observations{}
	inbox := projection.NewMemoryInbox()
	cps := eventstore.NewMemoryCheckpointStore()

	p := projection.New("observed_inbox")
	projection.OnIntegration(p, func(ctx context.Context, _ paymentCapturedV1, _ broker.Message) error {
		obs.seen(ctx)
		return nil
	})

	msg, err := broker.NewMessage("billing/1#1", "1", time.Now(), paymentCapturedV1{OrderID: "42"},
		map[string]string{"traceparent": "00-abc-def-01"})
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Append(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	cfg := fastCfg
	cfg.Observe = obs.observer()
	run(t, "InboxReader", projection.NewInboxReader(p, inbox, cps, cfg).Run)

	waitFor(t, "observation", func() bool { items, _ := obs.snapshot(); return len(items) == 1 })
	items, _ := obs.snapshot()
	got := items[0]
	if got.Source != projection.SourceInbox || got.Name != "billing.payment_captured" || got.ID != "billing/1#1" {
		t.Errorf("item = %+v, want the inbox message's identity", got)
	}
	if got.Metadata["traceparent"] != "00-abc-def-01" {
		t.Errorf("item metadata = %v, want the envelope's trace context", got.Metadata)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.marked != 1 {
		t.Error("handler did not run under the observer's context")
	}
}

// TestNilObserverIsZeroCost pins the default: no observer configured
// means the handler's context is the runner's own, untouched — no
// wrapper values, no allocation, no behavior change.
func TestNilObserverIsZeroCost(t *testing.T) {
	obs := &observations{}
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()

	type ctxKey struct{}
	var handlerCtx context.Context
	var mu sync.Mutex
	p := projection.New("unobserved")
	projection.On(p, func(ctx context.Context, _ orderPlaced, _ projection.Meta) error {
		mu.Lock()
		defer mu.Unlock()
		handlerCtx = ctx
		return nil
	})
	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 1})

	if fastCfg.Observe != nil {
		t.Fatal("default Config must have no observer")
	}
	runCtx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "runner"))
	t.Cleanup(cancel)
	catchup := projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg)
	go func() { _ = catchup.Run(runCtx) }() //nolint:errcheck // cancelled by cleanup
	waitFor(t, "apply", func() bool { mu.Lock(); defer mu.Unlock(); return handlerCtx != nil })

	mu.Lock()
	defer mu.Unlock()
	if handlerCtx.Value(ctxKey{}) != "runner" {
		t.Error("handler context is not the runner's own")
	}
	if items, _ := obs.snapshot(); len(items) != 0 {
		t.Error("observer invoked despite being nil")
	}
}
