package cqrs_test

import (
	"context"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/cqrs"
)

// TestConsumerPutsEnvelopeMetadataOnTheContext: a handler must be able
// to see what the delivery carried — the trace context and the flag
// decisions pinned where the flow started (ADR-0021) — without the
// command's payload having to smuggle it.
func TestConsumerPutsEnvelopeMetadataOnTheContext(t *testing.T) {
	seen := make(chan map[string]string, 1)
	f := newConsumerFixture(t, func(ctx context.Context, _ capturePayment) error {
		select {
		case seen <- cqrs.MetaFromContext(ctx):
		default:
		}
		return nil
	})

	msg, err := cqrs.NewCommandMessage("saga/42#2", "42", time.Now().UTC(),
		capturePayment{OrderID: "42", Cents: 100},
		map[string]string{"traceparent": "00-abc-def-01", "gohex.flags": "fake-omise"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.broker.Publish(context.Background(), "billing.commands", msg); err != nil {
		t.Fatal(err)
	}

	select {
	case md := <-seen:
		if md["gohex.flags"] != "fake-omise" {
			t.Errorf("metadata = %v, want the envelope's pinned flags", md)
		}
		if md["traceparent"] != "00-abc-def-01" {
			t.Errorf("metadata = %v, want the envelope's trace context", md)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestMetaFromContextIsNilForALocalDispatch: an HTTP edge dispatching
// its own command has no envelope. Nil is the safe answer — a flag
// absent from pinned metadata is off.
func TestMetaFromContextIsNilForALocalDispatch(t *testing.T) {
	bus := cqrs.NewBus()
	seen := make(chan map[string]string, 1)
	cqrs.Handle(bus, func(ctx context.Context, _ capturePayment) error {
		seen <- cqrs.MetaFromContext(ctx)
		return nil
	})

	if err := bus.Dispatch(context.Background(), capturePayment{OrderID: "42"}); err != nil {
		t.Fatal(err)
	}
	if md := <-seen; md != nil {
		t.Errorf("MetaFromContext = %v, want nil for a command with no envelope", md)
	}
}

// TestWithMetaIsHowAHandlerTestSuppliesAnEnvelope: the handler under
// test should not need a broker to be given its delivery context.
func TestWithMetaIsHowAHandlerTestSuppliesAnEnvelope(t *testing.T) {
	ctx := cqrs.WithMeta(context.Background(), map[string]string{"gohex.flags": "fake-omise"})
	if got := cqrs.MetaFromContext(ctx)["gohex.flags"]; got != "fake-omise" {
		t.Errorf("gohex.flags = %q", got)
	}
	if cqrs.MetaFromContext(cqrs.WithMeta(context.Background(), nil)) != nil {
		t.Error("empty metadata should leave the context alone")
	}
}
