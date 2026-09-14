package o11y_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/o11y"
	"github.com/akaporn-katip/gohex/projection"
)

// paymentCapturedV1 is a foreign fact for the inbox-side hook test.
type paymentCapturedV1 struct {
	OrderID string `json:"order_id"`
}

func (paymentCapturedV1) EventName() string    { return "billing.payment_captured" }
func (paymentCapturedV1) ContractVersion() int { return 1 }

func TestProjectionHookLinksInsteadOfContinuing(t *testing.T) {
	exporter := setup(t)
	meta, origin := originMeta(t)

	hook := o11y.ProjectionHook()
	ctx, done := hook(context.Background(), projection.Item{
		Projection: "order_summary",
		Source:     projection.SourceInbox,
		Name:       "billing.payment_captured",
		ID:         "billing/1#1",
		Metadata:   meta,
	})
	if trace.SpanContextFromContext(ctx).TraceID() == origin.TraceID() {
		t.Error("hook continued the origin trace; the default is a linked new trace (ADR-0015)")
	}
	done(nil)

	stub := spanNamed(t, exporter.GetSpans(), "project order_summary billing.payment_captured")
	if stub.Parent.IsValid() {
		t.Error("projection span must be a new root")
	}
	if !linkedTo(stub, origin) {
		t.Errorf("projection span not linked to the event that caused it: %v", stub.Links)
	}
	if !hasAttr(stub, "gohex.projection.source", "inbox") ||
		!hasAttr(stub, "gohex.message.type", "billing.payment_captured") ||
		!hasAttr(stub, "messaging.message.id", "billing/1#1") {
		t.Errorf("item identity missing from attributes: %v", stub.Attributes)
	}
	if stub.Status.Code == codes.Error {
		t.Error("a nil completion error must not mark the span")
	}
}

func TestProjectionHookContinueTraceOptsOut(t *testing.T) {
	exporter := setup(t)
	meta, origin := originMeta(t)

	hook := o11y.ProjectionHook(o11y.ContinueTrace())
	ctx, done := hook(context.Background(), projection.Item{
		Projection: "order_summary",
		Source:     projection.SourceStore,
		Metadata:   meta,
	})
	if got := trace.SpanContextFromContext(ctx).TraceID(); got != origin.TraceID() {
		t.Errorf("trace = %s, want the origin trace %s", got, origin.TraceID())
	}
	done(errors.New("read model db down"))

	// The item carries no type, so the name falls back to the projection.
	stub := spanNamed(t, exporter.GetSpans(), "project order_summary")
	if stub.Status.Code != codes.Error {
		t.Error("handler failure must mark the span")
	}
}

// TestProjectSpanNameCarriesTheFact: one projection applies many kinds
// of event, and only the name shows in a waterfall row (ADR-0016).
func TestProjectSpanNameCarriesTheFact(t *testing.T) {
	exporter := setup(t)

	hook := o11y.ProjectionHook()
	_, done := hook(context.Background(), projection.Item{
		Projection: "billing_views",
		Source:     projection.SourceInbox,
		Name:       "tenancy.lease_started",
		ID:         "tenancy/1#1",
	})
	done(nil)

	stub := spanNamed(t, exporter.GetSpans(), "project billing_views tenancy.lease_started")
	if !hasAttr(stub, "messaging.operation.name", "project") ||
		!hasAttr(stub, "messaging.operation.type", "process") {
		t.Errorf("operation attributes missing: %v", stub.Attributes)
	}
}

// TestProjectionHookRunsInsideTheRunners wires the real hook into the
// real InboxReader — the wiring services copy.
func TestProjectionHookRunsInsideTheRunners(t *testing.T) {
	exporter := setup(t)
	meta, origin := originMeta(t)

	p := projection.New("order_summary")
	applied := make(chan trace.SpanContext, 1)
	projection.OnIntegration(p, func(ctx context.Context, _ paymentCapturedV1, _ broker.Message) error {
		select {
		case applied <- trace.SpanContextFromContext(ctx):
		default:
		}
		return nil
	})

	inbox := projection.NewMemoryInbox()
	msg, err := broker.NewMessage("billing/1#1", "1", time.Now(), paymentCapturedV1{OrderID: "42"}, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Append(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	reader := projection.NewInboxReader(p, inbox, eventstore.NewMemoryCheckpointStore(), projection.Config{
		PollInterval: 2 * time.Millisecond,
		Observe:      o11y.ProjectionHook(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = reader.Run(ctx) }() //nolint:errcheck // cancelled by cleanup

	select {
	case sc := <-applied:
		if !sc.IsValid() {
			t.Fatal("handler ran without a span")
		}
		if sc.TraceID() == origin.TraceID() {
			t.Error("apply continued the origin trace")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("projection never applied the message")
	}

	// The span is exported once the runner's completion func fires.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range exporter.GetSpans() {
			if s.Name == "project order_summary billing.payment_captured" && linkedTo(s, origin) {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no linked projection span was exported")
}
