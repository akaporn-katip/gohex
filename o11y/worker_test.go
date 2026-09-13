package o11y_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
	"github.com/akaporn-katip/gohex/o11y"
)

// originMeta returns metadata stamped by a finished "origin" span,
// standing in for a worklist row written minutes ago.
func originMeta(t *testing.T) (map[string]string, trace.SpanContext) {
	t.Helper()
	ctx, span := rootSpan(t)
	span.End()
	return o11y.Inject(ctx, nil), span.SpanContext()
}

func TestLinkFromCarriesOriginSpanContext(t *testing.T) {
	setup(t)
	meta, origin := originMeta(t)

	link := o11y.LinkFrom(meta)
	if !link.SpanContext.IsValid() {
		t.Fatal("link has no span context")
	}
	if link.SpanContext.TraceID() != origin.TraceID() || link.SpanContext.SpanID() != origin.SpanID() {
		t.Errorf("link = %s/%s, want %s/%s", link.SpanContext.TraceID(), link.SpanContext.SpanID(),
			origin.TraceID(), origin.SpanID())
	}
}

// TestLinkFromDegradesGracefully pins the "droppable empty link"
// contract: nothing here may panic, and every case yields an invalid
// span context the caller can drop.
func TestLinkFromDegradesGracefully(t *testing.T) {
	setup(t)
	cases := map[string]map[string]string{
		"nil":              nil,
		"empty":            {},
		"unrelated keys":   {"tenant": "acme"},
		"empty value":      {"traceparent": ""},
		"malformed":        {"traceparent": "not-a-traceparent"},
		"all-zero trace":   {"traceparent": "00-00000000000000000000000000000000-0000000000000000-01"},
		"truncated origin": {o11y.OriginTraceparentKey: "00-abc"},
	}
	for name, meta := range cases {
		t.Run(name, func(t *testing.T) {
			if link := o11y.LinkFrom(meta); link.SpanContext.IsValid() {
				t.Errorf("LinkFrom(%v) = valid link, want droppable zero link", meta)
			}
			ctx, span := o11y.StartLinked(context.Background(), "work", meta)
			span.End()
			if !trace.SpanContextFromContext(ctx).IsValid() {
				t.Error("StartLinked must still produce a usable span")
			}
		})
	}
}

// TestStartLinkedDetachesAndLinks is the headline of ADR-0015: the
// worker's span is a NEW trace (not a child of the origin, not a child
// of the tick), linked back to both.
func TestStartLinkedDetachesAndLinks(t *testing.T) {
	exporter := setup(t)
	meta, origin := originMeta(t)

	batchCtx, batch := o11y.StartBatch(context.Background(), "settle batch", 3)
	workCtx, work := o11y.StartLinked(batchCtx, "settle payment", meta)
	workSC := trace.SpanContextFromContext(workCtx)
	work.End()
	batch.End()

	if workSC.TraceID() == origin.TraceID() {
		t.Error("worker span continued the origin trace; it must be a new root")
	}
	if workSC.TraceID() == batch.SpanContext().TraceID() {
		t.Error("worker span is inside the tick's trace; it must be a new root")
	}

	stub := spanNamed(t, exporter.GetSpans(), "settle payment")
	if stub.Parent.IsValid() {
		t.Errorf("WithNewRoot did not detach: parent = %v", stub.Parent)
	}
	if stub.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kind = %v, want consumer", stub.SpanKind)
	}
	if !linkedTo(stub, origin) {
		t.Errorf("no link to the origin span; links = %v", stub.Links)
	}
	if !linkedTo(stub, batch.SpanContext()) {
		t.Errorf("no link to the batch span; links = %v", stub.Links)
	}
	if !hasAttr(stub, "gohex.origin.trace_id", origin.TraceID().String()) {
		t.Errorf("origin trace id attribute missing: %v", stub.Attributes)
	}

	batchStub := spanNamed(t, exporter.GetSpans(), "settle batch")
	if batchStub.Parent.IsValid() {
		t.Error("batch span must be its own root")
	}
}

// TestOriginMetadataRoundTrip: stamp, store, restore.
func TestOriginMetadataRoundTrip(t *testing.T) {
	setup(t)
	meta, origin := originMeta(t)

	stamped := o11y.OriginMetadata(meta)
	if stamped[o11y.OriginTraceparentKey] != meta["traceparent"] {
		t.Fatalf("stamp = %v, want the origin traceparent under %q", stamped, o11y.OriginTraceparentKey)
	}
	if stamped["traceparent"] != "" {
		t.Error("stamp must not carry a live traceparent — it would be mistaken for continuation")
	}
	if link := o11y.LinkFrom(stamped); link.SpanContext.SpanID() != origin.SpanID() {
		t.Errorf("round trip lost the origin: %v", link.SpanContext)
	}

	// A second hop keeps the FIRST origin rather than re-stamping the
	// intermediate trace.
	next, _ := originMeta(t)
	for k, v := range stamped {
		next[k] = v
	}
	if again := o11y.OriginMetadata(next); again[o11y.OriginTraceparentKey] != stamped[o11y.OriginTraceparentKey] {
		t.Errorf("re-stamping replaced the origin: %v", again)
	}

	if o11y.OriginMetadata(nil) != nil || o11y.OriginMetadata(map[string]string{"x": "y"}) != nil {
		t.Error("metadata without trace context must stamp nothing")
	}
}

// TestWorkerHandoffStampsOriginOnEvents is the chain end-to-end: a
// worker picks a row, works under a linked trace, and the events it
// produces carry BOTH the new trace and the origin stamp — so the next
// service can link one hop further without a database lookup.
func TestWorkerHandoffStampsOriginOnEvents(t *testing.T) {
	setup(t) // installs the event-store metadata hook
	meta, origin := originMeta(t)

	registry := eventstore.NewRegistry()
	eventstore.Register[counterCreated](registry)
	store := eventstore.NewMemoryStore()
	repo := eventstore.NewRepository(store, registry, "counter", func() *counter { return &counter{} })

	workCtx, work := o11y.StartLinked(context.Background(), "settle payment", meta)
	c := &counter{}
	if err := kernel.Raise(c, counterCreated{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(workCtx, rawID("1"), c); err != nil {
		t.Fatal(err)
	}
	work.End()

	recs, _ := store.ReadAll(context.Background(), 0, 0)
	if len(recs) != 1 {
		t.Fatal("no events stored")
	}
	md := recs[0].Metadata
	if md["traceparent"] == "" {
		t.Fatal("stored event carries no trace context")
	}
	if got := trace.SpanContextFromContext(o11y.Extract(context.Background(), md)); got.TraceID() == origin.TraceID() {
		t.Error("stored event continues the origin trace; the worker's trace must be its own")
	}
	if md[o11y.OriginTraceparentKey] == "" {
		t.Fatalf("stored event lost the origin stamp: %v", md)
	}
	if link := o11y.LinkFrom(map[string]string{o11y.OriginTraceparentKey: md[o11y.OriginTraceparentKey]}); link.SpanContext.SpanID() != origin.SpanID() {
		t.Errorf("origin stamp points at %v, want %s", link.SpanContext, origin.SpanID())
	}
}

// TestWithOriginIsOptional: no origin on the context means nothing extra
// is stamped — ordinary flows are untouched.
func TestWithOriginIsOptional(t *testing.T) {
	setup(t)
	ctx, span := rootSpan(t)
	defer span.End()

	md := eventstore.MetadataFromContext(o11y.WithOrigin(ctx, nil))
	if md["traceparent"] == "" {
		t.Fatal("live trace context missing")
	}
	if _, ok := md[o11y.OriginTraceparentKey]; ok {
		t.Errorf("unexpected origin stamp: %v", md)
	}
}

// --- helpers ---

func spanNamed(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("span %q not found in %v", name, spanNames(spans))
	return tracetest.SpanStub{}
}

func linkedTo(s tracetest.SpanStub, sc trace.SpanContext) bool {
	for _, l := range s.Links {
		if l.SpanContext.TraceID() == sc.TraceID() && l.SpanContext.SpanID() == sc.SpanID() {
			return true
		}
	}
	return false
}
