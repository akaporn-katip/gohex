package o11y_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/cqrs"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
	"github.com/akaporn-katip/gohex/o11y"
)

// setup installs an in-memory tracer provider (via Init's collector-less
// path plus a span recorder) and returns the exporter to inspect spans.
func setup(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	if _, err := o11y.Init(context.Background(), o11y.Config{ServiceName: "test", WithoutExporter: true}); err != nil {
		t.Fatal(err)
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return exporter
}

func rootSpan(t *testing.T) (context.Context, trace.Span) {
	t.Helper()
	return otel.Tracer("test").Start(context.Background(), "http.request")
}

func TestInjectExtractRoundTrip(t *testing.T) {
	setup(t)
	ctx, span := rootSpan(t)
	defer span.End()

	meta := o11y.Inject(ctx, nil)
	if meta["traceparent"] == "" {
		t.Fatal("Inject added no traceparent")
	}

	out := o11y.Extract(context.Background(), meta)
	if got := trace.SpanContextFromContext(out).TraceID(); got != span.SpanContext().TraceID() {
		t.Errorf("extracted trace %s, want %s", got, span.SpanContext().TraceID())
	}
}

func TestInjectWithoutSpanIsNil(t *testing.T) {
	setup(t)
	if meta := o11y.Inject(context.Background(), nil); meta != nil {
		t.Errorf("Inject(no span) = %v, want nil", meta)
	}
}

func TestPublisherInjectsOnlyIfAbsent(t *testing.T) {
	setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)
	ctx, span := rootSpan(t)
	defer span.End()

	original := map[string]string{"traceparent": "00-11111111111111111111111111111111-2222222222222222-01"}
	if err := pub.Publish(ctx, "t",
		broker.Message{ID: "fresh"},
		broker.Message{ID: "stamped", Metadata: original},
	); err != nil {
		t.Fatal(err)
	}

	got := collect(t, mem, "t", 2)
	if got[0].Metadata["traceparent"] == "" {
		t.Error("fresh message not injected")
	}
	if got[1].Metadata["traceparent"] != original["traceparent"] {
		t.Error("existing traceparent was clobbered — the relay's preserved context must win")
	}
}

func TestSubscriberContinuesTraceAndSpansErrors(t *testing.T) {
	exporter := setup(t)
	mem := broker.NewMemoryBroker()
	sub := o11y.Subscriber(mem)

	ctx, root := rootSpan(t)
	meta := o11y.Inject(ctx, nil)
	root.End()

	var mu sync.Mutex
	var handlerTrace trace.TraceID
	fails := 1
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = sub.Subscribe(runCtx, "t", "g", func(hctx context.Context, m broker.Message) error {
			mu.Lock()
			defer mu.Unlock()
			if fails > 0 {
				fails--
				return errors.New("transient")
			}
			handlerTrace = trace.SpanContextFromContext(hctx).TraceID()
			close(done)
			return nil
		})
	}()
	if err := mem.Publish(context.Background(), "t", broker.Message{ID: "m1", Type: "x", Metadata: meta}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never succeeded")
	}

	if handlerTrace != root.SpanContext().TraceID() {
		t.Errorf("handler trace %s, want %s (must continue the message's trace)", handlerTrace, root.SpanContext().TraceID())
	}
	// Two consume spans: the failed attempt (error status) and the success.
	spans := exporter.GetSpans()
	var consumeErr, consumeOK bool
	for _, s := range spans {
		if s.Name != "consume t" {
			continue
		}
		if s.Status.Code == codes.Error {
			consumeErr = true
		} else {
			consumeOK = true
		}
	}
	if !consumeErr || !consumeOK {
		t.Errorf("want an errored and a successful consume span, got %+v", spanNames(spans))
	}
}

type testCmd struct{}

func (testCmd) CommandName() string { return "billing.capture_payment" }

func TestCommandMiddlewareRejectionIsNotSpanError(t *testing.T) {
	exporter := setup(t)
	declined := kernel.NewDomainError("payment_declined", "card declined")
	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()),
		cqrs.WithRetry(cqrs.RetryPolicy{Attempts: 1}))
	cqrs.Handle(bus, func(context.Context, testCmd) error { return declined })

	_ = bus.Dispatch(context.Background(), testCmd{})

	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "command billing.capture_payment" {
		t.Fatalf("spans = %v", spanNames(spans))
	}
	if spans[0].Status.Code == codes.Error {
		t.Error("rejection marked as span error; it is an expected outcome")
	}
	if !hasAttr(spans[0], "gohex.rejection_code", "payment_declined") {
		t.Errorf("missing rejection code attribute: %v", spans[0].Attributes)
	}
}

func TestCommandMiddlewareInfraErrorIsSpanError(t *testing.T) {
	exporter := setup(t)
	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()),
		cqrs.WithRetry(cqrs.RetryPolicy{Attempts: 1}))
	cqrs.Handle(bus, func(context.Context, testCmd) error { return errors.New("db down") })

	_ = bus.Dispatch(context.Background(), testCmd{})
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("infra error must mark the span: %+v", spans)
	}
}

// counterCreated is a minimal domain event for the store-stamping test.
type counterCreated struct {
	ID string `json:"id"`
}

func (counterCreated) EventName() string { return "test.counter_created" }

type counter struct {
	kernel.Root
	id string
}

func (c *counter) Apply(e kernel.DomainEvent) error {
	c.id = e.(counterCreated).ID
	return nil
}

func TestRepositorySaveStampsTraceContext(t *testing.T) {
	setup(t) // Init installs the eventstore metadata hook
	registry := eventstore.NewRegistry()
	eventstore.Register[counterCreated](registry)
	store := eventstore.NewMemoryStore()
	repo := eventstore.NewRepository(store, registry, "counter", func() *counter { return &counter{} })

	ctx, span := rootSpan(t)
	defer span.End()

	c := &counter{}
	if err := kernel.Raise(c, counterCreated{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, rawID("1"), c); err != nil {
		t.Fatal(err)
	}

	recs, _ := store.ReadAll(context.Background(), 0, 0)
	if len(recs) != 1 {
		t.Fatal("no events stored")
	}
	if recs[0].Metadata["traceparent"] == "" {
		t.Error("stored event carries no trace context — the through-the-database weave is broken")
	}
}

type rawID string

func (r rawID) String() string { return string(r) }

// TestWeaveEndToEnd is the headline: one trace from a command span,
// through a stored event's metadata, relay-style copy onto a message,
// and a broker consume on the other side.
func TestWeaveEndToEnd(t *testing.T) {
	setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)
	sub := o11y.Subscriber(mem)

	registry := eventstore.NewRegistry()
	eventstore.Register[counterCreated](registry)
	store := eventstore.NewMemoryStore()
	repo := eventstore.NewRepository(store, registry, "counter", func() *counter { return &counter{} })

	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()))
	cqrs.Handle(bus, func(ctx context.Context, _ testCmd) error {
		c := &counter{}
		if err := kernel.Raise(c, counterCreated{ID: "1"}); err != nil {
			return err
		}
		return repo.Save(ctx, rawID("1"), c) // stamps ctx (command span) into the event
	})

	ctx, root := rootSpan(t)
	if err := bus.Dispatch(ctx, testCmd{}); err != nil {
		t.Fatal(err)
	}
	root.End()

	// Relay behavior: copy stored metadata onto the outgoing message.
	recs, _ := store.ReadAll(context.Background(), 0, 0)
	msg := broker.Message{ID: "counter/1#1", Type: "test.counter_created", Metadata: recs[0].Metadata}
	if err := pub.Publish(context.Background(), "counter.events", msg); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var consumerTrace trace.TraceID
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = sub.Subscribe(runCtx, "counter.events", "g", func(hctx context.Context, _ broker.Message) error {
			mu.Lock()
			defer mu.Unlock()
			consumerTrace = trace.SpanContextFromContext(hctx).TraceID()
			close(done)
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message never consumed")
	}

	mu.Lock()
	defer mu.Unlock()
	if consumerTrace != root.SpanContext().TraceID() {
		t.Errorf("consumer trace %s != origin trace %s: the async weave is broken",
			consumerTrace, root.SpanContext().TraceID())
	}
}

func TestLogHandlerAddsTraceIDs(t *testing.T) {
	setup(t)
	var buf bytes.Buffer
	logger := slog.New(o11y.NewLogHandler(slog.NewJSONHandler(&buf, nil)))

	ctx, span := rootSpan(t)
	defer span.End()
	logger.InfoContext(ctx, "hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["trace_id"] != span.SpanContext().TraceID().String() {
		t.Errorf("trace_id = %v, want %s", rec["trace_id"], span.SpanContext().TraceID())
	}
	if rec["span_id"] == nil {
		t.Error("span_id missing")
	}
}

// --- helpers ---

func collect(t *testing.T, b *broker.MemoryBroker, topic string, n int) []broker.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var msgs []broker.Message
	go func() {
		_ = b.Subscribe(ctx, topic, "collect-"+t.Name(), func(_ context.Context, m broker.Message) error {
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
	t.Fatalf("timed out collecting %d messages", n)
	return nil
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name
	}
	return names
}

func hasAttr(s tracetest.SpanStub, key, value string) bool {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key && kv.Value.AsString() == value {
			return true
		}
	}
	return false
}
