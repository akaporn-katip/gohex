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

// TestPublisherInjectsOnlyIfAbsentInEveryBranch walks the same rule
// through all three parenting branches: whichever home the publish span
// finds, a message that already carries a trace keeps it, so consumers
// continue the ORIGINAL trace and never the publish span.
func TestPublisherInjectsOnlyIfAbsentInEveryBranch(t *testing.T) {
	cases := []struct {
		name     string
		inCtx    bool
		stamped  int // messages already carrying a creation context
		fresh    int // messages carrying none
		sameMeta bool
	}{
		{name: "in-request, mixed batch", inCtx: true, stamped: 1, fresh: 1},
		{name: "relay, one shared context", stamped: 2, sameMeta: true},
		{name: "relay, mixed contexts", stamped: 2},
		{name: "relay, mixed and absent", stamped: 1, fresh: 1},
		{name: "relay, nothing carried", fresh: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setup(t)
			mem := broker.NewMemoryBroker()
			pub := o11y.Publisher(mem)

			ctx := context.Background()
			if tc.inCtx {
				var span trace.Span
				ctx, span = rootSpan(t)
				defer span.End()
			}
			var msgs []broker.Message
			var want []string
			shared, _ := originMeta(t)
			for i := 0; i < tc.stamped; i++ {
				meta := shared
				if !tc.sameMeta {
					meta, _ = originMeta(t)
				}
				msgs = append(msgs, broker.Message{ID: "s", Type: "x", Metadata: cloneMeta(meta)})
				want = append(want, meta["traceparent"])
			}
			for i := 0; i < tc.fresh; i++ {
				msgs = append(msgs, broker.Message{ID: "f", Type: "x"})
				want = append(want, "")
			}
			if err := pub.Publish(ctx, "t", msgs...); err != nil {
				t.Fatal(err)
			}

			got := collect(t, mem, "t", len(msgs))
			for i, tp := range want {
				switch {
				case tp == "" && got[i].Metadata["traceparent"] == "":
					t.Errorf("message %d was never injected", i)
				case tp != "" && got[i].Metadata["traceparent"] != tp:
					t.Errorf("message %d: traceparent %q, want the carried %q",
						i, got[i].Metadata["traceparent"], tp)
				}
			}
		})
	}
}

// TestPublisherAdoptsASharedCreationContext is the fix for the 115ms
// hole: the relay publishes from a poll tick that carries no span, so
// the batch's own stored traceparent — the custom creation context the
// messaging conventions talk about — becomes the publish span's parent
// instead of the span orphaning itself into a trace of its own.
func TestPublisherAdoptsASharedCreationContext(t *testing.T) {
	exporter := setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)
	meta, origin := originMeta(t)

	if err := pub.Publish(context.Background(), "tenancy.events",
		broker.Message{ID: "tenancy/1#1", Type: "tenancy.lease_started", Metadata: cloneMeta(meta)},
		broker.Message{ID: "tenancy/1#2", Type: "tenancy.lease_activated", Metadata: cloneMeta(meta)},
	); err != nil {
		t.Fatal(err)
	}

	stub := spanNamed(t, exporter.GetSpans(), "publish tenancy.events")
	if stub.Parent.TraceID() != origin.TraceID() || stub.Parent.SpanID() != origin.SpanID() {
		t.Errorf("publish parent = %v, want the batch's creation context %s/%s",
			stub.Parent, origin.TraceID(), origin.SpanID())
	}
	if len(stub.Links) != 0 {
		t.Errorf("no links wanted when the context is the parent: %v", stub.Links)
	}
}

// TestPublisherLinksAMixedBatchFromANewRoot: a batch belonging to
// several traces has no honest parent, so it gets links instead.
func TestPublisherLinksAMixedBatchFromANewRoot(t *testing.T) {
	exporter := setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)

	first, one := originMeta(t)
	second, two := originMeta(t)
	if err := pub.Publish(context.Background(), "tenancy.events",
		broker.Message{ID: "a", Metadata: cloneMeta(first)},
		broker.Message{ID: "a-again", Metadata: cloneMeta(first)}, // same context, one link
		broker.Message{ID: "b", Metadata: cloneMeta(second)},
		broker.Message{ID: "c"}, // no context at all
	); err != nil {
		t.Fatal(err)
	}

	stub := spanNamed(t, exporter.GetSpans(), "publish tenancy.events")
	if stub.Parent.IsValid() {
		t.Errorf("mixed batch must publish from a new root, parent = %v", stub.Parent)
	}
	if len(stub.Links) != 2 {
		t.Errorf("links = %d, want one per distinct creation context", len(stub.Links))
	}
	if !linkedTo(stub, one) || !linkedTo(stub, two) {
		t.Errorf("links %v do not cover both origins", stub.Links)
	}
	if !hasIntAttr(stub, "gohex.publish.trace_count", 2) {
		t.Errorf("trace count missing: %v", stub.Attributes)
	}
}

// TestPublisherCapsLinks: a relay tick can drain a hundred traces; the
// span reports how many it saw without carrying a link for each.
func TestPublisherCapsLinks(t *testing.T) {
	exporter := setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)

	const batch = 12
	var msgs []broker.Message
	for i := 0; i < batch; i++ {
		meta, _ := originMeta(t)
		msgs = append(msgs, broker.Message{ID: "m", Metadata: cloneMeta(meta)})
	}
	if err := pub.Publish(context.Background(), "tenancy.events", msgs...); err != nil {
		t.Fatal(err)
	}

	stub := spanNamed(t, exporter.GetSpans(), "publish tenancy.events")
	if len(stub.Links) != 8 {
		t.Errorf("links = %d, want the cap of 8", len(stub.Links))
	}
	if !hasIntAttr(stub, "gohex.publish.trace_count", batch) {
		t.Errorf("capped links must still report the true count: %v", stub.Attributes)
	}
}

// TestPublisherInRequestKeepsItsCaller guards the synchronous path: a
// publish inside a request stays a child of that request, whatever the
// messages carry.
func TestPublisherInRequestKeepsItsCaller(t *testing.T) {
	exporter := setup(t)
	mem := broker.NewMemoryBroker()
	pub := o11y.Publisher(mem)
	foreign, origin := originMeta(t)

	ctx, root := rootSpan(t)
	if err := pub.Publish(ctx, "tenancy.events",
		broker.Message{ID: "fresh"},
		broker.Message{ID: "carried", Metadata: cloneMeta(foreign)},
	); err != nil {
		t.Fatal(err)
	}
	root.End()

	stub := spanNamed(t, exporter.GetSpans(), "publish tenancy.events")
	if stub.Parent.SpanID() != root.SpanContext().SpanID() {
		t.Errorf("publish parent = %v, want the calling span %s", stub.Parent, root.SpanContext().SpanID())
	}
	if !linkedTo(stub, origin) {
		t.Errorf("a carried creation context must still be linked: %v", stub.Links)
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
		if s.Name != "consume g x" {
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

// TestConsumeSpanNameCarriesConsumerAndFact pins ADR-0016's naming and
// its fallbacks: the group says who is consuming, the type says what.
func TestConsumeSpanNameCarriesConsumerAndFact(t *testing.T) {
	cases := []struct {
		name, group, msgType, want string
	}{
		{"group and type", "billing.billing_views", "tenancy.lease_started",
			"consume billing.billing_views tenancy.lease_started"},
		{"typeless message", "billing.billing_views", "", "consume billing.billing_views"},
		{"groupless subscriber", "", "tenancy.lease_started", "consume tenancy.events"},
		{"neither", "", "", "consume tenancy.events"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := setup(t)
			stub := deliverOne(t, exporter, "tenancy.events", tc.group,
				broker.Message{ID: "tenancy/1#1", Type: tc.msgType})
			if stub.Name != tc.want {
				t.Errorf("span name = %q, want %q", stub.Name, tc.want)
			}
		})
	}
}

// TestConsumeSpanKeepsSemconvAttributes: the name changed, the queryable
// surface did not — everything a dashboard groups by is still there.
func TestConsumeSpanKeepsSemconvAttributes(t *testing.T) {
	exporter := setup(t)
	stub := deliverOne(t, exporter, "tenancy.events", "billing.billing_views",
		broker.Message{ID: "tenancy/1#1", Type: "tenancy.lease_started"})

	for key, want := range map[string]string{
		"messaging.destination.name":    "tenancy.events",
		"messaging.consumer.group.name": "billing.billing_views",
		"messaging.message.id":          "tenancy/1#1",
		"messaging.operation.name":      "consume",
		"messaging.operation.type":      "process",
		"gohex.message.type":            "tenancy.lease_started",
	} {
		if !hasAttr(stub, key, want) {
			t.Errorf("%s != %q; attributes = %v", key, want, stub.Attributes)
		}
	}
}

// TestFanOutConsumersAreDistinguishableByName is the defect this naming
// exists to fix: four services pulling the same fact used to render as
// four identical waterfall rows.
func TestFanOutConsumersAreDistinguishableByName(t *testing.T) {
	exporter := setup(t)
	groups := []string{"billing.billing_views", "notification.outbox", "payment.ledger", "subscription.plans"}
	for _, group := range groups {
		deliverOne(t, exporter, "tenancy.events", group,
			broker.Message{ID: "tenancy/1#1", Type: "tenancy.lease_started"})
	}

	names := map[string]bool{}
	for _, s := range exporter.GetSpans() {
		names[s.Name] = true
	}
	for _, group := range groups {
		want := "consume " + group + " tenancy.lease_started"
		if !names[want] {
			t.Errorf("missing distinct span %q; got %v", want, spanNames(exporter.GetSpans()))
		}
	}
}

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
// the publish itself, and a broker consume on the other side — with
// every span named so a human reading the waterfall knows who did what.
func TestWeaveEndToEnd(t *testing.T) {
	exporter := setup(t)
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
		_ = sub.Subscribe(runCtx, "counter.events", "reporting.counter_views", func(hctx context.Context, _ broker.Message) error {
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

	// The relay published from a poll tick with no span of its own, and
	// the span still belongs to the request's trace — no gap between the
	// command and the consumer.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		spans := exporter.GetSpans()
		if len(spans) >= 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	publish := spanNamed(t, exporter.GetSpans(), "publish counter.events")
	if publish.SpanContext.TraceID() != root.SpanContext().TraceID() {
		t.Errorf("publish span landed in trace %s, want the originating %s",
			publish.SpanContext.TraceID(), root.SpanContext().TraceID())
	}
	command := spanNamed(t, exporter.GetSpans(), "command billing.capture_payment")
	if publish.Parent.SpanID() != command.SpanContext.SpanID() {
		t.Errorf("publish parent = %v, want the command span that created the event", publish.Parent)
	}
	spanNamed(t, exporter.GetSpans(), "consume reporting.counter_views test.counter_created")
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

// deliverOne runs one message through the traced subscriber and returns
// the consume span it produced. Each subscriber gets its own broker so
// group offsets never interfere.
func deliverOne(t *testing.T, exporter *tracetest.InMemoryExporter, topic, group string, msg broker.Message) tracetest.SpanStub {
	t.Helper()
	mem := broker.NewMemoryBroker()
	if err := mem.Publish(context.Background(), topic, msg); err != nil {
		t.Fatal(err)
	}
	before := len(exporter.GetSpans())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := make(chan struct{})
	var once sync.Once
	go func() {
		_ = o11y.Subscriber(mem).Subscribe(ctx, topic, group, func(context.Context, broker.Message) error {
			once.Do(func() { close(delivered) })
			return nil
		})
	}()
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("message never consumed")
	}
	// The span ends after the handler returns, so wait for the export.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if spans := exporter.GetSpans(); len(spans) > before {
			return spans[len(spans)-1]
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no consume span was exported")
	return tracetest.SpanStub{}
}

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

func cloneMeta(meta map[string]string) map[string]string {
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name
	}
	return names
}

func hasIntAttr(s tracetest.SpanStub, key string, value int64) bool {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key && kv.Value.AsInt64() == value {
			return true
		}
	}
	return false
}

func hasAttr(s tracetest.SpanStub, key, value string) bool {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key && kv.Value.AsString() == value {
			return true
		}
	}
	return false
}
