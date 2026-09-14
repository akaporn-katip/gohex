package o11y

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/broker"
)

// Publisher wraps a broker publisher with a producer span, injecting the
// current trace context into each message that does not already carry
// one. Inject-if-absent matters: the relay publishes messages whose
// metadata holds the ORIGINAL trace (stamped when the event was stored)
// — that context must win over the relay's own polling loop.
//
// Where the span itself lands depends on what the caller and the batch
// bring (the messaging conventions ask a Send span to at least link to
// the creation context injected into the message; the relay's copied
// traceparent IS that custom creation context):
//
//   - ctx already has a span (an in-request publish): child of ctx, plus
//     a link to every distinct creation context the batch carries.
//   - ctx has no span (the relay's polling loop) and the whole batch
//     shares one creation context: child of THAT context, so the publish
//     lands inside the trace that caused it instead of orphaning itself.
//   - ctx has no span and the batch is mixed: a new root linked to each
//     distinct creation context, capped, with gohex.publish.trace_count.
//
// Parenting the publish to the creation context does not re-attach the
// poller to business traces (ADR-0015): what is parented is the publish
// I/O between two spans already in that trace, not the tick, whose lag
// and health stay on the relay's own spans.
func Publisher(next broker.Publisher) broker.Publisher {
	return &tracingPublisher{next: next}
}

// publishLinkCap bounds a publish span's links. One relay tick can drain
// a hundred events from a hundred traces; the first few answer "where
// did this batch come from", and gohex.publish.trace_count reports how
// many there really were.
const publishLinkCap = 8

type tracingPublisher struct {
	next broker.Publisher
}

func (p *tracingPublisher) Publish(ctx context.Context, topic string, msgs ...broker.Message) error {
	ctx, opts := publishSpanStart(ctx, topic, msgs)
	ctx, span := tracer().Start(ctx, "publish "+topic, opts...)
	defer span.End()

	for i := range msgs {
		if msgs[i].Metadata["traceparent"] == "" {
			msgs[i].Metadata = Inject(ctx, msgs[i].Metadata)
		}
	}
	err := p.next.Publish(ctx, topic, msgs...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// publishSpanStart chooses the publish span's parent and links, and
// returns the context to start it from. The interesting case is the
// relay's: no span on ctx, every message carrying the same stored
// traceparent, which makes that creation context the honest parent —
// otherwise the publish becomes an orphan root and the originating trace
// shows an unexplained gap between the command and the consumers.
//
// The cost is deliberate: the span's duration covers the whole Publish
// call, so a batch that is mostly one trace over-attributes the few
// milliseconds spent on its other messages. Milliseconds of
// over-attribution buy an explained gap.
func publishSpanStart(ctx context.Context, topic string, msgs []broker.Message) (context.Context, []trace.SpanStartOption) {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.destination.name", topic),
		attribute.String("messaging.operation.name", "publish"),
		attribute.String("messaging.operation.type", "send"),
		attribute.Int("messaging.batch.message_count", len(msgs)),
	}
	caller := trace.SpanContextFromContext(ctx)
	creation, carried := creationContexts(msgs, caller)

	// Adopting the batch's context as a parent is only honest when the
	// WHOLE batch belongs to it: one message without a creation context
	// makes the batch mixed, and mixed batches link instead.
	if !caller.IsValid() && len(creation) == 1 && carried == len(msgs) {
		return trace.ContextWithSpanContext(ctx, creation[0]), []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attrs...),
		}
	}

	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindProducer)}
	if len(creation) > 0 {
		attrs = append(attrs, attribute.Int("gohex.publish.trace_count", len(creation)))
		linked := creation
		if len(linked) > publishLinkCap {
			linked = linked[:publishLinkCap]
		}
		links := make([]trace.Link, 0, len(linked))
		for _, sc := range linked {
			links = append(links, trace.Link{SpanContext: sc})
		}
		opts = append(opts, trace.WithLinks(links...))
	}
	return ctx, append(opts, trace.WithAttributes(attrs...))
}

// creationContexts returns the distinct trace contexts the batch already
// carries, in first-encounter order, plus how many messages carried one
// at all. The caller's own span is skipped — linking a span to its own
// parent says nothing. Only a live traceparent counts: an origin stamp
// is a link target, never a parent (ADR-0015).
func creationContexts(msgs []broker.Message, caller trace.SpanContext) ([]trace.SpanContext, int) {
	type identity struct {
		traceID trace.TraceID
		spanID  trace.SpanID
	}
	var out []trace.SpanContext
	carried := 0
	seen := map[identity]bool{}
	for i := range msgs {
		sc := liveSpanContextFrom(msgs[i].Metadata)
		if !sc.IsValid() {
			continue
		}
		carried++
		id := identity{sc.TraceID(), sc.SpanID()}
		if id == (identity{caller.TraceID(), caller.SpanID()}) || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, sc)
	}
	return out, carried
}

// Subscriber wraps a broker subscriber so every delivered message is
// handled inside a consumer span that continues the trace carried in the
// message's metadata. Each redelivery gets its own span.
//
// The span is named "consume <group> <messageType>" (ADR-0016): one fact
// published to one topic is consumed by every service integrating on it,
// and the topic alone renders those rows identically in a waterfall.
func Subscriber(next broker.Subscriber) broker.Subscriber {
	return &tracingSubscriber{next: next}
}

type tracingSubscriber struct {
	next broker.Subscriber
}

func (s *tracingSubscriber) Subscribe(ctx context.Context, topic, group string, handler broker.Handler) error {
	return s.next.Subscribe(ctx, topic, group, func(ctx context.Context, msg broker.Message) error {
		ctx = Extract(ctx, msg.Metadata)
		ctx, span := tracer().Start(ctx, consumeSpanName(topic, group, msg.Type),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.destination.name", topic),
				attribute.String("messaging.consumer.group.name", group),
				attribute.String("messaging.message.id", msg.ID),
				attribute.String("messaging.operation.name", "consume"),
				attribute.String("messaging.operation.type", "process"),
				attribute.String("gohex.message.type", msg.Type),
			))
		defer span.End()

		err := handler(ctx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	})
}

// consumeSpanName degrades one field at a time: the group identifies WHO
// is consuming (services name theirs "<service>.<projection>"), the
// message type WHAT they are consuming. A subscriber wired without a
// group, or a message with no type, still gets a name that says where
// the work happened.
func consumeSpanName(topic, group, messageType string) string {
	switch {
	case group != "" && messageType != "":
		return "consume " + group + " " + messageType
	case group != "":
		return "consume " + group
	default:
		return "consume " + topic
	}
}
