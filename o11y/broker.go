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
func Publisher(next broker.Publisher) broker.Publisher {
	return &tracingPublisher{next: next}
}

type tracingPublisher struct {
	next broker.Publisher
}

func (p *tracingPublisher) Publish(ctx context.Context, topic string, msgs ...broker.Message) error {
	ctx, span := tracer().Start(ctx, "publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", topic),
			attribute.Int("messaging.batch.message_count", len(msgs)),
		))
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

// Subscriber wraps a broker subscriber so every delivered message is
// handled inside a consumer span that continues the trace carried in the
// message's metadata. Each redelivery gets its own span.
func Subscriber(next broker.Subscriber) broker.Subscriber {
	return &tracingSubscriber{next: next}
}

type tracingSubscriber struct {
	next broker.Subscriber
}

func (s *tracingSubscriber) Subscribe(ctx context.Context, topic, group string, handler broker.Handler) error {
	return s.next.Subscribe(ctx, topic, group, func(ctx context.Context, msg broker.Message) error {
		ctx = Extract(ctx, msg.Metadata)
		ctx, span := tracer().Start(ctx, "consume "+topic,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.destination.name", topic),
				attribute.String("messaging.consumer.group.name", group),
				attribute.String("messaging.message.id", msg.ID),
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
