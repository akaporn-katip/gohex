package o11y

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Metadata keys carrying the ORIGIN trace of a durable hand-off
// (ADR-0015). They ride the same metadata map as "traceparent" — on
// event metadata, on the envelope, on a worklist row — and let a chain
// of linked traces stay queryable one hop past the link itself.
const (
	OriginTraceparentKey = "gohex.origin.traceparent"
	OriginTracestateKey  = "gohex.origin.tracestate"
)

// LinkFrom turns stored metadata into a link back to the span that
// produced it. It reads "traceparent"/"tracestate" and falls back to
// [OriginTraceparentKey]/[OriginTracestateKey], so a row that kept only
// the origin keys still links correctly.
//
// Metadata that is empty, missing a traceparent, or malformed yields the
// zero Link — callers drop it rather than emitting a link to nowhere
// (check trace.Link.SpanContext.IsValid()).
func LinkFrom(meta map[string]string) trace.Link {
	sc := spanContextFrom(meta)
	if !sc.IsValid() {
		return trace.Link{}
	}
	return trace.Link{SpanContext: sc}
}

// spanContextFrom resolves the span context stored in meta, preferring a
// live traceparent over the origin keys.
func spanContextFrom(meta map[string]string) trace.SpanContext {
	if len(meta) == 0 {
		return trace.SpanContext{}
	}
	carrier := map[string]string{}
	switch {
	case meta["traceparent"] != "":
		carrier["traceparent"] = meta["traceparent"]
		if ts := meta["tracestate"]; ts != "" {
			carrier["tracestate"] = ts
		}
	case meta[OriginTraceparentKey] != "":
		carrier["traceparent"] = meta[OriginTraceparentKey]
		if ts := meta[OriginTracestateKey]; ts != "" {
			carrier["tracestate"] = ts
		}
	default:
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(Extract(context.Background(), carrier))
}

// OriginMetadata returns the origin-trace stamp for meta: the trace
// context in meta rewritten under [OriginTraceparentKey]. Metadata that
// already carries an origin stamp keeps it — the FIRST origin in a chain
// of hand-offs is the interesting one. Returns nil when meta carries no
// trace context at all.
//
// Workers rarely call this directly: [StartLinked] puts the origin on
// the context, and Init's event-store hook stamps it onto every event
// the worker's commands produce.
func OriginMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	if tp := meta[OriginTraceparentKey]; tp != "" {
		out := map[string]string{OriginTraceparentKey: tp}
		if ts := meta[OriginTracestateKey]; ts != "" {
			out[OriginTracestateKey] = ts
		}
		return out
	}
	if tp := meta["traceparent"]; tp != "" {
		out := map[string]string{OriginTraceparentKey: tp}
		if ts := meta["tracestate"]; ts != "" {
			out[OriginTracestateKey] = ts
		}
		return out
	}
	return nil
}

type originContextKey struct{}

// WithOrigin puts the origin stamp derived from meta on ctx, so every
// event saved under ctx carries [OriginTraceparentKey] alongside its own
// traceparent. [StartLinked] does this for you.
func WithOrigin(ctx context.Context, meta map[string]string) context.Context {
	origin := OriginMetadata(meta)
	if origin == nil {
		return ctx
	}
	return context.WithValue(ctx, originContextKey{}, origin)
}

// originFromContext returns a fresh copy of ctx's origin stamp, or nil.
func originFromContext(ctx context.Context) map[string]string {
	origin, _ := ctx.Value(originContextKey{}).(map[string]string)
	if len(origin) == 0 {
		return nil
	}
	out := make(map[string]string, len(origin))
	for k, v := range origin {
		out[k] = v
	}
	return out
}

// StartLinked starts the span a polling worker does its work under: a
// NEW root trace (the poll tick is not part of the originating request,
// ADR-0015) carrying a link back to the origin span stored in meta, plus
// a link to the worker's own enclosing span — the batch/tick span from
// [StartBatch] — when ctx has one. The returned context also carries the
// origin stamp, so events the work produces are traceable back to the
// origin without a link hop ([WithOrigin]).
//
// Invalid or absent metadata is not an error: the span is simply an
// unlinked new root. Attributes mirror the broker instrumentation
// (span kind consumer, messaging.operation.type "process"); pass
// trace.WithAttributes for the worklist's own identifiers.
func StartLinked(ctx context.Context, name string, meta map[string]string,
	opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	origin := spanContextFrom(meta)

	var links []trace.Link
	if origin.IsValid() {
		links = append(links, trace.Link{SpanContext: origin})
	}
	if batch := trace.SpanContextFromContext(ctx); batch.IsValid() {
		links = append(links, trace.Link{SpanContext: batch})
	}

	attrs := []attribute.KeyValue{
		attribute.String("messaging.operation.type", "process"),
	}
	if origin.IsValid() {
		attrs = append(attrs,
			attribute.String("gohex.origin.trace_id", origin.TraceID().String()),
			attribute.String("gohex.origin.span_id", origin.SpanID().String()))
	}

	start := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if len(links) > 0 {
		start = append(start, trace.WithLinks(links...))
	}
	ctx, span := tracer().Start(ctx, name, append(start, opts...)...)
	return WithOrigin(ctx, meta), span
}

// StartBatch starts a worker's per-tick span: its own root trace,
// recording how much work the tick picked up. It is the home for worker
// health — tick duration, batch size, failures — which no originating
// request could sensibly parent. Use its context as the parent argument
// to [StartLinked] so each item links back to the tick that ran it.
func StartBatch(ctx context.Context, name string, size int) (context.Context, trace.Span) {
	return tracer().Start(ctx, name,
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.operation.type", "process"),
			attribute.Int("messaging.batch.message_count", size),
		))
}
