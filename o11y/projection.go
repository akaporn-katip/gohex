package o11y

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/projection"
)

// ProjectionHook is the observer for the projection runners: every
// applied event gets a span carrying the item's identity, so read-model
// work stops being invisible. The span is named
// "project <projection> <messageType>" (ADR-0016) — a projection applies
// many kinds of fact, and only the name shows in a waterfall row.
//
//	projection.Config{Observe: o11y.ProjectionHook()}
//
// By default each apply is a NEW trace linked back to the event that
// caused it — the runners are pollers over a durable log (ADR-0015), and
// a rebuild replaying a year of history must not graft spans onto a year
// of old traces. Pass [ContinueTrace] to make live applies children of
// the originating trace instead: legitimate for the sub-second tailing
// case (seeing "write → read model visible" in one trace), wrong for
// anything business-paced.
func ProjectionHook(opts ...ProjectionOption) projection.Observer {
	var cfg projectionConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return func(ctx context.Context, item projection.Item) (context.Context, func(error)) {
		name := projectSpanName(item)
		attrs := []attribute.KeyValue{
			attribute.String("gohex.projection.name", item.Projection),
			attribute.String("gohex.projection.source", string(item.Source)),
			attribute.String("gohex.message.type", item.Name),
			attribute.String("messaging.message.id", item.ID),
			attribute.String("messaging.operation.name", "project"),
		}

		var span trace.Span
		if cfg.continueTrace {
			// StartLinked stamps messaging.operation.type itself; this branch
			// bypasses it, so match what the linked span reports.
			attrs = append(attrs, attribute.String("messaging.operation.type", "process"))
			ctx, span = tracer().Start(Extract(ctx, item.Metadata), name,
				trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
		} else {
			ctx, span = StartLinked(ctx, name, item.Metadata, trace.WithAttributes(attrs...))
		}
		return ctx, func(err error) {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}
	}
}

// projectSpanName falls back to the bare projection name for items that
// carry no type — a runner is still worth locating in a trace.
func projectSpanName(item projection.Item) string {
	if item.Name == "" {
		return "project " + item.Projection
	}
	return "project " + item.Projection + " " + item.Name
}

// ProjectionOption configures [ProjectionHook].
type ProjectionOption func(*projectionConfig)

type projectionConfig struct {
	continueTrace bool
}

// ContinueTrace makes [ProjectionHook] continue the originating trace
// instead of starting a linked one — the deliberate opt-out from
// ADR-0015 for near-real-time tailing.
func ContinueTrace() ProjectionOption {
	return func(c *projectionConfig) { c.continueTrace = true }
}
