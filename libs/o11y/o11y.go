// Package o11y weaves OpenTelemetry through the framework so ONE trace
// spans the whole asynchronous flow: HTTP edge -> command bus ->
// aggregate events (stamped into the event store) -> relay -> broker ->
// saga -> commands -> more services. Trace context rides the envelope's
// Metadata (ADR-0005) and the stored event's metadata; nothing here is
// business data.
//
// Wiring, once at startup:
//
//	shutdown, err := o11y.Init(ctx, o11y.Config{ServiceName: "ordering"})
//	defer shutdown(ctx)
//
//	publisher  := o11y.Publisher(kafkaBroker)   // inject-if-absent
//	subscriber := o11y.Subscriber(kafkaBroker)  // extract + consumer span
//	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()))
//
// Init also installs the event-store metadata hook, which is the subtle
// part: Repository.Save stamps the current trace context onto every
// appended event, and the relay copies stored metadata onto outgoing
// messages — so the trace survives THROUGH the database into the broker.
//
// This slice covers traces and slog; metrics land with the example
// services.
package o11y

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/libs/eventstore"
)

// Config configures Init.
type Config struct {
	// ServiceName names this service in traces (resource service.name).
	ServiceName string
	// OTLPEndpoint overrides the OTLP/HTTP endpoint, e.g.
	// "localhost:4318" (plain HTTP). Empty uses the standard
	// OTEL_EXPORTER_OTLP_* environment variables.
	OTLPEndpoint string
	// WithoutExporter skips exporter setup — propagation, the metadata
	// hook, and logging still work. For tests and collector-less runs.
	WithoutExporter bool
}

// Init wires OpenTelemetry and slog for a service: W3C propagation, the
// OTLP/HTTP trace exporter, the event-store metadata hook, and a JSON
// slog default whose records carry trace_id/span_id. The returned
// shutdown flushes pending spans.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	eventstore.SetContextMetadata(func(ctx context.Context) map[string]string {
		return Inject(ctx, nil)
	})
	slog.SetDefault(slog.New(NewLogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	if cfg.WithoutExporter {
		return func(context.Context) error { return nil }, nil
	}

	var opts []otlptracehttp.Option
	if cfg.OTLPEndpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint), otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("o11y: exporter: %w", err)
	}
	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(cfg.ServiceName)))
	if err != nil {
		return nil, fmt.Errorf("o11y: resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

const scope = "github.com/akaporn-katip/gohex/libs/o11y"

func tracer() trace.Tracer { return otel.Tracer(scope) }

// Inject writes ctx's trace context into meta (allocating it if nil) and
// returns it. Returns nil when ctx carries no span and meta was nil.
func Inject(ctx context.Context, meta map[string]string) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return meta
	}
	if meta == nil {
		meta = map[string]string{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(meta))
	return meta
}

// Extract returns ctx extended with the trace context carried in meta.
func Extract(ctx context.Context, meta map[string]string) context.Context {
	if len(meta) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(meta))
}
