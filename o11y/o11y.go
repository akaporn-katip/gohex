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
// Span names say who is doing what, not just where (ADR-0016). The OTel
// messaging conventions ask for "<operation> <destination>" but allow a
// documented system-specific format, and this is the documentation:
//
//	publish <topic>                    e.g. publish tenancy.events
//	consume <group> <messageType>      e.g. consume billing.billing_views tenancy.lease_started
//	project <projection> <messageType> e.g. project billing_views tenancy.lease_started
//	command <commandName>              e.g. command tenancy.start_lease
//
// A producer's destination is the topic, so "publish" keeps it. A
// consumer's destination is shared by every service integrating on the
// fact, so the consumer group (conventionally "<service>.<projection>")
// and the message type carry the name instead; without them a fan-out of
// five services renders as five identical rows. Both are compile-time
// sets, so cardinality stays bounded. Names degrade field by field:
// "consume <group>", then "consume <topic>".
//
// The relay publishes from a poll tick that has no span of its own, so
// [Publisher] takes its parent from the batch when the caller offers
// none: a batch that all came from one trace makes the publish a child
// of that trace, a mixed batch makes it a linked new root. The publish
// step therefore appears where a reader expects it, between the command
// that wrote the event and the consumers that read it.
//
// Durable hand-offs that a POLLING worker picks up later (a pending-list
// row, a worklist table) do not continue the trace — they start a new
// one linked to the origin (ADR-0015). That boundary is about the
// waiting, not about the I/O either side of it: the tick, its lag and
// its health live on the worker's own spans, and an origin stamp is
// always a link, never a parent. See [StartLinked], [StartBatch],
// [LinkFrom], [OriginMetadata] and, for the projection runners,
// [ProjectionHook].
//
// All three signals travel the same OTLP/HTTP pipe (ADR-0017). Logs
// fan out: the JSON line still goes to stdout for "kubectl logs", and
// the same record is bridged to the OTLP LoggerProvider with trace
// correlation intact. Metrics cover what only this framework can know —
// the backlogs behind its durable hand-offs, sampled from checkpoints;
// see [WatchRelayLag], [WatchProjectionLag] and [WatchInboxDepth].
// Latency and error rates are derived from the spans above at the
// collector, so no instrument here duplicates them.
package o11y

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/akaporn-katip/gohex/eventstore"
)

// Config configures Init.
type Config struct {
	// ServiceName names this service in every signal — traces, logs and
	// metrics all carry it as the resource's service.name.
	ServiceName string
	// OTLPEndpoint overrides the OTLP/HTTP endpoint, e.g.
	// "localhost:4318" (plain HTTP). Empty uses the standard
	// OTEL_EXPORTER_OTLP_* environment variables.
	OTLPEndpoint string
	// WithoutExporter skips exporter setup for every signal —
	// propagation, the metadata hook, stdout logging and the backlog
	// instruments still work, they just reach no collector. For tests and
	// collector-less runs.
	WithoutExporter bool
}

// Init wires OpenTelemetry and slog for a service: W3C propagation, the
// event-store metadata hook, a JSON slog default whose records carry
// trace_id/span_id, and OTLP/HTTP exporters for all three signals —
// traces, logs and metrics. Logs keep going to stdout as well; the OTLP
// side is an addition, not a replacement (ADR-0017).
//
// The returned shutdown flushes and stops every provider Init
// installed, so nothing buffered is lost on a clean exit.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	eventstore.SetContextMetadata(func(ctx context.Context) map[string]string {
		// The origin stamp (if a worker put one on ctx, ADR-0015) travels
		// alongside the live trace context, so a linked trace stays
		// traceable to its origin one hop further on.
		return Inject(ctx, originFromContext(ctx))
	})
	// Stdout first and unconditionally: a service with no collector, or
	// one whose exporter setup fails below, still logs the same lines.
	slog.SetDefault(slog.New(NewSlogHandler(os.Stdout, cfg.ServiceName, nil)))

	if cfg.WithoutExporter {
		return noShutdown, nil
	}

	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(cfg.ServiceName)))
	if err != nil {
		return nil, fmt.Errorf("o11y: resource: %w", err)
	}

	// Each provider is registered as it is built, and every shutdown
	// collected, so a failure half-way through still tears down what
	// already exists instead of leaking a batcher goroutine.
	var shutdowns []func(context.Context) error
	fail := func(err error) (func(context.Context) error, error) {
		_ = flushAll(ctx, shutdowns)
		return nil, err
	}

	traceExporter, err := otlptracehttp.New(ctx, otlpTraceOptions(cfg)...)
	if err != nil {
		return fail(fmt.Errorf("o11y: trace exporter: %w", err))
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)
	shutdowns = append(shutdowns, tracerProvider.Shutdown)

	logExporter, err := otlploghttp.New(ctx, otlpLogOptions(cfg)...)
	if err != nil {
		return fail(fmt.Errorf("o11y: log exporter: %w", err))
	}
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	logglobal.SetLoggerProvider(loggerProvider)
	shutdowns = append(shutdowns, loggerProvider.Shutdown)
	// Only now does the default logger gain its second sink: the stdout
	// half is byte-for-byte what it was a moment ago.
	slog.SetDefault(slog.New(NewSlogHandler(os.Stdout, cfg.ServiceName, loggerProvider)))

	metricExporter, err := otlpmetrichttp.New(ctx, otlpMetricOptions(cfg)...)
	if err != nil {
		return fail(fmt.Errorf("o11y: metric exporter: %w", err))
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)
	shutdowns = append(shutdowns, meterProvider.Shutdown)

	return func(ctx context.Context) error { return flushAll(ctx, shutdowns) }, nil
}

func noShutdown(context.Context) error { return nil }

// flushAll shuts every provider down, even if an early one fails —
// a stuck trace exporter must not cost the metrics their last export.
func flushAll(ctx context.Context, shutdowns []func(context.Context) error) error {
	var errs []error
	for _, fn := range shutdowns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("o11y: shutdown: %w", err)
	}
	return nil
}

// An explicit Config.OTLPEndpoint means "plain HTTP, this host"; empty
// leaves each exporter to read the standard OTEL_EXPORTER_OTLP_*
// environment variables, signal-specific ones included.
func otlpTraceOptions(cfg Config) []otlptracehttp.Option {
	if cfg.OTLPEndpoint == "" {
		return nil
	}
	return []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.OTLPEndpoint), otlptracehttp.WithInsecure(),
	}
}

func otlpLogOptions(cfg Config) []otlploghttp.Option {
	if cfg.OTLPEndpoint == "" {
		return nil
	}
	return []otlploghttp.Option{
		otlploghttp.WithEndpoint(cfg.OTLPEndpoint), otlploghttp.WithInsecure(),
	}
}

func otlpMetricOptions(cfg Config) []otlpmetrichttp.Option {
	if cfg.OTLPEndpoint == "" {
		return nil
	}
	return []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(cfg.OTLPEndpoint), otlpmetrichttp.WithInsecure(),
	}
}

const scope = "github.com/akaporn-katip/gohex/o11y"

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
