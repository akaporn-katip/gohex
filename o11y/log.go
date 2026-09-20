package o11y

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// NewSlogHandler builds the handler Init installs on the default
// logger: every record is written as JSON to w — the line an operator
// reads with "kubectl logs", carrying trace_id/span_id attributes — and,
// when provider is non-nil, the SAME record is also emitted as an OTLP
// log record through provider, where the SDK stamps the trace and span
// IDs from the context onto the record itself.
//
// The two sides are deliberately not the same shape: stdout gets a flat
// JSON line whose trace fields are string attributes (a format
// downstream collectors already parse), while the OTLP record gets
// first-class correlation fields, so a backend joins logs to traces
// without parsing anything. A nil provider yields the stdout-only
// handler, which is what the collector-less path installs.
func NewSlogHandler(w io.Writer, name string, provider log.LoggerProvider) slog.Handler {
	stdout := NewLogHandler(slog.NewJSONHandler(w, nil))
	if provider == nil {
		return stdout
	}
	return Fanout(stdout, otelslog.NewHandler(name, otelslog.WithLoggerProvider(provider)))
}

// NewLogHandler wraps a slog handler so records logged with a
// span-carrying context gain trace_id and span_id attributes —
// correlating logs with traces. Init installs it on the default logger.
func NewLogHandler(inner slog.Handler) slog.Handler {
	return &logHandler{inner: inner}
}

// Fanout returns a slog handler that delivers every record to all of
// hs, each getting its own copy. It is enabled when any handler is, and
// returns the joined errors of the handlers it called — one broken sink
// never costs another sink its record.
func Fanout(hs ...slog.Handler) slog.Handler {
	return fanout(hs)
}

type fanout []slog.Handler

func (f fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f fanout) Handle(ctx context.Context, rec slog.Record) error {
	var errs []error
	for _, h := range f {
		if !h.Enabled(ctx, rec.Level) {
			continue
		}
		// Handlers may add attributes, so each gets its own copy.
		if err := h.Handle(ctx, rec.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanout) WithGroup(name string) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}

type logHandler struct {
	inner slog.Handler
}

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, rec)
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{inner: h.inner.WithGroup(name)}
}
