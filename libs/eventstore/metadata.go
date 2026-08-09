package eventstore

import (
	"context"
	"sync/atomic"
)

// contextMetadata extracts cross-cutting metadata (trace context) from a
// context. The o11y module installs the real extractor at startup;
// without it, no metadata is stamped.
var contextMetadata atomic.Pointer[func(context.Context) map[string]string]

// SetContextMetadata installs the extractor [Repository.Save] uses to
// stamp metadata onto appended events. Metadata travels with the stored
// event and onto every message the relay publishes from it — this is how
// a trace that started at the HTTP edge survives through the event store
// into the broker. Call once at startup (o11y.Init does).
func SetContextMetadata(fn func(context.Context) map[string]string) {
	contextMetadata.Store(&fn)
}

// MetadataFromContext returns the installed extractor's metadata for
// ctx, or nil when no extractor is installed or it has nothing to add.
func MetadataFromContext(ctx context.Context) map[string]string {
	fn := contextMetadata.Load()
	if fn == nil {
		return nil
	}
	md := (*fn)(ctx)
	if len(md) == 0 {
		return nil
	}
	return md
}
