package eventstore

import (
	"context"
	"sync"
)

// contextMetadata holds all extractors registered via AddContextMetadata.
// MetadataFromContext calls each in registration order and merges their
// outputs; a later extractor's key-value pair overwrites an earlier one
// that set the same key.
var (
	contextMetadataMu         sync.RWMutex
	contextMetadataExtractors []func(context.Context) map[string]string
)

// AddContextMetadata registers an extractor [Repository.Save] will call
// to stamp metadata onto appended events. Metadata travels with the
// stored event and onto every message the relay publishes from it — this
// is how a trace that started at the HTTP edge survives through the
// event store into the broker. Call at startup (o11y.Init does, and the
// featureflag pinning extractor must be registered here as well).
//
// Extractors run in registration order. When two extractors return the
// same key, the later one wins (ADR-0021).
func AddContextMetadata(fn func(context.Context) map[string]string) {
	contextMetadataMu.Lock()
	contextMetadataExtractors = append(contextMetadataExtractors, fn)
	contextMetadataMu.Unlock()
}

// SetContextMetadata installs a single extractor, replacing any
// previously installed one. Use [AddContextMetadata] to accumulate
// extractors instead (trace context, pinned flags); SetContextMetadata
// is kept for backward compatibility.
//
// Deprecated: Use [AddContextMetadata], which merges outputs from
// multiple extractors rather than silently replacing the previous one.
func SetContextMetadata(fn func(context.Context) map[string]string) {
	contextMetadataMu.Lock()
	contextMetadataExtractors = []func(context.Context) map[string]string{fn}
	contextMetadataMu.Unlock()
}

// MetadataFromContext returns merged metadata from all registered
// extractors for ctx, or nil when no extractors are registered or they
// all returned empty. Later extractors' keys override earlier ones.
func MetadataFromContext(ctx context.Context) map[string]string {
	contextMetadataMu.RLock()
	extractors := contextMetadataExtractors
	contextMetadataMu.RUnlock()

	if len(extractors) == 0 {
		return nil
	}

	var merged map[string]string
	for _, fn := range extractors {
		md := fn(ctx)
		if len(md) == 0 {
			continue
		}
		if merged == nil {
			merged = make(map[string]string, len(md))
		}
		for k, v := range md {
			merged[k] = v
		}
	}

	if len(merged) == 0 {
		return nil
	}
	return merged
}
