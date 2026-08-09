// Package eventstore defines the event store port (ADR-0003): append-only
// streams of serialized domain events with optimistic concurrency, plus
// the machinery that bridges it to kernel aggregates — a JSON codec with
// upcasters (ADR-0010), a generic repository, and an in-memory store for
// tests and single-process wiring.
package eventstore

import (
	"context"
	"errors"
	"time"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// ErrVersionConflict is returned by Append when the stream has moved past
// the expected version. It is deliberately NOT a DomainError: a conflict
// is transient — the command bus reloads the aggregate and retries
// (ADR-0012).
var ErrVersionConflict = errors.New("eventstore: version conflict")

// ErrNotFound is returned when a stream has no events. It IS a
// DomainError: a command addressing an aggregate that does not exist is
// definitively rejected, never retried.
var ErrNotFound = kernel.NewDomainError("not_found", "aggregate does not exist")

// StreamID identifies one aggregate's (or saga's) stream: a category
// (e.g. "order") plus the aggregate's identifier.
type StreamID struct {
	Category string
	ID       string
}

// EventData is an event ready to append: the serialized payload plus the
// envelope fields the store persists verbatim. Produced by
// [Registry.Encode]; Metadata carries cross-cutting context such as trace
// propagation and is never part of the domain payload.
type EventData struct {
	EventName     string
	SchemaVersion int
	Payload       []byte
	Metadata      map[string]string
}

// RecordedEvent is an event as stored: EventData plus the positions and
// timestamp the store assigned.
type RecordedEvent struct {
	// GlobalSeq is the event's position in the store-wide order that the
	// relay and projections tail (ADR-0003, ADR-0006). Strictly
	// increasing; not guaranteed contiguous.
	GlobalSeq int64
	Stream    StreamID
	// Version is the event's 1-based position within its stream.
	Version       int64
	EventName     string
	SchemaVersion int
	Payload       []byte
	Metadata      map[string]string
	OccurredAt    time.Time
}

// Store is the event store port. Implementations must be safe for
// concurrent use.
type Store interface {
	// Append atomically appends events to a stream. expectedVersion is
	// the number of events the caller believes the stream holds (0 for a
	// new stream); on mismatch Append fails with ErrVersionConflict and
	// stores nothing. Events must become visible to ReadAll in commit
	// order, so tailing by GlobalSeq never skips events.
	Append(ctx context.Context, stream StreamID, expectedVersion int64, events []EventData) error

	// Load returns the stream's events with Version > afterVersion, in
	// version order. A missing stream yields an empty slice, not an
	// error; afterVersion 0 loads the full stream.
	Load(ctx context.Context, stream StreamID, afterVersion int64) ([]RecordedEvent, error)

	// ReadAll returns up to limit events with GlobalSeq > afterSeq in
	// global order (limit <= 0 means no limit). This is the tailing read
	// used by the relay and projections.
	ReadAll(ctx context.Context, afterSeq int64, limit int) ([]RecordedEvent, error)
}
