package o11y

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/akaporn-katip/gohex/eventstore"
)

// Backlog metrics answer the one question traces cannot: how much work
// is waiting RIGHT NOW. A span says how long one hand-off took; nothing
// in a trace says the relay is nine thousand events behind, because the
// events that are behind have no span yet (ADR-0017).
//
// Every durable hand-off in this framework has the same shape — an
// append-only log with a head, and a durable checkpoint saying how far
// a worker has processed it (ADR-0003, ADR-0006) — so every backlog is
// one subtraction: head minus position, in events or messages. That is
// what these instruments sample, once per collection interval:
//
//	gohex.relay.lag        store head    - relay checkpoint
//	gohex.projection.lag   source head   - projection checkpoint
//	gohex.inbox.depth      inbox head    - inbox reader checkpoint
//
// They are asynchronous gauges: the callback runs only when the reader
// collects, so a scrape costs one cheap query per watched backlog and
// an idle service costs nothing between collections.
//
// Wiring, after Init, per worker the service runs:
//
//	o11y.WatchRelayLag("ordering.relay",
//	    o11y.PositionFunc(store.Head),
//	    o11y.CheckpointPosition(checkpoints, "ordering.relay"))
//
// The head is supplied by the caller because the eventstore.Store and
// projection.Inbox ports do not report one yet — see the note on
// [Position].

// Position reports a position on an append-only log: either the head
// (the last position that exists) or a checkpoint (the last position a
// worker finished). Both sides are supplied by the caller, keeping
// OpenTelemetry out of the relay, projection and event-store modules
// (ADR-0009).
//
// Checkpoints have a port already: [CheckpointPosition] adapts any
// eventstore.CheckpointStore. Heads do not — neither eventstore.Store
// nor projection.Inbox exposes "the last sequence you hold", so the
// head is a closure over whatever the service's store can answer (for
// eventstore-postgres, "select coalesce(max(global_seq), 0) from
// events"). Adding a Head method to those ports is the follow-up that
// turns each of these calls into a one-liner; it is a port change, so
// it belongs in eventstore/projection minor releases, not in o11y.
type Position func(context.Context) (int64, error)

// PositionFunc adapts a method with the same shape as a Position, so a
// store's own head query can be passed without a wrapper closure.
func PositionFunc(fn func(context.Context) (int64, error)) Position { return fn }

// CheckpointPosition reads the named cursor from a checkpoint store —
// the "processed" side of every backlog in the framework.
func CheckpointPosition(store eventstore.CheckpointStore, name string) Position {
	return func(ctx context.Context) (int64, error) { return store.Get(ctx, name) }
}

// Unwatch stops sampling a backlog. Long-running services never call
// it; tests and services that stop a worker at runtime do.
type Unwatch func() error

// WatchRelayLag samples how far the relay named name trails its event
// store: gohex.relay.lag{gohex.relay.name=name}, in events. A rising
// relay lag means integration events are not reaching the broker, which
// every downstream service feels as staleness.
func WatchRelayLag(name string, head, checkpoint Position) (Unwatch, error) {
	return watchBacklog(backlog{
		instrument:  "gohex.relay.lag",
		unit:        "{event}",
		description: "Events appended to the store that the relay has not published yet.",
		attrs:       []attribute.KeyValue{attribute.String("gohex.relay.name", name)},
		head:        head,
		checkpoint:  checkpoint,
	})
}

// WatchProjectionLag samples how far a projection runner trails the log
// it tails: gohex.projection.lag{gohex.projection.name,
// gohex.projection.source}, in events. Source is "store" for a CatchUp
// runner and "inbox" for an InboxReader — the same projection can run
// both, and they fall behind independently, so the source is part of
// the identity and not a second metric name. This is the number behind
// "the read model is stale" (ADR-0006, ADR-0014).
func WatchProjectionLag(projection, source string, head, checkpoint Position) (Unwatch, error) {
	return watchBacklog(backlog{
		instrument:  "gohex.projection.lag",
		unit:        "{event}",
		description: "Events available to a projection runner that it has not applied yet.",
		attrs: []attribute.KeyValue{
			attribute.String("gohex.projection.name", projection),
			attribute.String("gohex.projection.source", source),
		},
		head:       head,
		checkpoint: checkpoint,
	})
}

// WatchInboxDepth samples how many stored foreign messages an inbox
// holds beyond its reader's checkpoint:
// gohex.inbox.depth{gohex.inbox.name}, in messages. Depth is the
// projection-lag question asked of the inbox itself, and it separates
// "we never received the fact" from "we received it and have not
// applied it" — the two failures look identical from the read model.
func WatchInboxDepth(name string, head, checkpoint Position) (Unwatch, error) {
	return watchBacklog(backlog{
		instrument:  "gohex.inbox.depth",
		unit:        "{message}",
		description: "Messages stored in the inbox that its reader has not applied yet.",
		attrs:       []attribute.KeyValue{attribute.String("gohex.inbox.name", name)},
		head:        head,
		checkpoint:  checkpoint,
	})
}

type backlog struct {
	instrument  string
	unit        string
	description string
	attrs       []attribute.KeyValue
	head        Position
	checkpoint  Position
}

// watchBacklog registers one asynchronous gauge callback. With no
// MeterProvider installed — Config.WithoutExporter, or a collector-less
// laptop run — the global meter is a no-op, so this succeeds and
// samples nothing: the same "silent but functional" contract the other
// two signals keep.
func watchBacklog(b backlog) (Unwatch, error) {
	if b.head == nil || b.checkpoint == nil {
		return nil, fmt.Errorf("o11y: %s: both head and checkpoint positions are required", b.instrument)
	}
	meter := otel.Meter(scope)
	gauge, err := meter.Int64ObservableGauge(b.instrument,
		metric.WithUnit(b.unit),
		metric.WithDescription(b.description),
	)
	if err != nil {
		return nil, fmt.Errorf("o11y: %s: %w", b.instrument, err)
	}
	set := attribute.NewSet(b.attrs...)
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		head, err := b.head(ctx)
		if err != nil {
			return fmt.Errorf("o11y: %s: head: %w", b.instrument, err)
		}
		position, err := b.checkpoint(ctx)
		if err != nil {
			return fmt.Errorf("o11y: %s: checkpoint: %w", b.instrument, err)
		}
		o.ObserveInt64(gauge, lag(head, position), metric.WithAttributeSet(set))
		return nil
	}, gauge)
	if err != nil {
		return nil, fmt.Errorf("o11y: %s: %w", b.instrument, err)
	}
	return reg.Unregister, nil
}

// lag floors the subtraction at zero. A checkpoint ahead of the head is
// a torn read — the head query and the checkpoint query are not one
// transaction, so a worker can advance between them — and reporting a
// negative backlog would put a nonsense point on every dashboard that
// sums these gauges.
func lag(head, position int64) int64 {
	if head <= position {
		return 0
	}
	return head - position
}
