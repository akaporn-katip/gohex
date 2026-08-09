package kernel

import (
	"fmt"
	"slices"
)

// Aggregate is an event-sourced consistency boundary: its state exists
// only as the replay of its domain events (ADR-0002).
//
// A concrete aggregate embeds [Root] and implements Apply; the unexported
// method on Root means embedding it is the only way to satisfy this
// interface. Behavior methods decide, then record:
//
//	func (o *Order) Cancel() error {
//		if o.cancelled {
//			return ErrAlreadyCancelled // DomainError: decide first
//		}
//		return kernel.Raise(o, OrderCancelled{ID: o.id})
//	}
type Aggregate interface {
	// Apply transitions state for one event. It is called both when an
	// event is first raised and on every replay, so it must be pure state
	// assignment: no decisions, no side effects, no clock reads. Apply is
	// conventionally an explicit type switch over the aggregate's events;
	// unknown events are an error.
	Apply(DomainEvent) error

	// Version is the number of committed events in the aggregate's
	// stream — the expected version for optimistic concurrency. Events
	// raised but not yet taken by a repository do not count.
	Version() int64

	// UncommittedEvents returns the events raised since the last
	// rehydrate/take, in order, without draining them.
	UncommittedEvents() []DomainEvent

	root() *Root
}

// Root is the embeddable base every aggregate (and saga) builds on. Its
// zero value is ready to use.
type Root struct {
	version int64
	pending []DomainEvent
}

// Version implements [Aggregate].
func (r *Root) Version() int64 { return r.version }

// UncommittedEvents implements [Aggregate].
func (r *Root) UncommittedEvents() []DomainEvent { return slices.Clone(r.pending) }

func (r *Root) root() *Root { return r }

// Raise applies e to the aggregate and, only if Apply succeeds, records
// it as uncommitted. An event that cannot be applied is never recorded,
// so a stream can always be replayed.
func Raise(a Aggregate, e DomainEvent) error {
	if err := a.Apply(e); err != nil {
		return fmt.Errorf("raise %s: %w", e.EventName(), err)
	}
	r := a.root()
	r.pending = append(r.pending, e)
	return nil
}

// Rehydrate replays committed history onto a zero-valued aggregate,
// advancing its version and leaving no uncommitted events. Repositories
// call it after loading a stream (and after restoring a snapshot, with
// the tail of events since it).
func Rehydrate(a Aggregate, history ...DomainEvent) error {
	for i, e := range history {
		if err := a.Apply(e); err != nil {
			return fmt.Errorf("rehydrate: event %d (%s): %w", i, e.EventName(), err)
		}
	}
	a.root().version += int64(len(history))
	return nil
}

// TakeUncommitted drains the uncommitted events and advances the version
// past them. Repositories call it after successfully appending the events
// to the store; the aggregate is then consistent with the stream again.
func TakeUncommitted(a Aggregate) []DomainEvent {
	r := a.root()
	events := r.pending
	r.pending = nil
	r.version += int64(len(events))
	return events
}
