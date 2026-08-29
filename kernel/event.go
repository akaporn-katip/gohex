package kernel

// DomainEvent is a private fact recorded in an aggregate's (or saga's)
// stream. It is the persistence format and source of truth, and it never
// leaves the service: only integration events produced by an explicit
// translator are published (ADR-0004).
//
// Implementations are immutable data carrying value objects, not
// primitives (ADR-0011).
type DomainEvent interface {
	// EventName is the stable, unique name the event is stored under,
	// conventionally "<context>.<fact>" in snake_case, e.g.
	// "ordering.order_placed". It never changes once events with this
	// name have been stored; schema changes are handled by upcasters at
	// deserialization (ADR-0010), not by renaming.
	EventName() string
}
