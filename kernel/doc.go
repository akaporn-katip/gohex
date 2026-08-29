// Package kernel is gohex's zero-dependency domain kernel: the building
// blocks every bounded context imports and nothing more. It contains no
// I/O, no infrastructure, and depends only on the standard library.
//
// # Building blocks
//
//   - [Aggregate] and [Root]: event-sourced aggregates. State exists only
//     as the replay of domain events ([Raise], [Rehydrate]).
//   - [DomainEvent]: a private fact recorded in an aggregate's stream.
//     Domain events never leave the service; only mapped integration
//     events do (ADR-0004).
//   - [ID]: typed identifiers. ID[Order] and ID[Customer] are distinct
//     types, so mixing them up is a compile error.
//   - [DomainError]: a definitive business-rule rejection, never retried.
//     Any other error is infrastructure and is retried (ADR-0012).
//
// # Value-object conventions
//
// Aggregate state and domain events hold value objects, never raw
// primitives (ADR-0011). The kernel ships the machinery; each domain
// defines its own types (Money, SKU, Quantity, ...) following these
// conventions:
//
//   - Immutable: no setters; operations return new values.
//   - Parse, don't validate: the only constructor is NewX(...) (X, error);
//     a value that exists is valid.
//   - The zero value is invalid and never silently marshaled.
//   - Implement encoding.TextMarshaler/TextUnmarshaler (or json.Marshaler)
//     so the value serializes stably inside stored events. A change to a
//     value object's serialized shape IS an event schema change and must
//     go through an upcaster (ADR-0010).
package kernel
