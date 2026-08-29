# gohex Framework

The ubiquitous language of the gohex framework itself — the concepts its libraries expose and its documentation teaches. The example system's business contexts have their own glossaries (see [gohex-example](https://github.com/akaporn-katip/gohex-example)).

## Language

### Domain building blocks

**Aggregate**:
An event-sourced consistency boundary; its state exists only as the replay of its stream.
_Avoid_: Entity (reserve for non-root objects inside an aggregate), model

**Value Object**:
An immutable, self-validating domain type (e.g. `Money`, `OrderID`) constructed only via a parsing constructor; aggregates and domain events hold value objects, never raw primitives.
_Avoid_: Wrapper type, newtype

**Domain Event**:
A private fact recorded in an aggregate's or saga's stream; the persistence format and source of truth. Never leaves the service.
_Avoid_: Internal event, stream event

**Integration Event**:
A versioned public fact (e.g. `OrderPlacedV1`) published to the broker, produced from a domain event by a Translator. The only events other services may consume.
_Avoid_: Public event, external event

**Translator**:
An explicit mapping from a domain event to an integration event. Absence of a translator means the domain event stays private.
_Avoid_: Mapper, converter

**Domain Error**:
A business-rule violation (e.g. insufficient stock): a definitive rejection, never retried; surfaces as a 4xx at the edge or a rejection integration event for sagas to compensate on. Everything else is an infrastructure error, which is retried and never encodes business meaning.
_Avoid_: Business error, validation error (both are domain errors; use the one term)

**Command**:
An instruction to change state, addressed to exactly one handler; delivered between services as a message on a command topic, never by synchronous call.

### Event store & delivery

**Stream**:
The ordered sequence of domain events for one aggregate or saga instance, identified by stream ID and guarded by optimistic concurrency on version.

**Relay**:
The framework component that tails the event store's global sequence and reliably publishes translated events (and requested commands) to the broker, checkpointing its position. At-least-once, ordered.
_Avoid_: Outbox processor, forwarder, publisher

**Envelope**:
The standard wrapper around every message on the broker: event ID, type, version, occurred-at, trace context.

**Upcaster**:
A pure function that lifts a stored event payload from one schema version to the next during deserialization; chainable. Stored payloads are never rewritten.

**Snapshot**:
An opt-in, periodically stored capture of aggregate state used to shorten replay; never a source of truth.

**Checkpoint**:
A durable cursor recording how far a relay or projection has processed the global sequence.

### Read side

**Projection**:
A set of event handlers that maintain a read model. Fed from the service's own store (own events) and from the broker (foreign integration events).
_Avoid_: View builder, materializer

**Read Model**:
A denormalized, query-shaped table owned by one service; disposable and rebuildable from events.
_Avoid_: View, query model

**Inbox**:
A local record of consumed foreign integration events, kept so projections can rebuild past broker retention.

### Coordination

**Saga**:
A long-running, event-sourced process manager: it reacts to integration events, issues commands, and compensates on failure. Orchestration-style; the flow lives in one place.
_Avoid_: Process manager, workflow (both mean this; "saga" is the canonical term here)

**Compensation**:
A command a saga issues to semantically undo an earlier step after a downstream failure.
