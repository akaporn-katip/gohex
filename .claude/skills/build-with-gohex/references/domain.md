# Domain building blocks: aggregates, value objects, events, handlers

Canonical examples in the gohex checkout:

- `services/ordering/internal/domain/order.go` — the model file: value objects
  with parsing constructors and stable JSON shapes, domain events, the
  aggregate with `Apply`, a static factory (`Place`) using `kernel.Raise`.
- `services/billing/internal/domain/payment.go` — an aggregate with more than
  one lifecycle step (capture → refund) and replay-safe rejection.
- `services/ordering/internal/app/app.go` — command/query handlers, event
  registration, repository construction.
- `libs/kernel/doc.go` and the kernel source — the aggregate contract itself.
- ADR-0002 (event sourcing everywhere), ADR-0010 (upcasters, opt-in
  snapshots), ADR-0011 (value objects), ADR-0012 (domain errors).

## Aggregates

An aggregate embeds `kernel.Root`, and its state exists only as the replay of
its events:

- **Decide** in a static factory or method: validate, then `kernel.Raise` the
  event. Raise applies the event immediately — decide-first, so a failed
  business rule records nothing (`libs/kernel/aggregate_test.go` pins this).
- **Apply** is a pure state transition on the event — no validation, no I/O,
  no errors except "unknown event". All invariant checks live in the deciders.
- Persistence goes through `eventstore.Repository[A]` (constructed with the
  store, registry, a stream **category** constant, and a zero-value factory).
  Save is optimistic-concurrency-guarded; stale saves conflict
  (`libs/eventstore/repository_test.go`).
- Lifecycle the aggregate can't decide alone (cross-service outcomes) does
  **not** belong on it — that's the saga's and the read model's job. Note how
  small `Order` stays.

## Value objects (ADR-0011)

Every field on an aggregate or domain event is a value object, never a bare
primitive. The pattern (see `Money`, `Quantity` in order.go):

- Unexported fields; the zero value is invalid; construct only via a parsing
  constructor (`NewMoney`) returning a `DomainError` on bad input.
- Explicit `MarshalJSON`/`UnmarshalJSON` against a named serialized shape —
  the stored payload is event schema. Changing it = new event version +
  upcaster, never a rewrite.
- IDs are `kernel.ID[T]` with a marker type per identity (`OrderID =
  kernel.ID[Order]`; empty struct markers for foreign identities like
  `Customer`). For streams keyed by an external string (billing keys payments
  by order ID), use the store's string-ID form as billing does.

## Domain events

Private structs in the domain package with an `EventName()` of the form
`<service>.<past_tense_fact>`. They never leave the service (that's the
translator's job — `references/integration.md`). Register every one in the
service's `RegisterEvents` (see app.go); the registry round-trips them and
fails loudly on unregistered types.

**Evolving an event**: keep the stored shape, add an upcaster
(`eventstore.Upcaster`, a pure payload→payload function registered for the
old version) — chainable, tested in `libs/eventstore/registry_test.go`
(`TestUpcasterChain`). Snapshots are opt-in and never a source of truth
(ADR-0010).

## Domain errors (ADR-0012)

`kernel.NewDomainError(code, message)` for every business rejection, declared
as package-level `Err…` vars in the domain package. The split is behavioral:
domain errors are never retried and surface as 4xx or rejection events;
everything else is infrastructure and gets retried
(`libs/cqrs/command_test.go` — `TestDomainErrorsAreNeverRetried`).

## Command and query handlers

Application-layer methods on a `Handlers` struct holding the repository and
ports (imitate ordering's app.go): load-or-create via repository, call the
domain, save. Register with the generic `cqrs.Handle` / `cqrs.HandleQuery`.
Local commands (dispatched by your own HTTP edge) are plain structs with
`CommandName()`; commands arriving over the wire are contracts — see
`references/integration.md`, including why wire handlers must be replay-safe.

## Tests you must ship (non-negotiable)

Style: pure Go tests, framework in-memory fakes, no mocks, no containers.
Imitate these files:

- **Aggregate tests** — `libs/kernel/aggregate_test.go`: decide-first
  (rejected command records nothing), raise-records-and-applies, rehydrate
  rebuilds state, rehydrate-then-raise continues the stream. Given
  events → command → expect events/error.
- **Value-object tests** — `libs/kernel/id_test.go` shape: constructor rejects
  bad input, JSON round-trips, zero value refuses to marshal.
- **Handler tests** — use `eventstore.NewMemoryStore()` + the registry to
  drive handlers end-to-end (save/load round-trip, stale-conflict:
  `libs/eventstore/repository_test.go`). Wire-facing handlers additionally
  prove replay-safety: handling the same command twice is a no-op, not a
  duplicate event.
