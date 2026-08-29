# Cross-service integration: contracts, translators, wire commands, projections, sagas

Canonical examples in the gohex checkout:

- `gohex-example/contracts/contracts.go` — the contract catalog: every integration
  event, command, and topic name in one dependency-free module.
- `gohex-example/ordering/internal/app/app.go` (`RegisterTranslators`) and
  `gohex-example/billing/internal/app/app.go` — translators; billing also shows
  wire-command registration and replay-safe handlers.
- `gohex-example/ordering/internal/app/saga.go` — the fulfillment saga, including
  compensation.
- `gohex-example/ordering/internal/app/projection.go` — the hybrid `order_summary`
  projection.
- `gohex-example/billing/cmd/billing/main.go` — wiring a wire-command consumer with
  postgres dedup.
- ADR-0004 (domain vs integration events), ADR-0005 (envelope wire format),
  ADR-0006 (hybrid projections), ADR-0007 (sagas), ADR-0008 (async-only).

## Contracts: the only shared vocabulary

One module, importable by every service, depending on nothing. Contracts are
deliberately boring: flat structs of primitives with stable JSON tags.
Integration events carry `EventName()` + `ContractVersion()`; commands carry
`CommandName()`; the framework interfaces are satisfied structurally. Topic
names live here too: `<service>.events` and `<service>.commands`.

**Once published, a version never changes.** Evolution = add `ThingV2`
alongside `ThingV1`; consumers migrate at their own pace. If you're editing an
existing contract struct, stop.

## Translators: opting a fact into publicity (ADR-0004)

Domain events are private. To publish a fact, register a translator on the
relay (`relay.Translate`): a pure function from the domain event to the
contract. Returning not-ok keeps that instance private. No translator = the
event never leaves — the relay skips it but still advances its checkpoint
(`relay/relay_test.go`, `TestRelaySkipsPrivateEventsButAdvances`).

The relay gives you at-least-once, in-order publication with deterministic
message IDs (`category/id#version` — `relay.MessageID`), resume-from-checkpoint
without duplicates, and retry-without-advancing on publish failure — all
pinned in `relay/relay_test.go`. Never publish to the broker from a
handler; append to the store and let the relay do it.

## Consuming commands off the wire

The receiving service (imitate billing):

1. Register consumable commands in a `cqrs.Registry` (`RegisterCommands` in
   the app package).
2. Wire `cqrs.NewConsumer` with the bus, registry, a **postgres**
   deduplicator (`cqrs-postgres`), and its config; run it on the
   service's `<service>.commands` topic.
3. Handlers must be **replay-safe** even with dedup (a crash between execute
   and mark redelivers): billing's `CapturePayment` treats an already-decided
   payment as a no-op; `RefundPayment` treats already-refunded as success.

Consumer semantics — pinned in `cqrs/consumer_test.go` and
`cqrs-postgres/dedup_test.go`: duplicates by envelope ID are dropped;
domain errors are **acked** (definitive rejection — the answer is a rejection
integration event, not a retry); transient errors are redelivered.

## Hybrid projections (ADR-0006)

A read model is a disposable, query-shaped table fed from two sources:

- **Own events** from the store via catch-up (`projection.On` +
  `projection.NewCatchUp`) — no broker round-trip for your own facts.
- **Foreign facts** via the durable inbox: one `projection.NewInboxWriter` per
  foreign topic persists envelopes; `projection.NewInboxReader` feeds them to
  `projection.OnIntegration` handlers. The inbox is what makes rebuilds
  possible past broker retention (`projection/projection_test.go`,
  `TestRebuildReplaysInboxWithoutBroker`).

Handlers must be **idempotent and commutative** — cross-source ordering is not
guaranteed. Imitate ordering's status-rank trick: `SetStatus` only ever raises
the rank, so late or repeated facts can't regress the row. Rebuild = reset
checkpoints (`projection.Reset`), replay store + inbox.

## Sagas (ADR-0007)

Orchestration-style: the whole flow reads top-to-bottom in one file. A
definition is `saga.New[State]` plus one `saga.OnEvent` per integration event,
each giving a correlation function (event → saga instance ID, e.g. the order
ID) and a **pure decision handler**: mutate state, return
`saga.Send(commands…)` / `saga.End()` / both (compensation + end). No I/O in
handlers.

The machinery guarantees, pinned in `saga/saga_test.go`:

- **Atomic decide-and-send** — the decision event and its outgoing commands
  are one append to the saga's stream; the relay routes the commands
  (`saga.RegisterCommandRouting` on the hosting service's relay).
- Redeliveries are deduplicated by envelope ID; unrelated events are ignored;
  state survives restarts.

Hosting checklist (imitate ordering): `saga.RegisterEvents` into the event
registry, `saga.RegisterCommandRouting` onto the relay, `saga.NewRunner` run
against a consumer group over all topics the saga listens on.
**Compensation** is just another decision: on a failure fact, send the undoing
command (refund after stock rejection) and end.

## Tests you must ship (non-negotiable)

- **Saga**: imitate `saga/saga_test.go` — for each step: given prior
  decisions, when event, expect commands/end; plus a compensation path and a
  redelivery-dedup case. Memory store + memory broker.
- **Projection**: imitate `projection/projection_test.go` — own events
  project via catch-up and resume without reapplying; foreign facts flow
  through a `MemoryInbox`; out-of-order/duplicate delivery leaves the row
  correct (prove commutativity + idempotency).
- **Wire handlers**: same command twice = one outcome; a domain-error path
  produces the rejection fact, not a retry loop.
