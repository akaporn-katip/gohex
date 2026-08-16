---
name: build-with-gohex
description: Build event-driven Go microservices with the gohex framework libraries (github.com/akaporn-katip/gohex). Use when the user mentions gohex, or the project already imports github.com/akaporn-katip/gohex/libs/* — for scaffolding a new service, adding aggregates/events/commands, integrating services via contracts/sagas/projections, or wiring OpenTelemetry through the async flow. Do not use for generic event-sourcing or CQRS advice in projects that don't use gohex.
version: 1.0.0
---

# Build with gohex

gohex (`github.com/akaporn-katip/gohex`) is a set of importable Go framework
libraries for event-driven microservices: hexagonal architecture, event-sourced
aggregates, CQRS, hybrid read-model projections, and event-sourced sagas — plus
a runnable four-service example system that is the canonical usage reference.

## Rule 0 — read the source, never trust remembered signatures

This skill deliberately contains **no API signatures**. gohex evolves; the
source is the only truth. Before writing any code that calls a gohex lib:

1. Locate a gohex checkout. This skill ships inside the gohex repo, so when
   working in it (or a project that vendors/workspaces it), the checkout is
   already at hand — every path cited in this skill is relative to that repo
   root. When building a gohex-based service in an unrelated project, clone
   one (reuse if it already exists):

   ```sh
   [ -d /tmp/gohex-src ] || git clone --depth 1 https://github.com/akaporn-katip/gohex /tmp/gohex-src
   ```

2. Read the exact constructors/types you are about to use in `libs/<module>/`,
   and imitate the canonical example in `services/` (pointers in the reference
   files below).

The checkout also gives you `docs/adr/0001`–`0012` (every architectural
decision, numbered; reference files cite them as ADR-NNNN), `CONTEXT.md` (the
framework's ubiquitous language — use its terms verbatim: Aggregate, Value
Object, Domain Event, Integration Event, Translator, Relay, Envelope, Upcaster,
Checkpoint, Projection, Inbox, Saga, Compensation), and `CONTEXT-MAP.md` (how
the example services relate).

## The mental model

A service is one binary hosting a hexagon. Inside: an event-sourced domain
(aggregates whose state is the replay of their stream). At the edges: adapters.
Between services: **only** the broker — commands in on `<service>.commands`
topics, integration events out on `<service>.events` topics. Never a
synchronous call, never another service's database (ADR-0008).

The modules (each is its own Go module under `libs/`):

| Module | Role |
|---|---|
| `kernel` | Zero-dependency domain kernel: `Root`/`Raise`/`Apply` aggregates, typed `ID[T]`, `DomainError` |
| `eventstore` (+`-postgres`) | Streams, optimistic concurrency, registry + upcasters, global-sequence tailing, checkpoints, `Repository[A]` |
| `broker` (+`-kafka`) | Publish/subscribe port, the standard envelope (`Message`), `IntegrationEvent` |
| `relay` | Tails the store, runs Translators, publishes, checkpoints — the only path out of a service |
| `cqrs` (+`-postgres`) | Command/query buses, retry policy, wire-command consumer with dedup |
| `projection` (+`-postgres`) | Read models fed by own events (catch-up from store) + foreign facts (durable inbox) |
| `saga` | Event-sourced orchestration: pure decision handlers, atomic decide-and-send |
| `o11y` | OpenTelemetry: one trace from HTTP edge through store and broker to other services |

## The guarantees you must not break

Every design choice below is pinned by a test in `libs/` — the reference files
cite them. When your code fights one of these, you are holding it wrong:

- **Private by default.** Domain events never leave the service. Only an
  explicit Translator on the relay publishes a versioned Integration Event
  (ADR-0004). No translator = private, and that is a feature.
- **Atomic decide-and-send.** A saga's decision and its outgoing commands are
  one event-store append; the relay does the actual sending. Never publish
  directly from a handler.
- **At-least-once everywhere, dedup where it matters.** Deterministic message
  IDs (`category/id#version`), consumer-side dedup, idempotent projection
  handlers. Write every consumer assuming redelivery.
- **Reject vs retry.** A `kernel.DomainError` is a definitive business
  rejection — never retried, acked on the wire, surfaced as 4xx or a rejection
  event. Everything else is infrastructure — retried, never given business
  meaning (ADR-0012).
- **Value objects over primitives.** Aggregates and domain events hold
  self-validating value objects constructed via parsing constructors, never
  bare primitives (ADR-0011). Stored payload shapes are event schema; changing
  one requires an upcaster, never a rewrite (ADR-0010).

## Testing is not optional

Every aggregate, saga, and projection you build ships with tests in the gohex
style, using the framework's in-memory implementations (memory event store,
memory broker, memory inbox, memory deduplicator) — no mocks, no containers.
Each reference file names the canonical `_test.go` files in `libs/` whose
patterns to imitate and which guarantee they pin.

## Workflow guides

Load the one matching the task:

- **[references/scaffold.md](references/scaffold.md)** — create a brand-new
  service: module layout, the composition-root wiring order, migrations,
  Dockerfile/compose entry, smoke-testing it.
- **[references/domain.md](references/domain.md)** — add domain pieces to a
  service: aggregate, value objects, domain events, upcasters, command/query
  handlers, and their tests.
- **[references/integration.md](references/integration.md)** — make services
  talk: contracts, translators, wire commands with dedup, hybrid projections
  with the inbox, sagas and compensation.
- **[references/o11y.md](references/o11y.md)** — weave OpenTelemetry through
  the async flow and verify one trace spans the system.
