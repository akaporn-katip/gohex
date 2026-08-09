# gohex

A reference codebase for event-driven microservices in Go: hexagonal
architecture, domain-driven design, event sourcing, CQRS, and sagas —
as **importable framework libraries** plus a **runnable example system**
demonstrating them.

Every architectural decision is recorded in [`docs/adr/`](docs/adr/); the
ubiquitous language lives in [`CONTEXT.md`](CONTEXT.md) and
[`CONTEXT-MAP.md`](CONTEXT-MAP.md).

## The framework (`libs/`)

| Module | What it is |
|---|---|
| `kernel` | Zero-dependency domain kernel: event-sourced aggregates, typed `ID[T]`, value-object conventions, `DomainError` |
| `eventstore` (+`-postgres`) | Event store port: append-only streams, optimistic concurrency, global-sequence tailing, upcasters, checkpoints |
| `broker` (+`-kafka`) | Messaging port: at-least-once publish/subscribe, the standard envelope, versioned integration events |
| `relay` | The delivery backbone: tails the store, translates domain events into public contracts, publishes, checkpoints |
| `cqrs` (+`-postgres`) | Command/query buses with the reject-vs-retry split; wire-side command consumer with dedup |
| `projection` (+`-postgres`) | Hybrid read models: own events from the store, foreign facts via a durable inbox |
| `saga` | Event-sourced orchestration: workflows that decide, send, and compensate — atomically |
| `o11y` | OpenTelemetry woven through the async flow: one trace from HTTP edge to the last service |

Key guarantees, all pinned by tests:

- **Atomic decide-and-send** — a saga's decision and its outgoing command
  are one event-store append; the relay does the sending.
- **At-least-once everywhere, dedup where it matters** — deterministic
  message IDs (`category/id#version`), consumer dedup, envelope-ID dedup
  in saga streams.
- **Private by default** — domain events stay in the service; only
  explicitly translated, versioned contracts are published.
- **One trace across the async hops** — trace context survives *through
  the event store* into the broker and on to other services.

## The example (`services/`)

E-commerce order fulfillment across four services — `ordering`,
`billing`, `inventory`, `shipping` — talking **only** through Kafka:
commands in, facts out. The `ordering` service hosts the fulfillment
saga and an `order_summary` read model fed by all four services' events.

### Run it

```sh
docker compose up --build -d     # Postgres, Kafka, HyperDX (ClickStack), 4 services
```

Place an order and watch it flow:

```sh
curl -s -X POST localhost:8080/orders \
  -d '{"cents": 4999, "currency": "USD", "qty": 2}' | tee /tmp/order.json

ORDER=$(jq -r .order_id /tmp/order.json)
curl -s localhost:8080/orders/$ORDER | jq .status
# placed -> paid -> reserved -> shipped  (re-run to watch it advance)
```

Failure paths (the demo rules):

```sh
# payment declined: cents > 99999
curl -s -X POST localhost:8080/orders -d '{"cents": 250000, "currency": "USD", "qty": 1}'
# -> status: payment_failed

# out of stock: qty > 10 — payment is captured, then REFUNDED (saga compensation)
curl -s -X POST localhost:8080/orders -d '{"cents": 4999, "currency": "USD", "qty": 50}'
# -> status: rejected, then refunded
```

Then open **http://localhost:8090** (HyperDX — the ClickHouse-backed
ClickStack, OTLP built in) and find the trace: one timeline from
`POST /orders` through the saga, billing, inventory, and shipping —
across every Kafka hop.

## Repository layout

```
libs/        framework modules (one Go module each; ports and adapters split)
services/    the example system (one Go module per service + shared contracts)
  ordering/  internal/{domain,app,ports,adapters} + cmd — the full hexagon
docs/adr/    why everything is the way it is
```

## License

Apache-2.0
