# gohex

A framework for event-driven microservices in Go: hexagonal
architecture, domain-driven design, event sourcing, CQRS, and sagas —
as **importable framework libraries**. A runnable example system
demonstrating them lives in
[gohex-example](https://github.com/akaporn-katip/gohex-example).

Every architectural decision is recorded in [`docs/adr/`](docs/adr/); the
ubiquitous language lives in [`CONTEXT.md`](CONTEXT.md).

## The modules

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

Each module is independently versioned and importable:

```sh
go get github.com/akaporn-katip/gohex/kernel@v0.1.0
go get github.com/akaporn-katip/gohex/eventstore@v0.1.0
```

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

## The example system

[gohex-example](https://github.com/akaporn-katip/gohex-example) is
e-commerce order fulfillment across four services — `ordering`,
`billing`, `inventory`, `shipping` — talking **only** through Kafka:
commands in, facts out. Clone it and `docker compose up --build -d` to
see the whole flow, including one OpenTelemetry trace across every
Kafka hop.

## License

Apache-2.0
