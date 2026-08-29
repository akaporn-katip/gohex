# gohex-example

A runnable example system for the
[gohex](https://github.com/akaporn-katip/gohex) framework: e-commerce
order fulfillment across four services — `ordering`, `billing`,
`inventory`, `shipping` — talking **only** through Kafka: commands in,
facts out. The `ordering` service hosts the fulfillment saga and an
`order_summary` read model fed by all four services' events.

Each service is a full hexagon
(`internal/{domain,app,ports,adapters}` + `cmd`); the shared public
contracts live in [`contracts/`](contracts/). The bounded contexts and
their relationships are mapped in [`CONTEXT-MAP.md`](CONTEXT-MAP.md);
the framework's own architecture decisions live in
[gohex's ADRs](https://github.com/akaporn-katip/gohex/tree/main/docs/adr).

## Run it

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

With [Task](https://taskfile.dev) installed, `task up`, `task demo`,
`task logs`, and `task down` wrap the above.

## Developing against a local gohex checkout

The services pin released gohex versions. To hack on the framework and
the example together, clone [gohex](https://github.com/akaporn-katip/gohex)
next to this repo and create an untracked workspace overlay:

```sh
cp go.work go.work.dev
go work edit $(for m in kernel broker broker-kafka eventstore eventstore-postgres \
  cqrs cqrs-postgres relay projection projection-postgres saga o11y; do
  printf ' -use ../gohex/%s' $m; done) go.work.dev
GOWORK=$(pwd)/go.work.dev go build ./ordering/...
```

`go.work.dev` is gitignored; the committed `go.work` keeps resolving
gohex from the released tags.

## License

Apache-2.0
