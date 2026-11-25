# gohex-example

A runnable example system for the
[gohex](https://github.com/akaporn-katip/gohex) framework: e-commerce
order fulfillment across four services — `ordering`, `billing`,
`inventory`, `shipping` — talking **only** through Kafka: commands in,
facts out. The `ordering` service hosts the fulfillment saga and an
`order_summary` read model fed by all four services' events. It also runs
the reference **polling worker**: the projection queues customer
notifications on a worklist, and `ordering`'s Notifier drains it every
few seconds — each row's work is its own trace, *linked* back to the
request that caused it (gohex ADR-0015), never a continuation of it.

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

A feature flag decided once, at the edge:

```sh
# Place an order with a @horstory.test email: the e2e-accounts rule in the
# GO Feature Flag relay-proxy serves express-shipping=enabled for *.test domains.
curl -s -X POST localhost:8080/orders \
  -d '{"email": "alice@horstory.test", "cents": 4999, "currency": "USD", "qty": 2}'
# -> shipment will be express

# Place an order with a normal email: express-shipping=disabled.
curl -s -X POST localhost:8080/orders \
  -d '{"email": "bob@example.com", "cents": 4999, "currency": "USD", "qty": 2}'
# -> shipment will NOT be express
```

The HTTP edge is the only place that asks `express-shipping`; the answer
rides on the order's events as `gohex.flags` metadata, and the saga reads
it back with `featureflag.PinnedIn` when stock is reserved, turning it
into `CreateShipment{Express: true}`. Flip the flag off while an order is
mid-flight and that order still ships express: the decision was taken
when it was placed, not when it reached shipping
([gohex ADR-0021](https://github.com/akaporn-katip/gohex/blob/main/docs/adr/0021-feature-flags-evaluated-at-the-edge-and-pinned-into-metadata.md)).
An order already in flight keeps the decision it was placed with.

To edit flags and reload immediately, change `deploy/goff/flags.yaml` and
tell the relay-proxy to refresh:

```sh
curl -X POST localhost:1031/admin/v1/retriever/refresh
```

The relay-proxy has no write API — flags change by editing the retriever's
source (a file, a git repo, an S3 bucket, etc.) and either waiting for
`pollingInterval` or calling the refresh endpoint. Production deployments
ship a different `flags.yaml` with production-specific rules.

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
go work edit -use ../gohex go.work.dev
GOWORK=$(pwd)/go.work.dev go build ./ordering/...
```

`go.work.dev` is gitignored; the committed `go.work` keeps resolving
gohex from the released tags.

## License

Apache-2.0
