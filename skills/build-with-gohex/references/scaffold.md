# Scaffold a new gohex service

Canonical examples in the checkouts — read them before and while writing:

- `gohex-example/ordering/` — the full-featured service: HTTP edge, aggregate, saga,
  hybrid projection. Its `cmd/ordering/main.go` is **the** composition-root
  reference; the wiring order below comes from it.
- `gohex-example/billing/` — the minimal service shape: wire-command consumer +
  aggregate + relay, no HTTP, no projection. Start from this shape unless you
  need an edge or a read model.
- ADR-0013 (framework and example split into two repos), ADR-0020 (the framework
  is one module; one `require` line, import paths per capability, the old
  per-capability module paths retracted), ADR-0009 (domain imports only the
  kernel).

## Layout

One binary per service; composition lives in `cmd/<name>/main.go` and nowhere
else:

```
<name>/
  CONTEXT.md                    # the service's ubiquitous language (imitate gohex-example/ordering/CONTEXT.md)
  cmd/<name>/main.go            # composition root
  internal/domain/              # aggregates + value objects; imports ONLY the kernel module
  internal/app/                 # handlers, registrations, saga defs, projections
  internal/ports/               # interfaces the app needs (e.g. a read-model store)
  internal/adapters/            # postgres/http implementations of ports
  go.mod                        # module github.com/<you>/<project>/<name>
```

The domain package importing only `kernel` is load-bearing (ADR-0009): if
`internal/domain` needs `eventstore` or `broker`, the design is wrong.

## Wiring order (from `gohex-example/ordering/cmd/ordering/main.go`)

Follow this order; each step's exact constructors are in the cited lib — read
them, don't guess:

1. **Signal context** — `signal.NotifyContext` for SIGINT/SIGTERM.
2. **o11y first** — init tracing before anything that emits spans; defer the
   shutdown flush. See `references/o11y.md`.
3. **Postgres pool + migrations** — connect with retry (Postgres may still be
   starting; ordering's `connectPostgres` loops with backoff), then run each
   lib's migration: event store, projections/inbox, plus your own adapters'.
   Each `-postgres` lib exposes its own `Migrate`.
4. **Event registry** — one registry; register every domain event the service
   stores (and saga stream events if it hosts a saga) in one `RegisterEvents`
   function in `internal/app`. Unregistered events fail loudly — that's a test
   (`eventstore/registry_test.go`).
5. **Store, checkpoints, adapters** — the postgres event store, checkpoint
   store, and your read-model store.
6. **Broker** — Kafka client, then wrap it with the o11y publisher/subscriber
   decorators. Always use the wrapped ones downstream.
7. **Buses** — command bus with o11y middleware (+ query bus if the service
   answers queries); construct app handlers and register them on the buses. If
   the service consumes wire commands, also: a `cqrs` command registry, a
   postgres deduplicator, and the wire consumer (see `references/integration.md`).
8. **Relay** — one per service, with a unique name and the service's events
   topic; register translators (and saga command routing if hosting a saga).
9. **Saga runner / projections** — if applicable; see `references/integration.md`.
10. **Supervise the background runners; block on the server** (ADR-0019) —
    relay, saga runner, projection catch-up + inbox reader + one inbox writer
    per foreign topic, consumer, worklist worker. Each goes to
    `supervise.Supervisor` under a name (`"<service>.<what>"`, the name that
    shows on `gohex.runner.restarts`), wired with `o11y.RunnerRestartHook()`
    as the restart counter and a `Report` callback that logs the failure, its
    retry delay and, for a recovered panic, its stack. Then serve HTTP on the
    main goroutine, cancel the context when it returns, and `sup.Wait()`.

    **Never put a runner in `errgroup.WithContext` beside the HTTP server.**
    The group cancels every member on the first error, so one message a
    projection cannot apply takes the API down and the orchestrator restarts
    the process into the same message — the runners keep durable checkpoints
    precisely so a failure can be a restart instead of an outage. The
    composition root is the only place that decides what is fatal: the HTTP /
    health server is, a background runner is not. Copy
    `gohex-example/ordering/cmd/ordering/main.go`.

## Migrations

The `-postgres` libs own their schemas and expose idempotent `Migrate`
functions — call them at startup, don't hand-write their DDL. Your own
read-model tables get a `Migrate` in your postgres adapter package (imitate
`gohex-example/ordering/internal/adapters/postgres/summary.go`).

## Dockerfile / compose

The example uses one parameterized Dockerfile for all services (build arg
`SERVICE`, workspace copied so `go.work` resolves local modules) — see
`gohex-example/Dockerfile` and the per-service stanza in
`gohex-example/docker-compose.yml` (env
`DATABASE_URL`, `KAFKA_BROKERS`, `PORT`; depends_on postgres + kafka healthy).
Copy that stanza for a new service; each service gets its own database.

## Smoke test

`task up` / `task demo` (see `gohex-example/Taskfile.yml`) or the curl flow in
the gohex-example README: place an order, poll status until it walks `placed → paid → reserved →
shipped`. For a new service: verify its consumer acts on a command, its facts
appear on its events topic, and its rows survive a restart without
double-processing (at-least-once + dedup working).

## Tests to ship with a scaffold

The composition root itself stays untested (it's wiring), but nothing else:
every aggregate, consumer handler, projection, and saga you scaffold ships
with tests per the other reference files. Run the whole workspace the way the
repo does (see the `test` task in each `Taskfile.yml`): `go test ./...` once in
the framework, which is a single module, and once per service module in the
example, which is still a workspace of its own.
