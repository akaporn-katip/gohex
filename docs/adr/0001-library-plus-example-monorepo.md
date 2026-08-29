# gohex is a framework library plus a runnable example, in one monorepo

**Superseded by [ADR 0013](0013-split-framework-and-example-repos.md)** — the example system now lives in its own repository.

gohex exists as a reference codebase that other Go projects follow. We decided consumers get it two ways: importable Go modules (the framework) and a runnable example microservice monorepo demonstrating them. The framework's scope is deliberately broad — domain kernel, CQRS/application layer, event-driven plumbing, sagas, and observability/transport scaffolding — accepting the API-stability burden of a framework in exchange for consumers not re-implementing the plumbing in every project.
