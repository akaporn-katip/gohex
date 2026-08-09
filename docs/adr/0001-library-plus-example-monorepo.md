# gohex is a framework library plus a runnable example, in one monorepo

gohex exists as a reference codebase that other Go projects follow. We decided consumers get it two ways: importable Go modules (the framework) and a runnable example microservice monorepo demonstrating them. The framework's scope is deliberately broad — domain kernel, CQRS/application layer, event-driven plumbing, sagas, and observability/transport scaffolding — accepting the API-stability burden of a framework in exchange for consumers not re-implementing the plumbing in every project.
