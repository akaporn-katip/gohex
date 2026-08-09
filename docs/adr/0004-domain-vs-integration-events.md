# Domain events are private; only mapped, versioned integration events are published

Because aggregates are event-sourced (ADR-0002), stream events are the persistence format — publishing them directly would make each service's storage schema its public API. We decided the relay only publishes events that pass through an explicitly registered translator, which maps a domain event to a versioned integration event (e.g. OrderPlaced → OrderPlacedV1). Unmapped events stay private by default, so internal refactors never break subscribers; the cost is one small mapper per public event.
