# Aggregates are event-sourced; the event stream is the source of truth

We considered state-based persistence with a transactional outbox (the mainstream, easier-to-teach default) but decided all aggregates in gohex are event-sourced: state is rehydrated by replaying the stream, and read models are projections. This gives the purest event-driven story and makes CQRS natural, at the cost of the framework owning the hard parts — event versioning/upcasting, snapshots, and projection rebuilds — as first-class public API.
