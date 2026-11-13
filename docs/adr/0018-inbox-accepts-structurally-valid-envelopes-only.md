# The inbox accepts structurally valid envelopes only; a broken one is acked and reported, never retried

`projection.Inbox.Append` now states what it accepts — non-empty ID and
Type, a Version of 1 or more, and a Payload that is valid JSON — and the
rule is one exported function, `projection.ValidateMessage`, that every
adapter calls, rejecting with an error wrapping
`projection.ErrInvalidMessage`. The port previously said nothing, and the
implementations disagreed: `MemoryInbox` accepted anything, while
`projection-postgres` ran `json.Marshal` on a `broker.Message` whose
`Payload` is a `json.RawMessage`, so a payload that is not JSON failed
encoding, deterministically, on every attempt. Because the inbox writer
returned that failure to the broker, and the broker's contract is
at-least-once redelivery until success (`broker-kafka` retries every
250ms and commits only after the handler returns nil), one hand-written
record on a topic stopped that consumer group's partition forever: no
crash, no failing probe, and no moving gauge, since `gohex.inbox.depth`
is head minus checkpoint (ADR-0017) and a wedged writer never advances
the head — the backlog metric read healthy while facts silently stopped
arriving. The asymmetry is the deeper defect: the in-memory inbox the
framework tells services to test against was more permissive than the
one they run, so the suite could not reproduce production, which is why
`projection/inboxtest` now exists — one contract suite, run by
`MemoryInbox` and by every durable adapter, with the rejection cases in
it. `InboxWriter` gained `InboxWriterConfig{OnRejected}`, mirroring
`cqrs.ConsumerConfig.OnRejected` field for field: a structurally invalid
message is acknowledged, handed to the callback and skipped, so the two
consumer edges of the framework finally make the same reject-vs-retry
decision (ADR-0012); a nil callback drops the rejection silently, and
every other append failure (storage down, context cancelled) stays
retryable exactly as before. Validity is deliberately STRUCTURAL only:
an unknown Type or a version no handler is registered for is stored
normally, because a projection extended and rebuilt tomorrow replays the
inbox and must find those facts there (ADR-0006) — "nobody handles this
yet" is not a rejection. The mechanism is the framework's and the policy
is the application's: gohex surfaces the rejected envelope and the
reason, and never decides that it goes to a table, a topic or an alerting
backend. Separately, `broker-kafka`'s `fromRecord` stopped swallowing
header parse errors; a `gohex-version` of `"v1"` used to become version 0,
which matches no `(name, version)` handler and is skipped by
`InboxReader` with no log, no metric and no error — silent data loss,
worse than a stuck partition — so a present-but-unparseable header now
fails the subscription with `kafkabroker.ErrMalformedEnvelope` naming the
partition and offset, while an ABSENT header still leaves its field zero
and is caught, reported and acked at the consumer edge where the
rejection callback lives. Rejected: a dead-letter or retry topic
(skipping messages in an ordered durable log makes read models silently
wrong, and the inbox is the replay source for rebuilds, so a
dead-letter decision that is not itself durable makes a rebuild diverge
from live state); dropping invalid messages inside the adapter without
telling anyone (the same silence in a different place); and validating
inside `broker.Message` or at publish time only (the hole is records
gohex did not publish, which is precisely the case that matters).
