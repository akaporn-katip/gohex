# Feature flags are evaluated at the edge and pinned into metadata

A feature flag is read exactly once per causal chain, at the edge that
still knows who the request is for — an HTTP handler, a command handler,
the entry point of a flow — and never inside an aggregate, because an
aggregate that asks a flag question during `Apply` makes replay depend on
today's configuration and destroys the one property event sourcing
sells: state is the replay of the stream. What the edge decided then
travels with the work rather than being asked again: the evaluating
`Client` records every flag it answered `true` into a recorder installed
on the context (`featureflag.Record(ctx)`), and an extractor registered
through `eventstore.AddContextMetadata` stamps them onto every appended
event under a single `gohex.flags` key holding a comma-separated list of
enabled flag names. From there the existing plumbing carries them
unchanged — the relay copies event metadata onto the envelope, the saga
runner copies it onto the commands it issues, projections receive it on
the item — so a hop resuming three days later reads
`featureflag.PinnedIn(md, "new-pricing")` and gets the answer the edge
gave, not the answer operations would give today. Reading it needs a
handler that is given the metadata: projections always were
(`projection.Meta`, `projection.Item`), saga handlers were not, and
`saga.OnEventWithMeta` closes that gap by passing `saga.Meta` alongside
the event — which keeps handlers pure, because the triggering envelope
is already stored verbatim in the saga's own stream and replays
identically. Re-evaluating
downstream was rejected for exactly that reason: a kill switch flipped
mid-flow would leave half an order running new logic and half running
old, with compensation unable to tell which. Carrying the subject
forward so downstream *could* re-evaluate was rejected as the same
mistake wearing a disguise. A flag that is absent from the list is off,
which makes a lost or truncated metadata map fail to the safe default
rather than to whatever happens to be configured.

Scope is deliberately narrow: boolean release flags and operational kill
switches, with targeting (allowlists, percentage canaries) computed
entirely by the `Source` behind the port, since that is what a real flag
backend is for. The port — `Client`, `Source`, `Subject{Key, Attrs}`,
`NoSubject`, and a `Static` source for tests — depends on nothing but the
standard library and names its own concepts; the OpenFeature SDK lives
behind `featureflag-openfeature/`, the same way Kafka lives behind
`broker-kafka/`, so swapping flag vendors is an adapter change and the
port never inherits another project's vocabulary or release cadence.
`Subject` is a required argument rather than variadic, because "forgot
to pass the subject" and "deliberately has no subject" must not look
alike: a canary that silently sees an empty key reports a clean 0%
rollout and nobody notices. gohex keeps no flag registry of its own —
the backend already owns flag definitions, and a second list that drifts
out of sync is worse than no list — and ships no env-reading source,
which OpenFeature providers already do. Downstream constructors
(sagas, projections, worklist workers) are not given a `Client` at all;
making the wrong call impossible to express is cheaper than documenting
that it is wrong. Escape hatch, recorded so the expensive path is not
taken by accident: if valued flags (`pricing_strategy="v2"`) are ever
needed, add a *new* metadata key alongside `gohex.flags`; never change
the syntax of `gohex.flags` itself, because event metadata is stored
forever and is never rewritten (ADR-0010), so a syntax change would
require every reader, past and future, to understand two grammars and
would break service instances mid-rolling-deploy. Related change, made
first and standing on its own: `eventstore.SetContextMetadata` installed
a single extractor in an `atomic.Pointer` and silently replaced whatever
was there, which worked only while o11y was its only caller;
`AddContextMetadata` accumulates extractors and merges their output, so
trace context and pinned flags coexist. Also rejected: treating canary
releases as purely an ingress concern (traffic splitting covers HTTP
ingress but not consumers, which is most of an event-driven service's
work), and using an OpenFeature client directly as the port (it would
put a third-party SDK in the module graph of everyone who imports gohex
for `kernel` alone, and tie the framework's API to that SDK's release
cadence).
