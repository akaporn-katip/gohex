// Package featureflag provides boolean release flags and operational kill
// switches, evaluated once at the edge and pinned into metadata so every
// downstream hop acts on the decision the edge made rather than
// re-evaluating (ADR-0021).
//
// A [Client] checks flags via [Client.Enabled] and, when a recorder is
// installed on the context ([Record]), records every flag that evaluated
// true. An extractor registered with [eventstore.AddContextMetadata]
// stamps the recorded flags onto appended events under the [FlagsKey]
// metadata key; from there the existing plumbing — relay, saga runner,
// projection handlers — carries them unchanged. Downstream readers use
// [PinnedIn] to read the decision the edge gave, never re-evaluating.
//
// The [Subject] is who a flag is evaluated for — a user, a tenant, an
// order — reduced to a key and optional attributes that the [Source]
// uses for targeting (allowlists, percentage canaries). The port
// ([Client], [Source], [Subject], [NoSubject], and [Static]) depends
// only on the standard library; real flag backends live behind adapters
// (featureflag-openfeature/) so swapping vendors is an adapter change.
//
// Scope is deliberately narrow: boolean flags only. The backend already
// owns flag definitions, so gohex keeps no registry. Services that need
// no backend use [Static].
package featureflag
