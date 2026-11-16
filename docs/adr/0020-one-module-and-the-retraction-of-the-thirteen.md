# One module, and the retraction of the thirteen

The thirteen library modules of ADR-0009 are now one module,
`github.com/akaporn-katip/gohex`, and every import path is unchanged —
the old module paths were already the new module's package paths, so
`import "github.com/akaporn-katip/gohex/eventstore"` resolves exactly as
it did, and not one `.go` file changed in the unification. What was
bought by the split never arrived and what was paid for it kept
compounding. "Piecemeal adoption" assumed consumers who wanted the
eventstore without the relay; the only consumers that exist — the four
example services and the downstream product — import the stack, and
pruning already spares a consumer the dependencies of packages it does
not import, so a service using `eventstore` and `supervise` pulls
neither pgx nor franz-go from the unified module. Against that, every
change crossing a port and its adapters became an ordered sequence of
tag-and-push steps, each one a release whose go.sum lines only the
public proxy may write; RELEASING.md records what that cost when the
rule was broken once (three repair releases, four modules failing
checksum verification in standalone builds), and the procedure that
prevents it costs one hand-back per dependency level to an owner who can
push. Adding a method to a port — which v0.x does freely and which this
repo did as recently as `Store.Append` reporting its position — meant
touching every adapter module, in order, across three levels. The
dependency direction that the split was said to enforce is enforced by
the import graph regardless: `kernel` imports nothing, ports import
`kernel`, adapters import their port, and a package that violates that
now fails as an import cycle rather than as a missing `require`.
Versioning stays v0.x rather than reaching for v1.0.0 precisely because
the ports are interfaces users implement — `Store`, `CheckpointStore`,
`Publisher`, `Subscriber`, `Inbox`, `Deduplicator`, `Aggregate`, with
`storetest`, `brokertest` and `inboxtest` shipped so they can be
implemented outside this repo — and adding a method to one is a minor
here and a major under v1; v1.0.0 would convert the next port change
into `/v2` and an import-path rewrite for every user. The split tags
could not simply be abandoned: Go resolves an import to the most
specific module path that exists, so a consumer writing the unchanged
import with no requirement yet still landed on `eventstore v0.3.0`, and
published versions cannot be withdrawn. Each old module therefore got
one final tag whose go.mod carries a `// Deprecated:` notice and a
`retract` range covering every version including itself, which leaves no
unretracted version for the resolver to choose, so it skips the module
path and finds the package in the parent — verified against a synthetic
module pair on a local proxy before the real tags were pushed, and again
against the public proxy after: a new consumer resolves to
`github.com/akaporn-katip/gohex v0.7.0`, an existing pin keeps building
with a retraction warning rather than breaking, and `go list -m -u`
reports both the retraction reason and the deprecation. Consumers
migrate by deleting every `github.com/akaporn-katip/gohex/<name>`
requirement in the same change that adds the single unified one; leaving
one behind makes the package ambiguous — two modules would provide it —
and the build fails loudly instead of choosing silently. Rejected: the
hybrid of a stdlib-only core module plus separate adapter modules for
pgx, franz-go and OpenTelemetry (it keeps the multi-module release
ceremony for the modules that change together most often, to spare a
go.sum that pruning already spares); keeping the split and automating
the release (the automation would still have to push a tag and wait for
the proxy between every level, and the scar tissue came from an agent
that could not push, not from a human who could not count); `.gitattributes
export-ignore` to keep `docs/` and `skills/` out of the module zip (the
go command writes `* -export-ignore` into the repo before running `git
archive`, by design, so ziphashes stay identical across git versions —
issue golang/go#27153); and nested `go.mod` files as exclusion markers
for those directories (reintroducing the module boundaries this decision
exists to remove, to save 150 KB).
