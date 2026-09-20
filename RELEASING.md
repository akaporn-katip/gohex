# Releasing

gohex is one repository of many Go modules, each tagged on its own
(`eventstore/v0.3.0`, `o11y/v0.5.1`). Modules require each other by
version tag, never by `replace`, so a released module builds the same
inside this repo and outside it.

One rule carries the whole procedure: **public before pinned** — a tag
is on origin before anything pins it. Every go.sum line is written by
the toolchain resolving a pushed tag through the public proxy, verified
against sum.golang.org as it lands; nothing else writes one. If
`go mod tidy` cannot fetch the version you are pinning, the dependency's
tag is not public yet — push it, then rerun.

That rule is written in scar tissue. The v0.3.0/v0.4.0/v0.5.1 releases
pinned sibling tags that existed only locally, with go.sum lines
computed from hand-built archives served off a local file proxy. The
archives were not canonical module zips: once the tags were pushed,
sum.golang.org hashed the real ones differently, standalone builds of
four modules failed checksum verification, and v0.3.1/v0.4.1/v0.5.2
exist only to carry the corrected lines.

## Procedure

1. **Develop against the workspace.** `go.work` compiles cross-module
   changes before any tag exists. Gate: `task test` green, and for each
   module being released, `go vet ./...` and `gofmt -l .` clean.

2. **Order the set by dependency.** A module is tagged only after every
   gohex module it requires is tagged *and pushed*. The `require` blocks
   are the order; when in doubt it runs kernel → eventstore/broker/cqrs →
   their adapters and projection → relay/saga/o11y.

3. **Per module, in that order:**
   - Pin: bump sibling requirements in `go.mod`, then
     `GOWORK=off go mod tidy`.
   - Prove standalone: `GOWORK=off go build ./...` and
     `GOWORK=off go test ./...`. `GOWORK=off` is what a consumer is;
     the workspace masks a broken pin.
   - Commit and tag `<module>/vX.Y.Z`.
   - Push the commit and tag now, before touching the next module.

4. **Commit style** (see `git log` for precedent):
   - work release: `release: <module> vX.Y.Z — <what changed>`
   - pure pin bump: `release: pin <modules> against the <module> vX.Y.Z tag`

Versioning is v0.x semver: adding a method to a port is a minor of the
port's module and of every adapter implementing it (the `Head` release:
eventstore v0.3.0, eventstore-postgres v0.3.0, projection v0.4.0,
projection-postgres v0.3.0). Consumer-only modules of a changed
interface need no release — Go interfaces bind at the implementation.

## Releasing when you cannot push

Handoff and review flows keep push in the owner's hands, and that is
exactly where the scar above came from: an agent that cannot push
cannot mint a single correct go.sum line for a new sibling tag. Split
the release at the tag boundary instead: prepare, test, commit and tag
the dependency level; hand back for review and push; resume with the
dependents once their requirement is public. A release spanning N
dependency levels is N hand-backs. That is the price of go.sum lines
the world agrees with, and it is far cheaper than the repair release.
