# Releasing

gohex is a single Go module tagged at the root (`vX.Y.Z`). All packages
are versioned together, and a consumer depends on the unified module
(`github.com/akaporn-katip/gohex`), not on subpackages separately.

## History: Why We Stopped Multi-Module

Earlier releases used a multi-module layout: 12 separate modules tagged
individually (`eventstore/v0.3.0`, `o11y/v0.5.1`, etc.). The design
promised independent versioning but introduced severe operational
overhead — every release required pinning siblings in dependency order,
pushing tags sequentially, and maintaining hand-off cycles between
preparation and push rights. The scar tissue that codified the **public
before pinned** rule exists because v0.3.0/v0.4.0/v0.5.1 shipped with
go.sum lines computed from local archives, which failed checksum
verification once tags were public, requiring repair releases
(v0.3.1/v0.4.1/v0.5.2).

The unified module eliminates the entire problem: one tag, one `go mod tidy`,
no sibling dependencies to manage, no dependency levels to traverse.

## Procedure

1. **Develop and test.** Gate: `task test` green, `GOWORK=off go vet ./...`
   and `gofmt -l .` (excluding `gohex-example/`) clean.

2. **Version decision.** Follow v0.x semver: a new method on a public
   interface is a minor bump; a breaking change (parameter type, method
   signature, behavior guarantee) is a major bump.

3. **Commit, tag, and push:**
   - Commit style: `release: vX.Y.Z — <what changed>`
   - Tag at root: `git tag vX.Y.Z`
   - Push together: `git push origin main --tags`

4. **Verification.** A clean consumer build proves the release is
   correct:
   ```sh
   mkdir /tmp/consumer-test && cd /tmp/consumer-test
   go mod init example.com/m
   GOPROXY=https://proxy.golang.org go get github.com/akaporn-katip/gohex@vX.Y.Z
   go list -m github.com/akaporn-katip/gohex
   ```

## Consumer Migration from Multi-Module Tags

Consumers upgrading from multi-module releases (tags like
`eventstore/v0.3.0`) to the unified module must remove all subpackage
`require` lines in one pass. Go's module resolution does not handle
ambiguous imports: if `go.mod` contains both
`require github.com/akaporn-katip/gohex vX.Y.Z` and
`require github.com/akaporn-katip/gohex/eventstore v0.3.0`, builds fail.

Migration steps:
1. Remove all legacy `require github.com/akaporn-katip/gohex/<subpackage>` lines.
2. Add `require github.com/akaporn-katip/gohex vX.Y.Z`.
3. Run `go mod tidy`.
4. No import statement changes are needed — `import "github.com/akaporn-katip/gohex/eventstore"`
   still works; the module path prefix is now the unified module, and the
   remainder is the package path.

The first unified release will be `v0.7.0` or later (not yet determined).
