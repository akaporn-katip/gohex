# build-with-gohex changelog

The copy in the gohex repo (`skills/build-with-gohex/`) is the source
of truth; copies elsewhere (e.g. katipwork/skills) should match its `version`.

Bump rules:

- **patch** — typo fixes, updated file/test pointers after a gohex refactor.
- **minor** — new guidance or a new section within the existing five files.
- **major** — restructured workflow files, or guidance changed because a gohex
  guarantee or API contract changed.

## 2.0.0 — 2026-08-30

The framework and the example system split into two repos (ADR-0013): libs
flattened to the gohex repo root (`github.com/akaporn-katip/gohex/<module>`,
released as `<module>/v0.1.0`), the example moved to
`github.com/akaporn-katip/gohex-example`. All `libs/*` and `services/*` path
pointers rewritten; Rule 0 now clones both repos; `CONTEXT-MAP.md` lives in
gohex-example.

## 1.0.0 — 2026-08-15

Initial release: SKILL.md router (mental model, guarantees, live-lookup rule)
plus references for scaffold, domain, integration, and o11y, each with a
mandatory testing section. Written against gohex commit `3a424da`.
