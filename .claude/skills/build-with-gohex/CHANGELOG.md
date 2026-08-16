# build-with-gohex changelog

The copy in the gohex repo (`.claude/skills/build-with-gohex/`) is the source
of truth; copies elsewhere (e.g. katipwork/skills) should match its `version`.

Bump rules:

- **patch** — typo fixes, updated file/test pointers after a gohex refactor.
- **minor** — new guidance or a new section within the existing five files.
- **major** — restructured workflow files, or guidance changed because a gohex
  guarantee or API contract changed.

## 1.0.0 — 2026-08-15

Initial release: SKILL.md router (mental model, guarantees, live-lookup rule)
plus references for scaffold, domain, integration, and o11y, each with a
mandatory testing section. Written against gohex commit `3a424da`.
