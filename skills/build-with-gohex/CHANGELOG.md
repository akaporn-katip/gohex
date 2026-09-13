# build-with-gohex changelog

The copy in the gohex repo (`skills/build-with-gohex/`) is the source
of truth; any copy elsewhere should match its `version`.
(katipwork/skills, the former mirror, is retired.)

Bump rules:

- **patch** — typo fixes, updated file/test pointers after a gohex refactor.
- **minor** — new guidance or a new section within the existing five files.
- **major** — restructured workflow files, or guidance changed because a gohex
  guarantee or API contract changed.

## 2.2.0 — 2026-09-13

gohex adds trace boundaries at durable hand-offs (ADR-0015): o11y v0.3.0
ships `StartLinked`, `StartBatch`, `LinkFrom`, `OriginMetadata` and
`ProjectionHook`; projection v0.3.0 adds the optional `Config.Observe`
hook the runners call per item. New "Polling workers: link, don't
continue" section in o11y.md, plus a sharper Verify step (worker work is
a separate, linked trace by design) and pointers to the example's
Notifier. SKILL.md ADR range is now 0001–0015.

## 2.1.0 — 2026-09-13

gohex v0.2.0 adds read-your-writes on a service's own views (ADR-0014):
`Store.Append` reports the appended position, `eventstore.CapturePosition`
collects it at the edge, `projection.WaitForCheckpoint` waits — bounded — for
the view to catch up. New "Read-your-writes on own views" section in
integration.md; SKILL.md module table and ADR range (now 0001–0014) updated.
Existing guidance unchanged — services don't implement `Store`, so the
signature change needs no new advice.

## 2.0.1 — 2026-08-30

Reference-file headers said "in the gohex checkout" while citing mostly
`gohex-example/...` paths; now "in the checkouts", matching Rule 0's
two-checkout setup.

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
