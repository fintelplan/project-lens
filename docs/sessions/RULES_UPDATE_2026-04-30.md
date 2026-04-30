# Rules Update — Apr 30, 2026 (LR-088, LR-089, LR-090)

## LR-088 — State actor lenses must be modeled as `entity_type='state_office'`

**Type**: Architecture | **Added**: LENS-021 | **Status**: RATIFIED

State-actor lenses (`trump_office`, `xi_office`, `khamenei_office`, future
`modi_office`/`putin_office`/etc.) are categorically distinct from individual
officials and must be persisted in `lens_entities` with `entity_type='state_office'`.
Individual officials (people) use `entity_type='official'`. This separation matters
for downstream aggregation: a query filtering by `entity_type='official'` should
return individual humans (analysts, named officials), not abstract analytical
constructs representing offices/institutions.

The `canonical_name` field for state offices = the lens key string used in
`state_actor_lens` columns (e.g., `'trump_office'`). This enables 1:1 lookup via
`(entity_type, canonical_name)` UNIQUE constraint.

**Implementation**: see `code/lens_s2f_helpers.py::get_state_office_entity_id()`.

**Origin**: LENS-021 silent finding-write failure post-mortem. The 3 S2-F
aggregators were writing `entity_id=None` while the schema enforced NOT NULL.
The original LENS-020 design had assumed `entity_id` would be nullable
("TODO: wire entity registry in LENS-021"), but the constraint was set NOT NULL
in migration. Either fix the constraint or wire the registry — we wired it.

---

## LR-089 — Hard gates protocol (debugging)

**Type**: Process | **Added**: LENS-021 | **Status**: RATIFIED

When debugging, the 6-step Error Fighting Protocol's first 3 steps are
**hard gates**, not guidelines:

1. **BIRD-EYE** — read full state of related files (use `view`, `grep`, schema queries)
2. **DEEP ANALYSIS** — root cause, not symptom
3. **SWOT if architectural** — schema/architecture/contract changes = L2

No recommendation may appear in a Claude message until evidence from steps 1–3
has been shown to the operator. When ground truth is absent (e.g., schema not
yet introspected, related files not yet read), the correct response is:

> "I don't have enough to lean — your call. Here's the data I do have: ..."

**NOT** a confident A/B recommendation with a premature lean.

**Origin**: LENS-021 Pattern Match Bias incident. Opus 4.7 jumped to ALTER TABLE
recommendation on first sight of "TODO comment + NOT NULL violation" without
reading the 2 other affected aggregators or verifying entity infrastructure. James
escalated correction twice. Recovery only after second push-back.

**Reference**: previous Claude protocol from LENS-007/014 (`lens-DOC-005_collab.md`
Error Fighting Protocol section). Marathon Claude (Apr 28-30) followed this
implicitly; over-instructed Opus 4.7 needs it explicitly enforced.

---

## LR-090 — Model selection by task tier

**Type**: Process | **Added**: LENS-021 | **Status**: RATIFIED

Project Lens daily work falls into three tiers. Match model to tier:

| Tier | Task character | Model |
|---|---|---|
| **Daily driver** | Patches, calibration, SQL, doc gen, audits, T1–T7 | Sonnet 4.6 adaptive |
| **Architecture** | New systems, schema design, web-app architecture, S4-B | Opus 4.7 adaptive |
| **Cross-cutting** | Multi-pillar coordination, novel cross-lab eval | Opus 4.7 adaptive |

Cause: Opus 4.7 burns ~3-5× weekly limit per session vs Sonnet 4.6. Using Opus
for daily-driver work creates a calendar bottleneck (1 marathon → forced reset).
Using Sonnet for architecture risks missing subtle gotchas Opus catches.

**Empirical evidence**: LENS-021 (T2 + entity wiring) consumed ~80% session weekly
in ~2.5h with Opus 4.7. Same work would have been ~25% on Sonnet 4.6. Marathon
session (Apr 28-30) was Opus-appropriate (cross-lab eval + architecture v4 lock);
LENS-021 was not.

**Decision rule**: default to Sonnet 4.6 adaptive. Pull Opus 4.7 only when the
next task explicitly requires architectural reasoning across systems.

**Origin**: post-LENS-021 retrospective with operator. Both Anthropic plan limits
and project completion velocity favored split assignment.

---

**Rules update**: 16:30 Thai, Apr 30 2026
