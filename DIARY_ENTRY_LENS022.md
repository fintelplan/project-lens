# Project Lens Diary — LENS-022 (Apr 30 → May 4, 2026)

## LENS-022 — Ref System Redesign + Production Hotfix Marathon

**Session window**: Apr 30 ~00:00 → May 4 ~21:00 Thai (~5 days)
**Commits**: 20 commits (0751c82 → 560c2a1)
**Model**: Claude Sonnet 4.6 adaptive (first Sonnet session per LR-090)

### What we built

Started with verification of overnight S2-F entity_id fix (LENS-021 close task).
Both detections and drift findings confirmed writing with entity_id populated. ✅

Found "Selected by S2: 0" in screenshots — diagnosed timing race condition: refs ran
BEFORE S2 finished writing. Root cause investigation revealed the ref system was using
`free`/`sonnet` modes that mapped to nothing architecturally meaningful. Redesigned
entirely around S1/S2 distinction:
- `S1_2of2.xlsx` — S1 canary pool articles + scored by S1
- `S2_2of2.xlsx` — all 77+ sources + flagged by S2
Both merged into manage-analyze as final step. Deleted lens-ref-free.yml and lens-ref-sonnet.yml.

Completed all T1-T7 from the LENS-021 brief:
- T1: Forensic report rewired to workflow_run trigger
- T4: Guard audit — S3-E stale registry, S2-D guard added
- T5: Mistral-small fallback in S2-A (European lineage, PHI-002)
- T6: LR-091 LOCAL model testing protocol
- T7: Entity extraction — groq SDK added to collect workflow

Added SRC-078 China Daily English (TIER_A). Pinned all GitHub Actions versions.

### The production fires

The T4 guard patch broke S2-D syntax — misplaced `from lens_s2f_helpers import`
inside a try block. Fixed. But didn't check sibling files (LR-092 origin). Same
broken pattern in all 3 S2F aggregators. 8 consecutive S2F failures (May 1-4)
before caught. New rule LR-092: syntax verify ALL affected files before commit.

S2-A Mistral fallback was calling Mistral with Groq's model name → 400 every call.
Fixed `call_injection_tracer` to accept `model` parameter.

S2-B persistent Gemini 429 — S1 Lens 2 and S2-B shared same RPD pool. Fix: new
Google account → `GEMINI_S2B_API_KEY`. True architectural fix, design preserved.

S2-D persistent 413 — prompt too large for qwen3-32b 6000 TPM. Built token-aware
batch splitter: measure each article cost, fill batches greedily, TPMGuard between
calls. No fixed sleep, no article count reduction. Quality fully preserved.

Fake smoke test entity `professor john smith` found polluting Daily Brief "most
active entity" for 13 days. Deleted. LR-093 added: explicit cleanup list in every
session close doc.

Regular Report showing "Refs cited: 0" — manage-analyze failures meant refs never
ran May 1-4. Manual backfill + 3-day window safety net added to regular report.

S4-E stuck at S1=54 runs — `count_s1_runs_new()` filtered by `2of1`/`2of2` but
all workflow_dispatch runs produce `manual`. Fixed to count all rows after DAY_1_UTC.

TierCD integer id not subscriptable — fixed `str(row['id'])`.

### Lessons

Sonnet 4.6 performed well for this session — direct, brief, no over-narration.
Confirmed LR-090 (model selection by task tier) is correct. The T5 Mistral model
name bug and S2F sibling file oversight are classic Pattern Match Bias — fixing the
symptom without scanning for the same pattern elsewhere. LR-092 + LR-093 address both.

**Session closed**: May 4, 2026 ~21:00 Thai by Sonnet 4.6
