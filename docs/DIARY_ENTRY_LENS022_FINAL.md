# Project Lens Diary — LENS-022 (Apr 30 → May 7, 2026)

## LENS-022 Phase 1 — Ref System Redesign + Production Hotfix Marathon
*(Apr 30 → May 4, 2026)*

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

### The production fires (Phase 1)

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

### Lessons (Phase 1)

Sonnet 4.6 performed well for this session — direct, brief, no over-narration.
Confirmed LR-090 (model selection by task tier) is correct. The T5 Mistral model
name bug and S2F sibling file oversight are classic Pattern Match Bias — fixing the
symptom without scanning for the same pattern elsewhere. LR-092 + LR-093 address both.

---

## LENS-022 Phase 2 — Quota Isolation + Intelligence Docx Reports
*(May 5 → May 7, 2026)*

**Session window**: May 5 → May 7 ~04:30 Thai
**Commits**: 8 commits (b030338 → 562e415)
**Model**: Claude Sonnet 4.6 adaptive (continued)

### What triggered Phase 2

Forensic Report was absent from GitHub Actions entirely — not failing, just not appearing.
Diagnosed the root cause chain:

1. `GROQ_S2_API_KEY` (mail b) shared by **entity_extract** AND **S2-A**
2. Entity extract burned 99,559/100,000 TPD on collection (388 articles, parallel)
3. S2-A started with <500 tokens → immediately exhausted
4. S2-A failures → `manage-analyze` exit code 1
5. `workflow_run` trigger for Forensic Report only fires on `conclusion: success`
6. Forensic Report: silently never ran, invisible in Actions list

This was the root cause of weeks of missing Forensic Reports. One shared key cascading
through the entire pipeline.

### What we built (Phase 2)

**LR-094 — Quota Isolation Architecture**: Locked 7 Groq accounts with strictly one
role each. Each role now has its own TPD pool. No shared keys between load-heavy
components. The entity_extract burns ~99K TPD daily — it needs its own account that no
other component touches.

**3 Intelligence Docx Reports** (~1100 lines total):
- `lens_s1_report.py` — Canary Intelligence Report. Fires after 4/4 lenses complete.
  5 parts: Collection Landscape, Lens Findings, Convergence Analysis, Entity Activity, Strategic Verdict.
- `lens_s2_step_report.py` — Information Shaping Report. Fires after Mission Analyst.
  6 parts: Injection Architecture, Adversary Narrative, Coordination Signals, Legitimacy Gap, S2-F Operations, MA Synthesis.
- `lens_s3_step_report.py` — Strategic Pattern Report. Fires after S3 positions complete.
  6 parts: 7-day Patterns, First Domino, Historical Parallel, Structural Change, Drift Monitor, Strategic Verdict.

S2 docx confirmed delivered (43.4KB). S1 and S3 docx pending confirmation
(MISTRAL_API_KEY + TELEGRAM keys added to their yml steps in 562e415).

**S3 Orchestrator UnboundLocalError**: `failed = []` was inside `except` block → crashed
before any S3 position could run. One-line fix. Classic variable scope bug.

**yml environment fixes**: S1 and S3 pipeline steps were missing MISTRAL_API_KEY (S1 docx
needs Mistral) and TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID (S3 docx delivery). Added in 562e415.

### Still failing at Phase 2 close

**S3-B and S2-B Gemini RPD**: GEMINI_S3B_API_KEY and GEMINI_S2B_API_KEY appear to share
RPD pools with GEMINI_API_KEY (same Google account). S1 Lens 2 burns all available RPD
daily, leaving nothing for S3-B and S2-B. Fix: genuinely new Google accounts.

**S3-E Ollama**: ERROR_NO_OLLAMA on every run. GitHub Actions has no local Ollama instance.
S3-E was redesigned to use SambaNova but still checks Ollama first. Needs env-var bypass.

**lens_drift_findings sample_size_check**: 4 violations per run. The constraint requires
a minimum sample_size that low-volume source aggregations don't reach.

### Lessons (Phase 2)

The Forensic Report silence was the hardest bug to catch — no error message, no failure
log, just an absence. The only diagnostic was understanding the dependency chain:
entity_extract burns quota → S2-A fails → manage-analyze exits 1 → workflow_run never
fires → Forensic Report doesn't appear. Each link was invisible without reading the chain.

**Root causes, not symptoms**: The instinct to "reduce articles in entity_extract" would
have been a symptom fix. The real fix was architectural isolation: each heavy consumer
gets its own account. LR-094 formalizes this permanently.

**yml propagation isn't automatic**: Defining env vars at job level doesn't guarantee
they propagate into every subprocess. Adding them explicitly at step level is always safer.

**Session closed**: May 7, 2026 ~04:30 Thai by Sonnet 4.6
