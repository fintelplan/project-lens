# Session Audit — LENS-022 (Sonnet 4.6 adaptive)
# Multi-day session: Apr 30 → May 4, 2026

**Session model**: Claude Sonnet 4.6 adaptive
**Operator**: James Maverick (Bro Alpha)
**Time**: Apr 30 ~00:00 → May 4 ~21:00 Thai (~5 days)
**Last commit**: `639e3d9`
**Status**: CLOSED ✅

---

## What we accomplished

### Core LENS-022 work

**Ref system redesign (S1/S2 architecture)**
- Root cause: refs ran BEFORE S2 finished → Selected by S2: 0
- Fix: redesigned from free/sonnet modes → S1/S2 architecture
- `20260504_S1_2of2.xlsx` — S1 canary pool, scored articles
- `20260504_S2_2of2.xlsx` — S2 full pool, flagged articles
- Merged into manage-analyze as final step after S2
- Deleted lens-ref-free.yml and lens-ref-sonnet.yml
- Verified live: S1 350 collected/165 scored, S2 441 collected/57 flagged ✅

**T1 — Forensic Report rewired** (`ee0e910`)
- schedule cron → workflow_run trigger after manage-analyze completes

**T4 — Guard system audit** (`7cbe784`)
- S3-E stale SambaNova registry entry removed
- S2-D guard_check_with_fallback added

**T5 — Mistral-small fallback in S2-A** (`10d1de5`)
- European lineage, PHI-002
- Model param fix: `call_injection_tracer` now accepts model parameter

**T6 — LR-091 LOCAL model testing protocol** (`4213edd`)

**T7 — Entity extraction unblocked** (`ad30d7b`)
- groq SDK + GROQ_API_KEY added to lens-collect.yml

**Sources**
- SRC-078 China Daily English added (TIER_A, `24001f8`)

**GitHub Actions hygiene**
- All actions pinned to v4.2.2 / v5.3.0 (`83ed377`)
- stale comments cleaned in requirements.txt and lens-collect.yml

---

## Hotfixes (post-session production fires)

| Commit | Fix |
|---|---|
| `eb662d5` | S2-D syntax error from T4 guard patch (broken try block) |
| `f634b78` | S2F 3 aggregators syntax + S2-A Mistral wrong model name |
| `36763c3` | LR-092 post-patch syntax verification rule |
| `101cabd` | S2-B GEMINI_S2B_API_KEY + S2-D token-aware batch |
| `9bb313a` | GEMINI_S2B_API_KEY wired into manage-analyze yml |
| `4e83499` | LR-093 explicit cleanup tracking rule |
| `effc12f` | S4-E cycle label fix + Regular Report 3-day ref window |
| `639e3d9` | TierCD int id not subscriptable |

---

## Failure patterns observed

### Pattern Match Bias on patch scope (LR-092 origin)
Fixed S2-D syntax but didn't check S2F aggregators — same broken pattern in 3 sibling files. 8 consecutive S2F failures (May 1-4) before caught. Recovery: LR-092 (syntax verify ALL affected files before commit).

### S2-A Mistral fallback wrong model name (T5 bug)
`call_injection_tracer` used hardcoded `MODEL` constant (Groq's model name) even when Mistral client passed. Every Mistral fallback returned 400 Bad Request. Fixed by adding `model` parameter to function.

### Fake smoke test entity in production (LR-093 origin)
`professor john smith` persisted 13 days in lens_entities, appearing as most active entity in every Daily Brief. Recovery: deleted directly + LR-093 (explicit cleanup list in session close docs).

### Refs table gap (May 1-4)
manage-analyze failures caused refs step to never run → lens_article_refs empty for 4 days → Regular Report "Refs cited: 0". Recovery: manual backfill + 3-day window extension.

### S4-E stuck at 54 runs
`count_s1_runs_new` filtered by CANONICAL_CYCLES (`2of1`/`2of2`) but all workflow_dispatch runs use `manual`. Fixed to count all rows after DAY_1_UTC regardless of label.

---

## Rules added this session

| Rule | Description |
|---|---|
| LR-088 | State actor lenses as entity_type='state_office' |
| LR-089 | Hard gates protocol (debugging) |
| LR-090 | Model selection by task tier |
| LR-091 | LOCAL model testing protocol |
| LR-092 | Post-patch syntax verification on ALL affected files |
| LR-093 | Explicit cleanup tracking in session close docs |

---

## CLEANUP completed this session
- [x] Deleted smoke test entity `professor john smith` from lens_entities
- [x] Deleted lens-ref-free.yml and lens-ref-sonnet.yml
- [x] Removed stray root-level files (lens_ref_system.py, patch scripts)

---

## Pending → LENS-023

### URGENT (check first)
- Verify tonight's cron: S2-B uses GEMINI_S2B_API_KEY (new key, first live run)
- Verify S2-D token-aware batching works (no more 413)
- Verify S2F Watch/Clarity/Verification pass (8 consecutive failures fixed)

### IMPORTANT
- T3 — v4 steno calibration Article 6 (Chosunbiz) — run when quota resets
- S3 context stale — S3-A: Apr 15, S3-D: Apr 16 — S3 never completes (Groq exhausted by time S3 runs)
- S2-D guard uses `GROQ_API_KEY` but S2-D is on same key as S1 Lens 1 — may need isolation

### DEFERRED
- S4-B build: July 2026
- Direction A / web app: after S4-B
- Node.js 20→24: June 2, 2026 deadline

---

**Session closed**: May 4, 2026 ~21:00 Thai. Next session: LENS-023, Claude Sonnet 4.6 adaptive.


---

## ADDENDUM — Post-close fixes (May 4, 2026 late session)

### Root cause analysis: Forensic Report not firing

Forensic report showed zero runs since workflow_run trigger was added (T1, LENS-022).
Root cause chain:
1. S2-A uses GROQ_S2_API_KEY shared with S2-GAP
2. By second daily run, shared 100K TPD depleted
3. S2-A fails → orchestrator exits code 1 (S2-A is designated critical)
4. workflow_run trigger requires source workflow to complete
5. GitHub silently drops workflow_run events on consistently failing source
6. Forensic report = never fires

### Fix (commits a6010d6 + b5fa81f)
- GROQ_S2A_API_KEY: dedicated Groq account for S2-A only (100K TPD isolated)
- S2-GAP moved to GROQ_API_KEY (off GROQ_S2_API_KEY)
- LR-094: quota isolation mandatory for critical positions

### Expected result tonight
- S2-A succeeds (fresh 100K TPD from dedicated account)
- manage-analyze exits 0
- workflow_run fires forensic report
- Opus docx delivered to Telegram for first time since Apr 30

### Lessons (why we couldn't estimate this)
1. Guard ledger tracks what our code logged, not actual Groq consumption
2. Shared-key quota depletion is invisible until it fails in production
3. LR-058/LR-094: same lesson learned 3 times (GNI S30, LENS-010, LENS-022)
   — must be checked at architecture design time, not after failure

### Updated rules count: LR-088 to LR-094 (7 new rules this session)


---

## ADDENDUM 2 — Quota isolation complete (May 5, 2026 ~02:30 Thai)

### Full key distribution locked
- GROQ_API_KEY (mail a): S1-L1 only
- GROQ_S2_API_KEY (mail b): Entity extract + GROQ_MANAGER
- GROQ_S2E_API_KEY (mail c): S2-E only
- GROQ_S3_API_KEY (mail d): S3-A only (key regenerated)
- GROQ_MA_API_KEY (mail e): Mission Analyst only
- GROQ_S2A_API_KEY (mail f): S2-A only
- GROQ_S2DGCOM_API_KEY (mail g): S2-D + S2-GAP + Compendium

### Additional fixes
- S3 orchestrator UnboundLocalError fixed (failed var outside try/except)
- GEMINI_S3B_API_KEY wired into S3 step
- cohere added to manage-analyze pip install (S3-C)
- S3-C was silently failing: cohere SDK not installed
