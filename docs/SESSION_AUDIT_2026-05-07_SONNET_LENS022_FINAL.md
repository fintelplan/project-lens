# Session Audit — LENS-022 Final (May 5 → May 7, 2026)
# Phase 2 addendum to SESSION_AUDIT_2026-05-04_SONNET_LENS022.md

**Model**: Claude Sonnet 4.6 adaptive  
**Repo**: github.com/fintelplan/project-lens  
**Phase 2 window**: May 5 → May 7, 2026 ~04:30 Thai  
**Last commit**: `562e415`  
**Working tree**: Clean  
**Branch**: main  

---

## Phase 2 Origin — Why We Didn't Close on May 4

Phase 1 closed May 4 with manage-analyze "green" but the **Forensic Report** was completely absent from GitHub Actions. 

**Root cause chain discovered in Phase 2:**
1. `GROQ_S2_API_KEY` (mail b) was shared by **entity_extract** AND **S2-A**
2. Entity extract burned ~99,559 of 100,000 TPD during collection (388 articles, parallel calls)
3. S2-A started with <1,000 tokens remaining → exhausted immediately
4. S2-A failures caused `manage-analyze` to exit with code 1
5. `workflow_run` trigger for Forensic Report only fires on success conclusion
6. Forensic Report silently never ran — not even appearing in Actions list

This one root cause explains weeks of missing Forensic Reports.

---

## What Was Built — Phase 2

### 1. Quota Isolation Architecture — LR-094 ✅

Full Groq key distribution locked. **7 Groq accounts, one role each:**

| Secret | Mail account | Role | TPD |
|---|---|---|---|
| `GROQ_API_KEY` | mail a | S1-L1 only | 100K |
| `GROQ_S2_API_KEY` | mail b | entity_extract + GROQ_MANAGER | ~4K shared |
| `GROQ_S2E_API_KEY` | mail c | S2-E only | 100K |
| `GROQ_S3_API_KEY` | mail d | S3-A only | 100K |
| `GROQ_MA_API_KEY` | mail e | Mission Analyst only | 100K |
| `GROQ_S2A_API_KEY` | mail f | S2-A only (dedicated) | ~20K |
| `GROQ_S2DGCOM_API_KEY` | mail g | S2-D + S2-GAP + Compendium | ~25K |

**GROQ_API_KEY_2 removed** — was ghost key created in GNI S30, never wired, caused confusion.

### 2. S3 Orchestrator UnboundLocalError — Fixed ✅

`failed` variable was inside `except` block → `UnboundLocalError` before any S3 position ran.
Moved `failed = []` before `try`. Commit `b030338`.

### 3. S1/S2/S3 Intelligence Docx Reports — Built ✅

Three new report generators (~1100 lines total):

| File | Report | Provider | Parts |
|---|---|---|---|
| `code/lens_s1_report.py` | S1 Canary Intelligence Report | Mistral-small | Collection Landscape, Lens Findings, Convergence, Entities, Verdict |
| `code/lens_s2_step_report.py` | S2 Information Shaping Report | Mistral-small | Injection Architecture, Adversary Narrative, Coordination, Legitimacy Gap, S2-F Operations, MA Synthesis |
| `code/lens_s3_step_report.py` | S3 Strategic Pattern Report | Cerebras | 7-day Patterns, First Domino, Historical Parallel, Structural Change, Drift Check, Strategic Verdict |

**Wiring:**
- S1 report: fires at end of `lens_orchestrator.py` (after 4/4 lenses complete)
- S2 report: fires at end of `lens_s2_orchestrator.py` (after MA + S4-E complete)
- S3 report: fires at end of `lens_s3_orchestrator.py` (after all S3 positions)
- `python-docx` and `cohere` added to manage-analyze pip install

### 4. yml Environment Fixes — ✅ (`562e415`)

**S1 step (lens_orchestrator.py)**: Added `MISTRAL_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`  
**S3 step (lens_s3_orchestrator.py)**: Added `MISTRAL_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`  
These were missing → S1 Canary docx got `AI_FAILED` (Mistral 400), S3 docx got `Telegram keys not set`.

### 5. Additional Fixes

| Fix | Detail |
|---|---|
| GEMINI_S3B_API_KEY added to S3 step | Was missing from manage-analyze env |
| GROQ_MANAGER_API_KEY | Updated to use mail b's key value |
| S2-E new key | GROQ_S2E_API_KEY regenerated for mail c |
| S3-A new key | GROQ_S3_API_KEY regenerated for mail d |
| cohere pip install | Added to manage-analyze requirements |

---

## Commits — Phase 2

| Commit | Description |
|---|---|
| `b030338` | S2-D/GAP/Compendium→GROQ_S2DGCOM + S3 UnboundLocalError fixed |
| `72ec936` | Entity extract→GROQ_S2_API_KEY (mail b) |
| `36f8ae4` | GROQ_API_KEY_2 removed from lens-collect.yml |
| `91d4fb1` | GEMINI_S3B_API_KEY to S3 step in manage-analyze |
| `bae4921` | cohere added to pip install |
| `3d56327` | S1/S2/S3 intelligence docx reports (3 new files, ~1100 lines) |
| `ccbf408` | Session audit phase 2 addendum |
| `562e415` | MISTRAL+TELEGRAM keys to S1 and S3 steps ← **LAST COMMIT** |

---

## What Was Verified Working (Telegram Screenshots May 5-7)

- ✅ S2 docx delivered — `20260506_S2_Shaping_Intelligence_DC1831.docx` (43.4KB)
- ✅ S1 "WHAT THE CANARY SEES" Telegram text — full 4-lens content
- ✅ S3 "WHAT IS ACTUALLY BEING BUILT" Telegram text
- ✅ S2-A on dedicated key — FALSE_EQUIV conf=0.90, EMOTIONAL_PRIME conf=0.90
- ✅ S2-D token-aware batching — 2 batches, 60 articles, merged results
- ✅ S2-E working — 9 LOW legitimacy actors flagged
- ✅ S4-E counter: 619/100 (54 legacy + 565 new)
- ✅ S3-A running — 3 patterns, quality 0.8
- ✅ Daily Brief firing properly
- ✅ S1 refs: 797 articles, S2 refs: 877 articles
- ✅ manage-analyze exits 0 consistently (S2-A dedicated quota)
- ✅ GitHub Actions #69 and #70 both ✅ green

---

## Still Pending / Failing at Session Close

### URGENT — Verify at LENS-023 open

**S1 Canary docx**
- Status: `562e415` added MISTRAL_API_KEY to S1 step
- May 6 18:18 UTC run showed `Mistral 400 attempt 1/2/3` (BEFORE fix was applied)
- **Next cron (after 562e415)** will be first true test
- Success indicator: `S1 REPORT COMPLETE | sent=True` in logs
- Failure indicator: `AI_FAILED` → investigate Mistral prompt structure, not keys

**S3 Strategic docx**
- Status: `562e415` added TELEGRAM keys to S3 step  
- May 6 18:33 UTC run showed `S3 REPORT COMPLETE | sent=False` (keys not set)
- **Next cron** will be first true test
- Success indicator: docx file in Telegram after S3 step completes

**Forensic Report**
- Root cause fixed: S2-A now dedicated key → manage-analyze exits 0 → workflow_run fires
- BUT: has not appeared in any GitHub Actions run observed yet
- **Next cron**: check if "Lens Forensic Report" appears in All Workflows list after manage-analyze
- If still missing: read `lens-forensic-report.yml` to verify workflow_run trigger syntax
- The run #70 (Today at 1:08 AM) is the first post-fix run — check Telegram for Forensic docx

### PERSISTENT FAILURES (require architectural fixes)

**S3-B Gemini 429 — RPD exhaustion**
- Root cause: `GEMINI_API_KEY` (mail a Google account) shared by S1 Lens 2 AND S3-B
- Lens 2 burns ~18-20 RPD per day → S3-B finds 0 remaining
- Fix required: New Google account → `GEMINI_S3B_API_KEY` registered separately
- Note: `GEMINI_S3B_API_KEY` secret exists in GitHub but uses SAME Google account as GEMINI_API_KEY

**S2-B Gemini 429 — RPD exhaustion**  
- Same root cause: `GEMINI_S2B_API_KEY` was supposed to be separate but appears to share RPD
- Check if GEMINI_S2B_API_KEY was actually created from a different Google account than GEMINI_API_KEY
- If same account: create genuinely separate Google account for S2-B

**S3-E Ollama — ERROR_NO_OLLAMA**
- GitHub Actions runner has no localhost:11434
- S3-E was redesigned to use SambaNova but still checks Ollama first
- Fix: In `code/lens_s3e_selfcheck.py`, skip Ollama check if not local environment
- Or: detect GitHub Actions via `$GITHUB_ACTIONS` env var and route directly to SambaNova

**lens_drift_findings sample_size_check constraint**
- 4 findings per run fail with: `violates check constraint "lens_drift_findings_sample_size_check"`
- Failing rows are being generated by `lens_s2f_watch_aggregator.py`
- Check: what is the minimum `sample_size` the constraint requires vs what the aggregator produces
- Low-volume sources naturally have fewer detections → undercount → constraint violation

**entity_extract TPD — GROQ_S2_API_KEY (mail b)**
- Burns 99,559/100,000 tokens during collection (388 articles, parallel)
- Leaves <500 tokens for GROQ_MANAGER_API_KEY which shares mail b
- As source count grows this will worsen
- Medium priority — not breaking yet, but monitor

---

## Rules Added — Phase 2

| Rule | Description |
|---|---|
| LR-094 | Quota isolation architecture: 7 Groq accounts, one-key-one-role principle |

---

## CLEANUP Section

**Completed this phase:**
- [x] GROQ_API_KEY_2 removed from lens-collect.yml (was ghost key)
- [x] Fake entity `professor john smith` verified deleted (done in Phase 1)

**For LENS-023 close:**
- [ ] Verify no stray test data in lens_entities / lens_entity_mentions
- [ ] Check for stray patch files in repo root
- [ ] Update lens-DOC-001_diary.md
- [ ] Update lens-DOC-004_status.md
- [ ] Update lens-DOC-002_rules.md with LR-094

---

## Failure Patterns Observed — Phase 2

### Silent Forensic Report disappearance
The Forensic Report was not running for an unknown period before this session — no error surfaced because the workflow_run simply never triggered (manage-analyze was exiting 1 silently). The workflow_run trigger is only as reliable as the upstream workflow's exit code. **Lesson**: Add Forensic Report presence to daily Brief checklist or monitoring.

### Pattern Match Bias on quota root cause (LR-094 origin)
Initial diagnosis pointed to "entity extract burns too many tokens." The real root cause was architectural: S2-A and entity_extract sharing one key, with no isolation between components. Jumping to "reduce article count" would have been a symptom fix. Evidence-gathering (watching actual TPD numbers in logs) led to the architectural diagnosis.

### yml env propagation assumption
Assumed environment variables defined at `env:` level of a job propagate to all steps. They don't always — step-level scripts that call subprocess or run a new Python process may not inherit. The explicit per-step `env:` addition was the correct fix, not a workaround.

---

**LENS-022 fully closed**: May 7, 2026 ~04:30 Thai by Sonnet 4.6  
**Next session**: LENS-023, Claude Sonnet 4.6 adaptive
