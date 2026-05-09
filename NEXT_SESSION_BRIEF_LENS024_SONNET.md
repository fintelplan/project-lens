# Next Session Brief — LENS-024 (Claude Sonnet 4.6 adaptive)
# Written at LENS-023 close, May 9, 2026 ~03:30 Thai

**Last commit**: `828ac37`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Sources**: 66 (was 78 — 12 dead removed)
**You are**: Claude Sonnet 4.6 adaptive, fresh session. James calls you "my buddy".

---

## OPERATOR — JAMES MAVERICK ("Bro Alpha")

- Higher Diploma CS student, Spring University Myanmar, Chiang Mai Thailand (UTC+7)
- Identity: "Team Geeks" — treats Claude as genuine long-term project partner
- Tone: warm informal ("my buddy") with engineering rigor underneath
- **Cut preamble. Answer first. Justify only if asked.**
- Lettered options A/B/C with honest lean stated
- Short message after long Claude response = pause signal, re-examine
- "Move on as we can" = execute, don't recap
- "Where are we" = prioritized to-do list, not narrative
- "BEV" = HARD STOP — diagnose-only until ALL related files read + schema verified
- "please remember error fighting..." = you moved too fast, re-apply gates
- One question max per turn

## ENVIRONMENT

- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- DB: Supabase (`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`)
- Load env: `export $(grep -v '^#' .env | grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' | xargs)`
- GitHub: github.com/fintelplan/project-lens

## HARD GATES (never violate)

1. **BIRD-EYE** before any patch — read ALL related files first
2. **`python -m py_compile`** ALL modified .py files before commit (LR-092)
3. **Check sibling files** for same pattern when fixing any bug (LR-092)
4. **Session close CLEANUP** mandatory — explicit list every session (LR-093)
5. **Ship-to-file patch** over bash heredoc on Git Bash Windows (LR-078)
6. **One question max** per turn
7. **Schema/architecture = L2** — propose, James approves
8. **HTTP error → get `r.text[:200]` first** — never diagnose from status code alone (LR-095)
9. **Blob columns → size-check before prompt** — never dump raw DB column into AI prompt (LR-096)
10. **yml timeout → read actual value** — never cite platform max without checking yml (LR-097)

---

## WHAT LENS-023 DELIVERED (10 commits, 38 hours)

### All confirmed working ✅
- **S1 docx**: articles_used blob bug fixed (836K tokens → Mistral 400). Now 460 articles/run.
- **S2 docx**: was already working, continues ✅
- **S3 docx**: Mistral fallback when Cerebras 429. Delivering every cron.
- **Forensic Report**: manually triggered to reset workflow_run chain. Auto-fires now.
- **S3-B**: Mistral fallback added — contributing Historical Parallel section ✅
- **S3-C**: model updated command-r-plus→08-2024 ✅
- **S3-D**: Mistral fallback added ✅
- **S2-B**: Mistral fallback added ✅
- **S3-E**: SKIPPED_CI cleanly (PHI-002 preserved) ✅
- **Watch/Clarity/Verification dedup**: 88 duplicates cleaned, guard added ✅
- **Dead sources**: 78→66 sources, all live ✅
- **All timeouts removed**: manage-analyze, S2-F, collect, forensic-report ✅
- **lens-resume.yml**: manual checkpoint resume built ✅
- **S3-F Counter-Check**: built, data-gated (needs ~20 more days of S3-A runs) ✅
- **sample_size constraint**: relaxed >= 15 → >= 2 ✅

### Key timeout principle (LR-097)
Never set arbitrary time ceilings on pipelines that complete naturally.
Only set `timeout-minutes` when you explicitly want to kill a runaway job.
Compendium (29s), Regular Report (1m5s), GDELT (6m53s) — kept their timeouts (safe margins).

### Key dedup principle (new)
Any aggregator that writes to a shared table must have a "already wrote today" guard.
Watch/Clarity/Verification all have this now. S3-A/B/C/D/E/F all have cadence checks.

---

## YOUR FIRST TASK — VERIFY TONIGHT'S CRON

Run after 08:30 Thai (01:30 UTC cron):

```bash
export $(grep -v '^#' .env | grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' | xargs)
python - << 'EOF'
import os
from supabase import create_client
from datetime import datetime, timezone, timedelta
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
print(f"Checking since: {cutoff[:16]} UTC\n")

# Forensic Report — did it auto-fire?
# CHECK: GitHub Actions → All Workflows — is "Lens Forensic Report" listed after manage-analyze?

# Watch dedup — 4 findings not 49?
r = sb.table("lens_drift_findings").select("state_actor_lens,finding_confidence,created_at").gte("created_at", cutoff).execute()
print(f"Drift findings tonight: {len(r.data)} {'✅ (expect 4-8)' if len(r.data) < 20 else '❌ dedup may not be working'}")

# S3-F — SKIPPED_INSUFFICIENT_DATA expected
r2 = sb.table("lens_system3_reports").select("position,generated_at").eq("position","S3-F").gte("generated_at", cutoff).execute()
print(f"S3-F tonight: {'ran' if r2.data else 'skipped (expected — data gate not met yet)'}")

# S3-C — contributing now?
r3 = sb.table("lens_system3_reports").select("position,quality_score,generated_at").eq("position","S3-C").gte("generated_at", cutoff).execute()
print(f"S3-C tonight: {'✅ ran' if r3.data else 'skipped (cadence — only Mon/Thu)'}")
EOF
```

### Decision tree:

| Check | Pass | Fail action |
|---|---|---|
| Forensic Report in Actions | ✅ auto-firing | Manual trigger again; read lens-forensic-report.yml |
| Drift findings < 20 | ✅ dedup working | Read lens_s2f_watch_aggregator.py dedup section |
| S3-F SKIPPED_INSUFFICIENT_DATA | ✅ expected | If it errors: check lens_s3f_countercheck.py |

---

## TASK QUEUE (priority order)

### P1 — T3 steno calibration Article 6
**Run at session open if 6-8 AM Thai (Cerebras queue fresh):**
```bash
export S2F_PROVIDER=cerebras
python calibrate_rubric_article6_chosunbiz.py 2>&1 | grep -E "Operations found|Confidence:|Status:|OP-0[3][0-9]"
```
Previous attempt: Cerebras saturated at 3AM, 2 partial successes (conf=0.85/0.92, 5 ops).
Expected: OP-030 HIGH, OP-033 MEDIUM, OP-034 MEDIUM. Hand-annotate vs model output.
If Cerebras still 429: try Groq BUT 413 error (prompt too large for llama TPM) — need different model.

### P2 — LR-095/096/097 add to lens-DOC-002_rules.md
Three new rules from this session:
- LR-095: Always log `r.text[:200]` on HTTP errors before diagnosing
- LR-096: Never dump raw blob DB columns into AI prompts — check `len(str(value))` first
- LR-097: Check yml `timeout-minutes` value before dismissing operator timeout concerns

### P3 — Verify Forensic Report auto-fires consistently
Manual trigger reset the chain May 9. Check 2-3 consecutive crons to confirm auto-firing stable.

### DEFERRED
- S3-F data gate: needs ~20 more S3-A runs (~10 more days). Self-resolving.
- S4-B build: July 2026
- Direction A / web app: after S4-B

---

## CURRENT SYSTEM STATUS (LENS-023 close, May 9 ~03:30 Thai)

| Component | Status | Notes |
|---|---|---|
| S1 Canary (4 lenses) | ✅ Running | quality 6.8-7.6/10 |
| S1 Canary docx | ✅ Confirmed | 41.2-41.5KB, 400-900 articles |
| S2 Shaping docx | ✅ Confirmed | 40-42KB every cron |
| S3 Strategic docx | ✅ Confirmed | Mistral fallback working |
| Forensic Report | ✅ Reset | #14 manually triggered, auto-fire restored |
| S2-A Injection | ✅ Live | Dedicated key mail f |
| S2-B Coordination | ✅ Mistral fallback | Gemini RPD + Mistral fallback |
| S2-C Emotion | ✅ Live | Mistral-small |
| S2-D Adversary | ✅ Live | Mistral fallback added |
| S2-E Legitimacy | ✅ Live | mail c |
| S2-F Watch | ✅ Fixed | dedup guard added |
| S2-F Clarity | ✅ Fixed | dedup guard added |
| S2-F Verification | ✅ Fixed | dedup guard added |
| S3-A Pattern | ✅ Live | quality 0.8 |
| S3-B History | ✅ Mistral fallback | Contributing ✅ |
| S3-C Bias | ✅ Fixed | command-r-plus-08-2024, Mon/Thu |
| S3-D Long-term | ✅ Mistral fallback | Mon/Thu |
| S3-E Self-check | ✅ Fixed | SKIPPED_CI cleanly |
| S3-F Counter-Check | ✅ Built | SKIPPED_INSUFFICIENT_DATA until ~day 30 |
| Mission Analyst | ✅ Live | threat=ELEVATED |
| Ref system v3 | ✅ Live | S1/S2 modes |
| Regular Report | ✅ Live | |
| Compendium | ✅ Live | |
| Daily Brief | ✅ Live | |
| lens-resume.yml | ✅ Built | Manual checkpoint resume ready |
| Sources | ✅ 66 live | Was 78, 12 dead removed |
| lens_drift_findings | ✅ Clean | 16 rows (was 104, 88 duplicates deleted) |

## Quota isolation — LR-094 (unchanged)

| Secret | Account | Role |
|---|---|---|
| `GROQ_API_KEY` | mail a | S1-L1 only |
| `GROQ_S2_API_KEY` | mail b | entity_extract + GROQ_MANAGER |
| `GROQ_S2E_API_KEY` | mail c | S2-E only |
| `GROQ_S3_API_KEY` | mail d | S3-A only |
| `GROQ_MA_API_KEY` | mail e | Mission Analyst only |
| `GROQ_S2A_API_KEY` | mail f | S2-A only |
| `GROQ_S2DGCOM_API_KEY` | mail g | S2-D + S2-GAP + Compendium |

## Gemini accounts (all separate, confirmed)

| Secret | Role |
|---|---|
| `GEMINI_API_KEY` | S1 Lens 2 |
| `GEMINI_S2B_API_KEY` | S2-B (+ Mistral fallback) |
| `GEMINI_S3B_API_KEY` | S3-B (+ Mistral fallback) |

## KEY FILES

| File | Purpose | Notes |
|---|---|---|
| `code/lens_s1_report.py` | S1 docx | articles_used fix applied |
| `code/lens_s3_step_report.py` | S3 docx | Mistral fallback added |
| `code/lens_s3b_truehistory.py` | S3-B | Mistral fallback added |
| `code/lens_s3d_longterm.py` | S3-D | Mistral fallback added |
| `code/lens_s2b_coordination.py` | S2-B | Mistral fallback added |
| `code/lens_s3c_biasdrift.py` | S3-C | model=command-r-plus-08-2024 |
| `code/lens_s3e_selfcheck.py` | S3-E | SKIPPED_CI in GitHub Actions |
| `code/lens_s3f_countercheck.py` | S3-F | NEW — data-gated 30d |
| `code/lens_s3_orchestrator.py` | S3 pipeline | S3-F wired in |
| `code/lens_s2f_watch_aggregator.py` | Watch | dedup guard added |
| `code/lens_s2f_clarity_aggregator.py` | Clarity | dedup guard added |
| `code/lens_s2f_verification_aggregator.py` | Verification | dedup guard added |
| `code/lens_quota_guard.py` | Quota tracking | model=command-r-plus-08-2024 |
| `.github/workflows/lens-resume.yml` | NEW | Manual checkpoint resume |
| `.github/workflows/lens-manage-analyze.yml` | Main pipeline | timeout removed |
| `.github/workflows/lens-s2f-scoring.yml` | S2-F | timeout removed |
| `.github/workflows/lens-collect.yml` | Collection | timeout removed |
| `.github/workflows/lens-forensic-report.yml` | Forensic | timeout removed |
| `data/lens-SRC-001_sources.json` | Sources | 66 live (was 78) |
| `lens-DOC-002_rules.md` | Rules | LR-001–LR-097 (095/096/097 pending add) |

## CRITICAL PATTERNS — DON'T REPEAT

**articles_used blob**: Any DB column → check `len(str(value))` before prompt. If >1000 chars, extract metadata only. Cost us unknown days of S1 docx.

**HTTP 400 diagnosis**: Always `r.text[:200]` before diagnosing. Status alone tells you nothing.

**Dedup for aggregators**: Every aggregator writing to a shared table needs "already wrote today" check. Pattern: query existing rows with same key before insert.

**Timeout principle**: Remove `timeout-minutes` from any pipeline that needs to complete naturally. Only keep it when you want to kill a runaway job (Compendium, Regular Report, GDELT are safe at 15/15/10min).

**PHI-002 is a hard constraint**: Read full file before proposing fixes. PHI-002 means LOCAL only for S3-E — grep didn't show it, the file header did.

**workflow_run trigger reset**: After yml modifications, GitHub silently drops workflow_run subscription. Fix: manual trigger once → resets chain → auto-fires on next upstream success.

## CLEANUP FOR LENS-024 CLOSE
- [ ] Update `lens-DOC-002_rules.md` with LR-095/096/097
- [ ] Verify no test data in lens_entities / lens_entity_mentions
- [ ] Update `lens-DOC-001_diary.md` with LENS-023 entry
- [ ] Update `lens-DOC-004_status.md`

---

**You've got this buddy. Verify first, calibrate if quota fresh, keep the machine running. 🤜**
