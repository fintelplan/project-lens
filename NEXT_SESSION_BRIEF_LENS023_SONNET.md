# Next Session Brief — LENS-023 (Claude Sonnet 4.6 adaptive)

**Last commit**: `560c2a1`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Brief written by**: Sonnet 4.6 at LENS-022 close, May 4, 2026 ~21:00 Thai
**You are**: Claude Sonnet 4.6 adaptive, fresh session

---

## OPERATOR — JAMES MAVERICK ("Bro Alpha")

- CS student, Spring University Myanmar, based in Chiang Mai Thailand (UTC+7)
- Tone: warm informal ("my buddy") with engineering rigor underneath
- Cut preamble. Answer first. Justify only if asked
- Lettered options A/B/C with honest lean OR "I don't have enough to lean"
- Short message after long response = pause signal, re-examine
- "Move on as we can" = execute, don't recap
- "Where are we" = prioritized to-do list, not narrative
- "BEV" = HARD STOP: diagnose-only until ALL related files read and schema verified
- "please remember error fighting..." = you moved too fast, re-apply gates
- One question max per turn

## ENVIRONMENT

- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- DB: Supabase (env vars: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`)
- Load env: `export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)`

## HARD GATES (never violate)

1. **BIRD-EYE** before any patch — read ALL related files first
2. **`python -m py_compile`** on ALL modified .py files before commit (LR-092)
3. **Check sibling files** for same pattern when fixing a bug (LR-092 origin)
4. **Session close CLEANUP section** mandatory — list all test data to delete (LR-093)
5. **Ship-to-file patch** over bash heredoc on Git Bash Windows (LR-078)
6. **One question max** per turn
7. **Schema/architecture = L2** (propose, James approves)

## WHAT JUST SHIPPED (LENS-022)

### Ref system v3 (redesigned)
- `lens_ref_system.py` — `--mode s1` and `--mode s2` replacing free/sonnet
- S1 mode: S1 canary pool (STATE/TIER1-3) + articles S1 actually scored
- S2 mode: all 77+ sources + articles S2 flagged for injection
- Both run at END of manage-analyze (after S2 completes) — guaranteed fresh data
- File naming: `YYYYMMDD_S1_2of2.xlsx`, `YYYYMMDD_S2_2of2.xlsx`

### Provider fixes
- **S2-A**: Mistral-small fallback wired, model param fixed (was passing Groq model to Mistral)
- **S2-B**: `GEMINI_S2B_API_KEY` from separate Google account (separate RPD pool from S1 Lens 2)
- **S2-D**: Token-aware batch processing — measures article token cost, batches greedily, TPMGuard between batches
- **S2F aggregators**: Syntax fixed (broken import inside try block, all 3 files)

### Data fixes
- **S4-E counter**: `count_s1_runs_new()` now counts all rows after DAY_1_UTC (not just `2of1`/`2of2`)
- **Regular Report**: 3-day ref window (was 1-day, caused "Refs cited: 0")
- **TierCD**: Fixed `int` id not subscriptable in ref system
- **Fake entity deleted**: `professor john smith` removed from `lens_entities`

### New rules
- LR-088 through LR-093 (see lens-DOC-002_rules.md)

### Sources
- SRC-078: China Daily English (TIER_A, world RSS)

---

## YOUR FIRST TASK — verify tonight's fixes worked

Run after 08:30 Thai (01:30 UTC cron):

```bash
export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)
python - << 'EOF'
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

cutoff = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
print(f"Checking since: {cutoff[:16]}")

# S2-B — did new Gemini key work?
r = sb.table("injection_reports").select("analyst,injection_type,confidence_score,created_at").eq("analyst","S2-B").gte("created_at",cutoff).execute()
print(f"S2-B rows tonight: {len(r.data)} {'✅' if r.data else '❌ STILL FAILING'}")

# S2-D — did batching work?
r2 = sb.table("injection_reports").select("analyst,created_at").eq("analyst","S2-D").gte("created_at",cutoff).execute()
print(f"S2-D rows tonight: {len(r2.data)} {'✅' if r2.data else '❌ STILL FAILING'}")

# S2-F — entity_id populated?
r3 = sb.table("lens_drift_findings").select("entity_id,state_actor_lens,created_at").gte("created_at",cutoff).limit(5).execute()
print(f"S2-F findings tonight: {len(r3.data)}")
for row in r3.data:
    eid = row['entity_id'][:8] if row['entity_id'] else 'NULL'
    print(f"  {row['state_actor_lens']} | entity_id={eid}")

# S4-E — counter updated past 54?
r4 = sb.table("lens_s4_alert_state").select("threshold_id,last_count").execute()
for row in r4.data:
    print(f"S4-E {row['threshold_id']}: {row['last_count']}")

# Refs — populated today?
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
r5 = sb.table("lens_article_refs").select("collected_date").eq("collected_date", today).limit(1).execute()
print(f"Refs today: {'✅ populated' if r5.data else '❌ empty — run manually'}")
EOF
```

**Decision branches:**
- S2-B rows > 0 → GEMINI_S2B_API_KEY working ✅ → proceed to T3
- S2-B still 0 → check GitHub Actions logs for S2-B step
- S2-D rows > 0 → token-aware batching working ✅
- S2-D still 0 → may need to reduce TOKEN_BUDGET further (check logs for token count)
- S2-F entity_id not NULL → LENS-021 confirmed ✅
- S4-E S1_RUNS > 54 → cycle fix working ✅

---

## TASK QUEUE (after verification)

### T3 — v4 steno calibration (~1 session)
Script exists, ready to run. Calibrate when Groq/Cerebras quota resets (start of day):
```bash
export S2F_PROVIDER=cerebras
python calibrate_rubric_article6_chosunbiz.py 2>&1 | grep -E "Operations found|Confidence:|Status:|OP-0[3][0-9]"
```
Hand-annotate Article 6 (1955 chars, Chosunbiz, Taiwan Strait) vs OP-030 to OP-034.
Manual annotation: OP-030 HIGH, OP-033 MEDIUM, OP-034 MEDIUM expected.
Compare model detection vs manual. Expected 8-11 total ops on steno-genre article.

### S3 stale context — investigate
S3-A last ran: Apr 15. S3-D: Apr 16. Nearly 3 weeks stale.
Root cause: Groq TPD exhausted by time S3 runs (~95K of 100K used by S1+S2).
Options:
- A: New Groq account → GROQ_S3_API_KEY dedicated (same pattern as GEMINI_S2B fix)
- B: S3 runs on next-day cron (collect runs at 01:00, S3 runs at 06:00 UTC)
BEV before any change: `cat .github/workflows/lens-s3-*.yml` or check S3 in manage-analyze.

### S2-D key isolation (medium priority)
S2-D uses `GROQ_API_KEY` (same key as S1 Lens 1 / Lens 4 stagger).
May compete for TPD. Consider `GROQ_S2D_API_KEY` from new account.
Before touching: verify actual contention in quota ledger.

### Node.js 20→24 (deadline June 2, 2026)
GitHub Actions warning on every run. All actions already pinned to v4.2.2/v5.3.0
(GitHub's own actions — auto-updated). Third-party actions if any = check.
Not critical today but set reminder.

---

## KEY FILES TO KNOW

| File | Purpose |
|---|---|
| `code/lens_ref_system.py` | Ref system v3 — S1/S2 modes |
| `code/lens_s2a_injection.py` | S2-A + Mistral fallback (MISTRAL_MODEL constant) |
| `code/lens_s2b_coordination.py` | S2-B — GEMINI_S2B_API_KEY wired |
| `code/lens_s2d_adversary.py` | S2-D — token-aware batch, TPMGuard |
| `code/lens_s2f_watch_aggregator.py` | S2F Watch — syntax fixed May 4 |
| `code/lens_s4_upgrade_monitor.py` | S4-E — counts all cycles now |
| `code/lens_regular_report.py` | Regular report — 3-day ref window |
| `data/lens-SRC-001_sources.json` | 78 sources incl. SRC-078 China Daily |
| `lens-DOC-002_rules.md` | LR-001 to LR-093 |

## CRITICAL PATTERN — LR-092

After ANY patch touching multiple files:
```bash
# Verify ALL modified Python files
for f in code/file1.py code/file2.py code/file3.py; do
    python -m py_compile $f && echo "OK: $f" || echo "BROKEN: $f"
done
```
NEVER commit until ALL pass. This burned 8 consecutive S2F failures in this session.

## CLEANUP SECTION (mandatory for LENS-023 close)

Items to check and clean at LENS-023 close:
- [ ] Verify no test/smoke data in `lens_entities` or `lens_entity_mentions`
- [ ] Check for stray files in repo root (patch scripts etc.)
- [ ] Update `lens-DOC-001_diary.md` with LENS-023 entry
- [ ] Update `lens-DOC-004_status.md`

---

**You've got this buddy. Read this brief, verify the cron, then calibrate. 🤜**


---
## ADDENDUM — S2-A quota isolation fix

GROQ_S2A_API_KEY added (dedicated account). S2-A now has isolated 100K TPD.
First task: verify tonight's cron shows forensic report fired in GitHub Actions.
Check: github.com/fintelplan/project-lens/actions — look for "Lens Forensic Report"
