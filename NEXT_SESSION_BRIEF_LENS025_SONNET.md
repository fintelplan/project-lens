# Next Session Brief — LENS-025 (Claude Sonnet 4.6 adaptive)
# Written at LENS-024 close, May 17, 2026 ~10:45 AM Thai

**Last commit**: `e4d7ce3`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Sources**: 69 live
**You are**: Claude Sonnet 4.6 adaptive, fresh session. James calls you "my buddy."

---

## OPERATOR — JAMES MAVERICK ("Bro Alpha")

- Higher Diploma CS student, Spring University Myanmar, Chiang Mai Thailand (UTC+7)
- Identity: "Team Geeks" — genuine long-term project partner
- Tone: warm informal ("my buddy") with engineering rigor underneath
- **Cut preamble. Answer first. Justify only if asked.**
- Lettered options A/B/C with honest lean stated
- Short message after long Claude response = pause signal, re-examine
- "Move on as we can" = execute, don't recap
- "Where are we" = prioritized to-do list, not narrative
- **"BEV"** = HARD STOP — diagnose-only until ALL related files read + schema verified
- **"please remember error fighting..."** = you moved too fast, re-apply gates
- One question max per turn

## ENVIRONMENT

- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- Load env: `export $(grep -v '^#' .env | grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' | xargs)`
- GitHub: github.com/fintelplan/project-lens

## HARD GATES (never violate)

1. **BIRD-EYE** before any patch — read ALL related files first
2. **`python -m py_compile`** ALL modified .py files before commit (LR-092)
3. **Check sibling files** for same pattern — including both yml AND code files (LR-092/098)
4. **Ship-to-file patch** over bash heredoc on Git Bash Windows (LR-078)
5. **One question max** per turn
6. **Schema/architecture = L2** — propose, James approves
7. **HTTP error → `r.text[:200]`** first — never diagnose from status code alone (LR-095)
8. **Blob columns → size-check** before prompt — never dump raw DB column into AI prompt (LR-096)
9. **yml timeout → read actual value** — never cite platform max without checking yml (LR-097)
10. **pip package removal → grep code/** — always check code imports when removing pip packages (LR-098)

---

## WHAT LENS-024 DELIVERED (6 commits, May 12-17)

### Fix 1 — manage-analyze failing at 9s (mistralai pip)
`mistralai` not on PyPI → pip failed → entire pipeline dead. Removed from yml.
**Then LR-092 sibling check found**: `lens_s2c_emotion.py` still had `from mistralai.client import Mistral` hardcoded. S2-C completely rewritten to use `requests` directly.
**LR-098 born**: When removing a pip package, ALWAYS grep code/ for SDK imports first.

### Fix 2 — xlsx 1of2 missing since May 10
Morning manage-analyze failed daily (mistralai) → only evening cron produced xlsx → only `2of2` delivered for 3+ days. Fixed by:
- New `lens-ref-export.yml` standalone: runs at `02:30 UTC` (09:30 AM Thai) + `14:30 UTC` (09:30 PM Thai)
- Only needs 4 secrets: Supabase + Telegram
- Removed ref export from manage-analyze (no duplicate delivery)
- Both `1of2` + `2of2` confirmed delivered daily since May 13

### Added — CNN + NDTV sources
- SRC-079 CNN World News (TIER1, US, 29 entries)
- SRC-080 CNN Business (TIER1, US, 20 entries)
- SRC-081 NDTV World News (TIER1, India, 20 entries — first India source, PHI-003 non-Western democratic counterbalance)
- NDTV confirmed live in Telegram May 16 ✅

---

## YOUR FIRST TASKS

### Task 1 — Verify cron health (run immediately)
```bash
export $(grep -v '^#' .env | grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' | xargs)
python - << 'EOF'
import os
from supabase import create_client
from datetime import datetime, timezone, timedelta
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
cutoff = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()

# S3-F data gate progress
r = sb.table("lens_system3_reports").select("run_id").eq("position","S3-A").execute()
s3a_count = len(set(row["run_id"] for row in (r.data or [])))
r2 = sb.table("lens_system3_reports").select("run_id").eq("position","S3-D").execute()
s3d_count = len(r2.data or [])
print(f"S3-F data gate: S3-A={s3a_count}/20 runs, S3-D={s3d_count}/4 runs")
print(f"S3-F gate: {'OPEN ✅' if s3a_count >= 20 and s3d_count >= 4 else 'NOT YET'}")

# Drift findings count
r3 = sb.table("lens_drift_findings").select("id").execute()
print(f"Drift findings total: {len(r3.data)} (expect <50, dedup working)")

# Recent manage-analyze
print("\nCheck GitHub Actions for last manage-analyze run status")
EOF
```

### Task 2 — Add LR-095/096/097/098 to rules.md (OVERDUE since LENS-023)
```bash
cat lens-DOC-002_rules.md | tail -30
```
Then append four rules. They've been sitting as "pending" for two sessions.

### Task 3 — T3 steno calibration Article 6
Run at 6-8 AM Thai only (Cerebras queue fresh):
```bash
export S2F_PROVIDER=cerebras
python calibrate_rubric_article6_chosunbiz.py 2>&1 | grep -E "Operations found|Confidence:|Status:|OP-0[3][0-9]"
```
Previous attempts: 2 partial successes (conf=0.85/0.92, 5 ops each) at 3 AM Thai — Cerebras saturated. 6-8 AM is the correct window.

---

## CURRENT SYSTEM STATUS (LENS-024 close, May 17 ~10:45 AM Thai)

### Workflows
| Workflow | Schedule | Status |
|---|---|---|
| Lens Collection Pipeline | `0 1/13 UTC` | ✅ Green |
| Lens Manager + Analyze | `28 1/13 UTC` | ✅ Green |
| Lens S2-F Scoring Pipeline | `0 2/14 UTC` | ✅ Green |
| Lens GDELT Enrichment | scheduled | ✅ Green |
| Lens Regular Report | scheduled | ✅ Green |
| Lens Intelligence Compendium | scheduled | ✅ Green |
| **Lens Reference Export** | `30 2/14 UTC` | ✅ NEW — 37s runtime |
| Lens Forensic Report | workflow_run | ✅ Auto-firing |
| Lens Resume (Manual) | manual | ✅ Ready |

### Positions
| Position | Status | Provider | Notes |
|---|---|---|---|
| S1 Lens 1-4 | ✅ | Groq/Gemini/Cerebras | quality 6.6-7.5/10 |
| S2-A | ✅ | Groq mail f | dedicated key |
| S2-B | ✅ | Gemini → Mistral fallback | RPD exhausted → fallback |
| S2-C | ✅ FIXED | Mistral (requests) | SDK removed LENS-024 |
| S2-D | ✅ | Groq mail g | qwen3-32b |
| S2-E | ✅ | Groq mail c | llama |
| S2-GAP | ✅ | Groq mail g | |
| S2-F Watch | ✅ | DB only | dedup guard active |
| S2-F Clarity | ✅ | DB only | dedup guard active |
| S2-F Verification | ✅ | DB only | dedup guard active |
| S3-A | ✅ | Groq mail d | daily, quality 0.8 |
| S3-B | ✅ | Gemini → Mistral fallback | |
| S3-C | ✅ | Cohere command-r-plus-08-2024 | Mon/Thu |
| S3-D | ✅ | Cerebras → Mistral fallback | Mon/Thu |
| S3-E | ✅ | SKIPPED_CI | PHI-002 local only |
| S3-F | ⏳ | Mistral | SKIPPED_INSUFFICIENT_DATA ~10 more days |
| Mission Analyst | ✅ | Groq mail e | ELEVATED threat |
| S4-E | ✅ | DB monitor | threshold tracking |

### Sources (69 total)
- All 69 verified live at last check
- CNN World (SRC-079), CNN Business (SRC-080), NDTV (SRC-081) added LENS-024
- Next check if collection logs show dead sources

---

## KEY FILES CHANGED THIS SESSION

| File | Change |
|---|---|
| `.github/workflows/lens-manage-analyze.yml` | removed mistralai + removed ref export step |
| `.github/workflows/lens-ref-export.yml` | NEW — standalone xlsx yml |
| `code/lens_s2c_emotion.py` | complete rewrite — SDK → requests |
| `data/lens-SRC-001_sources.json` | +3 sources (CNN World, CNN Business, NDTV) |

---

## KEY RULES — MOST IMPORTANT FOR LENS-025

**LR-098** (LENS-024 new): When removing a package from pip yml, ALWAYS:
```bash
grep -rn "from <package>\|import <package>" code/
```
before committing. pip install and code imports must be consistent. Missing this caused two failures in one session.

**LR-097** (LENS-023): Check yml `timeout-minutes` value before dismissing operator timeout concerns.

**LR-096** (LENS-023): Check `len(str(value))` before using any DB column in AI prompt.

**LR-095** (LENS-023): Always `r.text[:200]` on HTTP errors before diagnosing.

*(All four still need to be added to lens-DOC-002_rules.md — Task 2 above)*

---

## PENDING RULE ADDITIONS (4 rules, 2 sessions overdue)

Add to `lens-DOC-002_rules.md`:

**LR-095**: Always log `r.text[:200]` on any HTTP error before diagnosing. Status code alone tells you nothing.

**LR-096**: Never pass raw blob DB columns into AI prompts. Check `len(str(value))` first. If >1000 chars, extract metadata only.

**LR-097**: Before dismissing any operator concern about yml limits, read the actual yml `timeout-minutes` value. Never cite platform maximums without checking operator-set overrides.

**LR-098**: When removing a package from pip install in yml, run `grep -rn "from <package>\|import <package>" code/` to verify no code file still imports it. pip install and code imports must be consistent.

---

**You've got this buddy. Read this brief, verify cron health, add the overdue rules, then calibrate T3 if quota fresh. 🤜**
