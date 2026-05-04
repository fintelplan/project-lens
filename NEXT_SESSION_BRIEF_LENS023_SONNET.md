# Next Session Brief — LENS-023 (Claude Sonnet 4.6 adaptive)

**Last commit**: `639e3d9`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Brief written**: May 4, 2026

---

## OPERATOR — JAMES MAVERICK ("Bro Alpha")
- Warm informal ("my buddy"), engineering rigor underneath
- Cut preamble. Answer first. Justify only if asked
- One question max per turn
- "BEV" = hard stop, diagnose only until all files read
- Short message after long response = pause signal

## ENVIRONMENT
- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- DB: Supabase (env: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`)

## HARD GATES
1. BIRD-EYE before any patch — read ALL related files
2. `python -m py_compile` on ALL modified .py files before commit (LR-092)
3. Session close CLEANUP section mandatory (LR-093)
4. One question max per turn
5. Ship-to-file patch over bash heredoc (LR-078)

## WHAT JUST SHIPPED (LENS-022)

### Architecture changes
- Ref system: S1/S2 modes replacing free/sonnet (lens_ref_system.py v3)
- Forensic report: workflow_run trigger (fires after manage-analyze)
- S2-A: Mistral-small fallback (European lineage, PHI-002)
- S2-B: GEMINI_S2B_API_KEY (separate from S1 Lens 2 key)
- S2-D: token-aware batch processing (no more 413)
- S4-E: counts all cycle labels including 'manual'
- Entity extraction: groq SDK in collect workflow

### Sources
- SRC-078: China Daily English (TIER_A)

### Rules
- LR-091: LOCAL model testing protocol
- LR-092: post-patch syntax verify ALL files
- LR-093: cleanup tracking in session close

---

## FIRST TASK — verify tonight's cron (01:30 UTC = 08:30 Thai May 5)

Run this diagnostic:
```bash
export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)
python - << 'EOF'
import os
from supabase import create_client
from datetime import datetime, timezone, timedelta
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

cutoff = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()

# S2-B: did it succeed?
r = sb.table("injection_reports").select("analyst,injection_type,confidence_score,created_at").eq("analyst","S2-B").gte("created_at",cutoff).execute()
print(f"S2-B rows tonight: {len(r.data)}")

# S2-D: did it succeed?
r2 = sb.table("injection_reports").select("analyst,created_at").eq("analyst","S2-D").gte("created_at",cutoff).execute()
print(f"S2-D rows tonight: {len(r2.data)}")

# S2-F: did it succeed?
r3 = sb.table("lens_drift_findings").select("entity_id,state_actor_lens,created_at").gte("created_at",cutoff).limit(5).execute()
print(f"S2-F findings tonight: {len(r3.data)}")
for row in r3.data:
    eid = row['entity_id'][:8] if row['entity_id'] else 'NULL'
    print(f"  {row['state_actor_lens']} | entity_id={eid}")

# S4-E: counter updated?
r4 = sb.table("lens_s4_alert_state").select("threshold_id,last_count").execute()
for row in r4.data:
    print(f"S4-E {row['threshold_id']}: {row['last_count']}")
EOF
```

**Three branches:**
- S2-B rows > 0 → GEMINI_S2B_API_KEY working ✅
- S2-D rows > 0 → token-aware batching working ✅
- S2-F entity_id not NULL → LENS-021 fix confirmed ✅
- S4-E S1_RUNS > 54 → cycle label fix working ✅

---

## TASK QUEUE (after verification)

### T3 — v4 steno calibration (~1 session)
Run when Groq/Cerebras quota resets. Script exists:
```bash
export S2F_PROVIDER=cerebras  # or groq
python calibrate_rubric_article6_chosunbiz.py
```
Hand-annotate Article 6 vs OP-030 to OP-034. Expected: 8-11 ops on steno-genre.

### S3 stale context — investigate
S3-A last: Apr 15. S3-D last: Apr 16. S3 never completes because Groq quota exhausted
by the time S3 runs (S1+S2 burn ~85-95K tokens/day). Options:
A) Dedicate GROQ_S3_API_KEY from separate account
B) Run S3 on separate cron offset from S2 by 24h

### S2-D key isolation (medium priority)
S2-D uses `GROQ_API_KEY` (same as S1 Lens 1). May compete for quota.
Consider isolating to `GROQ_S2D_API_KEY` from separate account.

### DEFERRED
- S4-B: July 2026
- Direction A / web app: after S4-B
- Node.js 20→24: June 2, 2026 deadline

---

## SESSION CLOSE PROTOCOL (LR-057 + LR-093)
At ~80% session usage:
1. Generate SESSION_AUDIT_YYYY-MM-DD_SONNET_LENS023.md
2. Generate NEXT_SESSION_BRIEF_LENS024.md
3. CLEANUP section: list any test data, stale refs, temp files to delete
4. Update lens-DOC-001_diary.md
5. Commit + push all docs

**You've got this buddy. Read this brief, check the cron, then calibrate. 🤜**
