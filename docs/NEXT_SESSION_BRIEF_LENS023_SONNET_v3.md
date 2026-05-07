# Next Session Brief — LENS-023 (Claude Sonnet 4.6 adaptive)
# Version 3 — Updated May 7, 2026 at session close

**Last commit**: `562e415`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Brief written by**: Sonnet 4.6 at LENS-022 close, May 7, 2026 ~04:30 Thai
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
- One question max per turn

## ENVIRONMENT

- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- DB: Supabase (`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`)
- Load env: `export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)`
- GitHub: github.com/fintelplan/project-lens

## HARD GATES (never violate)

1. **BIRD-EYE** before any patch — read ALL related files first
2. **`python -m py_compile`** ALL modified .py files before commit (LR-092)
3. **Check sibling files** for same pattern when fixing any bug (LR-092 origin)
4. **Session close CLEANUP** mandatory — explicit list every session (LR-093)
5. **Ship-to-file patch** over bash heredoc on Git Bash Windows (LR-078)
6. **One question max** per turn
7. **Schema/architecture = L2** — propose, James approves

---

## YOUR FIRST TASK — VERIFY LAST NIGHT'S CRONS

Run this after reading the brief. It tells you what Phase 2 fixes actually landed:

```bash
export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)
python - << 'EOF'
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
print(f"Checking since: {cutoff[:16]} UTC\n")

# 1. S1 docx — did Mistral finally work?
r = sb.table("lens_reports").select("generated_at,domain_focus,quality_score").gte("generated_at", cutoff).execute()
print(f"S1 reports tonight: {len(r.data)} {'✅' if len(r.data)>=4 else '❌'}")
# Also check Telegram — look for "S1 CANARY INTELLIGENCE REPORT" docx message

# 2. Forensic Report — did it appear?
# CHECK GITHUB ACTIONS: go to All Workflows — is "Lens Forensic Report" listed after manage-analyze?
# If not listed: the workflow_run trigger did not fire → read lens-forensic-report.yml

# 3. S2-A on dedicated key still working?
r2 = sb.table("injection_reports").select("analyst,confidence_score,created_at").eq("analyst","S2-A").gte("created_at", cutoff).execute()
print(f"S2-A rows tonight: {len(r2.data)} {'✅' if r2.data else '❌'}")

# 4. entity_extract TPD — how bad is it?
r3 = sb.table("lens_quota_ledger").select("provider,model,estimated_use,cron_time_utc").gte("cron_time_utc", cutoff).execute()
print(f"\nQuota ledger tonight ({len(r3.data)} rows):")
for row in r3.data:
    print(f"  {row['provider']}/{row['model']}: {row['estimated_use']} tokens")

# 5. lens_drift_findings constraint violations?
r4 = sb.table("lens_drift_findings").select("id,sample_size,state_actor_lens,created_at").gte("created_at", cutoff).execute()
print(f"\nDrift findings tonight: {len(r4.data)}")
for row in r4.data[:5]:
    print(f"  {row['state_actor_lens']} | sample_size={row.get('sample_size','?')}")

# 6. S3-B — still failing?
r5 = sb.table("lens_system3_reports").select("position,quality_score,generated_at").eq("position","S3-B").gte("generated_at", cutoff).execute()
print(f"\nS3-B tonight: {'✅ ran' if r5.data else '❌ STILL FAILING (Gemini RPD)'}")
EOF
```

### Decision tree after verification:

| Check | Pass | Fail action |
|---|---|---|
| S1 Canary docx in Telegram | ✅ done | Check logs for `AI_FAILED` — read `lens_s1_report.py` prompt |
| S3 Strategic docx in Telegram | ✅ done | Check logs for Cerebras 429 — Cerebras RPD? |
| Forensic Report in GitHub Actions | ✅ done | Read `lens-forensic-report.yml` — verify `workflow_run` trigger syntax |
| S2-A rows > 0 | ✅ done | S2-A dedicated key depleted — check mail f quota |
| S3-B rows > 0 | ✅ architectural fix needed | See task list below |

---

## WHAT LENS-022 DELIVERED (FULL PICTURE)

### Phase 1 (Apr 30 → May 4) — Ref system + hotfix marathon
- Ref system v3: `--mode s1` and `--mode s2`, merged into manage-analyze
- T1-T7 complete: Forensic trigger, guard audit, Mistral fallback, entity extraction
- SRC-078 China Daily English added (TIER_A)
- LR-088 through LR-093 added
- Last Phase 1 commit: `560c2a1`

### Phase 2 (May 5 → May 7) — Quota isolation + docx reports
- **Root cause found**: GROQ_S2_API_KEY shared by entity_extract + S2-A → S2-A depleted → manage-analyze exit 1 → Forensic Report never fired
- **LR-094**: 7 Groq accounts, one-key-one-role (see architecture below)
- **3 new docx reports**: S1 Canary, S2 Shaping, S3 Strategic
- **S3 UnboundLocalError fixed**: `failed = []` moved before try block
- **yml env fixes**: MISTRAL_API_KEY + TELEGRAM keys added to S1 and S3 steps
- Last Phase 2 commit: `562e415`

### Quota isolation architecture — LR-094

| Secret | Account | Role | Status |
|---|---|---|---|
| `GROQ_API_KEY` | mail a | S1-L1 only | ✅ stable |
| `GROQ_S2_API_KEY` | mail b | entity_extract + GROQ_MANAGER | ✅ wired |
| `GROQ_S2E_API_KEY` | mail c | S2-E only | ✅ wired |
| `GROQ_S3_API_KEY` | mail d | S3-A only | ✅ wired |
| `GROQ_MA_API_KEY` | mail e | Mission Analyst only | ✅ wired |
| `GROQ_S2A_API_KEY` | mail f | S2-A only (~20K TPD) | ✅ wired |
| `GROQ_S2DGCOM_API_KEY` | mail g | S2-D + S2-GAP + Compendium | ✅ wired |

---

## TASK QUEUE (priority order)

### P1 — Verify Phase 2 fixes (first 30 min)
Run verification script above. Check Telegram for S1 docx, S3 docx, Forensic Report.

### P2 — Forensic Report investigation (if still missing)
```bash
cat .github/workflows/lens-forensic-report.yml
```
Check: does `workflow_run` trigger reference the correct workflow name? Does it filter on `conclusion: success`? Verify manage-analyze workflow name matches exactly.

### P3 — S3-B Gemini RPD fix (architectural)
**Root cause**: `GEMINI_S3B_API_KEY` uses the SAME Google account as `GEMINI_API_KEY`. They share the same RPD pool. S1 Lens 2 burns ~18-20 RPD → S3-B finds 0.
**Fix**: Create genuinely new Google account (new Gmail) → enable Gemini API → get new key → update `GEMINI_S3B_API_KEY` secret in GitHub.
BEV first: read `code/lens_s3b_history.py` to confirm which env var it uses.

### P4 — S2-B Gemini RPD fix (verify + fix if needed)
Same question as P3: is `GEMINI_S2B_API_KEY` from a genuinely separate Google account?
Check by comparing API key prefixes in GitHub Secrets. If same account: same fix as P3.

### P5 — S3-E Ollama fix
**Root cause**: S3-E checks `localhost:11434` first. GitHub Actions runner has no Ollama.
**Fix in** `code/lens_s3e_selfcheck.py`:
```python
import os
if os.environ.get("GITHUB_ACTIONS"):
    # Skip Ollama check, route directly to SambaNova
    ...
```
BEV first: read full `lens_s3e_selfcheck.py` before touching.

### P6 — lens_drift_findings sample_size_check constraint
4 findings per run fail. Check constraint definition in Supabase:
```bash
# In Supabase SQL editor:
# SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'lens_drift_findings_sample_size_check';
```
Then check what `lens_s2f_watch_aggregator.py` produces for `sample_size`. Fix is either: relax constraint OR set `sample_size` to computed value before write.

### P7 — T3 v4 steno calibration (IMPORTANT, can defer)
Rubric calibration Article 6 (Chosunbiz, Taiwan Strait, 1955 chars). Script exists.
Run when quota fresh (start of day):
```bash
export S2F_PROVIDER=cerebras
python calibrate_rubric_article6_chosunbiz.py 2>&1 | grep -E "Operations found|Confidence:|Status:|OP-0[3][0-9]"
```
Expected: OP-030 HIGH, OP-033 MEDIUM, OP-034 MEDIUM. Hand-annotate vs model output.

### DEFERRED
- S4-B architecture build: July 2026
- Direction A / web app: after S4-B
- Node.js 20→24: deadline June 2, 2026 (Actions auto-pinned, check third-party only)

---

## KEY FILES

| File | Purpose |
|---|---|
| `code/lens_s1_report.py` | S1 Canary docx report (Mistral-small, 5 parts) |
| `code/lens_s2_step_report.py` | S2 Shaping docx report (Mistral-small, 6 parts) |
| `code/lens_s3_step_report.py` | S3 Strategic docx report (Cerebras, 6 parts) |
| `code/lens_ref_system.py` | Ref system v3 — S1/S2 modes |
| `code/lens_s2a_injection.py` | S2-A + Mistral fallback |
| `code/lens_s2b_coordination.py` | S2-B — GEMINI_S2B_API_KEY |
| `code/lens_s3b_history.py` | S3-B — GEMINI_S3B_API_KEY |
| `code/lens_s3e_selfcheck.py` | S3-E — needs Ollama bypass for GitHub Actions |
| `code/lens_s2f_watch_aggregator.py` | S2-F Watch — sample_size_check issue |
| `code/lens_s3_orchestrator.py` | S3-E UnboundLocalError fixed here |
| `.github/workflows/lens-manage-analyze.yml` | Main pipeline — all env vars live here |
| `.github/workflows/lens-forensic-report.yml` | Forensic — verify workflow_run trigger |
| `data/lens-SRC-001_sources.json` | 78 sources (SRC-078 = China Daily) |
| `lens-DOC-002_rules.md` | LR-001 to LR-094 |

## CURRENT SYSTEM STATUS (at LENS-022 close)

| Component | Status | Notes |
|---|---|---|
| S1 Canary (4 lenses) | ✅ Running | quality avg 7.5/10 |
| S1 Canary docx | ⚠️ Unconfirmed | 562e415 fix applied, next cron = first test |
| S2 Shaping docx | ✅ Confirmed | 43.4KB delivered May 6 |
| S3 Strategic docx | ⚠️ Unconfirmed | 562e415 fix applied, next cron = first test |
| Forensic Report | ⚠️ Unconfirmed | Root cause fixed, next cron = first test |
| S2-A (injection) | ✅ Live | Dedicated key, conf=0.90 |
| S2-B (coordination) | ❌ Gemini RPD | Needs separate Google account |
| S2-C (emotion) | ✅ Live | manipulation=0.9 |
| S2-D (adversary) | ✅ Live | 2-batch, consistency=0.85 |
| S2-E (legitimacy) | ✅ Live | 9 LOW actors |
| S2-F scoring | ✅ Live | scored=18, Cerebras primary |
| S2-F drift findings | ❌ sample_size constraint | 4 violations per run |
| S3-A (pattern) | ✅ Live | quality=0.8 |
| S3-B (history) | ❌ Gemini RPD | Shares key with S1 Lens 2 |
| S3-C (bias, weekly) | ✅ Cadence | Mon/Thu only |
| S3-D (long-term) | ✅ Cadence | Mon/Thu only |
| S3-E (selfcheck) | ❌ NO_OLLAMA | GitHub Actions has no localhost:11434 |
| entity_extract | ✅ Running | ~99K TPD on mail b — monitor |
| lens_entities | ✅ Growing | 3 state_office + 8+ people |

## GitHub Actions (last known state)

| Workflow | Last run | Status |
|---|---|---|
| Lens Collection #63 | Today 1:05 AM | ✅ |
| Lens Manager + Analyze #70 | Today 1:08 AM | ✅ 26m 39s |
| Lens S2-F Scoring #17 | Today 1:08 AM | ✅ 20m 59s |
| Lens GDELT Enrichment #58 | Today 2:11 AM | ✅ |
| Lens Forensic Report | Not seen in list | ❓ Unconfirmed |

---

## CRITICAL PATTERNS — DON'T REPEAT THESE

**LR-092**: After ANY patch touching multiple .py files:
```bash
for f in code/file1.py code/file2.py; do
    python -m py_compile $f && echo "OK: $f" || echo "BROKEN: $f"
done
```

**LR-094 awareness**: Every Groq key maps to exactly one account. If adding a new role, it needs its own account (create new Groq account, not just new key from existing account — they share TPD pools within the same org).

**Pattern Match Bias (GNI-R-233)**: When you find a bug in one file, scan ALL files from the same commit for the same pattern. Don't fix one and assume siblings are fine.

## CLEANUP FOR LENS-023 CLOSE

- [ ] Verify no test data in lens_entities / lens_entity_mentions
- [ ] Check for stray patch files in repo root
- [ ] Update `lens-DOC-001_diary.md` with LENS-023 entry
- [ ] Update `lens-DOC-004_status.md`
- [ ] Update `lens-DOC-002_rules.md` with LR-094 if not already added

---

**You've got this buddy. Verify first, fix what's failing, keep the machine running. 🤜**
