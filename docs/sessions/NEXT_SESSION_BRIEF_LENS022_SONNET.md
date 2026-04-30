# Next Session Brief — LENS-022 (for Claude Sonnet 4.6 adaptive)

**Last commit**: `49c46f6`
**Repo**: github.com/fintelplan/project-lens, main, clean
**Brief written by**: Opus 4.7 at LENS-021 close, Apr 30 2026 ~16:30 Thai
**You are**: Claude Sonnet 4.6 adaptive, in a fresh session

---

## OPERATOR — JAMES MAVERICK ("Bro Alpha")

- Tone: warm informal ("my buddy") with engineering rigor underneath
- Every word costs message budget — be tight
- Cut preamble. Answer first. Justify only if asked
- Lettered options A/B/C with honest lean OR "I don't have enough to lean — your call"
- Short message after long Claude response = pause signal, re-examine
- "Move on as we can" = execute, don't recap
- "Where are we" = prioritized to-do list, not narrative
- Ask one question max per turn

## ENVIRONMENT

- Local: `C:/school/lens` (Windows Git Bash)
- Startup: `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`
- DB: Supabase (env vars in `.env`: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`)
- Repo: `github.com/fintelplan/project-lens`
- Operator email: `planfintel@gmail.com`

## HARD GATES (do not violate)

1. **BIRD-EYE** before any patch — read full state of related files (use `view`, `grep`)
2. **No recommendation** until evidence is shown
3. **One question max** per turn
4. **Schema/architecture decisions = L2** (propose, James approves)
5. **Ship-to-file patch** over bash heredoc on Git Bash Windows (LR-078 — heredocs corrupt)
6. **Verify schema with actual queries**, never guess column names

## WHAT JUST SHIPPED (LENS-021)

Bug: 3 S2-F aggregators silently dropped findings due to NOT NULL on `entity_id`.

Fix landed in commit `49c46f6`:
- `lens_entities` CHECK constraint expanded to include `state_office`
- 3 rows seeded: `trump_office`, `xi_office`, `khamenei_office`
- New file `code/lens_s2f_helpers.py` — `get_state_office_entity_id(client, lens)` cached lookup
- Patched: `lens_s2f_watch_aggregator.py`, `lens_s2f_clarity_aggregator.py`, `lens_s2f_verification_aggregator.py`

State office UUIDs (for reference, not code):
- `trump_office` → `b3d97b46-890a-42ca-88f5-c16b7f951805`
- `xi_office` → `9786a936-1559-425d-842d-fe1b0516d629`
- `khamenei_office` → `b36bc3f0-070b-4635-9444-3b43d25249f9`

---

## YOUR FIRST TASK — verify the fix worked overnight

Tonight's scheduled cron should fire at:
- `01:30 UTC May 1` = **08:30 Thai May 1** — S2-F scoring + Watch/Clarity/Verification
- `02:00 UTC May 1` = **09:00 Thai May 1** — Forensic Report (Opus, ~$0.46)
- `02:10 UTC May 1` = **09:10 Thai May 1** — Regular Report (Mistral)
- `02:30 UTC May 1` = **09:30 Thai May 1** — Compendium

Run this diagnostic when you start:

```bash
export $(grep -E 'SUPABASE_URL|SUPABASE_SERVICE_KEY' .env | xargs)
python - << 'EOF'
import os
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

# 1. Detection rows from tonight
r = sb.table("lens_operation_detections").select("state_actor_lens,operation_count,scored_at").order("scored_at",desc=True).limit(10).execute()
print(f"Detections (top 10): {len(r.data)}")
for row in r.data:
    print(f"  {row['state_actor_lens']} | {row['operation_count']} ops | {row['scored_at'][:16]}")

# 2. Drift findings — entity_id populated?
print()
r = sb.table("lens_drift_findings").select("entity_id,state_actor_lens,finding_phrasing,created_at").order("created_at",desc=True).limit(5).execute()
print(f"Findings: {len(r.data)}")
for row in r.data:
    eid = row['entity_id'][:8] if row['entity_id'] else 'NULL'
    print(f"  {row['state_actor_lens']} | entity_id={eid} | {row['finding_phrasing'][:80]}")
EOF
```

**Three branches:**
- Findings rows with `entity_id` populated (not NULL) → fix confirmed ✅, proceed to T1
- Findings rows with NULL `entity_id` → patch broken, debug
- 0 findings rows → check GitHub Actions for cron failure logs first
- 0 detections rows → cron didn't fire (check Actions UI)

Also verify: Forensic Report 02:00 UTC delivered docx to Telegram with non-empty Part B (drift findings section).

---

## TASK QUEUE (after T2 verified)

### IMPORTANT tier — Sonnet-zone

**T1 — Opus Report rewire** (~1 session)
Currently reads pre-processed DB. Should run S2+MA+S2F live like other free reports do. File: `code/lens_opus_report.py` (or similar — verify with grep). Risk: cost — Opus is paid (~$0.46/run), so don't break the once-daily 02:00 UTC cron.

**T3 — v4 steno ops calibration** (~1 session)
Catalog v4 (35 ops, file: `lens-OPS-001_catalog_v4_0.json`) added 6 steno ops (OP-030 to OP-034 plus OP-035). Hand-annotate Article 6 (1955 chars) against OP-030 to OP-034 manually. Compare with what the model detects. Article 6 was 5 ops on v3.1 — v4 expectation is 5→8-11 ops on steno-genre articles.

**T4 — Guard system audit** (~1 session)
Walk all workflow YML files, confirm guard wiring consistent across S2-A, S2-E, S2-GAP, MA, S3-A. The F1 regression of LENS-014 (LR-074: QuotaResult list iteration pattern) needs verification it hasn't drifted back. Use `grep -rn "guard_check_with_fallback" code/`.

**T5 — Wire Mistral-small into S2-A** (~0.5 session)
Mistral provider branch already exists (commit `da862ae`). S2-A injection detection currently uses Groq llama-3.3-70b. Add Mistral as alternative. File: `code/lens_s2a_injection.py` (or similar — verify).

**T6 — LR for LOCAL model testing protocol** (~0.5 session)
Codify lessons from gemma4/gpt-oss/ministral-3:8b testing on Ollama. Add to `lens-DOC-002_rules.md`. Will become LR-088 or LR-089 (check existing registry).

**T7 — Entity Intelligence verification** (~0.5 session)
Separate from LENS-021. Verify `lens_entities` is being populated by `lens_entity_extract.py` for authors/experts during article ingestion. Currently 1 row visible (Apr 21, journalist test). Should be growing daily.

### DEFERRED (do not pull in unless James asks)
- S4-B build: needs 90 days predictions, ~July 2026
- Direction A / web app: after S4-B
- Forensic Report paid: only manual when needed
- Node.js 20 deprecation: June 2026 deadline
- EST winter shift: November

---

## SESSION CLOSE PROTOCOL

When you hit ~80% session usage (LR-057), start closing:
1. Generate `SESSION_AUDIT_YYYY-MM-DD_SONNET_LENS022.md`
2. Generate `NEXT_SESSION_BRIEF_LENS023.md`
3. Update `lens-DOC-001_diary.md` with today's entry (1 paragraph)
4. Update `lens-DOC-004_status.md` with current state
5. Add any new LRs to `lens-DOC-002_rules.md`
6. Commit + push all session docs

Do not wait for the wall. 80% is the trigger.

---

## OPUS-SPECIFIC FAILURE PATTERNS (avoid these)

The previous session's Claude (Opus 4.7) overfit to project lore. Symptoms:
- Pattern Match Bias on familiar templates ("ALTER TABLE for NOT NULL bug")
- Verbose meta-narration ("now applying bird-eye view, step 1...")
- Multi-paragraph self-analysis when 2 sentences suffice
- Compound questions instead of one
- Premature leans when ground truth is absent

**Sonnet's natural style is closer to what the operator wants.** Be direct, be brief. When in doubt: shorter wins.

---

## REFERENCE — recent rule additions (LR-074 to LR-087)

- **LR-074**: QuotaResult list iteration pattern (LENS-014)
- **LR-075**: Integration test before guard contract ship
- **LR-076**: Evidence-based audits over memory-based claims
- **LR-077**: Pause-over-push past hour 25
- **LR-078**: Ship-to-file patch over bash heredoc on Git Bash
- **LR-080**: Write-then-verify via FK enforcement
- **LR-083**: "Pattern may be forming" phrasing for Watch findings (never "confirmed")
- **LR-085**: Bias claims need cross-lab evidence from 2+ different-lineage models
- **LR-086**: Free-tier ≠ production infrastructure
- **LR-087**: Tier-specific model selection (Watch=speed/recall, Verification=precision/ensemble)

---

**You've got this, my buddy. Read THIS conversation first, then the audit doc, then start with the diagnostic. Don't pull stale records.** 🤜
