
---

## LR-091 — LOCAL model testing protocol (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

When testing LOCAL models for S2-F rubric calibration or S3-E self-check:

1. **Provider setup**: `export S2F_PROVIDER=ollama && export OLLAMA_HOST=localhost:11434 && export OLLAMA_MODEL=<model_name>`
2. **LM Studio alternative**: use port 1234 instead of 11434 — same OpenAI-compatible API
3. **RAM constraint (James machine: 32GB)**: models >24GB will cause OOM. Safe limit: 20GB model size
4. **JSON quality gate**: run calibration script first. If `LLM_FAILED` or malformed JSON on 2+ runs — reject model regardless of benchmark claims
5. **Proven LOCAL models (Apr 2026)**: `ministral-3:8b` (6GB, best quality, MistralAI lineage) ✅
6. **Rejected LOCAL models**: `gemma4:e4b` — failed JSON quality gate ❌
7. **No rate limits locally** — can run full 6-lens calibration in one pass, unlike cloud providers
8. **S3-E air-gap requirement**: S3-E self-check MUST use LOCAL model (Ollama). Remote API defeats the epistemic independence purpose (W-010)



---

## LR-092 — Post-patch syntax verification on ALL affected files (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

After ANY patch that touches multiple files, run python -m py_compile on
ALL modified files before committing — not just the file you intended to fix.

Pattern that caused two production outages (S2-D + 3 S2F aggregators):
- Patch script found insertion point correctly in one file
- Same broken import pattern existed in sibling files (same LENS-021 origin)
- Only the primary file was syntax-checked
- All sibling files broke silently on next cron

Rule: After any patch, verify ALL modified Python files:
  python -m py_compile code/file1.py && echo OK
  python -m py_compile code/file2.py && echo OK
Never commit until ALL modified Python files pass py_compile.


---

## LR-093 — Explicit cleanup tracking in session close docs (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

Every session close doc must include a CLEANUP section listing:
- Test/smoke data to delete from production DB
- Stale references to remove from code/docs
- Temporary files to delete from repo root

These items are BLOCKING for the next session — not optional backlog.

Origin: Smoke test entity 'professor john smith' persisted in lens_entities
for 13 days (Apr 21 -> May 4) because it was noted but never added to
any explicit cleanup list. It appeared as 'most active entity' in every
Daily Brief during that period, polluting operator intelligence.


---

## LR-094 — Quota isolation for critical positions (LENS-022)

**Type**: Architecture | **Added**: LENS-022 | **Status**: RATIFIED

Any position designated CRITICAL (sys.exit(1) on failure) MUST have its own
dedicated API key from a separate provider account.

Origin: S2-A was designated critical but shared GROQ_S2_API_KEY with S2-GAP.
By second daily run, shared quota was depleted -> S2-A failed -> manage-analyze
exit(1) -> workflow_run for forensic report never fired -> 0 Opus docx for days.

Pattern: Same lesson as GNI S30 (LR-058), LENS-010 S2-A/E isolation, LENS-022
GEMINI_S2B_API_KEY. Same-account keys share quota. Critical positions need
guaranteed headroom = dedicated account.

Rule: Before designating any position as sys.exit(1) critical, verify:
1. It has its own API key from a separate account
2. No other position shares that key
3. The key is added to all relevant yml env sections

---

## LR-095 — HTTP Error Diagnosis Discipline (LENS-023)
**Type**: Process | **Added**: LENS-023 | **Status**: RATIFIED
Always log `r.text[:200]` on any HTTP error before diagnosing.
Status code alone tells you nothing — 400/401/403/429 mean different
things across Groq, Gemini, Cerebras, Mistral, Cohere, Supabase REST.
Origin: LENS-023 — wrong fix applied twice because diagnosis was from
status code only. Actual error was in r.text, not the status category.
Rule: First line of any HTTP error handler must be:
    print(f"Error {r.status_code}: {r.text[:200]}")
Never write a diagnosis before reading the response body.

---

## LR-096 — Blob Column Size Gate (LENS-023)
**Type**: Architecture | **Added**: LENS-023 | **Status**: RATIFIED
Never pass raw blob DB columns into AI prompts without a size check.
Check len(str(value)) first. If >1000 chars, extract metadata only.
Origin: LENS-023 — full article body / large JSON blob passed directly
into Groq prompt. Context ballooned, quota wasted, output degraded.
LENS stores full article text, S2 findings, S3 pattern data, S2-F
operation histories — any of these can be 50,000+ chars.
Rule: Before any DB column enters an AI prompt:
    if len(str(value)) > 1000:
        value = str(value)[:500] + "..."  # metadata only
Applies to: full_finding, article_body, operations, any jsonb column.

---

## LR-097 — yml Timeout Reality Check (LENS-023)
**Type**: Process | **Added**: LENS-023 | **Status**: RATIFIED
Before dismissing any operator concern about pipeline timeouts, read
the actual yml timeout-minutes value. Never cite platform maximums
without checking operator-set overrides.
Origin: LENS-023 — dismissed timeout concern citing "GitHub max=360min"
but lens-manage-analyze.yml had timeout-minutes: 35. Pipeline was being
killed at 35 min. Platform maximum was irrelevant.
Rule: When any timeout concern is raised:
    grep "timeout-minutes" .github/workflows/<relevant>.yml
The yml value is the real constraint. Platform maximum is a ceiling,
not the operational limit.

---

## LR-098 — pip-vs-Code Consistency Check (LENS-024)
**Type**: Process | **Added**: LENS-024 | **Status**: RATIFIED
When removing a package from pip install in yml, always verify no code
file still imports it before committing.
Origin: LENS-024 — mistralai removed from pip install. lens_s2c_emotion.py
still had "from mistralai.client import Mistral" on line 17. Pipeline
failed again at S2-C import after pip was "fixed." Two failures, one root.
Rule: Before any pip package removal commit:
    grep -rn "from <package>\|import <package>" code/
If any result found → fix those files FIRST, then remove from pip.
Only commit when pip install yml AND all code imports are consistent.
LR-092 (sibling check) applies to BOTH yml files AND code files.

## LR-099 — Env Var and Column Naming Consistency Check (LENS-026)
**Type**: Process | **Added**: LENS-026 | **Status**: RATIFIED
When adding a new environment variable or DB column, verify the name
matches exactly across: the .env file, GitHub Actions secrets, and
every code reference. Mismatches cause silent failures with no error
message pointing to the real cause.
Origin: LENS past sessions — SUPABASE_KEY vs SUPABASE_SERVICE_KEY caused
silent failures across multiple sessions. The code checked one name,
the secret was stored under another. Hours lost to tracing a one-word mismatch.
Rule: When adding any new env var or DB column:
    1. Check .env — does the name exist exactly as written?
    2. Check GitHub Actions secrets — does the secret name match exactly?
    3. grep code/ for the name — does every reference use the same name?
All three must match before committing. If in doubt, echo the var
in the workflow to confirm it is loaded before any code runs.

## LR-090 — 5-Session Schema Checkpoint (LENS-026)
**Type**: Process | **Added**: LENS-026 | **Status**: RATIFIED
Every 5 sessions, run a full schema cross-check to verify that DB columns
and code references have not drifted apart silently.
Origin: As Lens grows across sessions, columns get added in code but
forgotten in DB, or renamed in DB but not updated in code. These cause
silent failures that are hard to trace weeks later.
Rule: At every 5th session (LENS-027, LENS-032, LENS-037...):
    1. List all Supabase tables and columns (information_schema.columns)
    2. grep code/ for every column name referenced in code
    3. Flag: any column in code not in DB → missing column
    4. Flag: any column in DB not referenced in code → orphaned column
    5. Resolve all flags before closing the checkpoint session.
Checkpoint sessions: LENS-027, LENS-032, LENS-037, LENS-042...
Due next: LENS-027.
