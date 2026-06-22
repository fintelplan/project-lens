
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
    5. Check pg_tables.rowsecurity for all lens% tables; flag any
       rowsecurity=false, enable + canary-verify before closing. (LR-100)
    6. Resolve all flags before closing the checkpoint session.
Checkpoint sessions: LENS-027, LENS-032, LENS-037, LENS-042...
Due next: LENS-027.

---

## LR-100 — RLS Does Not Inherit a Sweep (LENS-027)
**Type**: Process | **Added**: LENS-027 | **Status**: RATIFIED
New Supabase tables start RLS OFF (anon-exposed). A one-time sweep does not
protect tables added afterward — each new table must be locked at creation.
(a) ENABLE ROW LEVEL SECURITY on every new table at creation, matching the
default-deny pattern (RLS on, no policy, service key bypasses).
(b) Add an RLS-flag check to the LR-090 schema checkpoint: query
pg_tables.rowsecurity for all lens% tables, flag any false.
Origin: S34 swept 19 tables; 6 added later sat anon-exposed (~184k rows) until
the June 12 Supabase advisory. Fixed this session: 17 tables RLS-enabled,
canary-verified.
Rule: On every new Supabase table:
    1. ALTER TABLE <t> ENABLE ROW LEVEL SECURITY;  (at creation, default-deny)
    2. At each LR-090 checkpoint, also run:
       SELECT tablename, rowsecurity FROM pg_tables
       WHERE schemaname='public' AND tablename LIKE 'lens%';
    3. Flag any rowsecurity=false → enable + canary-verify (service R/W OK,
       anon read blocked) before closing the checkpoint.

---

## LR-101 — Trust Calibration (LENS-028)
**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: GNI-S46
Trust is a function of verified-by-the-current-model-in-detail, NOT a function
of runs-fine-routinely. "It's been green for weeks" is not evidence of
correctness — it is evidence that nothing has forced the bug to surface.
Confidence bands (calibrate every claim against these):
    - Claude-verified THIS session, read in detail:  ~90-95% (never 100%)
    - Unverified reasoning / plausible inference:     ~50-60%
    - Earlier-session or earlier-model unread code:   ~30-40%
    - Memory summaries / recollection:                ~40-50%
Guards do not get tenure. A protection that has "always worked" is re-earned,
not assumed — re-verify it like it shipped today.
Lens example: GDELT ran green for ~2 months while silently writing zeros; the
RLS tables "ran fine" the entire time they were anon-exposed (~184k rows). Both
ran fine routinely AND were broken. Routine success masked both failures.

---

## LR-102 — Model-Change Re-Audit Ritual (LENS-028)
**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: GNI-S46
On ANY model change — a new session OR a new model version — prior verifications
partially reset. The new model did not perform the old sign-offs and cannot
inherit their confidence. A version upgrade is therefore a free, high-value
re-audit opportunity, not a reason to assume continuity.
Rule: On a model change, before resuming work:
    1. Re-read the active guards, aggregators, and any code about to be trusted
       with FRESH eyes — do not lean on the prior model's conclusions.
    2. Downgrade trust on anything signed off by the previous model/session to
       the "earlier-model unread code" band (~30-40%, per LR-101) until re-read.
    3. Only then resume; treat the re-audit as the first task, not overhead.
Lens example: when the daily-driver model changes, re-audit S2/S3 guards and the
S2-F aggregators before trusting prior sign-off — the green history was earned by
a different reasoner.

---

## LR-103 — Protections Are Guilty Until BEV'd (LENS-028)
**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: GNI-S46
Mechanism guesses ("what does this code do?") are usually right. Protection /
blast-radius / "it's already handled" guesses are usually too optimistic — that
is exactly where confident wrongness lives. Treat every claimed protection as
guilty until BEV-verified.
Before ANY fix, BEV must explicitly answer:
    "What is SUPPOSED to protect against the bad case here —
     and have I CONFIRMED, not assumed, that it actually does?"
Discipline:
    1. Move assumptions OUT of memory and INTO code as runtime assertions, so a
       false assumption fails loud instead of riding along silently.
    2. Tag every claim as verified-vs-assumed. Never let an assumed claim wear
       the confidence of a verified one.
Lens example (this weekend): "logs say the IP is blocked" was a lie, BEV'd false
via curl; "dead file, safe to delete" needed the "dead != worthless" read before
removal; RLS-enable was canary-proved (service R/W OK, anon read blocked) before
it was trusted. Every "already-handled" claim that got checked was weaker than it
sounded.

---

## LR-104 — Live-State Discipline (LENS-028)
**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: GNI-S46
Current work-state must live in a durable artifact (memory + registry), NOT only
in Claude's transient context. It must be precise enough that a fresh session or
a new model can resume WITHOUT re-derivation.
A LENS LIVE STATE entry, updated every close, records:
    1. SHIPPED + VERIFIED items, with commit hashes.
    2. The active BLOCKER and its exact code location.
    3. NEXT ACTIONS in order, with their gating dependencies.
    4. Corrections to prior assumptions (so they don't get re-trusted).
Begin session close at 80% context (LR-057) — do not wait until context is full,
or the durable handoff gets truncated.
Lens example: the false "ahead by 6" git-tracking scare came from trusting
transient state instead of a durable record. Keep the LENS LIVE STATE entry
current at every close — and record the fintelplan-scoped push URL there, since a
fresh session that runs plain `git push` hits a 403.
