
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
---

## LR-105 — Registry Law (LENS-028)

**Type**: Architecture | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

Every model string, key env var, output budget and provider limit flows from
`code/lens_models.py`. A model string typed anywhere else is a bug.

Call sites run `assert_model_known(PROVIDER, MODEL)` immediately before each
request and MAY raise there — the blast radius is that position only, and a
position dying loudly beats a position 404ing silently for ten days under green
checks. The alignment guard is the opposite: it verifies LOG-ONLY, plus a CI test
and a pre-flight Telegram line, and **the guard NEVER raises** (fail-safe
contract).

Lens example: qwen/qwen3-32b 404'd from 2026-07-17 and ran dead for ten days
behind green checkmarks. The registry exists so that cannot recur — and in
LENS-029 the same law caught `GROQ_S2DGCOM_API_KEY` typed at a call site while
the registry, the docstring and the error message all said `GROQ_S2_API_KEY`.

---

## LR-106 — Probe Before Push (LENS-028)

**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

No call-site migration ships until that role's probe is green on mechanics AND
content-fitness, on the role's OWN key (LR-094), with real-prompt fixtures.

Bank dying-model baselines while they still breathe — after a decommission date
the comparison is gone forever.

Lens example (LENS-029): the probe caught that every Groq `max_out` was sized for
a non-reasoning model. `s3a_patterns` returned `finish=length` with invalid JSON
three times, one trial emitting zero characters. Without the probe, four
positions would have shipped straight into truncation.

---

## LR-107 — Probe Headroom (LENS-028, D-017)

**Type**: Engineering | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

For JSON-output roles, greater than 60% completion-budget consumption in a probe
is MARGINAL, not passing.

A probe certifies the prompt **size** it held, not merely its shape (R-S80-2
extended). Truncated JSON has no partial value.

Lens example: S2-D probed 3/3 clean at 79% of budget and then truncated in
production on a prompt 312 characters larger, losing 42 of 60 articles. In
LENS-029 the same rule blocked `s2gap` at 63/65% and `s2a_injection` at 64/66%
until both budgets were raised and re-probed.

Note: the rule is scoped to JSON roles deliberately. `lens1` produces prose
(`requires_json: False`), so a partial response retains value and the same
percentage carries genuinely less risk.

---

## LR-108 — Derived Ceilings (LENS-028)

**Type**: Architecture | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

Per-request ceiling = `min(CTX, TPM)` per **(provider, model)** — never a
hardcoded constant, never provider-only.

The old "Groq 8,192" was never a context limit: Groq's context is 131,072 and TPM
was always the binding cap. "No TPM exists" (Cohere, Cloudflare) and "nobody has
checked" (Gemini) must never collapse into the same silence — hence `METER`. An
unresolved ceiling returns the cap and logs an ERROR naming the pair; it does not
raise.

Lens example: `analyze_lens_multi.py:777` still keys `TPM_LIMITS` by provider
alone with a `.get(provider, 10_000)` default — which cannot be right, because
TPM differs *within* Groq (llama-3.3-70b 12,000 vs gpt-oss-120b 8,000).

Caveat learned in LENS-029: CTX is a **size** and TPM is a **rate**. Bounding a
single request by a per-minute budget only works if you assume one request per
minute — true at RPM 1, false at RPM 5.

---

## LR-109 — Never Pollute the Record to Save Time (LENS-028)

**Type**: Process | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

Do not `workflow_dispatch` an analysis pipeline outside its schedule to speed a
certification. It re-analyses the same collection pool and writes a duplicate
cycle into the evidence base, skewing S2-F's 45-day windows invisibly for months.

Use a read-only smoke test or a probe instead.

Lens example: the temptation arose during the LENS-028 cert wait. The cost would
have been silent and permanent — a corrupted analytical record is not fixable by
a later commit.

---

## LR-110 — Tests Derive, Never Hardcode (LENS-028)

**Type**: Engineering | **Added**: LENS-028 | **Status**: RATIFIED | **Origin**: LENS-028

Derive test fixtures from the registry. A hardcoded assumption in a test silently
changes what the test measures after a migration, instead of failing honestly.

Lens example: five guard tests broke on the Cerebras migration because they
encoded "these positions share one group". The tests were not wrong about the
code — they were wrong about a fact that had changed underneath them, and they
had no way to notice.

---

## LR-111 — Derived Logs Only (LENS-029)

**Type**: Engineering | **Added**: LENS-029 | **Status**: RATIFIED | **Origin**: LENS-029

The truth order `runtime log > ledger > config > docstring` (R-S79-2) holds ONLY
for log lines **derived** from the value they report.

A hand-written log literal is a docstring wearing a log's clothes and ranks BELOW
config. Before trusting a log line as evidence of a wire value, grep the f-string
that emits it.

Lens example: `lens_s2b_coordination.py` logs "S2-A calling gemini-1.5-flash" at
line 203 and "(gemini-1.5-flash, 1M context)" at line 388, while `MODEL` at line
30 is `gemini-2.0-flash`. Both log strings are hardcoded literals. The log lied,
and a full session turn was spent believing it before the bytes corrected it.
A third instance surfaced the same day in S2-F: `[ENSEMBLE] Running primary:
qwen-3-235b on Cerebras`, immediately followed by `Using Cerebras provider (model:
gpt-oss-120b)`.

---

## LR-112 — Mechanisms Derive, Never Duplicate (LENS-029)

**Type**: Architecture | **Added**: LENS-029 | **Status**: RATIFIED | **Origin**: LENS-029

The code-side sibling of LR-110. A duplicated mechanism is a hardcoded assumption
with a heartbeat.

Lens example: `class TPMGuard` is defined **seven times** across the repo, and all
seven bodies differ — 22, 26, 33, 35, 49, 61 and 75 lines. Each position
instantiates its own copy, so seven private windows each model their own view of
ONE shared 30,000 TPM budget. The per-position-guard versus per-key-limit mismatch
is not a design oversight; this duplication IS its mechanism. Two of the seven
still hardcode `tpm_limit=6000`, which cost a measured 50-second stall in a live
wave.

The irony worth remembering: LENS-028 moved `_art_cost` to module level
specifically so the probe would import production's logic instead of mirroring it
— while the entire rate-limiting class sat duplicated seven times a hundred lines
above.

---

## LR-113 — Never Infer Provider Health From Low Consumption (LENS-029)

**Type**: Engineering | **Added**: LENS-029 | **Status**: RATIFIED | **Origin**: LENS-029

Failed calls consume no quota. A dead model therefore shows the LOWEST usage and
reads as the HEALTHIEST provider on the board.

Absence of usage and absence of capability are indistinguishable on a usage meter.
Read call outcomes, not quota headroom.

Lens example: the pre-flight logged *"Gemini is OK with minimal RPD usage, which
is a GO"* about `gemini-2.0-flash`, shut down since 2026-06-01. The 429 body read
`limit: 0` on all three quota metrics — a zero allocation, not an exhausted one.
The corpse looked healthiest precisely because it was dead.

---

## LR-114 — Measure Every Position, Never Project From a Ratio (LENS-029)

**Type**: Process | **Added**: LENS-029 | **Status**: RATIFIED | **Origin**: LENS-029

Two data points make a ratio, not a law. Probe every position on its own real
prompt before sizing its budget.

Lens example: `s2gap` and `s2a_injection` both showed roughly a 4.5x completion
increase moving from llama-3.3-70b to gpt-oss-120b, because gpt-oss is a reasoning
model and Groq folds reasoning into `completion_tokens` with no separate field.
Projecting that ratio onto `lens1` predicted it needed ~2,900 tokens. Measured, it
peaks at ~1,104 and passes unchanged at 2,400. The same projection would also have
missed that `s3a_patterns` needed a different **provider** entirely, not a bigger
budget.

---

## LR-115 — Perishable Evidence Commits Immediately (LENS-029)

**Type**: Process | **Added**: LENS-029 | **Status**: RATIFIED | **Origin**: LENS-029

Evidence that cannot be re-created gets its own commit the moment it exists — not
at the end of the arc.

Lens example: the llama-3.3-70b baselines are unrepeatable after 2026-08-16. After
their probe run they sat modified-but-unstaged in `probe_results.jsonl`, existing
only on one local disk. Bank first, continue second.

---

## LR-116 — A Key Verified Locally Is Not Verified in CI (LENS-030)

**Type**: Process | **Added**: LENS-030 | **Status**: RATIFIED | **Origin**: LENS-030

Local `.env` and the CI secret store are **independent stores that diverge without
warning**. A probe proves a key works *where the probe ran* — nothing more.

Before moving a production position onto a different key env var, verify that key
in the environment that will actually use it. If that is not possible, accept
explicitly that the first live wave IS the test, and watch it.

Consensus among static sources is not verification. A registry row, a docstring
and an error message can all agree and all be describing something that no longer
exists in the environment that matters.

Lens example: CC-10 moved `s2gap` from `GROQ_S2DGCOM_API_KEY` to
`GROQ_S2_API_KEY` because the registry, the file's line-15 docstring and its
`RuntimeError` message all named the latter. Local probes passed 3/3 twice. In the
first production wave the position returned `status=ANALYSIS_FAILED` after three
attempts and three HTTP 401s — the Actions secret of that name was stale, last
updated three months prior against DGCOM's two. Reverted in `03461b7`.

Corollary worth keeping: the failure was caught on its first wave only because
CC-8's `ALARM ... TPD check is BLIND` line had shipped six hours earlier. Before
that instrumentation the pre-flight would have read its `999999` default and the
position would have failed silently for days. **Instrumentation added for one
reason caught a different mistake entirely — which is the argument for adding it
before you know what it will catch.**


## LENS-030 (2026-08-03/04) -- earned rules
LR-117  Fixtures select the WORST CASE the position actually sends, not the first sample that
        qualifies. A probe certifies the prompt SIZE it held (LR-107); certifying at 449 chars and
        shipping against a 3,000-char cap is how S3-A's 939-token collapse happened.
LR-118  Where a fail-safe guard wraps the call site, put assert_model_known OUTSIDE it. Inside, the
        guard swallows the raise the assert exists to produce.
LR-119  Fallback SELECTION is not fallback DELIVERY. Two independent mechanisms in this repo compute
        the right fallback and never hand it over. Audit every fallback for delivery.
LR-120  A silent fallback hides which path ran and is forbidden; a LOUD fallback that logs a warning
        is acceptable and often correct.
LR-121  Before citing a banked number, verify it describes the same thing you are measuring. Two
        false alarms this session came from a baseline mismatched to its subject.
LR-122  Log-grep patterns come from the LOG, not from the source's print statements. A zero-match
        grep indicts the pattern first and the world second.
LR-123  "CI green" is only as broad as what CI actually runs. Read the workflow before citing it.
