
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

---
## LR-124 — A surviving anchor is not idempotence (LENS-030)
**Type**: Process | **Added**: LENS-030 | **Status**: RATIFIED
A patch whose ANCHOR survives its own replacement will re-apply on a second run.
Guard on the ABSENCE OF THE NEW CONTENT, not the presence of the anchor, and assume
every patch script may run twice. Earned when a close-doc patch duplicated a section.

---
## LR-125 — No unbounded loops in patch scripts (LENS-030)
**Type**: Process | **Added**: LENS-030 | **Status**: RATIFIED
Never put an unbounded `while` in a patch script. Use a bounded
`str.replace(old, new, count)` with an asserted match count of exactly 1.

---
## LR-126 — A close is not complete until the tree is clean (LENS-030)
**Type**: Process | **Added**: LENS-030 | **Status**: RATIFIED
`git status --short` must be EMPTY before a session is closed. Writing the brief is
not shipping the work. The LENS-030 close audit found CC-26 written, compiled and
dry-run twice -- and never committed.

---
## LR-127 — A fixture worst case has TWO axes (LENS-031)
**Type**: Process | **Added**: LENS-031 | **Status**: RATIFIED
The prompt SIZE the position sends and the OUTPUT DEMAND it provokes. Maximising one
can zero out the other. Amends LR-117. Earned on entity_extract: ranking eligible
articles by length picked one with zero quoted experts and certified 6% of budget --
an instrument that was never loaded.

---
## LR-128 — Build instruments from the DATA, not from expectation (LENS-031)
**Type**: Discipline | **Added**: LENS-031 | **Status**: RATIFIED
Never build a grep, a cert criterion or a selector from what you expect the data to
look like. FIVE instances in one session: a log-grep from the source's print
statements; a cert criterion that ignored its own healing line; a density selector
that counted quote marks inside class= and href=; and a register grep written in a
format the register does not use -- while recording this very rule.
Generalises LR-122 beyond log-greps.

---
## LR-129 — Absent RED is not GREEN (LENS-031)
**Type**: Process | **Added**: LENS-031 | **Status**: RATIFIED
Verify the artifact under test is the one that actually ran (headSha) before reading
any cert. Two runs passed every grep while carrying pre-migration code.

---
## LR-130 — An empty result from an honest instrument is intelligence (LENS-031)
**Type**: Doctrine | **Added**: LENS-031 | **Status**: RATIFIED
Chasing "why isn't this producing?" is the gas-mask reflex. A populated table from a
loosened instrument is pretend-right bias: lens_entities held five quoted experts that
were an arXiv bibliography, and a full-looking table hid it for three months.
Canary doctrine, arm 3.

---
## LR-131 — The collection import path resolves LAZILY (LENS-031)
**Type**: Doctrine | **Added**: LENS-031 | **Status**: RATIFIED
Anything the Collection pipeline imports at module scope must resolve its registry
wiring INSIDE the call path. Collection is the canary's air supply and a module-scope
raise there takes down the whole wave. The certified module-scope pattern (S2-D,
compendium) is for STANDALONE SCRIPTS only. Canary doctrine, arm 2.

---
## LR-132 — When the fix needs a ruling, ship the visibility half (LENS-031)
**Type**: Process | **Added**: LENS-031 | **Status**: RATIFIED
Make the failure loud, keep control flow byte-for-byte unchanged, and say so in the
commit. A silent read failure that looks like an empty result can hide for months.

---
## LR-133 — A rule's SCOPE line is load-bearing (LENS-031)
**Type**: Doctrine | **Added**: LENS-031 | **Status**: RATIFIED
The canary doctrine read "before changing anything in System 1", so agents working on
Collection or Enrichment judged it irrelevant and skipped it for months. Scope a
doctrine by what it PROTECTS, not by where it was discovered.

---
## LR-134 — A chat-written BUILD spec is a LEAD, not evidence (LENS-032)
**Type**: Process | **Added**: LENS-032 | **Status**: RATIFIED
Every factual claim inside a spec -- line numbers, line endings, anchor uniqueness,
expected grep counts -- must be re-derived from bytes by the executor before any
edit. LENS-032 shipped five CC specs and three carried a factual error: line endings
recorded inverted, an anchor that appeared twice at the same indent, a gate count
computed against the wrong version of the file, and a scratch-script count that was
wrong in two successive documents. All were caught at BEV, none in execution. Scope:
every CC block, in both directions, forever. The BEV step is not a courtesy to the
spec -- it is the gate that actually works.

---
## LR-135 — Compute a gate against the file your change PRODUCES (LENS-032)
**Type**: Process | **Added**: LENS-032 | **Status**: RATIFIED
Not the file you read. CC-31's gate predicted grep -c "sambanova" -> 2; it read 5,
because the same spec's own replacement text wrote the word into three notes. The
gate's INTENT held, so the instrument was wrong rather than the change. Simulate the
post-change file before writing the number, or express the gate as its intent -- zero
fb_provider/fb_model/fb_key_env naming the dead provider -- instead of a raw count.
Amends LR-128, which covers instruments built from existing data; this covers
instruments built for data that does not exist yet.

---
## LR-136 — A spec must not state a number its reader can derive (LENS-032)
**Type**: Process | **Added**: LENS-032 | **Status**: RATIFIED
Write the COMMAND, not the answer. Line numbers, line endings, file counts, anchor
uniqueness, expected grep results and SHA ranges are all re-derived from bytes by the
executor at BEV time, so supplying my value adds nothing and risks everything: a stated
number is an ANCHOR that competes with the bytes. LENS-032 produced ~13 such errors
across nine specs -- line endings recorded inverted, a scratch-script count of 4 against
a real 21, a cron range corrected in the wrong direction, a gate that contradicted its
own spec, an unrunnable gate, and a SHA range covering four commits where nine were
listed. Every one was a number, address, size or line-ending stated with the confidence
of a measurement; none was a reasoning error. This is the AUTHOR-side duty; LR-134 is the
EXECUTOR-side duty, and a defence-only rule guarantees the error is always made. It does
not forbid numbers: a value MEASURED from a log or a byte read this session is evidence
and belongs in the spec -- a remembered one is an anchor and does not. Scope: every
chat->executor spec and every executor->chat receipt, both directions, scoped by what it
protects rather than where it was found (LR-133).

---
## LR-137 — Provenance records the WIRE, not the registry (LENS-032)
**Type**: Architecture | **Added**: LENS-032 | **Status**: RATIFIED
The registry is the source of truth for what a call site SHOULD use. A provenance field
must record what the WIRE actually did. LR-105 routes every model string through
`code/lens_models.py`, and read without this carve-out it covers provenance too. At CC-37
that reading would have stamped rows `mistral-small-2603` from the registry while the
request body posted `mistral-small-latest` -- writing a NEW mislabel inside the commit
that existed to remove mislabels, in the registry's own name. Errors dressed as
compliance are the hardest to see. The fix pattern: lift the wire value to a single
constant used for BOTH the request body and the provenance field, so the two cannot
drift. Where registry and wire disagree, that divergence is a SEPARATE defect with its
own commit -- never resolved by writing the aspiration into a record of what happened.
Scope: every DB write, log line or report field naming a model or provider. Recurs on
every fallback leg in the registry. Amends LR-105.

## LR-138 — An artifact is not verified by its own consistency (LENS-033)
Agreement happens in conversation; the write happens separately; nothing checks that
the write covers the agreement. git log --stat proves message-vs-contents. It cannot
prove contents-vs-agreement. Evidence: 657f5cf described three changes and shipped two;
fcde3ad shipped one of six agreed prompt changes and its message was ACCURATE. Before
committing a change agreed in conversation, grep ONE DISTINGUISHING PHRASE PER AGREED
ELEMENT and report the hits; absence of a hit means that element did not land. The
presence check is not enough on its own: 53fe0e3 created a DUPLICATE item number and a
presence-only checklist passed it, so also assert uniqueness whenever an ordered list
gains an item.
Scope: every document commit whose content was agreed in conversation before it was written.

## LR-139 — A guard whose expected value is hand-derived is the defect it exists to catch (LENS-034)
CC-48's patch script asserted a line delta of 13 that I had counted by hand. The real
delta was 18. The guard fired and nothing was written, so it failed safe -- but the
number was a banked estimate living inside a tool built to stop banked estimates, which
is root R3 reproduced in the instrument. Derive the expectation from the same data the
change is made from: sum over the edit list of new newlines minus old newlines. The same
applies to line-ending assertions: assert RELATIVE to the file's state read at the start
of the patch, never absolutely. This repo's autocrlf converts LF files to CRLF on the
next Windows checkout, so a hardcoded must-be-LF assert starts failing on a file nobody
touched.
Scope: every assertion inside a patch script.

## LR-140 — Never put a rollback command in the same message as the apply command (LENS-034)
CC-48 applied cleanly and compiled. The emergency restore one-liner offered in the same
message was then pasted along with everything else, reverting a good patch; the commit
was lost for a turn and the whole state had to be re-established. A rollback sitting in a
paste block is a rollback that gets run. Offer recovery separately, on request, and only
after the apply has been verified.
Scope: every patch or command block handed over for execution.

## LR-141 — Verify what ARRIVED, not what was fetched (LENS-034)
Mission Analyst's guard tests the FETCHED S1 list and passes when four reports come back.
MA #261 measured what was actually assembled: corrections=18560 s1=0 (0/4 reports)
s2=9341 (3/30). Zero analytical-lens input reached the synthesis prompt and the run
reported SUCCESS. A position that counts its inputs at fetch time and never at use time
cannot detect its own starvation, and the failure scales with upstream health -- more S2
findings produce more corrections, which exclude S1 more completely. Where a consumer
assembles inputs under a budget, log what was INCLUDED against what was AVAILABLE, and
treat zero inclusion of a required input as a failure rather than a quiet loop break.
Scope: every position that assembles bounded input from a larger fetched set.

## LR-142 — Read a function before unpacking it; never infer its shape from a sibling (LENS-036)
`wire()` returns four values, so `fallback()` was assumed to return four. It returns three
— `(provider, model, key_env)` — and its docstring says "or None". The module raised
ValueError at import and Mission Analyst was unimportable until corrected. The fix script
that finally worked called `fallback()` and ASSERTED both arity and contents before writing
a byte; that is the pattern to reuse. Corollary: `fallback()` returns bare `None` for a role
with no declared leg, so a module-scope unpack of it crashes at import for eight of the
twenty-four roles. Unpack defensively: `_FB = fallback(role) or (None, None, None)`.
Scope: every call to a function whose signature you have not read this session.

## LR-143 — Never predict grep counts for symbols you just authored (LENS-036)
Two miscounts in one session, both on freshly written code: `_s1_available` predicted 3,
actual 4; `FB_PROVIDER` predicted 4, actual 5. A predicted count is a hand-derived value
(LR-139) and hand-derived values are the ones that are wrong. Derive counts from the
patch's own `new_str`, not from memory of what you wrote. Both misses failed SAFE only
because the expected count was stated in advance, which is the entire argument for the
check. See LR-147 for the correct arithmetic.
Scope: every LR-138 verification block.

## LR-144 — AMENDS LR-119: the delivery audit, run repo-wide (LENS-036)
**LR-119 already said this** — "Fallback SELECTION is not fallback DELIVERY. Two
independent mechanisms in this repo compute the right fallback and never hand it over.
Audit every fallback for delivery." (LENS-030). It was live in the register and got
re-derived from scratch at LENS-036 because nobody opened the register. This entry
records what LR-119's audit actually found when finally run repo-wide, and is filed as
an amendment rather than a new rule so the duplication stays visible.
Every role in `lens_models.py` declared `fb_provider`/`fb_model`/`fb_key_env` from the
cliff migration onward, and a grep for those keys outside the registry returned NOTHING.
Not one call site had ever read a leg. A documented two-leg chain sat on paper while a
provider death took five positions down. Every redundancy claim needs a grep proving
reachability, and a leg that is reachable but points at a model already ruled out or
already dead is redundancy on paper too.
LR-120's corollary held too: every leg wired at LENS-036/037 logs a WARNING naming both
the exhausted primary and the leg being called, so no fallback in this repo is silent.
Scope: every claim that a position has a fallback, a retry path, or a second provider.

## LR-145 — Provider lifecycle mail is an operational input (LENS-036)
Cerebras announced the end of its free API tier on 2026-07-17 by email. It ended on
2026-08-17. Lens produced no intelligence for a full day and nobody knew until a query
happened to count rows. Record the EOL date in the registry note AND as a dated watch item
at ANNOUNCEMENT, not at death. Deprecation is weather (D-014), but weather has forecasts.
Scope: every provider email, deprecation notice, and model card change.

## LR-146 — `git diff` without `--no-pager` swallows every command after it (LENS-036)
In a multi-command block, `git diff` opens a pager, and everything after it in the block
silently does not run. Three commands were lost this way in one session — the same class
as the multi-block paste hazard, where absence of output looks like absence of a problem.
Always `git --no-pager diff` inside a block meant to be pasted.
Scope: every git command that can page — diff, log, show, blame.

## LR-147 — Derive grep counts as DELTAS from the edit list, and assert before writing (LENS-037)
LR-143 says do not predict counts by hand. This is how to compute them. A patch script that
accumulated the replacement text and counted totals predicted `fit_max_tokens` at 4 when the
true value was 3: the edit `    fit_max_tokens,` -> `    fallback,\n    fit_max_tokens,`
re-emits its own anchor, so the occurrence already counted in `pre` was counted twice. The
correct form is a delta sum:
`add = sum(new.count(sym) - old.count(sym) for old, new in edits)`, then assert
`pre + add == final` for every symbol, print a `pre / delta / pred / final` table, and put
the assert BEFORE `write_bytes` so a miss leaves the file untouched. This caught its error
before a byte moved — the first time in three sessions that happened. Note the trap: a
symbol whose `old` text contains zero occurrences passes under BOTH formulas, so a green
row is not evidence that the arithmetic is right.
Scope: every LR-078 patch script.

## LR-148 — Never put an angle-bracket placeholder in a block meant to be pasted (LENS-037)
`gh run view <RUN_ID> --log > /tmp/x.log` is not a template; bash reads `<RUN_ID>` as input
redirection, the command dies with "No such file or directory", and every command after it
in the block runs against a file that was never created — producing four confident-looking
"not found" lines and zero information. The session protocol's Notes already recorded this
as having happened twice; this was the third. Assign the value to a shell variable on its
own line, or derive the ID inside the command. Promoted from a protocol note to a rule
because a hazard carried unchanged into a third occurrence is a rule nobody registered.
STRONGER FORM, earned twice more in the same session: I then handed over `MA=32000000000   # replace this number` and `cp /path/to/downloads/...`. A dummy value and an invented path are placeholders too. FILE PLACEMENT AND ID SELECTION ARE HUMAN STEPS AND GET PROSE, NOT CODE. Only what the script can itself derive or verify belongs in a command block.
Scope: every command block handed over for execution.

## LR-149 — Chained `sed -e` rules are order-dependent; never stream-edit one instrument into another (LENS-037)
Deriving an S2-D probe from the S2-E probe with four chained `-e` rules: the first rule
rewrote the module name, which destroyed the anchor the third rule needed, so the alias
stayed `s2e` while pointing at the s2d module. It compiled and would have run. A measuring
instrument that silently mislabels what it measured is worse than no instrument, and this
is the same dual-source disease the registry cured. Write the second instrument fresh from
the production functions it is meant to exercise.
Scope: every probe, fixture, and verification script.

## LR-150 — A cert on mechanics alone is not a cert for a position that scores or extracts (LENS-037)
D-016 moved S2-E and S2-D to Cerebras on 2026-07-28. Both certs passed: 3/3 finish_reason
stop, valid JSON, budget_used 26-43%. Both positions then changed substantially and nobody
noticed for three weeks. S2-E's actors/row went 4.00 -> 8.50 the next day and stayed there;
S2-D's key_claims/row went 8.8 -> 26.6 while `narrative_consistency_score` — the headline
metric, stored as `confidence_score` — moved only 0.834 -> 0.853, so the obvious number
looked stable while the instrument changed. S2-D's `emotional_tone` also changed SHAPE, from
a repeated categorical label to a unique sentence per row, breaking any grouping built on it.
Mechanics prove a position RUNS. A band drawn from the position's own stored history proves
it still BEHAVES. Every migration cert states both, and the band comes from the DB — it is
free, it is months deep, and `probe_results.jsonl` cannot substitute because it banks
`content_head` and a sha, not the parsed response. Where the incumbent is already dead, say
plainly that no paired A/B is possible rather than dressing a mechanics probe as a
calibration one.
Scope: every provider migration, model swap, and budget change on a position that produces
scores, counts, or extractions.

## LR-151 — Never spell a string with `chr()` arithmetic to dodge quote nesting (LENS-037)
Twice in one session. Avoiding a nested single quote inside an f-string inside a patch-script
literal produced (a) a dead `X if False else Y` conditional whose unreachable branch called
`usage.get('')`, which py_compile accepted and which was caught only by re-reading the diff,
and (b) two dictionary keys spelled as `chr(107)+chr(101)+...`. Escaped single quotes inside
a double-quoted f-string work and were already in use two lines away. If the construct is
contorted enough to need explaining, that is the defect smell — build the string differently.
`chr(96) * 3` for a markdown fence remains the ONE legitimate use, because a literal backtick
in a patch body stalls the shell (LR-078 amendment).
Scope: every string built inside a patch script.
