# LENS — WHAT'S LEFT, PRIORITIZED
Written 2026-08-04 ~23:45 ICT by Claude (Opus 5) at LENS-031, HEAD `e0d8568`.
For the next session's agent. Read `LENS_CONTRACT.md` and the canary doctrine first.

---

## STATUS — READ THIS BEFORE ANYTHING ELSE

**The literal target is DONE AND CERTIFIED.** Zero live `llama-3.3-70b` strings.
The Jul-30 census of 16 resolved as **11 migrated + 5 proven inert**, and both of
today's commits certified green on tonight's scheduled runs:

- **CC-27** (`5f2dac5`) — Collection #243, run `83856966602`: **283** wire lines,
  all `entity_extract calling groq/openai/gpt-oss-120b (prompt N chars, max_tokens 1600)`.
  Zero RED.
- **CC-29** (`e609647`) — Manage+Analyze #252, run `83867608643`:
  `[AI5] groq/openai/gpt-oss-120b prompt 293 chars, max_tokens 1600`, `Verdict: GO`,
  `FULL RUN APPROVED`. Zero RED, zero `llama-3.3-70b` in the entire log.
- **CC-24 held a third wave**: ORCH qualities 5.0 / 6.5 / 4.8 / 6.8 — four different values.

**Everything below is the gap between "no dead strings" and "Lens survives Aug 16
still producing valid intelligence."** Treating the first as the finish line is
exactly the pretend-right bias the doctrine names.

Twelve days of slack remain to Aug 16. Nothing here needs to be rushed.

---

## TIER 1 — URGENT (finish the migration; all small, all mechanical, no rulings needed)

Total realistic time: **2–3 hours.** Do these first; they close the migration honestly.

### 1.1 — `lens_framing_rubrics.py:68` — the last string that can still fire
Dormant **by accident, not design**: `lens-s2f-scoring.yml:75` pins
`S2F_PROVIDER: "mistral"`, so the terminal groq branch (`:415`, `:423`) is never
reached. One env-var change and it's a live 404 after Aug 16.

**Decide by reading first** — if the branch is genuinely unreachable, *deleting the
branch* beats renaming the string. An unreachable path carrying a dead model is a
landmine one config change from firing.

```bash
cd /c/school/lens
sed -n '60,75p' code/lens_framing_rubrics.py
sed -n '405,425p' code/lens_framing_rubrics.py
grep -n "S2F_PROVIDER" code/lens_framing_rubrics.py .github/workflows/lens-s2f-scoring.yml
```

Ship in the same commit: the **silent key fallback at `:412-421`**
(`GROQ_S2F_API_KEY` → `GROQ_API_KEY`) — same defect CC-10/14/23/27 each removed —
and the stale doc line at `:304` claiming Cerebras serves llama-3.3-70b.

### 1.2 — `probe_lens_models.py:84 BASELINE_MODEL = "llama-3.3-70b-versatile"`
Our measuring instrument breaks on Aug 16: `--candidate baseline` starts 404ing.
Every baseline that matters is already banked. Make the probe **announce that the
baseline model is decommissioned** rather than fire a doomed request.

### 1.3 — Registry fallback rows still naming dead SambaNova
`lens1` and `lens4` carry `fb_provider: sambanova`. **SambaNova is dead** — HTTP 402,
`balance_units 0`, since Jul 28.

**This is an UNFINISHED RULING, not a new one.** D-015 already ruled *all fallbacks
become mistral-small*; lens2's row shipped, lens1's and lens4's never did. Completing
a ruling is cheaper and safer than making one.

Why it matters even though nothing reads these rows today: when someone finally
wires the child to the registry (Tier 3.1), **these rows are what they would wire in** —
and today they would wire in a corpse.

```bash
cd /c/school/lens
grep -n "sambanova\|SAMBANOVA" code/lens_models.py
grep -n '"lens1"' -A 8 code/lens_models.py
grep -n '"lens4"' -A 8 code/lens_models.py
grep -n "MISTRAL_SMALL\|mistral-small" code/lens_models.py
```

Use `MISTRAL_SMALL` (the dated `mistral-small-2603`), **not** the `-latest` alias —
2603 has a LIMITS row and resolves a ceiling; the alias does not (see 3.5).

### 1.4 — `tests/test_lens_write_guard.py:35` hardcodes `"ai_model": "llama-3.3-70b"`
Test-only, won't break production, but it will confuse the next grep. Sweep with 1.1.

**After Tier 1, run the ledger and expect only proven-dead hits:**
```bash
grep -rn "llama-3\.3-70b" --include=*.py --include=*.yml . | grep -v "/venv/"
```

---

## TIER 2 — IMPORTANT (broken NOW, measurable cost, but each needs a ruling or a console read)

Total realistic time: **6–10 hours** if all four are attempted. Pick by what James wants.

### 2.1 — Gemini corpses: S2-B and S3-B on `gemini-2.0-flash`, dead since **Jun 1**
Two months. Two 429 ladders per wave, **~5 minutes of every 21-minute wave**,
`limit: 0` on three quota IDs, and **Google returns a `retryDelay` that we ignore**,
sleeping a fixed 30/60/90 instead. `lens_s2b_coordination.py:30`,
`lens_s3b_truehistory.py:30`. Registry is already ahead — it points both at
`gemini-2.5-flash-lite`. Textbook LR-105.

**⚠️ BLOCKED ON A CONSOLE READ, AND THE HAZARD IS A DOCTRINE ONE.**
Gemini RPD is **20/day per PROJECT, not per key** (Google's own doc). lens2 —
System 1's Physical Reality lens — already draws 11/20 from that pool. Migrating two
more positions onto it could **starve the canary to fix two S2 positions**. That is
gas-mask arm 2: taking the canary's air.

**James must answer first:** do `GEMINI_S2B_API_KEY` and `GEMINI_S3B_API_KEY` belong
to *different Google projects* than lens2's `GEMINI_API_KEY`? (aistudio.google.com →
the project each key belongs to.) Two other idle Free-tier projects exist
(`gen-lang-client-0026461991`, `-0697867306`).

- **Different projects** → migrate; the RPD pools are separate and this is safe.
- **Same project** → do NOT migrate onto 2.5-flash-lite. Move them to Mistral or
  Cerebras instead, or leave them dead until a project is provisioned. **Never spend
  the canary's RPD on an S2 position.**

Ship the **hardcoded log literals at `lens_s2b_coordination.py:203` and `:388`** in
the same commit — they print "gemini-1.5-flash" and are lies (LR-111).

### 2.2 — entity_extract call volume: 283 calls/run, **116 × HTTP 429**
Measured tonight in run `83856966602`: 399 Groq requests for 283 successful calls.
Every 429 was absorbed by the SDK's internal retry, so nothing failed — but the run
took **23m 42s vs #242's 12m 57s**.

**Partly our doing, and the record must say so.** Groq's limiter counts *requested*
tokens (prompt + max_tokens). CC-27 raised max_tokens 600 → 1600, taking each call
from ~1,170 to ~2,170 requested against an **8,000 TPM** ceiling — from ~6.8 calls/min
to ~3.7, while the run fires ~12.

**DO NOT REVERT THE BUDGET.** That trades a throughput problem for a starvation one.
The root cause is call *volume*: `MIN_BODY_FOR_LLM = 300` gates on **raw chars
including HTML markup**, and the median article carries ~**111 chars of visible text
inside 2,000 chars of markup**. 241 of 400 articles (60%) clear a gate they should
not.

**Fix: gate on visible text, not markup.** This also fixes 3.7 (the arXiv
contamination) — they are one defect wearing two faces.

**⚠️ Doctrine check before touching it:** this changes what gets *enriched*, never
what gets *collected*. Repair the EXTRACTOR. **Never drop a source from the collection
pool to tidy a downstream table** — that is the gas mask.

**Still open, verify before alarming (LR-121): TPD.** ~283 accepted calls × ~670 real
tokens ≈ 190,000 per run × 2 runs/day against a banked 200,000 TPD for gpt-oss-120b.
That banked "92–93.5% headroom" was never computed against a 283-call volume, and TPD
readings are time-of-day dependent (the 97,635/100,000 reading was taken 7 minutes
before the midnight-Pacific reset). **Read `x-ratelimit-remaining-tokens-day` from a
live Groq response before treating this as a defect.**

### 2.3 — The budget counters that have never counted
`get_runs_today` and `get_gemini_calls_today` build a filter from `.isoformat()`,
which emits `+00:00`; `_sb_get` pastes it raw into a query string where `+` decodes as
a space. PostgREST returns **HTTP 400 / 22007**. Curl-verified: the same filter with
`Z` returns 200.

**Consequence: neither the `DAILY_BUDGET=2` runaway stop nor the Gemini RPD guard has
ever been able to fire.** `e0d8568` made the failure loud (both 400 lines now print in
production) but deliberately did not fix it.

**⚠️ DO NOT FIX THE FORMAT ALONE.** `lens_pipeline_runs` is written by
`fetch_text.py:240` (**Collection**) and by the dead `lens_manager.py` — **nothing in
Manage+Analyze writes it.** Correcting the timestamp would arm `DAILY_BUDGET` against
the *wrong pipeline's* run count, which is worse than dormant.

**Needs James's ruling:** should Manage+Analyze have its own run counter, or should
`DAILY_BUDGET` be retired? Then fix format and cross-wire together, in one commit,
with a shared helper (LR-112: two consumers, one mechanism) emitting
`strftime("%Y-%m-%dT%H:%M:%SZ")`.

**Related, same file:** `check_gemini` **never contacts Gemini** — it is arithmetic on
that dead counter (`rem = 20 - calls - 2`). AI-5 has printed `OK (0/20 RPD used)` into
every pre-flight while S2-B and S3-B hammered a decommissioned Gemini model. A budget
counter wearing a health check's label.

### 2.4 — Make "CI green" mean something
**"Lens CI green" currently means py_compile + registry self-test + ONE test file run
as a script.** The `tests/` tree has never run in CI (LR-123). Locally: **2 failed /
181 passed**, both stale fixtures in `test_lens_response_guard` (`valid_s2e`,
`valid_s2gap`). The guard is advisory in production and real responses pass.

Rebuild those two fixtures from captured production output, **then** wire pytest into
CI. Doing it in that order matters — wiring first turns CI red on a known-benign
failure and trains everyone to ignore it.

---

## TIER 3 — DOCUMENTED, NOT THIS WINDOW

Real defects, all banked with evidence. None threaten Aug 16.

### 3.1 — **System 1 has no working rescue at all** ← the biggest one
`analyze_lens_multi.py` (1,332 lines, CRLF, **frozen since `7b968d4` 2026-05-22**) has
**zero registry awareness** — the grep for `lens_models|wire(|fit_max_tokens|
assert_model_known` returns nothing. Four providers, four hand-rolled clients, not one
`assert_model_known`. The parent's `FALLBACKS` dict has **no delivery path** to the
child (`run_single_lens` passes only `--single-lens`). Lens 4's in-file fallback is
dead SambaNova. **Four lenses, no rescue, today.**

**This is not a reliability issue — it is an intelligence-validity one.** The doctrine
holds that epistemic diversity is a *security feature*. S1-001 proved what a missing
rescue does: the three survivors were Gemini plus `gpt-oss-120b` **on Cerebras twice**.
Two of three "independent" perspectives were the same model, same provider, same
articles. Agreement between them is one source counted twice — the LENS-008 trap,
self-inflicted.

**Give this a FRESH session, not a tired one.** It is the hardest file in the repo and
it touches the canary directly.

### 3.2 — lens1 reads 9 articles; lenses 2/3/4 read all 100
Measured at five budgets: **all 79 state articles are POWER**, and
`trim_articles_to_budget` does `state[:max_articles]` first, so no budget under ~96
articles admits a single scored article. **A budget raise is cosmetic at ANY value.**
Only a proportional trim fixes it — an analytical change to what the canary reads.
**Instrument-validity problem: cross-lens agreement is only meaningful if the lenses
read the same world.**

### 3.3 — D2: the child ignores `--single-lens` → every invocation runs all four
lenses, a **4× multiplier** on S1's entire API spend. Untouched since S1-001.
**Order hazard: do not fix the exit code before the model** (see the S1-001 record).

### 3.4 — `capture_output=True` swallows the child's whole log, so S1 emits **no wire
evidence at all**. That, more than the registry gap, is why S1 was invisible for
sixteen days.

### 3.5 — Mistral is floating + a LIMITS gap
Registry pins `mistral-small-2603`; production sets `mistral-small-latest`.
`fit_max_tokens` has **no ceiling row for (mistral, mistral-small-latest)** — it passes
`assert_model_known` but is absent from `LIMITS`. **`_KNOWN_WIRE` and `LIMITS` are
separate tables: known ≠ fittable.** 12 alias sites; D-015 forbids aliases.

### 3.6 — S2-F: three sources, three answers
Registry says cerebras+cloudflare, workflow says mistral, code default says groq. Its
real wire model **is not in the registry at all**, and S2-F never calls
`assert_model_known`.

### 3.7 — `lens_entities` is contaminated
`arXiv AI Papers` feeds paper abstracts to a news extractor; cited authors become
`quoted_expert` rows. The all-time top yield (`raw_article f65715ee`, 5 experts) is a
bibliography: Fishburn, Aleskerov, Bouyssou, Monjardet, Hansson — all
`context_snippet: null`. These surface in the Compendium's "Entities: 20 records" and
the brief's "Most active". **Fix at the extractor (2.2), never at collection.**

### 3.8 — Smaller, all evidenced
- CC-1d: `fit_max_tokens` uses `//3`; measured is **3.6–4.5 chars/token** and
  content-dependent. Overestimates by ~50%.
- Compendium `sections[:2]` is defeated by its own 3000-char truncation (section 1
  alone is 11,342 chars), so Entities never reaches the intro synthesizer.
- `_parse_report_id` has CC-24's identical disease — deliberately left (metadata only).
- S2-E confidence: clean level shift Jul 30 eve → Jul 31 morn, zero overlap 5v5, no
  mechanism found. **Direction is unknown — "lower" is not "worse."**
- Two committed scratch scripts in repo root: `patch_article4_provider.py`,
  `patch_cerebras_model.py` (LR-093).
- **Next cliff: `gemini-2.5-flash` dies Oct 16** and lens2 runs on it.

---

## RULINGS JAMES MUST GIVE (a new agent cannot decide these alone)

1. **2.1** — which Google project do the S2B/S3B Gemini keys belong to? Decides
   whether the corpse migration is safe or starves the canary.
2. **2.3** — should Manage+Analyze get its own run counter, or should `DAILY_BUDGET`
   be retired? Fixing the timestamp without this arms a gate on the wrong number.
3. **3.1** — when to open `analyze_lens_multi.py`, and whether the rescue is worth an
   analytical change to the canary.
4. **1.1** — delete the unreachable groq branch, or just sweep its string?

---

## HAZARDS THE NEXT AGENT WILL TRIP ON

- **`gh workflow run` fails: `HTTP 403 Must have admin rights`.** Manual dispatch must
  go through the GitHub web UI. `gh run list` and `gh run view --log` DO work.
- **Cron lines lie about timing.** Collect `0 1`/`0 13` UTC, manage-analyze `28 1`/`28 13`,
  but actual starts run **2.5–3.3 hours late** on the free tier.
- **ALWAYS check `headSha` before reading a cert log.** Two runs fooled me today: their
  greps came back empty and looked like a pass, but they carried pre-CC-27 code.
  **Absent RED is not GREEN.**
- **Build log-grep patterns from the LOG, never from the source's print statements**
  (LR-122). I broke this three times in one session, including in the session that
  minted it.
- **`capture_output=True`** means the S1 child's markers can never appear in the
  Actions log, however correct the pattern.
- **Fixtures have TWO worst-case axes** — prompt SIZE and output DEMAND. Maximising one
  can zero out the other (LR-117 amendment, earned on entity_extract).
- `probe_lens_models.py` is **LF**; everything in `code/` is **CRLF**. Detect per file.
- Patch anchors must be pure ASCII; use `chr(8220)`/explicit `\xe2\x80\x94` bytes for
  the em dashes these files contain (LR-101).
- Guard on the NEW content's absence, not the anchor's presence — assume every patch
  script runs twice (LR-124).

---

## SUGGESTED SEQUENCE FOR THE NEXT ~20 HOURS

**Block 1 (2–3h, no rulings needed):** all of Tier 1. Ends with a clean string ledger
and the migration honestly complete.

**Block 2 (2–3h):** 2.4 — rebuild the two stale fixtures, then wire pytest into CI. Do
this before the bigger fixes so everything after it is actually tested.

**Block 3 (3–4h, needs ruling #1):** 2.1 if the Gemini projects are separate;
otherwise 2.2 (the visible-text gate), which needs no ruling and removes both the
429 storm and the arXiv contamination.

**Block 4 (needs ruling #2):** 2.3.

**Leave Tier 3 alone.** Especially 3.1 — it deserves a session that starts fresh.

**And leave real slack.** Twelve days remain to Aug 16, and GNI is next. The migration
is done and certified; everything above makes it *safe*, not *finished*. Finishing it
tired is how S1-001 happened.
