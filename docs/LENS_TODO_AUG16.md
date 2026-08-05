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

> **TIER 1 COMPLETE 2026-08-05** -- `9301945` CC-30, `b1fb954` CC-31,
> `c79f594` CC-32, `b8e920b` CC-33. CI green on `b8e920b`
> (run `30969263288`, conclusion `success`, verified via `gh run list`
> `--json databaseId,headSha,conclusion` -- not from a checkmark).

Total realistic time: **2–3 hours.** Do these first; they close the migration honestly.

### 1.1 — `lens_framing_rubrics.py:68` — the last string that can still fire

> **DONE `9301945` (CC-30).** Ruling #4 answered: the branch was DELETED,
> not swept. The silent `GROQ_S2F_API_KEY` -> `GROQ_API_KEY` fallback and
> the `:304` Cerebras lie went in the same commit, as specified below.
> `S2F_PROVIDER` now has NO default -- unset or unrecognised returns
> `(None, None, None)` and logs which case it was.

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

> **DONE `c79f594` (CC-32).** `resolve_candidate()` raises `ProbeError`
> when `today >= BASELINE_DEAD_ON` (2026-08-16), before the LR-094
> provider check. Verified as a threshold, not a blanket refusal:
> 2026-08-15 PASS, 2026-08-16 raise, 2027-01-01 raise.

Our measuring instrument breaks on Aug 16: `--candidate baseline` starts 404ing.
Every baseline that matters is already banked. Make the probe **announce that the
baseline model is decommissioned** rather than fire a doomed request.

### 1.3 — Registry fallback rows still naming dead SambaNova

> **DONE `b1fb954` (CC-31).**
>
> **CORRECTION 2026-08-05:** the paragraph below says TWO rows. There were
> **THREE** -- `lens1` (`:79-80`), `lens4` (`:100-101`) and
> **`s2a_injection`** (`:123-124`). `s2a_injection` is a LIVE CERTIFIED
> position whose call site CC-14 had already given a Mistral fallback, so
> that row was contradicting its own code. This undercount was copied into
> the CC-31 spec and had to be caught during the build.
>
> All three now read `mistral` / `MISTRAL_SMALL` / `MISTRAL_API_KEY`.
> `SAMBANOVA_LLAMA_33_70B` is KEPT with a tombstone comment -- deleting the
> constant is provider retirement, a separate purpose. A rider in
> `TestRegistryAlignment` now asserts the pair is absent from `_KNOWN_WIRE`.

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

> **DONE `b8e920b` (CC-33).** Now `"openai/gpt-oss-120b"`. Fixture string
> only; the guard's behaviour does not depend on the value.

Test-only, won't break production, but it will confuse the next grep. Sweep with 1.1.

**After Tier 1, run the ledger and expect only proven-dead hits:**
```bash
grep -rn "llama-3\.3-70b" --include=*.py --include=*.yml . | grep -v "/venv/"
```

**LEDGER AT `b8e920b` (measured 2026-08-05):** **25 hits**, down from 28 at
`e0d8568`. **Zero on any reachable path.** Executable, and all inert:

| Site | Why it cannot fire |
| --- | --- |
| `code/lens_orchestrator.py:394-395` | `FALLBACKS` dict -- dead code, no delivery path to the child |
| `code/lens_regular_report.py:62` | dead inside dead (`_FORCE_PROVIDER` written, never read) |
| `probe_lens_models.py:84` | now behind CC-32's date refusal |
| `tests/test_lens_quota_guard.py:441` | tombstone assertion (see below) |

`lens_framing_rubrics.py:68` is **gone** -- CC-30 deleted the branch.

**DO NOT SWEEP.** These are CORRECT statements, not stale strings:
`code/lens_models.py:423`, `code/lens_quota_guard.py:81`,
`tests/test_lens_quota_guard.py:441` (asserting the pair is ABSENT from
`PROVIDER_LIMITS` -- sweeping it deletes the guard), `probe_lens_models.py:28`,
and `probe_lens_models.py:1183` (the `--candidate` help text).

---

## TIER 2 — IMPORTANT (broken NOW, measurable cost, but each needs a ruling or a console read)

Total realistic time: **6–10 hours** if all four are attempted. Pick by what James wants.

### 2.1 — S2-B and S3-B: ALIVE on a Mistral fallback, blocked on a token measurement

> **PREMISE REPLACED 2026-08-05 (CC-39).** This section used to read
> "Gemini corpses ... dead since Jun 1". **That was wrong about the positions.**
> Only the **Gemini leg** has been dead since Jun 1. Both positions have completed
> every wave for two months on their **Mistral fallback**, and both write real
> findings to the DB.
>
> **Proof, from the log, not from memory:** MA **#253**, databaseId
> `30975849325`, headSha `d7b79c3`, conclusion `success`. Mistral-small handled
> S2-B's 200-article / **105,122-char** prompt in **~6 seconds**. The registry
> note's "needs long context, probe on flash-lite" is falsified by production.

What is actually true: each wave still burns **two 429 ladders** (~7.5 min of a
~21 min wave) climbing a decommissioned `gemini-2.0-flash` before the fallback
takes over, on `limit: 0` across three quota IDs, and **Google returns a
`retryDelay` that we ignore**, sleeping a fixed 30/60/90 instead. That is waste,
not outage — and it is a cost, not a blocker.

**THE BLOCKER IS NOT A CONSOLE READ. It is a token measurement**, and it arrives
with tonight's 21:17-23:02 wave. CC-38 (`c5080f8`) logs `prompt_chars` and the
provider's `prompt_tokens` on the **same line** for both positions. Nothing below
can be sized before that line exists: these bodies are HTML and tokenize worse
than prose, so a chars-per-token rule of thumb is not usable.

**The work, in this order:**

1. **A1 — add a `LIMITS` row for `mistral-small-latest`.** It is the model on the
   wire for **BOTH** positions and has **no row at all**, so `fit_max_tokens` and
   the quota guard cannot resolve a ceiling for the live production path. See 3.5.
2. **A2 — re-size `MAX_TOTAL_CHARS`** (currently `800000`, sized for Gemini's 1M
   context). **TPM 50,000 binds tighter than CTX**, and the registry's own comment
   tags CTX 128,000 as "VERIFY, not VERIFIED". Use the **measured** ratio.
3. **A3 — THEN promote Mistral to primary** on `s2b_coordination` and
   `s3b_history`, and delete the Gemini leg. Recovers ~7.5 min of every ~21 min
   wave, removes the Oct-16 cliff for both positions, and returns the entire
   Gemini quota to the canary. **A3 MUST NOT SHIP BEFORE A2** — promoting an
   unsized prompt onto the primary path is how a working position becomes a
   broken one.

**The console question is DEFERRED, NOT RETIRED.** Do `GEMINI_S2B_API_KEY` and
`GEMINI_S3B_API_KEY` belong to *different Google projects* than lens2's
`GEMINI_API_KEY`? RPD is 20/day **per PROJECT, not per key**, and lens2 — System
1's Physical Reality lens — already draws 11/20 from that pool. **Never spend the
canary's RPD on an S2 position.** It still matters because **`gemini-2.5-flash`
dies Oct 16** and lens2 runs on it — but it goes **moot for Manage+Analyze** the
moment A3 lands, because MA then stops calling Gemini at all.

> **DONE `fe1d9b5` (CC-37).** The hardcoded `gemini-1.5-flash` log literals this
> section used to demand shipped (LR-111): both positions now record the model
> that actually ran. Separately, `d7b79c3` (CC-36) removed the silent
> `or GEMINI_API_KEY` fallback that made both positions reachable to lens2's key.

### 2.2 — entity_extract call volume: 283 calls/run, **116 × HTTP 429**
Measured tonight in run `83856966602`: 399 Groq requests for 283 successful calls.
Every 429 was absorbed by the SDK's internal retry, so nothing failed — but the run
took **23m 42s vs #242's 12m 57s**.

**WEAKENED 2026-08-05 -- measured, do not repeat this claim as-is.** Run
`30703472197` ran **23m 35s on Aug 1, pre-CC-27**, and `30463762826` ran
**27m 15s on Jul 29**, also pre-CC-27. Across 14 scheduled collect runs
(Jul 29 - Aug 4) the split is by SLOT, not by commit: **evening runs are
volatile (7m53s - 27m15s), morning runs are stable (11m00s - 12m57s)**.
The `#242` in the line above is a MORNING run and `#243` is an EVENING one,
so the comparison that produced this inference was cross-slot.
**Compare matched slots or say nothing.**

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

### 2.5 — S2-F's mistral default is a floating alias
`S2F_PROVIDER=mistral` with `MISTRAL_MODEL` unset yields
**`mistral-medium-latest`** (`code/lens_framing_rubrics.py`) -- a floating
alias, on a tier with **no registry row**, against D-015.

Lower risk than the groq default CC-30 replaced -- a typo now fails closed
with `(None, None, None)` instead of firing at a dead model -- but it is
**the next landmine in that file**. Related to 3.5, which records the same
disease at `mistral-small-latest`.

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

> **CORRECTION 2026-08-05 (CC-39): THIS IS NOT DORMANT.** `mistral-small-latest`
> is the **live production path for two positions** — S2-B and S3-B have run on it
> every wave for two months (see 2.1). The missing `LIMITS` row is therefore not
> Tier-3 documentation debt; it is **work item A1**, and it gates the re-size that
> gates the promotion. Its placement under "DOCUMENTED, NOT THIS WINDOW" is a
> mis-prioritisation.

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
  **CORRECTION 2026-08-05 (measured):** there are **21** tracked
  `patch_*.py` scripts in the repo root, not two.
  `git ls-files | grep -c "^patch_.*\.py$"` -> **21**. Only these two carry
  a `llama-3.3-70b` string; the other 19 are the same LR-093 debt and are
  invisible to the cliff ledger.
- **Next cliff: `gemini-2.5-flash` dies Oct 16** and lens2 runs on it.

---

## RULINGS JAMES MUST GIVE (a new agent cannot decide these alone)

1. **2.1** — which Google project do the S2B/S3B Gemini keys belong to?
   **DEFERRED, not retired** (see 2.1): both positions are alive on Mistral, so
   nothing is blocked on this today, and it goes moot for Manage+Analyze if A3
   lands. It still decides the Oct-16 `gemini-2.5-flash` path for lens2.
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
  but actual starts run late by an amount that **depends on the SLOT**
  (measured over 26 scheduled runs, Jul 29 - Aug 4, both workflows):
  - **01:00 / 01:28 UTC slot: 2.8 - 3.6 h late** -> lands **10:48 - 12:02 ICT**
  - **13:00 / 13:28 UTC slot: 1.3 - 2.6 h late** -> lands **21:17 - 23:02 ICT**

  A single blanket range is what made this wrong twice. Do not collapse it.
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
- **LINE ENDINGS -- THIS WAS RECORDED BACKWARDS. Verified by bytes 2026-08-05:**
  `probe_lens_models.py` is **CRLF** (1206/1206). The pure-**LF** files are
  `tests/test_lens_write_guard.py` (0/312) and **both docs in `docs/`**.
  `code/` is CRLF. **Detect per file from the bytes, never assume** -- the
  inverted note cost real time in CC-32.
- Patch anchors must be pure ASCII; use `chr(8220)`/explicit `\xe2\x80\x94` bytes for
  the em dashes these files contain (LR-101).
- Guard on the NEW content's absence, not the anchor's presence — assume every patch
  script runs twice (LR-124).
- **Never carry a line number between documents; re-grep it.** `FALLBACKS` in
  `lens_orchestrator.py` is at **`:394-395`** and was at `:394` at `e0d8568`
  too -- `git log e0d8568..1bbb6f3 -- code/` is EMPTY, so the `:377-378` in the
  LENS-031 brief was wrong when written, not shifted afterwards.
- **`gh run list` `databaseId` is NOT the id in the LENS-031 brief's CERTS
  section** -- those are log/job ids. The run databaseIds are
  **`30922934732`** (CC-27, Lens Collection Pipeline) and **`30926516684`**
  (CC-29, Lens Manager + Analyze), both on `e0d8568`.
- **Anchor uniqueness is not free.** `if which == "baseline":` appears TWICE
  at the same indent in `probe_lens_models.py`. Assert `count == 1` and pick
  a different anchor when it fails.

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
