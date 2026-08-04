# NEXT SESSION BRIEF — LENS-031
Written 2026-08-04 ~07:20 ICT by Claude (Opus 5) at the LENS-030 close.
SUPERSEDES NEXT_SESSION_BRIEF_LENS030.md entirely.

## TRUST TAGS
[V] verified this session by bytes/logs (~90%)  [I] inferred (~50-60%)  [B] banked, unverified (~30-40%)

## PART 0 — FIRST ACTIONS (in this order)

1. [V] `git ls-remote https://github.com/fintelplan/project-lens.git main`. Last CODE commit is
   `898cc97` (CC-25); HEAD will be this brief's own docs commit or later. A close doc can never
   name its own SHA -- record what ls-remote says and move on (LR-104).
2. [V] **CERT CC-24** on the newest Manager+Analyze run:
   - GREEN = 16 rows not 20 in `lens_reports` for the wave window
   - GREEN = the ORCH table shows FOUR DIFFERENT qualities (before CC-24 all four were lens1's)
   - GREEN = NO `LENS_QUALITY marker absent` warning (its presence means the child edit did not reach production)
   - GREEN = no `Quality N below floor -- healing` unless a lens genuinely earns it
3. [V] **CERT CC-23** on the newest Intelligence Compendium (~05:56 UTC daily):
   - GREEN = the Telegram intro carries real counts and Office names
   - RED = the canned sentence "Project Lens Intelligence Compendium - full daily intelligence package."
4. [V] Then resume the sweep at STEP 1 below.

## THE CLIFF — 3 STRINGS, 2 POSITIONS LEFT (census began at 16)

### STEP 1 — entity_extract (fixture READY, just probe and wire)
- [V] `python probe_lens_models.py --role entity_extract --candidate primary --trials 3`
- [V] Dry run already green: prompt 3,324 chars (system 1,160 + user 2,164), longest of 48 eligible
      bodies is 2,000 chars (the corpus caps at exactly 2,000, so the 3,000 truncation never fires),
      fit_max_tokens 1600 [ceiling 8000], est call ~2,708 tokens on GROQ_API_KEY.
- [V] `requires_json=True` -> LR-107 applies: >60% completion budget = MARGINAL, not passing.
- [V] Then CC-27: wire `lens_entity_extract.py` to `wire("entity_extract")`, budget 600 -> 1600,
      delete the silent `or GROQ_API_KEY` at :189.
- [V] KEY IS SETTLED: only `lens-manage-analyze.yml` supplies GROQ_S2_API_KEY, and extraction runs in
      the COLLECTION pipeline (via fetch_text.py:339) where it is unset -> the `or` already resolves to
      GROQ_API_KEY, which is exactly what the registry says. Deleting the `or` is safe.
- [V] TPD is fine: Groq gives gpt-oss-120b 200,000 TPD, 92-93.5% headroom. (The old 97,635/100,000
      figure was llama-3.3-70b's ceiling -- a DIFFERENT model.)

### STEP 2 — the AI-5 pair (last position; needs a ruling first)
- [V] `lens_manager.py:174` and `lens_orchestrator.py:174`, plus the two log literals
      `lens_manager.py:290` and `lens_orchestrator.py:264` (`print('AI 5 verdict (llama-3.3-70b):')`).
      The literals MUST ship in the same commit or the fix births a fresh lying log line.
- [V] **THE TWO PROMPTS HAVE DRIFTED -- they are VARIANTS, not duplicates.**
      manager: 3-line system, user includes `minutes_since_last`, "List any concerns",
               "Suggest next safe run time if WARN or STOP".
      orchestrator: 1-line compressed system, user OMITS all three.
- [V] **RULING NEEDED: unify into one shared builder, or keep both with two fixtures.**
      Unifying gives the orchestrator's verdict three fields it does not currently see -- an
      ANALYTICAL change. Keeping both is safer under deadline but leaves the LR-112 smell.
- [V] Both take a plain dict of scalars, both use module-level `GROQ_MANAGER_KEY`, both max_tokens=300
      (registry says 1600, note "was max_tokens=300 (starvation bomb)"), both temperature=0.1.
- [V] Prompt lift is CC-25-shaped: each system/user pair is a self-contained f-string.
- [B] `6df2e34` once renamed GROQ_MANAGER_API_KEY -> GROQ_MA_API_KEY across all files, so BOTH names
      have history. VERIFY which one resolves before wiring.

### ALREADY OUT OF SCOPE -- PROVEN, do not re-migrate
- [V] `lens_orchestrator.py:343-344` FALLBACKS x3 = DEAD CODE. `apply_playbook` computes
      `pb["fallback"]`; the healing loop consumes it only as `result.fallback_used=True` before
      re-running `run_single_lens(lens_id)`, which passes ONLY `--single-lens`.
- [V] `lens_framing_rubrics.py:68` = DORMANT. `lens-s2f-scoring.yml:75` pins S2F_PROVIDER "mistral";
      MODEL is returned only by the terminal groq branch (:415,:423), never reached.
- [V] `lens_regular_report.py:62` = DEAD INSIDE DEAD. `call_llm` sets `_FORCE_PROVIDER` and
      `get_llm_client()` NEVER READS IT, so the "fallback" re-invokes the provider that just failed.
      Plus a 71,839-char prompt (~16,000 real tokens) against Groq's 8,000 TPM.

## WHAT SHIPPED THIS SESSION (8 commits)
- [V] `38a2bd6` CC-19  lens1 qwen/qwen3-32b -> openai/gpt-oss-120b + registry note corrected
- [V] `2a3b163` CC-20  registry rows compendium_intro + regular_report, inert; MISTRAL_SMALL_LATEST
- [V] `21660a8` CC-21  pure move: compendium intro prompt -> INTRO_SYSTEM_PROMPT
- [V] `0e028a5` CC-22  fixtures compendium_intro + regular_report
- [V] `7d1ed93` CC-23  compendium wired to registry, budget 200 -> 1200, silent key fallback deleted
- [V] `c9120ce` CC-24  _parse_quality(out, lens_id) reads each lens's OWN score
- [V] `dbd7ec6` probe evidence banked (LR-115)
- [V] `898cc97` CC-25  pure move: entity_extract user message -> build_user_msg (byte-identical)

## INCIDENT S1-001 -- CLOSED
- [V] Lens 1 (Foundation, the canary's GCSP baseline) produced NOTHING from Jul 17 to Aug 3 while the
      system reported 4/4. The child ignores `--single-lens` and runs all four lenses per invocation;
      lens1's dead qwen primary failed, the child exited 0, and `_parse_quality` scraped lens2's score
      under every label. Pre-fix 12 rows/wave; post-fix 16. Canary now prints four distinct
      perspectives including [Foundation -- GCSP human rights]. Held two waves.
- [V] **THE INSTRUMENT CHANGED AT THE CC-19 BOUNDARY -- do not compare S1 quality or contamination
      across it.** `CROSS-LENS SIGNALS: 10 found` is the post-CC-19 baseline.

## STILL OPEN FROM S1-001 (not cliff work)
- [V] D2: the child ignores `--single-lens` -> a 4x invocation multiplier, untouched.
- [V] lens1 reads 9 POWER state-actor articles; lenses 2/3/4 read all 100. Measured at five budgets:
      ALL 79 state articles are POWER, and `trim_articles_to_budget` does `state[:max_articles]` first,
      so no budget under ~96 articles (~21,000 tokens vs 8,000 TPM) admits a single scored article.
      **A budget raise is cosmetic at ANY value.** Only a proportional trim fixes it -- an ANALYTICAL
      change to what the canary reads. This is an INSTRUMENT-VALIDITY problem: cross-lens agreement is
      only meaningful if the lenses read the same world.
- [V] `_parse_report_id` has CC-24's identical disease; left deliberately (metadata, drives nothing).
- [V] `capture_output=True` swallows the child's entire log, so S1 emits no wire evidence at all.

## NON-CLIFF DEFECT LIST (each its own commit)
- [V] FALLBACK SELECTION IS IMPLEMENTED, DELIVERY IS NOT -- two independent instances (orchestrator
      playbook, regular_report provider chain). AUDIT EVERY FALLBACK FOR DELIVERY.
- [V] `fit_max_tokens` has NO ceiling row for (mistral, mistral-small-latest): passes
      assert_model_known but absent from LIMITS. `_KNOWN_WIRE` and `LIMITS` are SEPARATE tables.
- [V] S2-F: three sources, three answers (registry cerebras+cloudflare / workflow mistral / code
      default groq). Its real wire model is not in the registry at all; S2-F never calls
      assert_model_known.
- [V] Mistral is FLOATING: registry pins mistral-small-2603, production sets mistral-small-latest.
- [V] Silent key fallback still live at `lens_framing_rubrics.py:412-421`.
- [V] The compendium's `sections[:2]` slice is defeated by its own 3000-char truncation (section 1
      alone is 11,342 chars), so Entities never reaches the intro synthesizer.
- [V] Gemini corpse pair (S2-B, S3-B on dead gemini-2.0-flash): two 429 ladders per wave, ~5 min of a
      21-min wave, `limit: 0` on three quota IDs. Google RETURNS a retryDelay and we ignore it.
- [V] TIMING_SYNC from S2-B has been top injection at exactly conf=0.95 for FOUR consecutive waves.
- [V] LOCAL TESTS: 2 failed / 181 passed, both stale fixtures in test_lens_response_guard
      (valid_s2e / valid_s2gap). The guard is ADVISORY in production and real responses PASS.
      Fix = rebuild those fixtures from captured production output. THEN wire pytest into CI.
- [V] **"Lens CI green" means py_compile + registry self-test + ONE test file run as a script.**
      The tests/ tree has never run in CI.

## S2-E CONFIDENCE -- OPEN, NOT A BLOCKER
- [V] Ratio, ten-wave means: 0.438 0.541 0.408 0.445 0.555 || 0.332 0.329 0.292 0.276 0.315.
      Clean level shift between Jul 30 eve and Jul 31 morn, ZERO overlap 5v5. No mechanism found.
      The response guard is advisory, so it is NOT the cause. DIRECTION IS UNKNOWN -- "lower" is not
      "worse" until someone reads how the ratio is computed. Log it, hold it.

## PROBE RESULTS BANKED
- [V] lens1 / groq gpt-oss-120b: 3/3 stop, 30/40/43%, prompt 1,684 tok / 7,596 chars = 4.51 chars/tok,
      reasoning 236/350/484.
- [V] compendium_intro: 3/3 stop, 23/18/18%, prompt 813 tok / 3,267 chars = 4.0 chars/tok, reasoning
      179/87/100 vs completions 280/213/219 -- all three exceed the old 200-token cap.
- [V] fit_max_tokens overestimates by ~50% using //3; measured is 4.0-4.5 chars/token and
      CONTENT-DEPENDENT. Do not treat one ratio as universal. This is CC-1d's evidence.
