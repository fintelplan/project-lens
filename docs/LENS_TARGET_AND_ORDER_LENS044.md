# LENS TARGET AND ORDER — LENS-044
Regenerated 2026-09-20 at the LENS-043 close. Supersedes LENS043 entirely.
Item numbers are NEW. Nothing carries its old number.

## THE TARGET (unchanged since LENS-036)
A daily intelligence pipeline that runs at $0/month, tells the truth about
its own failures, and delivers a complete Regular Report to Telegram.
Loud failure always beats silent success.

## THE CANON — READ BEFORE CHANGING ANYTHING
`CLAUDE.md` §0 (four-arm gas-mask test), §0a (charters), §0b (AI origin),
§0c (block contract) · `docs/LENS_FOUNDATIONS_LENS042.md` (every position's
charter, the philosophy behind it, and where the code has drifted).
A change that moves a position away from its charter is a STOP-and-ask.

## WHAT LENS-043 SETTLED — DO NOT RE-OPEN
- **The S2-F label lie is fixed.** `DetectionResult` carries `provider`,
  `model` and `ensemble_mode`; the cron reads them instead of hardcoding
  `"ensemble"`. `ensemble_mode` is True in the merge branch only.
- **A zero-scored S2-F run exits 1** (`scoring_exit_code`). A partially failed
  run still exits 0 -- see item 10, that gap is deliberate and recorded.
- **The Clarity aggregator's rejected writes are counted and make the step
  exit 1**, and the DB constraint now permits `sample_size >= 0`, so a total
  dissolution can be recorded.
- **All three S2-F aggregators have a logging handler.**
- **The `qwen-3-235b` log label is gone** -- the line prints the env model.
- **Lens CI has six gates**, including two new offline test files.
- **The S2-F detector is not broken.** Low confidence on most articles is the
  model answering `not_applicable`, exactly as the prompt asks. Do not
  re-litigate this without new evidence.

---

# THE MISSION — ITEM 1

## 1. THE STRUCTURAL DETECTOR IS STILL GONE, AND A CORPSE IS STILL CALLED
`detect_operations_ensemble` calls Cerebras first on every article. It answers
**402 on every call** -- 24 per run, 48 per day. The rows now tell the truth
about this, which is why it is no longer urgent and no longer hidden. What is
still missing is the thing the pair existed for.

`LENS-020_S2F_architecture_decision_v4.md` chose two models for
**complementary detection profiles**: qwen caught OP-024-029 structural
operations (the Sectarian Trap family, PHI-003's core concern) and gpt-oss
caught rhetorical ones. Cerebras is dead AND qwen is barred by the AI-origin
guideline, so the structural half has been absent since ~2026-08-17.

### 1.1 Two candidates, not three
LENS043's order named Mistral, Cohere and Groq gpt-oss. Two are ruled out:

- **Groq `gpt-oss-120b` -- ruled out on doctrine.** The surviving leg is
  `@cf/openai/gpt-oss-120b`. Same model, different host. Two legs agreeing
  would be one source counted twice -- the LENS-008 pretend-right-bias trap,
  self-inflicted. `GROQ_API_KEY` is also Lens 1's key (arm 2).
- **Cohere -- ruled out on arithmetic.** S2-F asks for ~48 calls/day, ~1,440 a
  month. The trial key allows **1,000 a month** and already carries Lens 3,
  S3-A and S3-C. It would kill the canary's Lens 3 in the first week.

That leaves **Mistral (`ministral-8b`, not `mistral-small`)** -- the European
lineage LENS-020 itself reached for -- and **Gemini
(`gemini-2.5-flash-lite`)**, which adds a fourth position to a provider whose
shutdown dates keep moving.

### 1.2 The probe is an arm-4 decision, and it has a right time
**Both candidate keys are canary air.** `MISTRAL_API_KEY` is Lens 4's key;
`GEMINI_API_KEY` is Lens 2's. `canary_air_guard` will refuse them without a
written reason in `LENS_ALLOW_CANARY_AIR`, and refusing is correct.

Run the matrix **at the start of a session, in a window with no wave running**
(`gh run list` first, never by the clock -- GitHub delays these crons 2.5-3.7h),
with the call budget written down before the first call. LENS-020 used articles
1, 3, 6, 7 with a cross-lab matrix; those fixtures are the comparison.
**Do not ship a second leg without that matrix** (LR-106/107).

### 1.3 Cloudflare capacity bounds the answer
Free allocation is 10,000 neurons/day, shared across models. Measured ~7.6k/day
with S2-F asking ~45-48 calls/day, and nine runs since Aug 31 wrote zero rows
(all 429) while showing a green tick. That last part is now caught by
`scoring_exit_code`. A second leg elsewhere also relieves this.

### DEFINITION OF DONE FOR ITEM 1
Either a second leg is shipped with a cross-lab matrix behind it and the
Cerebras call removed, or the decision not to have one is recorded with its
reason and the Cerebras call removed anyway. Either way, no position calls a
provider that has answered 402 for a month.

---

# ITEMS

## 2. S3-D CERT — MONDAY 2026-09-21
Carried unchanged from LENS043 item 3. CC-82 rewired S3-D to
mistral/ministral-8b at 8000 tokens with usage and `finish_reason` logged, no
fallback, the real leg recorded. Last saved row **2026-09-03**. If the Monday
wave does not produce `saved=YES`, read the log before changing anything.

## 3. CC-85 CERT — MONDAY 2026-09-21
Every `lens_operation_detections` row written Monday must carry
`provider='cloudflare'` and `ensemble_mode=false`. Any row still saying
`'ensemble'` means the cron did not pick up the change.

## 4. CC-87 CERT — MONDAY 2026-09-21
The Clarity step must log with `[S2F-CLARITY]` timestamps, end with
`Clarity writes: N ok`, and produce no `Clarity write failed`. Rows with
`sample_size` 0 or 1 should now exist. A RED S2-F step here means the
constraint reverted -- check `pg_get_constraintdef` before touching code.

## 5. ONE TPM GUARD SERVES TWO PROVIDERS
`_tpm_guard = TPMGuard(tpm_limit=6000)` is a single module-level object.
`log_usage` is called only on success, so Cerebras's 402s cost nothing, but
**Cloudflare's successful tokens throttle the next call whoever makes it** --
48 `TPM -- waiting 10s` lines, 480s, in a 17-minute run, most of them spent
waiting to call a provider that will 402 in 100ms. The label
`S2F-<lens>-cerebras` names whoever is about to call, which reads as a
Cerebras-specific limit and is not one. Cloudflare publishes no TPM at all
(LR-108: "no TPM exists" must never collapse into "nobody checked"). Key the
guard per provider, or drop it for the leg that has no such limit.

## 6. VERIFICATION'S PROVIDER CONFIG IS DECORATIVE
`lens-s2f-scoring.yml` gives the Verification step `S2F_PROVIDER: mistral`,
`MISTRAL_API_KEY` and `MISTRAL_MODEL: mistral-small-latest`.
`lens_s2f_verification_aggregator.py` imports only supabase and dotenv and
calls no model. The run contacted no Mistral host. Either wire it or delete the
env block -- "written but never wired" is the class that produced DET-DEAD and
`_FORCE_PROVIDER`. Note `mistral-small*` 429s class-wide, so wiring it as
written would fail immediately.

## 7. WATCH AND VERIFICATION STILL SWALLOW WRITE FAILURES
Both have the `except Exception: log.error(...)` shape that Clarity had, with
no counter and no exit code. Read both files in full first -- LENS-043 only
grepped them. Same fix shape as CC-87.

## 8. THE CANARY'S AIR AND INPUT (Lens 1)
Carried from LENS043 item 2, with one correction: `GROQ_API_KEY` is read by
**six** modules, not two -- `analyze_lens_multi` (Lens 1), `lens_entity_extract`
(Collection), `lens_manager`, `lens_orchestrator`, `lens_compendium`,
`lens_regular_report`. Groq limits are organization-level.
- **AIR:** Lens 1 failed `429_tpd` on the Sep 18 evening wave; Foundation rows
  are missing from many evening waves.
- **INPUT:** `trim_articles_to_budget` takes STATE-tier articles first, so
  Lens 1 reads 9 of 106 articles, all STATE, one domain, while the other three
  lenses read all 106. Cross-lens agreement is therefore an instrument of
  unknown validity (LENS-030 found this on Aug 3; CC-22 never shipped).
**Measure the consumption of all six FIRST.** A wider prompt on a saturated
bucket is arm 2.

## 9. WATCH ROWS ARE NEVER CLOSED
`get_active_watch_voices` filters `reviewed_by_operator=False`, and nothing
ever sets it True. 345 Watch rows are live, deduplicating to 2 unique voices,
and Clarity re-derives and re-writes the same findings every wave -- 636
developing plus 431 dissolved rows accumulated. Decide what closes a Watch row
(operator action, age, or a Clarity verdict) before item 5's row count grows
further. This also bears on `pg_database_size`, last measured at 374 MB.

## 10. THREE SMALLER TRUTHS, ALL RECORDED ON PURPOSE
- **A partial failure is still green.** `scoring_exit_code(1, 23)` returns 0,
  and `tests/test_s2f_provenance.py` asserts that deliberately so no future
  session can call it an oversight. Decide the threshold.
- **Clarity's "insufficient ops" path records nothing** (line ~152: log and
  `continue`). Not dissolved, not developing, absent. The other dissolution
  path writes a row; this one does not.
- **The S2-F workflow comment claims four schedules** (13:30, 17:30, 21:30,
  04:30 UTC) and the cron block has two (`30 13`, `30 1`).

## 11. CARRIED FORWARD UNCHANGED FROM LENS043
- **S3-A and S3-D read the OLDEST rows of their window** (`order asc` + limit),
  so the "90-day" run sees about three days of June. `fetch_s3_data` has the
  same shape. Design an even sample, state it in the prompt header, then ship.
- **Cohere is a shared, scarce, trial-key dependency** -- Lens 3, S3-A, S3-C on
  1,000 calls/month; the registry LIMITS row says `RPD: 1000`, wrong by 30x.
  Cohere is also the slowest leg (S3-C 394s).
- **Charter tensions F1** (DOC-006 Pattern 3 sanitization vs "S1 is never
  protected") **and F6** (S3-E local-only, SambaNova dead) -- unruled.
- **`lens_escalations` and `lens_run_meta` do not exist**, so every escalation
  including "SLA BREACHED" is discarded.
- **The registry is decorative for the lens engine** -- `analyze_lens_multi.LENSES`
  hardcodes the four lenses.
- **S3-B's primary is still a corpse** -- `gemini-2.0-flash`, 404 since
  2026-06-01, called three times per wave with 30s+60s sleeps.
- **Housekeeping:** `calibrate_rubric_*.py` (8 files) and old patch scripts at
  the repo root read API keys directly and `canary_air_guard` cannot see them;
  a May checkpoint row sits unclosed; D-022 retention never implemented.

---

# OPEN RULINGS
- **R10** — Claude's lean is RULE OUT, recorded at LENS-039 and unchanged.
  Never taken.
- **F1 / F6** — item 11.
- **Item 1.1 fork** — Mistral or Gemini, and only after the matrix exists.
- **Item 10's threshold** — what proportion of failures makes a run red.

# EARNED THIS SESSION (LR numbers unassigned — check the register first)
- **A verification step that cannot fail is not a gate.** CC-87 was shipped on
  a dry run that had parsed its own flag as a lens name and queried nothing.
  Before citing a check as evidence, confirm it would have gone RED had the
  change been wrong.
- **A grep pattern built from memory indicts itself first.** Two zero-match
  greps this session came from writing the pattern from the shape I expected
  rather than from a known-good line in the log. The second one nearly became
  a false finding.
- **Count before alarming.** "0 ops on every row" was raised from the head of a
  grep; the tail held three rows at conf 0.86.
