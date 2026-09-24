# LENS TARGET AND ORDER — LENS-045
Regenerated 2026-09-21 at the LENS-044 close. Supersedes LENS044 entirely.
Item numbers are NEW. Nothing carries its old number.

## THE TARGET (unchanged since LENS-036)
A daily intelligence pipeline that runs at $0/month, tells the truth about
its own failures, and delivers a complete Regular Report to Telegram.
Loud failure always beats silent success.

## THE CANON — READ BEFORE CHANGING ANYTHING
`CLAUDE.md` §0 (four-arm gas-mask test), §0a (charters), §0b (AI origin),
§0c (block contract) · `docs/LENS_FOUNDATIONS_LENS042.md`.
A change that moves a position away from its charter is a STOP-and-ask.

## WHAT LENS-044 SETTLED — DO NOT RE-OPEN
- **CC-85 and CC-87 are certified** (rows name their leg; dissolutions of 0-1
  articles are written; the constraint is `>= 0`).
- **A zero-row S2-F run is RED, and no longer skips the aggregators** (CC-91).
- **Verification is arithmetic by charter.** It wants no LLM; the env block that
  pretended otherwise is deleted.
- **The S3-D truncation was a long answer, not a loop** (every key once, no
  repeated windows). Do not treat it as degenerate generation.
- **S3-D reads its whole window**, evenly IN TIME, past the 1000-row cap; a gap
  lends its place to the nearest real row; the 90-day prompt fits (item 3).
- **Cerebras is gone from S2-F** (Bro Alpha's ruling, LENS-044).
- **Mistral is $0/month** on the Free plan with a $10 included allowance.
- **Gemini is out as S2-F's second leg on the evidence**: RPD 20 per project,
  Lens 2 already at 16/20, and no 2.5-flash-lite row exists for this project.
  Reopen only with a console reading that shows otherwise.

---

# THE MISSION — ITEM 1

## 1. S2-F HAS ONE LEG, AND THAT LEG CANNOT CARRY A DAY
The dead leg is gone (CC-95). What remains is capacity and the missing
structural profile (LENS-020's two complementary detectors).

### 1.1 The numbers
- Cloudflare: ~292 neurons per call, 10,000 a day -> **~34 calls a day**.
  S2-F asks **48** (24 articles x 2 waves). The evening wave was dead on
  Sep 18 and Sep 20. **Confirmed in production on Sep 21:** 24 morning +
  15 evening = 39 calls, then `4006`; 9 of the evening's 24 went unscored.
- Every article calls every leg (`ENSEMBLE_LEGS`), so **adding a leg does NOT
  reduce Cloudflare's load**. The LENS044 order said it would; it was wrong.

### 1.2 RULING NEEDED FIRST — how the legs are called (Bro Alpha)
Before any matrix. Four shapes:
- **(i) every leg, every article** (today's shape) — Cloudflare still asks 48.
- **(ii) Mistral first, Cloudflare only when Mistral fails** — Cloudflare near
  zero; no ensemble on most articles.
- **(iii) alternate by wave** — morning = Cloudflare + Mistral (CF 24, fits 34);
  evening = Mistral alone. One real ensemble a day; the evening is never blind.
- **(iv) fewer articles** — `S2F_MAX_ARTICLES` 8 -> 5 = 30 calls/day, fits,
  stays $0, cuts coverage ~37%; no second leg.
Claude has no lean yet between (ii) and (iii); (i) is ruled out by arithmetic.

### 1.3 The second leg — lean Mistral `ministral-8b-2512`
Measured, not assumed: TPM 625,000, RPS 3.13, no RPD, $10/month allowance at
$0.44 used. S2-F at 48 calls/day ~ 1,440/month; at the observed ~$0.00136 per
request that is ~$2 of the $10. Standards applied at LENS-044:
- **IEC 61508 common-cause failure** — the old Cerebras+Cloudflare pair was
  ONE model (gpt-oss-120b) on two hosts: identical redundancy, not diverse.
  And Sep 20's failure was common-cause on the QUOTA axis. The second leg must
  differ in model lineage AND in quota pool. Mistral does both.
- **DORA Art. 29 concentration** — Mistral already carries S3-D, Lens 4, S2-C,
  S1-RPT and several fallbacks. Adding S2-F raises that concentration; record
  it as a known cost, and see item 11 (exit detector).
- `MISTRAL_API_KEY` is Lens 4's key. Arm 4 applies to the probe and, if (i) or
  (iii) is chosen, to production: S2-F would share Lens 4's per-model rate.
  Consider a separate Mistral organisation (LR-094 isolation) — Bro Alpha's call.

### 1.4 The matrix, when the ruling exists
LENS-020's fixtures are articles 1, 3, 6, 7 with a cross-lab matrix — **find
where they live first**; they were not located at LENS-044. Build the fixture
the S3-D way (capture production's own request; never a hand-written prompt).
Window: `gh run list` shows nothing running, never the clock. Budget written
before the first call. `--json-mode` only if production sends it.

### DEFINITION OF DONE FOR ITEM 1
The leg-calling shape is ruled; S2-F's daily call count fits its providers by
arithmetic; any second leg has a matrix behind it (LR-106/107); and no wave
runs blind without saying so.

---

# ITEMS

## 2. DONE AT LENS-044 — THE 2026-09-21 EVENING WAVE; ONE CERT STILL OWED
Run `35640858699` (18:49Z, 17m23s, green): `scored=15 skipped=0 failed=9`.
- **CC-95 CERTIFIED:** `api.cerebras.ai` 0 lines; `Running leg 1/1: cloudflare`
  x24; `Running primary` 0; TPMGuard `-cloudflare` x37, `-cerebras` 0.
- **CC-92 CERTIFIED on the Cloudflare path:** nine refusals, each in the
  provider's own words — `code 4006`, "you have used up your daily free
  allocation of 10,000 neurons". Account ID masked; leak scan 0.
- **CC-91 STILL OWED:** Stage 1 wrote 15 rows and exited 0, so the guarded
  steps were never exercised. Read the next wave whose Stage 1 fails; the four
  later steps must show success or failure, never skipped.

## 3. DONE AT LENS-044 — S3-D'S 90-DAY PROMPT WAS PROBED
One call, `S3D_PROBE_WINDOW_DAYS=90` (`d047b72`): 70,435 chars / 18,077 tokens
in, 7,682 out, **48%** of 16000, **121s** against 240, `stop`, 14 of 14 keys.
Mechanically ready for Thursday. Probed BEFORE CC-96, so Thursday's prompt
differs in which rows it samples, not in size (CC-96 dry run: 70,625 chars).
The harness had timed out at 180s — stricter than production — now 300s.
Residual risk: a timeout plus its retry is 500s against the Manage + Analyze
wall clock (40m41s on Monday).

## 4. CERT — THURSDAY'S S3-D 90-DAY RUN (CC-93/94/96)
Expect: `S1 window=90d: N reports in window, 90 sampled evenly in time
(~30 in the newest third), spanning ...`; `S2 window=90d:` with N well past
1000, 60 sampled, ~20 in the newest third; no WARNING; the
`WINDOW FOR THIS RUN: 90 days` line in the prompt; `finish_reason=stop`;
`S3-D COMPLETE | saved=YES`; a stored row with `time_horizon='90_DAY'`.
Last saved S3-D row is still 2026-09-03. The Monday 2026-09-28 30-day run is
the second cert (`30 sampled`, ~10 in the newest third).

## 5. THE CLOUDFLARE REFUSAL IS THE DAILY ALLOCATION — BUILD THE BREAKER
- **Mechanism KNOWN** (CC-92, 2026-09-21 evening): HTTP 429 with `'code': 4006`,
  "you have used up your daily free allocation of 10,000 neurons". The daily
  pool, not a rate limit. Still unexplained: on 2026-09-20 the dashboard showed
  7.02k used when the first evening call was refused.
- **Circuit breaker, keyed on the REAL body:** after the first `4006`, stop
  calling Cloudflare for the rest of the run and record the remaining articles
  as skipped with that reason. The OpenAI SDK retried each refusal twice:
  27 calls for 9 articles on Sep 21, 72 for 24 on Sep 20, all hopeless.
  Decide with item 9 whether a breaker-tripped run is red.
- The pacing guard still logs usage only on success, so a run where every call
  fails has no pacing at all (Sep 20 evening: zero TPMGuard lines).

## 6. S3-D STORES 6 OF THE 14 FIELDS IT WRITES — RULING (Bro Alpha)
Not stored: `structural_accumulation`, `closing_windows`, `silent_builders`,
`injection_drift`, `convergence_signals`, `capability_2`, `ach_check`,
`sectarian_trap_30d`. The file's own docstring states its purpose as three
questions; the answers to two (`closing_windows`, `silent_builders`) are
discarded, as are the manufactured-causality, ACH and 30-day Sectarian Trap
answers. Choose: a JSONB column for the full analysis, specific columns, or a
smaller schema so the model stops spending ~half its tokens on text nobody
keeps. Also seen on every trial: `structural_accumulation` and
`injection_drift` returned as objects where the schema says strings, and
240-300 markdown `**` pairs inside JSON strings.

## 7. THE CANARY'S AIR AND INPUT (Lens 1)
Carried unchanged from LENS044 item 8. Measure the Groq consumption of all six
`GROQ_API_KEY` readers FIRST. A wider prompt on a saturated bucket is arm 2.

## 8. WATCH ROWS ARE NEVER CLOSED — NOW WITH ITS COST
Carried from LENS044 item 9, with numbers: on Monday Verification logged
**1,792 lines** (~900 Supabase GETs) because it loops once per MEDIUM Clarity
row, not once per voice, and produced the same finding three times in one dry
run (the write dedup kept one). Decide what closes a Watch row.
Probable, NOT checked: Direction B reported "5 findings delivered" on both
Monday waves — likely the same five, re-sent every wave because nothing ever
marks them reviewed.

## 9. TWO SMALLER TRUTHS, RECORDED ON PURPOSE
- **A partial failure is still green.** `scoring_exit_code(1, 23)` returns 0,
  asserted deliberately in gate 5. Decide the threshold. Concrete now: the
  2026-09-21 evening wave left 9 of 24 unscored (37.5%) and went green.
- **Clarity's "insufficient ops" path records nothing** (line ~152).

## 10. RLS RE-AUDIT (new)
The Supabase project URL appeared in two local scratch logs. It is not a
credential; what it is worth to anyone depends on LENS-026's RLS fix (17 of 24
tables had been anon-readable/writable) still holding. Verify, table by table,
that the anon key can neither read nor write.

## 11. A PROVIDER-DEATH DETECTOR (new — DORA exit strategy)
SambaNova (2026-07-28, 402) and Cerebras (~2026-08-17, 402) both died with
nothing watching; each was found weeks later. The registry still lists
Cerebras rows as if alive (`s2f_primary` among them). A cheap daily check of
each provider's cheapest endpoint, reported in the Telegram pre-flight, would
have caught both on day one. Design it against the canary-air rule.

## 12. CARRIED FORWARD
- **S3-A and `fetch_s3_data` still read the OLDEST rows** of their windows
  (`order asc` + limit). S3-D was fixed by CC-93/94; reuse `sample_window`.
- **The `mistral-small*` class**: `mistral-small-2603` has TPM 20,000 — 31x
  tighter than ministral-8b — the strongest explanation yet for its class-wide
  429 (formerly UNKNOWN). 96 calls went to it in Sep 1-20 from unswept alias
  sites (D-015). Sweep them.
- **AI-origin ban list**: the Mistral catalogue now offers `glm-5-2` and
  `zai-glm-5-3`. Confirm the registry's self-test bans `glm-` and `zai-`.
- **The registry is decorative for S2-F** (`ENSEMBLE_LEGS` is a literal) and
  for the lens engine (`analyze_lens_multi.LENSES`).
- Cohere scarcity (1,000 calls/month trial across Lens 3, S3-A, S3-C; the
  LIMITS row still says RPD 1000). Charter tensions F1 and F6, unruled.
  `lens_escalations` and `lens_run_meta` do not exist. S3-B's primary is still
  the dead `gemini-2.0-flash`. Root `calibrate_rubric_*.py` scripts read keys
  outside `canary_air_guard`. D-022 retention never implemented.
- Housekeeping: `probe_out_s3d*` bodies untracked — commit or ignore.

---

# OPEN RULINGS
- **Item 1.2** — the leg-calling shape (i)-(iv). Blocks item 1.
- **Item 1.3** — confirm Mistral `ministral-8b-2512` as the second leg, and
  whether it gets its own organisation/key.
- **Item 6** — what S3-D stores.
- **Item 9** — the partial-failure threshold.
- **R10** — Claude's lean is RULE OUT, unchanged since LENS-039. Never taken.
- **F1 / F6** — item 12.

# EARNED THIS SESSION (LR numbers unassigned — check the register first)
- **Scan every file against the `.env` VALUES before it leaves the machine.**
  Not against patterns: a pattern is a guess about what a secret looks like.
  Two scratch logs held the project URL; the scan found both.
- **A test double must enforce production's limits.** CC-93's fake returned
  every row, so it could not see PostgREST's 1000-row cap.
- **A check that a projection can satisfy is not an evidence check.** Count
  dates on or before today, not the latest date anywhere in the text.
- **Do not raise a budget to revive a position whose input is stale.** S3-D at
  16000 on the old query would have saved Aug 22-Sep 2 analysis as the current
  30 days — pretend-right bias, worse than eighteen honest dark days.
- **One prompt at temperature 0.3 varied by more than 1,800 output tokens.**
  Size on the long run (LR-114 corollary).
- **A guard fed only by successes is inert exactly when everything fails.**
- **A grep pattern built from memory indicts itself first** — broken twice
  more this session. Build it from a line already read in that file.
- **Even by row count is not even in time** when the pipeline's own bugs have
  changed how many rows a day it writes. Sample the dimension the charter
  names (CC-96: "what accumulates across 90 days" means days, not rows).
- **An instrument must not be stricter than what it measures.** The probe
  harness timed out at 180s while production allows 240s: a call production
  would finish would have read as a failure.
- **Set a live threshold on the real data's shape, not on the synthetic one.**
  CC-96's `>= 25` assumed no gaps; the live window had seven empty days.
