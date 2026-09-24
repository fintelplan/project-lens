# LENS TARGET AND ORDER — LENS-046
Regenerated 2026-09-23 at the LENS-045 close. Supersedes LENS045 entirely.
Item numbers are NEW. Nothing carries its old number.

## THE TARGET (unchanged since LENS-036)
A daily intelligence pipeline that runs at $0/month, tells the truth about
its own failures, and delivers a complete Regular Report to Telegram.
Loud failure always beats silent success.
**Open (D1):** the target has no finish line. Whether and how to write a
completion test (Partner A pattern) is Bro Alpha's to decide after discussion.

## THE CANON — READ BEFORE CHANGING ANYTHING
`CLAUDE.md` §0 (four-arm gas-mask test), §0a (charters), §0b (AI origin),
§0c (block contract) · `docs/LENS_FOUNDATIONS_LENS042.md`.
A change that moves a position away from its charter is a STOP-and-ask.

## WHAT LENS-045 SETTLED — DO NOT RE-OPEN
- **CC-91 is certified** (2026-09-22 morning: Stage 1 exited 1; Watch, Clarity,
  Verification and Direction B all ran).
- **RLS holds on all 24 tables** (item 10 closed): `rls_on=true`, zero policies,
  anon reads `200 []` on the 20 non-empty tables, anon schema listing `401`,
  no exposed views. Not checked: `rpc/` functions (item 9).
- **The China ban now catches `zai`** (CC-98, proven both ways by a bite).
- **`lens_provider_events` exists** (CC-103/104): RLS on, zero policies, anon INSERT
  refused `401`, column `n` counts identical events. Service key writes only.
- **Item 11 rulings (delegated to Claude, "your call"):** (a) the provider-death
  detector is PASSIVE — it reads production's own refusals, makes no calls;
  (b) its Telegram block is operator information, not the canary's voice, and
  must be visibly separate from canary content.
- **The canary's voice counts THIS wave** (CC-114): no surface may fill a missing lens
  with an older row; `lens_canary_wave.select_wave` is the one reader.
- **`fetch_s3_data` reads the NEWEST rows**, not the oldest (the LENS045 order
  said oldest; bytes say `desc=True`). Its real defect is item 9's `limit(12)`.
- **S3-C reads its whole 30-day window evenly in time** (CC-108): it had read the
  oldest 40 rows — ~5 days of S1, ~1 day of S2 — under a 30-day drift label.
- **S3-A reads its whole 7-day window evenly in time** (CC-99); the sampler
  lives in `code/lens_window_sample.py`, shared with S3-D.
- **Order wording:** S2-F's "24" per wave is 8 articles x 3 lenses, not 24 articles.

---

# THE MISSION
**Not declared** -- Bro Alpha rules it at LENS-046's open. Claude's suggestion: the D3
first task (an hour), then D2's LR-106 matrix and wiring with D6's coverage line.
**Not declared.** Bro Alpha ruled the LENS-045 sequence: finish the handover, then
urgent+important, then record OTHERS, then DISCUSS (D1–D7), then decide.
The mission for LENS-046 comes out of that discussion.

---

# ITEMS

## 1. S2-F CANNOT RELY ON CLOUDFLARE — RULED (D2 = A), NOT YET WIRED
**Ruling (delegated, LENS-045):** Mistral `ministral-8b-2512` scores every article;
Cloudflare is a best-effort second leg (ensemble when it answers; CC-97's breaker
makes a refusal cost one POST). Next: the LR-106 matrix on S2-F's real prompt
(LENS-020 articles), then `ENSEMBLE_LEGS = [mistral, cloudflare]`. Known cost:
Mistral concentration (DORA Art. 29), watched by item 3's detector.
- **2026-09-22 morning (run `35695267308`, 06:32Z): Cloudflare refused the FIRST
  call with `4006`** — six and a half hours after the documented 00:00 UTC reset,
  with no S2-F call earlier that UTC day. 21 of 21 refused, `scored=0`, RED.
- Cloudflare's docs say limits reset at 00:00 UTC; community reports describe
  4006 after the reset, some with the dashboard showing 0/10k. Our three
  observations (Sep 18, 20, 22) fit a rolling 24h window. **Hypothesis, not fact.**
- **Claude's (iii) lean is WITHDRAWN**: it rested on "morning has a fresh pool".
  Under a rolling window two mornings less than 24h apart share one pool.
- Standing facts: `ministral-8b-2512` TPM 625,000 / RPS 3.13; LENS-020's matrix
  lives in `LENS-020_S2F_architecture_decision_v3.md` / `v4.md` (ops/conf only,
  no article bodies — find the bodies before any matrix; see the Apr 28-29
  `SESSION_AUDIT` files). v4 designed Mistral as the Verification tier.
- The Mistral client in `lens_framing_rubrics.py` now defaults to
  `ministral-8b-2512` (CC-101). No Mistral leg is wired.

## 2. CERTS OWED — read with `lens045_certs.sh` (it proves each run carries the commit)
| Cert | Needs commit | Wave | Pass |
| --- | --- | --- | --- |
| CC-97 breaker -- **CERTIFIED Sep 23 morning (first trip)** | `b52f7f0` | next S2-F | <=1 `'code': 4006`, <=1 `[BREAKER]`, no Cloudflare `call failed` after the trip, `quota_skipped=N` |
| CC-100 S1 report -- **CERTIFIED Sep 22 evening** | `125c440` | next M+A | `[S1-RPT] ... finish_reason=stop` + `S1 docx sent to Telegram`; subtitle `ministral-8b-2512`; OR a RED step plus `S1 Canary Report FAILED today` in Telegram |
| CC-99 S3-A window -- **CERTIFIED Sep 23** | `efbb7e1` | next M+A that runs S3-A | `S1 window=7d: N reports in window, 20 sampled evenly in time (~6-7 in the newest third), spanning` ~7 days |
| CC-102 refusal lines -- **CERTIFIED Sep 22 evening** | `7e9e577` | next M+A and S2-F | `PROVIDER_REFUSAL provider=google model=gemini-2.0-flash class=model_gone status=404` from S2-B and S3-B every wave |
| CC-103/104 storage -- **CERTIFIED** | `3b4629a`, `bd9cd8b` | next M+A, Collection, S2-F | `PROVIDER_EVENTS stored: N rows, M events` at process exit; rows in `lens_provider_events` |
| CC-105 health message | `46916e8` | **CERTIFIED both waves (DOWN x2 on Sep 23)** | wave 1: a separate Telegram `PROVIDER HEALTH (operator, not the canary)` with `EXCEPTION (1 run, watching)  google/gemini-2.0-flash  model_gone`; wave 2: the same line reads **`DOWN`** |
| CC-106 gone-model retry -- **CERTIFIED Sep 22 evening** | `adb062a` | next M+A | S3-B: `Attempt 1 failed: 404` then at once `S3-B primary gemini-2.0-flash is gone -- not retrying, falling back now`; NO `Attempt 2`; S2-B: `failed after 1 of 3 attempts` |
| CC-107 S2/MA refusal lines | `97c28d3` | next M+A | a `PROVIDER_REFUSAL` line from any S2 position or MA that gives up; none if all answer |
| **CC-108 S3-C window** | `a336d46` | **Thu 2026-09-24** | `S1 window=30d: N reports in window, 40 sampled evenly in time (~13 in the newest third), spanning` ~3 weeks (the Aug 22-31 gap is real); `S3-C COMPLETE \| saved=YES` |
| CC-109/110 S3-F, RR, S3-D lines | `72924dd`, `4bfa018` | next M+A / Thu | a line only on a refusal; S3-D's success path is unchanged (diff guard) |
| **CC-113 canary air -- CERTIFIED Sep 23 (Lens 1 ✅ 6.8)** | `184c09e` | next morning M+A | `[ORCH] Lens 1: ✅`, no `429_tpd`; any Groq refusals now come from `fetch_text.py` on its own org |
| **CC-114 canary voice -- 4/4 path CERTIFIED; MISSING path owed** | `b9dde9b` | next M+A | `4/4` when all spoke; otherwise `N/4 ... MISSING: <lens>` on the block, the Brief and the S1 report; never a borrowed row |
| CC-115 retired primary | `7771891` | next M+A | `S2-D primary cerebras/gpt-oss-120b is RETIRED -- ... going straight to the fallback leg` (S2-D x2, S2-E x4, MA x1); no `calling cerebras`; no `provider=cerebras` refusal; S2 step shorter |
| CC-116 alert on change | `cdbd343`, `0c457f9` | next M+A | `CRITICAL alert not sent: unchanged ...` in the log and NO critical-alert message while the level holds; the Brief says `(N waves in a row)` |
| S3-D 90-day (CC-93/94/96) | `b1e28b7` | **Thu 2026-09-24** | as LENS045 item 4 |
| S3-D 30-day | same | Mon 2026-09-28 | `30 sampled`, ~10 in the newest third |

## 3. PROVIDER-DEATH DETECTOR — SHIPPED (LENS045 item 11), CERT OWED, COVERAGE TO DEEPEN
Shipped: CC-102 `7e9e577` (one line, one shape) -> CC-103 `3b4629a` (stored once per
process, ITIL record/store) -> CC-104 `bd9cd8b` (Groq via entity_extract + S2-A, Cohere
via S3-A: all FIVE live providers watched; identical events counted in `n`) -> CC-105
`46916e8` (a separate last step of Manager + Analyze sends PROVIDER HEALTH; DOWN =
payment/model_gone/auth in 2+ runs; always exits 0). DoD met in code; cert in item 2.
Found while building it:
- **The orchestrator's pre-flight has NO Telegram block** — `lens_orchestrator.py:677`
  is `# TODO LENS-011: Telegram alert for critical`, open since April. The LENS045
  order and Claude's first design both named a block that does not exist.
- **CC-2's rider 1 (the `REGISTRY MISALIGNMENT` Telegram line) never reached Telegram**:
  only `AI5_REGISTRY_MISALIGNMENT` exists, as a return string (`:221`). The LENS-028
  records say "in flight"; nothing later says shipped.
- Known limit: successes are not stored; DOWN means "refused in 2+ runs".
- `lens_provider_events` has no retention yet (D-022 kin); `n` keeps it small.
Coverage after CC-107..110 (`97c28d3`, `a336d46`, `72924dd`, `4bfa018`): SIXTEEN sites —
S1-RPT, S2-A/B/C/D/E, S2-GAP, S2-F, MA, S3-A/B/C/D/F, Regular Report, entity_extract.
CC-111 `9f5672c` added the Compendium (its CC-23 zero-failure guard now says its intro
is canned): SEVENTEEN sites. Remaining: the canary lenses (`analyze_lens_multi.py`)
— all four gas-mask arms first, a discussion item.
**First catch (Sep 22 evening, wave 1):** S2-D x2, S2-E x4 and MA x1 still call their
DEAD Cerebras primary every wave (402) before falling back to Mistral. **Fixed by CC-115
`7771891`:** `RETIRED_PROVIDERS` in the registry, one switch. Leftovers: `get_client()`
still builds a Cerebras client (keep the secret); the registry's `regular_report`
fallback and `s2f_primary` still name Cerebras (drift, nothing calls them). Also: `source` names
the process, not the position -- record the position too. BUG-002 lives: `S2-E ... for unknown`.
Found while wiring:
- **S2-C and S3-F have NO fallback leg**: when Mistral refuses, they produce nothing.
  With S1-RPT, S2-A/B/D/E and S3-A/B also falling back to Mistral, this is the
  concentration D2/D7 must weigh (DORA Art. 29).
- Regular Report: if `get_llm_client()` fails before reassigning `provider`, a fallback
  refusal is recorded under the primary's name (edge; behaviour left unchanged).

## 4. THE CANARY'S AIR — RULED (D7, reopened) AND SHIPPED; DEBT REMAINS
- Measured morning: 321 Groq POSTs, 58 TPD. Evening: **160 TPD, and Lens 1 failed
  `429_tpd_escalated`** -- the canary lost its Foundation lens (human rights).
- CC-113 `184c09e`: entity extraction -> GROQ_S3_API_KEY (idle, separate org proven by
  daily-request headroom, without touching the canary's key). Gate 25 is a RATCHET.
- **Remaining debt (gate 25's KNOWN list):** Lens 3's Cohere key with S3-A and S3-C
  (a ~1,000 calls/month trial); Lens 4's Mistral key with 13 roles (no rate contention
  by arithmetic, but one Mistral death silences the canary lens and those roles together).
- Still worth doing: entity extraction's usage logging and a TPD breaker (it now
  exhausts its own org nightly: ~160 hopeless calls).
- Voice: CC-114 `b9dde9b` (see settled). Canary cycle label `manual` on scheduled waves.

## 5. S3-D STORED 6 OF ITS 14 FIELDS — RULED (D4 = A) AND SHIPPED
CC-112 `d04bd9e`: `analysis_full jsonb` holds the whole answer. Thursday's cert adds:
the 90_DAY row's `analysis_full` is not null and carries `ach_check`,
`closing_windows`, `silent_builders`. Wiring these into MA or the Regular Report
is a later question.

## 6. WATCH ROWS ARE NEVER CLOSED — RULING (D5)
Unchanged from LENS045 item 8. **ANSWERED Sep 22:** Direction B's "5 findings" are ONE
trump_office Verification finding, one row per day Sep 18-22, all unreviewed, re-sent
every wave (seen in Telegram, 2026-09-23 01:08 ICT).

## 7. A PARTIAL FAILURE IS STILL GREEN — RULED (D6 = A), NOT BUILT
S2-F reads a 6h lookback: an article unscored in its wave is lost for good, so partial
failure costs coverage permanently (PHI-004's Verification needs its ~44-article sample).
Ruling: show coverage (scored/attempted) where the reader sees it; set the RED threshold
inside D2's wiring (with Mistral on every article, a failure is rare and serious). The
attempted count must first be stored somewhere -- a new table needs the Oct-30 grant rule.

## 8. THE S2 REPORT NEVER ARRIVES — RULING (D3), lean A
**Corrected:** it IS wired -- `lens_s2_orchestrator.py:246` calls `run_s2_report` (since
`3d56327`, LENS-022). Built as the trio's middle (S1 raw signal / S2 how it was shaped /
S3 strategic) so the operator can see the manipulation delta (PHI-001, PHI-004). No S2
docx reaches Telegram; the log shows no `S2-RPT` line (its prefix is probably
`[QUOTA_GUARD]` -- grep its own strings). It calls `mistral-small-latest`. Claude's lean:
A -- restore it the CC-100 way (probe ministral-8b, repoint, loud exit), as designed.

## 9. CARRIED FORWARD (OTHERS)
- ~~S3-B retries a dead model~~ and ~~S2-B's `failed after 3 attempts`~~ — CLOSED by
  CC-106 `adb062a` (cert in item 2).
- `fetch_s3_data`: `limit(12)` across all S3 positions can crowd one out.
- The registry is decorative for S2-F (`ENSEMBLE_LEGS`) and the lens engine
  (`analyze_lens_multi.LENSES`); the China-ban self-test does not see literals.
- `[lens-N] usage:` lines did not appear in the M+A log — how lens usage is
  logged in production is unverified.
- `rpc/` functions not checked for SECURITY DEFINER (RLS bypass).
- The Cloudflare pacing guard logs usage only on success.
- ~~Alert fatigue~~ -- CC-116 (`cdbd343` + `0c457f9`): alert on an escalation past the last
  ~24h; 14 -> 3 a week on the live history. Left open: **Mission Analyst flips HIGH<->CRITICAL
  wave to wave** -- the top of its scale may no longer discriminate (calibration, for D1).
- The Brief's "7-DAY TREND" shows the last THREE reports (`trend[:3]`) -- a label that lies
  about its window; it hid the HIGH/CRITICAL alternation from both of us.
- The Brief's "Patterns: 5" disagrees with the S3 report's "3 patterns" (same wave).
- `**` markdown is sent unrendered (Brief, S2 message, Regular Report caption); S3-B's
  historical parallel sometimes arrives as raw JSON (Sep 22), sometimes as text (Sep 23).
- S3-B primary is still the dead `gemini-2.0-flash`. Cohere scarcity. F1/F6.
  `lens_escalations` / `lens_run_meta` do not exist. `calibrate_rubric_*.py`
  read keys outside `canary_air_guard`. D-022 retention. `probe_out_s3d*` untracked.

## 10. DIRECTION (D1) — for discussion, not a ruling
Claude's analysis of Partner A's route (S92-S101) for Lens: ADOPT a byte-checkable
completion test, the roadmap/target split with a phase transition only Bro Alpha
declares, and the "we promise the truth about X" SLO framing (it resolves the
conflict between a coverage SLO and gas-mask arm 3). ADAPT the error-budget
policy (dependency deaths need an adaptive path, not a reliability loop) and
freshness (cron delay would measure GitHub). DO NOT ADOPT now the
self-description generators or the full process apparatus: Lens's failures are
external and live; Partner A's were internal. Partner A's own route is not yet proven.

---

# OPEN RULINGS (discussion first — Bro Alpha, LENS-045)
**Bro Alpha's method for every discussion: anchor on why the workflow was built; do not
drift from it.** Ruled at LENS-045: D2 = A, D4 = A (shipped), D6 = A, D7 = A (shipped).
Still open: D1, D3, and the new MA-calibration question inside D1.
- **D1** target + completion test · **D2** item 1 · **D3** item 8 · **D4** item 5 ·
  **D5** item 6 · **D6** item 7 · **D7** item 4.
- **R10** — Claude's lean is RULE OUT, unchanged since LENS-039.

# EARNED THIS SESSION (LR numbers unassigned — check the register first)
- **A substring check is only as good as everything else that contains the
  substring.** Five times in one session: `4006` inside a timestamp; `basicConfig`
  inside the module's own docstring; `Mistral call failed` when the failing branch
  logs `Mistral 429 attempt`; `mistral-medium-latest` inside the fix's own comment;
  `FAIL` inside `PARSE_FAILED`. Check the call-site shape; a fix's comment must
  not be able to satisfy or break its own check.
- **A cert read must first prove the run carries the commit.** The morning wave
  was nearly read as CC-97's cert; the SHA guard now refuses it.
- **A documented reset time is a claim, not a measurement.**
- **`|| true` on a step that delivers something is a silent-failure switch.**
- **A library's own log escapes your redaction.** httpx logged the Supabase URL
  in a local probe; scan every scratch file, your own probe logs included.
- **Measure the bucket before widening what breathes from it.**
- **A defect found in one position is a question for all its siblings.** The
  LENS045 order named S3-A's oldest-rows read; S3-C had the same one and was found
  only by coverage work, days before its next run.
- **A voice must count the wave, not the newest N rows.** `limit(4)` borrowed a
  morning row to cover an evening death, on three surfaces at once.
- **A gate can find debt before it prevents it.** Gate 25 as first written would never
  pass (Lens 3 and 4 share keys); written as a ratchet it stops new harm and keeps
  the debt in sight.
- **Prove an org boundary without touching the thing you protect.** Daily-request
  headroom on the idle key (999/1000) proved a separate org; the canary's key was never called.
- **Never hand a commit block in the same message as the script it depends on.** CC-116
  shipped once without its amendment that way.
- **Run the guard before you rely on it.** The CC-110 diff guard had never run;
  its first run crashed. A proof that has not been executed is a promise.
- **A design that names a place must first find the place.** The pre-flight
  Telegram block was designed onto for a session; it had never existed.
- **"In flight" in a record is not "shipped".** CC-2's rider 1 was recorded in
  flight in July and nothing ever closed it.
- The substring rule, a sixth time: `REGISTRY MISALIGNMENT` (space) vs the code's
  `REGISTRY_MISALIGNMENT`. The session records supplied the right word.
