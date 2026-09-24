# LENS TARGET AND WORKING ORDER — generated at the LENS-040 close (2026-09-14)
**Regenerated 2026-09-14. SUPERSEDES `LENS_TARGET_AND_ORDER_LENS040.md`.**
Ranks are TARGET-RELATIVE. Freshness confers no priority. This file holds the
target instance and the path; LENS_CONTRACT.md holds the mission and the method.

**THE FILENAME CARRIES THE CONSUMING SESSION** (CONTRACT v4): this file is
`docs/LENS_TARGET_AND_ORDER_LENS041.md` and the OPEN prompt resolves the LATEST
by glob.

## CURRENT TARGET (declared 2026-08-06, unchanged)
Every scheduled wave produces valid intelligence, unattended, with **NO SILENT
FAILURE**. Not "no defects". A system with logged, ordered, non-silent defects
is done. A system with one silent failure is not.

**Status right now:** the silent half is CLOSED — CC-60 was certified this
session and S2-C and S2-E now report `ANALYSIS_FAILED` when they save nothing.
What is left is loud and unmet: **five of seven S2 positions fail on every
wave, and the daily Regular Report has not been delivered since 3 September.**
The system is honest and not working. That is the right order to fix it in.

## ROOTS
- **R1 — Deprecation is weather, and we have no forecast.** Models and whole
  free tiers die on someone else's schedule. **Struck again 2026-09-04:**
  Mistral's `mistral-small*` class began refusing with no notice and no
  deprecation flag.
- **R2 — Written but never wired.** Code that declares a capability no call
  site reads.
- **R3 — Numbers set once, never re-derived against reality.** The registry's
  Mistral limits were VERIFIED-console 2026-07-28 and were wrong in **every
  field** 47 days later.
- **R4 — Epistemic diversity is assumed, not verified.**
- **R6 — The record cannot attribute.**
- **R7 — Nothing watches our external dependencies' lifecycle.**
- **R8 — Certification measures mechanics, not behaviour.** Sharpened this
  session: a 3-trial probe passed 3/3 and then 1/3 on byte-identical input.
- **R9 — Free-tier resources are CONSUMED, not merely rate-limited, and
  nothing meters what we accumulate.**
- **R10 — A position's declared status is not derived from its own work.**
  PROPOSED at LENS-039, **STILL BRO ALPHA'S TO RULE.** Evidence recorded against
  it at LENS-039 and unchanged: 20 top-level status literals in `code/`, 18
  already derive status from the work, only 2 hardcoded it. Claude's lean is
  **RULE R10 OUT** — 2 departures from a sound convention is a gate problem,
  not a structural root. CC-60 fixed both departures and was certified.
- **R11 — WE TAKE PROVIDER DEFAULTS AND NEVER ASK FOR THE GUARANTEES ON
  OFFER.** PROPOSED this session, **BRO ALPHA RULES.** `response_format` appears
  ZERO times in all of `code/`, across twelve Mistral legs that parse JSON.
  The server-side JSON constraint has existed the whole time. Distinct from R2
  (our capability, unwired) and R3 (our number, stale): R11 is a capability the
  PROVIDER offers that we have never requested. Measured: 4/6 parse without it,
  5/5 with it, same model, same prompt.

## CLOSED THIS SESSION
- **Old item 1 — SILENT FAILURE IN TWO POSITIONS. CERTIFIED.** `85f1988`
  (CC-60) ran its first wave at `34770305528` (2026-09-13 17:32Z). Every anchor
  in the banked prediction held: `=== S2-E COMPLETE` count **0**, S2-C and S2-E
  both `⚠️ status=ANALYSIS_FAILED`, `5 position(s) did not complete:
  ['S2-B','S2-C','S2-D','S2-E','Mission Analyst']`, `All positions complete`
  = 0. Re-confirmed on `34814620057` (09-14 07:07Z). The prediction was written
  out before the log was opened.
- **Old item 2.3 — RULED.** See D-025. The fork's premise was disproven before
  the ruling could be taken; both A and B are dead on measurement.
- **Old item 9.3 — SHIPPED.** `522ff8c` added the Mistral transport to
  `PROVIDER_ENDPOINTS`. The probe pack can now exercise the provider eight
  roles fall back to.

## WORKING ORDER

### URGENT

**1. MISTRAL'S SMALL CLASS IS BLOCKED AND FIVE POSITIONS HAVE NO LIVE LEG**
[R1, R7, R4] — rewritten. The LENS-040 order called this "every fallback
converges on one Mistral key". **That diagnosis was wrong** and the evidence
that disproves it was gathered this session.
- 1.1 **IT IS NOT A BURST.** 429s on one wave were spaced 11s, 82s, 76s, 75s,
  76s, 70s, **244s**, 21s, 167s, 20s. S2-E's four calls already sit behind a
  `Stagger 65s`. 100% refusal at 65–244 second spacing is not a rate limit.
- 1.2 **IT IS NOT THE KEY OR THE ACCOUNT.** `GET /v1/models` returns **200** on
  the same key. Subscription page: Free plan, **$0.16 of $10** used, 17 days
  left. Billing: $0.00. Nothing is exhausted.
- 1.3 **IT IS SCOPED TO A MODEL NAME FAMILY.** One key, seconds apart, same
  payload shape: `mistral-small-2603` **429**, `mistral-medium-latest` **429**,
  `mistral-small-2506` **429**, `mistral-small-2503` **429** — and the last two
  **are not in the account's model list at all**, so a usage limit cannot
  explain them. Meanwhile `ministral-14b-2512`, `ministral-8b-2512`,
  `ministral-3b-2512` and `codestral-2508` all return **200**. Models outside
  the tier return an honest `403 tier_not_allowed`, so `mistral-small` is
  IN the tier and refused anyway. **Why is UNKNOWN and must stay written as
  unknown.**
- 1.4 **DEAD SINCE 2026-09-04, not Sep 8.** `33847533497` at 07:10:58Z is the
  first 429; `33726430126` on Sep 3 07:07Z was the last success. The order's
  "five waves" undercounted by four days.
- 1.5 **BLAST RADIUS GREW EVERY TIME IT WAS MEASURED:** 7 refusals (order), 11
  (Sep 13 wave, adding S3-B and the S3 report), 15 (Sep 14 wave, adding S3-D,
  S3-F and S1-RPT). **Seven positions**, and Mistral successes: zero, two waves
  running.
- 1.6 **THE CANDIDATE IS PROBED AND READY.** `ministral-8b-2512` with
  `--json-mode`, on S2-E's real 9,457-char prompt: **5/5** HTTP 200 /
  finish=stop / JSON parses, refusal 0, budget 15–20%, actors 7/6/8/7/8 and low
  4/4/4/4/4 against the banked `mistral-small` band of actors 8/7/8, low 5/3/5.
  Context 262,144 — identical to `mistral-small`. `ministral-14b-2512` also
  answers and is closer in size (14B vs 24B) but is UNPROBED on S2-E.
- 1.7 **DO NOT WIRE IT WITHOUT ITEM 3.** Without `response_format` the same
  model parsed 4/6. The model is not the fix on its own.
- 1.8 **NOT ESTABLISHED:** whether the two models select the SAME actors, only
  that they count alike. The band is a count band; agreement on substance is
  item 12's work.

**2. THE REGULAR REPORT HAS BEEN RED FOR ELEVEN DAYS AND A MODEL SWAP WILL NOT
FIX IT** [R1, R8, R3] — NEW. Two independent defects, one hiding the other.
- 2.1 **DEFECT A — TRUNCATION, and it is the serious one.** Six probe trials,
  two models (`ministral-14b`, `ministral-8b`), three each: **6/6
  `finish_reason=length`, 6/6 `budget_used=100%`, `tokens=23479` identical on
  all six.** Every body stops mid-sentence inside PART 2. **PART 3 (FOOD FOR
  THOUGHT) and PART 4 (REFERENCES) are never generated.** Prompt is 64,968
  chars (~19,383 tokens) against `max_out: 4096`.
- 2.2 **RAISING THE CAP MAY NOT BE ENOUGH.** `MAX_REFS = 400` and PART 4 is the
  reference list. The report's design may exceed any single call. Fork to rule:
  (A) raise `max_out` and measure; (B) split into two calls (parts 1-2, parts
  3-4); (C) cut `MAX_REFS` and cap PART 4. Measure A before choosing.
- 2.3 **DEFECT B — the docx headings, minor.** The model emits
  `## **PART 1 — DETECTION**`; `lens_regular_report.py:396` tests
  `line_stripped.startswith("PART ")` and misses it, so headings become plain
  paragraphs. `:424/:428/:432` use substring tests and DO work, so section
  splitting is unaffected. Fix the test, not the prompt.
- 2.4 **WAS `mistral-small` TRUNCATING TOO? UNKNOWN.** The Sep 3 green log
  contains no `usage`, no `finish_reason`, no token counts at all — this
  position has never logged its own consumption. If it was truncating, the
  defect predates Mistral's refusal by an unknown period and every delivered
  report has been short two parts.
- 2.5 **DO NOT SWAP THE MODEL FIRST.** Today the failure is loud: 429,
  `{"status":"FAILED"}`, exit 1, red on the wall. A model swap alone turns that
  into HTTP 200 and a silently truncated report delivered to Telegram. That is
  the target's absolute rule, breached deliberately. Fix 2.1 first.
- 2.6 **DUAL SOURCE.** `lens_regular_report.py:49-55` hardcodes its own
  `PROVIDERS` chain and **does not import `lens_models` at all** — no
  `assert_model_known`, no registry read. The registry's `regular_report` row
  is decorative for this file. LR-105 violated. Separate work from the model
  choice; do not bundle them.

**3. NO JSON CONSTRAINT IS SENT ON ANY MISTRAL LEG** [R11, R3] — NEW.
`grep -rho 'response_format' code/*.py | wc -l` = **0**. Fourteen files call
`api.mistral.ai`; **twelve of them `json.loads` the result**
(`regular_report` and `s1_report` are prose and are exempt). Measured cost:
`ministral-8b` parses 4/6 without it and 5/5 with it, identical prompt — both
failures an unescaped `"` inside a string, both on `IMF "caved"` lifted from
the source report, so the trigger is CONTENT, not chance. `mistral-small`
never needed it, which is why the gap stayed invisible. One line per leg.
Census-before-sweep is DONE (the twelve are enumerated); the sweep is not.

**4. THE REGISTRY'S MISTRAL LIMITS ARE WRONG IN EVERY FIELD** [R3, R7] — NEW.
`code/lens_models.py:321`, tagged `VERIFIED-console 2026-07-28`:
| Field | Registry | Console 2026-09-14 |
| --- | --- | --- |
| TPM | `50_000` | **20,000** |
| RPD | `2_000` | **no RPD axis exists** |
| governing axis | `"RPD"` | TPM + RPS |
| CTX | `128_000` (VERIFY) | **262,144** (model card) |
The guard paces Mistral on an axis the provider does not have. Also owed: rows
for `ministral-8b-2512` (TPM 625,000 / RPS 3.13 / CTX 262,144) and
`ministral-14b-2512` (TPM 937,500 / **RPS 0.50** — the lowest RPS on the page)
so `fit_max_tokens` stops falling through to `ceiling UNRESOLVED`. The fail-safe
worked correctly when it did: it returned the cap and logged, and did not
borrow a neighbouring row's number. Model cards carry `deprecation` and
`deprecation_replacement_model` — machine-readable, and item 13's answer on the
Mistral side.

**5. RETENTION POLICY — DESIGNED, NOT BUILT** [R9] — was 3. D-022 recorded at
LENS-039. No code exists. Read-windows measured: raw_articles 48h, reports 30d,
injection_reports 45d, article_refs 3d.
- 5.1 **`lens_reports` MAY NEVER LOSE A ROW.** `lens_s4_upgrade_monitor.py:72`
  counts every row since `DAY_1_UTC` with no upper bound and that count IS the
  S4-E maturity counter. Deleting makes the system report itself less mature,
  silently. Shrink instead.
- 5.2 **DELETE DOES NOT RECLAIM SPACE.** Two-step delete → `VACUUM FULL`,
  outside a wave — hard while cron delay runs 3-5h.
- 5.3 **ARCHIVE BEFORE DELETE, unconditionally.** CSV to a GitHub release. A
  failed archive blocks the delete.
- 5.4 **RAW ARTICLES AND ARTICLE REFS SHARE A WINDOW** or refs point at
  missing articles.
- 5.5 **HISTORICAL SHRINK STILL OWED.** Aug 16 onward left fat on purpose;
  ~33 MB recoverable. Idempotency guard in the WHERE clause (LR-159).
- 5.6 All five rows of a wave carry a byte-identical `articles_used` (14,086
  bytes). Normalising to one wave-level row would cut the remainder ~80%. Low
  priority; recorded so it is not rediscovered.

**6. NO SIZE MONITORING** [R9, R7] — was 4. The last unaddressed half of R9.
- 6.1 `pg_database_size()` is not reachable through PostgREST; an `sb.rpc()`
  function is the implementation.
- 6.2 **THRESHOLDS (D-022):** WARN 400 MB, trigger 450 MB, target 350 MB.
- 6.3 **A FREE DETECTOR EXISTS AND IS UNREAD.** Database-dead manage-analyze
  runs last 19-27 SECONDS, healthy ones 21-32 MINUTES, absolute separation over
  38 runs. Under 60s means the database was unreachable. Used successfully this
  session to rule the database OUT of the Regular Report failure.
- 6.4 **MEASURED 2026-09-14:** `lens_reports` **3,978** rows (3,949 on Sep 11,
  ~12/day), `lens_raw_articles` **124,571** (122,577 on Sep 11, ~780/day).
  `pg_database_size` NOT re-measured this session — the late-October projection
  is still an 11-day projection.

**7. THE WRITE GUARD IS NEVER CALLED** [R2] — was 5. `lens_write_guard.py` is
253 lines documented as "Layer 4 of 5" with **zero call sites**. Two proofs it
has never run: `required` includes `generated_at`, which neither S1 record dict
sets; and `types` declares `"articles_used": int` when the value is a JSON
string. Wire it (fixing the schema first) or delete it and stop claiming five
layers.

**8. NOBODY READS THE RED WORKFLOWS** [R8, R6] — NEW, and structural.
Lens Regular Report went red on 2026-09-04 and stayed red for **eleven
consecutive runs**, each with `{"status":"FAILED"}` and exit code 1 on the
Actions wall. It was found this session by looking at a screenshot, not by any
alarm. LR-161 says a failing scheduled job is a free liveness detector — that
is only true if something reads it. Two reds are live and unexplained right now
and BOTH are item-worthy:
- 8.1 **Lens Regular Report** — item 2.
- 8.2 **manage-analyze went X on Sep 12 evening and Sep 13 evening**, both
  `##[error]The operation was canceled` about 4 minutes AFTER the S2-ORC
  summary printed, i.e. in the S3 phase. Green on Sep 14. Unexplained.
- 8.3 **The workflow's own ✓/X is not a health signal.** The Sep 12 X run and
  the Sep 14 ✓ run have identical S2 content — five positions failed in both.
  Proven twice this session.
- 8.4 Design a reader: one alarm when any scheduled workflow's conclusion is
  `failure`, or when manage-analyze runs under 60s (6.3). Cheap; nothing exists.

**9. SYSTEM 1 IS ONE SURVIVOR QUADRUPLED** [R2, R3, R4] — was 6.
- 9.1 `lens_orchestrator.py:375` passes `--single-lens`; `analyze_lens_multi.py`
  has no `sys.argv`, no `argparse`, no `LENS_ID`, so every invocation runs all
  four lenses. **Every wave runs 16 lens analyses, not 4.**
- 9.2 **CANARY THREE-ARM GAS-MASK TEST BEFORE ANY EDIT HERE.**
- 9.3 lens3 and lens4 remain on dead Cerebras; lens3's leg is groq/gpt-oss-20b
  — see item 10.
- 9.4 S1 burn is UNMEASURED; `subprocess.run(capture_output=True)` swallows the
  child's stdout.
- 9.5 CC-59 removed the storage argument (16 analyses now ~0.9 MB/day). Burn
  and diversity arguments stand.

**10. THE REGISTRY CONTRADICTS A RULING IT WAS SUPPOSED TO IMPLEMENT**
[R1, R2, R7] — was 7.
- 10.1 D-015 ruled ALL fallbacks uniform on mistral-small. Six roles still
  carry groq/gpt-oss-20b legs: lens3, ai5_watchdog, s2gap, entity_extract,
  s3a_patterns, s3d_longterm. **Uniformity was never achieved** — the current
  state is already mixed, which weakens any "D-015 must not be reversed"
  argument. See D-025.
- 10.2 **S3-A specifically.** Its leg "inherits max_out and hits the same
  939-token ceiling — broken and unprobed" (CC-12, 2026-08-01), still true.
- 10.3 **RULING NEEDED: do the dead Cerebras PRIMARIES stay in front of the new
  legs, or get repointed?** Priced: two wasted attempts and ~65s per position
  per wave, forever. **Lean REPOINT** — and Groq `openai/gpt-oss-120b` answered
  **200** on a cold call this session, same family as the dead Cerebras model,
  so a target exists. Constraint: Groq 8K TPM fits S2-D and S2-E, **not MA**.
- 10.4 **RULING NEEDED:** three fallback shapes exist — S2-A's hardcoded
  dual-path `model=None` (CC-14), MA/S2-E/S2-D's `_call_fallback_leg` copies,
  and nothing elsewhere. Extract a shared helper. Item 3's one-line change
  lands in every one of these; do them together or do the helper first.

**11. THE RECORD CANNOT ATTRIBUTE** [R6, R3] — was 8.
- 11.1 **CC-57 — the saved row does not say which leg produced it.** Design
  settled (`parsed["_wire"]` on both legs). BLOCKER: `lens_mission_analyst.py`
  `:487` does `truncate(json.dumps(evidence), MAX_S2_CHARS)`, so any key added
  to `evidence` enters MA's prompt. Fork: (A) accept and size it; (B) a real
  column or side table; (C) log-only. Not (D) overloading `source_id`.
- 11.2 `lens_reports.domain_focus` is the literal "ALL" on all rows.
- 11.3 `cycle` is the literal "manual" on every row.
- 11.4 Arrival counts ROWS, not distinct lens identity.
- 11.5 Zero of 243 macro reports have ever cited a named lens.
- 11.6 The `S2-E FALLBACK` log lines read "for unknown" (BUG-002) — still live,
  confirmed again on `34742970144`.
- 11.7 **NEW: `lens_s3b_truehistory.py` calls `mistral-small-latest`**, the
  floating alias, not the pinned dated id D-015 requires. Alias and pin are
  both live in the same codebase.

**12. CERTIFICATION MEASURES MECHANICS, NOT BEHAVIOUR** [R8] — was 9.
- 12.1 Make a calibration band part of every migration cert. **Now possible:**
  `--save-output` shipped this session, and the band scored `ministral-8b` on
  its first use.
- 12.2 **RETRO OWED:** MA, S3-A, S3-D, lens3, lens4 and s2f_primary all moved
  to Cerebras on 2026-07-28 and none was measured across that boundary.
- 12.3 **THREE TRIALS IS NOT A CERTIFICATION.** Proven this session:
  `ministral-8b` returned 3/3 valid JSON, then 1/3 on byte-identical input.
  Raise the floor to five and record the parse rate, not a pass/fail.
- 12.4 **CC-58's `⏭ SKIP` and `⚠️ DEGRADED` paths remain UNEXERCISED live.**
  CC-60's DEGRADED path is in the same position: shipped, never fired, because
  every failing wave has been total rather than partial.
- 12.5 **The probe borrows nothing and says so.** For an unregistered pairing
  `fit_max_tokens` logs `ceiling UNRESOLVED -> cap` rather than using a
  neighbouring row. Correct; do not "fix" it by inferring.

**13. NOTHING WATCHES PROVIDER LIFECYCLE** [R7, R1] — was 10. Cerebras
announced 07-17, died 08-17, still burns two attempts per position per wave.
Mistral's small class died 09-04 with no announcement at all. Known ahead:
**gemini-2.5-flash dies 2026-10-16** (lens2, S2-B, S3-B) — and `S2-C` still
calls `gemini-2.0-flash`, dead since 2026-06-01, so the October date will not
announce itself either. **Mistral's `/v1/models` exposes `deprecation` and
`deprecation_replacement_model` per model** — a watcher for that provider is
one scheduled call plus a diff.

**14. THE OUTAGE POST-MORTEM** [R3, R7] — was 11. Both claims discharged into
LR-160 and LR-161. Retained for one open fact: **why the Supabase restriction
actually lifted on Aug 31 is UNKNOWN**, the average-daily model does not
explain it, and it should stay written as unknown.

**15. PROMPT PACKING AND VALUE ORDER** [R3] — was 12. MA's S1 allotment
oscillates near the ceiling (96.0%, 95.5%). S2 admissions 3 of 27.
`S1_PARTIAL_ARRIVAL` has never fired live. **Blocked behind item 1** — MA has
not completed a synthesis since Sep 8, so no new packing data exists.

**16. GROQ TPD REFILL LAW** [R3] — was 13. Measured 2026-08-10, now **35 days
stale**, downstream of item 9.

**17. INPUT-QUALITY CLUSTER** [R3] — was 14. BUG-001: batch 1 held 44 articles,
the 9,000-char cap fired at 21, and the user message still states the
pre-truncation count.

**18. CI PROVES COMPILATION, NOT BEHAVIOUR** [R2, R8] — was 15.
- 18.1 CI runs three steps: `py_compile code/*.py`, the registry self-test, and
  one test file. 35 tests passed on this session's pushes.
- 18.2 **NEW: CI DOES NOT COVER REPO-ROOT FILES.** The glob is `code/*.py`.
  `probe_lens_models.py` lives at the root, so the two commits that changed it
  got a green CI that had never opened the file. **The instrument that
  certifies models before they are wired sits outside every gate.** Either move
  it under `code/` or widen the glob; do not leave the green misleading.

**19. DEAD-SYMBOL GATE** [R2] — was 16. `check_groq_tpm` is defined twice
byte-identically in `lens_s2_orchestrator.py` (:38-67, :69-98) AND in
`lens_s3_orchestrator.py` (:27-56, :58-87).

**20. THE MAP IS STALE IN MORE PLACES THAN THE TERRITORY** [R6] — was 17.
`lens_s2_orchestrator.py:15` still says `llama-3.3-70b / GROQ_S2E_API_KEY`;
`lens_s2e_legitimacy.py:49` still says max_out 10,000 against the registry's
16,000. Add: `lens_regular_report.py:4` says "Model: mistral-small-latest
(free) -> Cerebras fallback" and the Cerebras leg is unreachable (CC-44).

**21. REGISTER AND ROUTING HYGIENE** — was 18.
- 21.1 `grep -c "^## LR-"` under-reports by seven (LR-117..123 sit in a compact
  block). Precedent `1bbb6f3`.
- 21.2 CC-24 appears in NO decision record.
- 21.3 **UNTRACKED FILES NOW FIFTEEN.** The ten from LENS-039 plus this
  session's `_s40_mistral_models.json`, `_s40_wave2.log`, `_s40_rr_green.log`,
  `_s40_rr_red.log`, `_s40_bodies/`, `_s40_bodies_jsonmode/`,
  `_s40_rr_bodies/`. The `_s40_*` set is evidence from this session and should
  be either committed under `docs/sessions/` or deleted deliberately — not left
  to accumulate. Decide at the LENS-041 open.

**22. WAVE SEQUENCING AND CROSS-POSITION SPACING** [R3] — was 19.
Cross-position spacing cannot live in a per-position TPMGuard (LR-112).
TPMGuard paces against the PRIMARY's TPM on a fallback path.
- 22.1 Measured gaps between collect and manage-analyze starts over five waves:
  12m34s, 35m54s, **26 SECONDS**, 36m50s, 11m13s. If manage-analyze starts
  before collect finishes, MA analyses the previous wave and reports success.

**23. THE REFERENCE EXPORT IS PARTLY DECORATIVE** [R6, R2] — was 20.
- 23.1 `source_tier` and `also_s1_pool` are meaningless in every S2 Excel ever
  sent: `lens_article_refs` is selected without `source_id`
  (`lens_ref_system.py:102`, `:146`) while `:296`/`:558` look up
  `tier_map[source_id]`, so every row takes the `TIER2` default.
- 23.2 `s2_excel_offline.py` is untracked. Support it or delete it.
- 23.3 That tool reproduces 23.1's defect ON PURPOSE; fix both together or they
  stop being comparable.
- 23.4 This export is how a human would have seen S2-E producing nothing.

**24. TIMESTAMP ENCODING DEFECT** [R3] — was 21. The `+` in
`2026-09-10T00:00:00+00:00` reaches the server as a space; HTTP 400 `22007` on
`lens_pipeline_runs` and on the `lens_reports` gemini check. Two call sites,
both silently returning `[]`. The logger's `-- returning [] (NOT 'no rows')` is
exactly right and is why it was findable; copy that pattern.

### OTHERS

**25. MEASUREMENT ODDITIES, BANKED** — was 22.
- `actual_prompt - counted_total = 149` on twenty-two consecutive waves.
- chars/token on `mistral-small-2603`: 4.738 (MA), 4.19 (S2-E), 3.80 (S2-D) —
  CONTENT-dependent, never a model constant.
- `lens_raw_articles` is efficient (~1.3 KB/row).
- Cron delay ran 3h36m–4h53m (Sep 8-10) against 33-71 min on Aug 19.
- `reltuples` in `pg_class` is a last-ANALYZE snapshot wearing a live-looking
  column name.
- **NEW:** Mistral's Usage page showed $0.00 for September while the
  Subscription page showed $0.16 of the same allowance. Two numbers, one
  account, same day. Not chased.
- **NEW:** `ministral-8b` produced 9,041–10,598 chars on S2-E's prompt against
  `gpt-oss-120b`'s 4,690–5,085 on **S2-D's** — different positions, not
  comparable. Recorded because the comparison was drawn wrongly in-session and
  withdrawn.

## WATCH ITEMS
- **16 OCTOBER 2026** — gemini-2.5-flash dies (lens2, S2-B, S3-B).
- **~LATE OCTOBER 2026** — at ~1.6 MB/day from 374 MB the 450 MB trigger
  arrives in about 45 days. A PROJECTION from an 11-day sample, not re-measured
  since 2026-09-11.
- Billing cycle **11 Sep – 11 Oct**. Quota is average daily size, 500 MB.
- Mistral's Free plan allowance resets on the first of each calendar month —
  **1 October** is the next reset. It is NOT known whether that affects the
  small-class block, because the block is not an allowance problem (1.2).
- Partner A: `grep -rn "\.delete()"` across Partner A returns ZERO. R9 and D-022
  belong in the next Lens-to-Partner-A transfer packet.

## STANDING BLOCKER
$0/month. D-019 stands.

## DECISIONS RECORDED THIS SESSION
- **D-025 — item 2.3 (old numbering) is RULED, and the fork was wrong.**
  (B) shared pacer: **OUT** — disproven by measurement, 65–244s spacing refused
  100%. (A) second Mistral key: **OUT** — Mistral's limits are organisation-
  level, so a second key in the same org isolates nothing; this is LR-094's
  disease exactly. A genuinely separate Mistral *account* would be a different
  proposal and is not on the table at $0. (C)/(D) survive as one path:
  **repoint off the blocked model.** The reasoning-starvation evidence behind
  D-015 was re-read first, as the order required: it rules out `gpt-oss-20b`
  specifically, not diversity, so this is not a reversal of D-015.
- **D-026 — `ministral-8b-2512` + `response_format` is the certified candidate
  for S2-E.** 5/5 parse, band-consistent counts, 262,144 context, no
  deprecation flag. Wiring is NOT done and must not happen without item 3.
  `ministral-14b-2512` is the closer size match and stays open as an
  alternative — it is unprobed on S2-E and carries the page's lowest RPS
  (0.50), which matters for any position that calls more than once a minute.
- **D-027 — the Regular Report's model is NOT swapped until item 2.1 is
  fixed.** A swap alone converts a loud failure into a silently truncated
  delivery. Reversible-before-irreversible does not apply; loud-before-silent
  does.

## CHANGED THIS REGENERATION
- CLOSED: old item 1 (CC-60 certified on two waves against a banked
  prediction); old 2.3 (ruled, D-025); old 9.3 (shipped, `522ff8c`).
- REWRITTEN, not re-ranked: old item 2 becomes item 1 with a **different
  diagnosis**. Convergence/burst is disproven. Recorded as a correction rather
  than silently replaced.
- NEW ROOT PROPOSED: **R11** — we take provider defaults and never ask for the
  guarantees on offer. BRO ALPHA RULES.
- NEW: item 2 (Regular Report truncation, two defects), item 3 (no JSON
  constraint on twelve legs), item 4 (registry Mistral limits wrong in every
  field), item 8 (nobody reads the red workflows), 18.2 (CI does not cover
  repo-root files), 11.7 (alias vs pin live together), 6.4 (row counts
  re-measured).
- RE-RANKED: old 3→5, 4→6, 5→7, 6→9, 7→10, 8→11, 9→12, 10→13, 11→14, 12→15,
  13→16, 14→17, 15→18, 16→19, 17→20, 18→21, 19→22, 20→23, 21→24, 22→25.
- DECISIONS: D-025..D-027 recorded.
- ITEM NUMBERS: 1..25, asserted unique, no gaps. Old items checked off
  one-by-one against `LENS_TARGET_AND_ORDER_LENS040.md` — the LENS-039 close
  dropped an item on its first pass and this check exists because of it.

## NEXT SESSION'S MISSION
**Item 3 — send `response_format` on all twelve Mistral JSON legs — then item 1,
repoint the blocked positions onto `ministral-8b-2512`, and certify both on one
wave.**

Item 3 first, because item 1's certified candidate parses 4/6 without it and
5/5 with it: wiring the model before the constraint ships a position that fails
a third of the time. Item 4 (the registry rows) is the same commit's natural
companion — `fit_max_tokens` cannot fit what it cannot resolve.

Item 2 is the visible daily loss and is deliberately NOT the mission: it needs
2.2 ruled first, and its fix is a budget question, not a model question.

Bank the prediction before opening any log. The gate for item 1 is a wave in
which S2-E returns COMPLETE with a non-zero `reports_saved` — anything else,
including an honest ANALYSIS_FAILED, means the repoint did not take.
