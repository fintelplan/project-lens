# LENS TARGET AND WORKING ORDER — generated at the LENS-039 close (2026-09-11)
**Regenerated 2026-09-11 at the LENS-039 close. SUPERSEDES the 2026-08-24 version.**
Ranks are TARGET-RELATIVE. Freshness confers no priority. This file holds the
target instance and the path; LENS_CONTRACT.md holds the mission and the method.

**THE FILENAME CARRIES THE CONSUMING SESSION** (CONTRACT v4): this file is
`docs/LENS_TARGET_AND_ORDER_LENS040.md` and the OPEN prompt resolves the LATEST
by glob.

## CURRENT TARGET (declared 2026-08-06, unchanged)
Every scheduled wave produces valid intelligence, unattended, with **NO SILENT
FAILURE**. Not "no defects". A system with logged, ordered, non-silent defects
is done. A system with one silent failure is not.

**Note on the target's status right now:** Lens is ONLINE and waves are green —
and the target is BREACHED. Two positions report success while doing nothing.
That is the whole of item 1.

## ROOTS
- **R1 — Deprecation is weather, and we have no forecast.** Models and whole
  free tiers die on someone else's schedule.
- **R2 — Written but never wired.** Code that declares a capability no call
  site reads. **Struck twice more this session:** `all_collected` was written
  on every S1 row since April and read by nothing; `lens_write_guard.py` is a
  253-line Layer 4 guard with zero call sites.
- **R3 — Numbers set once, never re-derived against reality.**
- **R4 — Epistemic diversity is assumed, not verified.** Now has a second,
  sharper form — see item 2.
- **R6 — The record cannot attribute.**
- **R7 — Nothing watches our external dependencies' lifecycle.** RULED IN.
- **R8 — Certification measures mechanics, not behaviour.** RULED IN.
- **R9 — Free-tier resources are CONSUMED, not merely rate-limited, and
  nothing meters what we accumulate.** RULED IN by James at LENS-039.
  Confirmed: the write side was measured live at ~268 KB/row on 2026-09-11 and
  is now fixed; nothing still measures database size.
- **R10 — A POSITION'S DECLARED STATUS IS NOT DERIVED FROM ITS OWN WORK.**
  PROPOSED at LENS-039, **JAMES RULES.** `lens_s2e_legitimacy.py` returns
  top-level `"status": "COMPLETE"` in the same JSON object that carries
  `"reports_saved": 0` and four sub-results of `"status": "FAILED"`. The
  orchestrator's allowlist reads the top level and is correct to do so. The lie
  is inside the position. Distinct from R2 (a capability nothing calls) and
  from R8 (a cert that measures the wrong thing): R10 is a component that
  computes an answer and then reports something else. Evidence: S2-E and S2-C,
  five consecutive waves, live today.

## CLOSED THIS SESSION
- **Old item 1.2 — RESTORATION.** The 402 is gone; live SELECTs on
  `lens_reports`, `lens_raw_articles`, `injection_reports` and
  `lens_macro_reports` all returned rows on 2026-09-11 02:41Z. Nothing had to
  be re-enabled because 1.5 was never executed — see below.
- **Old item 1.3 — THE WRITE SIDE.** Shipped as `2a9639c` (CC-59) and
  CERTIFIED the same session against a prediction banked before the wave:
  `pg_column_size` 14,086 bytes against a predicted ~14 KB, shape
  `{"selected":[{"id","url"}]}` exactly, and a full-table check —
  not a sample — returning **0** rows containing `title`, `domain` or the
  second list. Measured effect **−94.7%** per row.
- **Old item 1.5 — THE DISABLE RULING.** Answered by events, and the order's
  lean was WRONG. See item 11.1; this is now LR-161.
- **Old item 2 — CC-58 CERT.** Certified on five consecutive waves
  (34256456580, 34318718752, 34381537666, 34444756075, 34505961936). Every
  anchor in the 2.1 prediction held: `⚠️  status=ANALYSIS_FAILED` in `_run`,
  `❌` in the summary, `N position(s) did not complete: [...]`, and
  **`grep -c "All positions complete"` = 0 on all five**. Default-deny did its
  job: MA returned `ANALYSIS_FAILED`, a string absent from the banked MA status
  universe, and CC-58 caught it instead of ticking green.

## WORKING ORDER

### URGENT

**1. SILENT FAILURE IS LIVE IN TWO POSITIONS** [R10, R8, R2] — NEW, and it
breaches the target's only absolute rule.
Measured on `34505961936` (2026-09-10 17:27Z), five waves deep:
- 1.1 **S2-E REPORTS `COMPLETE` AND SAVED NOTHING.** Its own JSON, in the log:
  `"status": "COMPLETE"`, `"reports_analyzed": 4`, `"reports_saved": 0`,
  `"results": [{"status":"FAILED"} x4]`, `"elapsed_seconds": 304.3`. Four lenses
  failed, zero reports were written, and the position declared success. The
  orchestrator printed `✅ S2-E`. CC-58 classified it correctly — `COMPLETE` IS
  on the allowlist. The allowlist is not the defect.
- 1.2 **S2-C IS THE SAME SHAPE.** `S2-C failed after 2 attempts for ALL` appears
  FOUR times (17:15:54, 17:16:38, 17:17:18, 17:17:59), preceded by
  `QUOTA_GUARD ERROR Model not found: gemini-2.0-flash — check model name`, and
  the summary still printed `✅ S2-C`. `gemini-2.0-flash` has been in the corpse
  map since 2026-06-01.
- 1.3 **THIS WAS SEEN AND WAVED THROUGH FIVE WEEKS AGO.** The LENS-038 status
  sweep found `FAILED`/`OK` at `lens_s2c_emotion.py:369,379` and
  `lens_s2e_legitimacy.py:595,607` and recorded them as "per-lens sub-results,
  not top-level status". True, and it stopped one question short: does the top
  level AGGREGATE them? It does not. Now LR-165.
- 1.4 **FIX SHAPE (not yet designed):** the top-level return must be derived
  from the sub-results — all FAILED = FAILED, some = DEGRADED, none = COMPLETE.
  Both files, and every other position must be audited for the same pattern
  before this is called done. S2-B, S2-D and MA are NOT in this class: they
  reported their failure honestly.
- 1.5 **FALSIFIABLE PREDICTION, BANKED 2026-09-11 (unread).** If the fix lands,
  a wave whose S2-E lenses all fail MUST show a top-level status that is NOT
  `COMPLETE`, `❌ S2-E` in the summary, S2-E's name inside the
  `did not complete:` list, and `grep -c "All positions complete"` still 0.
  If `reports_saved: 0` ever again sits next to `"status": "COMPLETE"`, the fix
  did not take.

**2. EVERY FALLBACK CONVERGES ON ONE MISTRAL KEY, AND IT IS 429** [R4, R1, R3]
— NEW. This is the proximate cause of S2-B, S2-D and MA failing on five
consecutive waves.
- 2.1 **MEASURED on `34505961936`.** The pattern is identical at every
  position: Cerebras primary returns `402 payment_required` on attempts 1 and
  2 (expected — Cerebras died 2026-08-17, items 7 and 10), then the Mistral
  fallback returns `HTTP 429 {"code":"1300","message":"Rate limit exceeded"}`.
  Seven fallback attempts in one wave, seven 429s: S2-D x2 (17:18:22, 17:18:33),
  S2-E x4 (17:19:58, 17:21:14, 17:22:29, 17:23:44), MA x1 (17:24:56).
- 2.2 **THE CAUSE IS OUR OWN RULING, NOT THE PROVIDER.** D-015 made every
  fallback uniform on mistral-small. While primaries were healthy that was one
  or two calls a wave. With the primaries dead it is every call in the wave,
  through one key, inside four minutes. LR-094's quota isolation covers
  primaries only. Now LR-164.
- 2.3 **RULING NEEDED.** Fork: (A) a second Mistral key, isolated per position
  group — cheapest, does not touch model choice, but it is a workaround for
  self-inflicted burst; (B) stagger fallback calls behind a shared pacer —
  correct, and item 18 says cross-position spacing has no home to live in;
  (C) restore per-role diversity in the fallback legs, which is D-015 partially
  reversed and needs the reasoning-starvation evidence re-read first.
  **Lean (A) now and (B) as the real fix**, because the primaries are not
  coming back and the 429 is costing every wave's intelligence today. James
  rules. Do NOT take (C) without re-reading why D-015 was made.
- 2.4 **THE 402s IN THESE LOGS ARE CEREBRAS, NOT SUPABASE.** A bare
  `grep -c "402"` on a wave log lumps a dead-provider constant with a database
  outage and cannot indict either (LR-153 again — it happened again this
  session). Count `payment_required` per POSITION, never per file.

**3. RETENTION POLICY — DESIGNED, NOT BUILT** [R9] — was 1.4. D-022 recorded
below. No code exists. Consumer read-windows are now MEASURED, not assumed:
raw_articles 48h (`analyze_lens_multi.py:95`), reports 30d
(`s3b`/`s3c`/`s3d LOOKBACK_DAYS`), injection_reports 45d
(`VERIFICATION_WINDOW_DAYS`), article_refs 3d (`lens_regular_report.py:159`).
- 3.1 **`lens_reports` MAY NEVER LOSE A ROW.** `lens_s4_upgrade_monitor.py:72`
  counts every row since `DAY_1_UTC` (2026-04-17) with no upper bound, and that
  count IS the S4-E maturity counter. Deleting old rows makes the system report
  itself as less mature, and it would do so silently. Shrink instead.
- 3.2 **DELETE DOES NOT RECLAIM SPACE.** Rows become dead tuples; plain VACUUM
  does not shrink the file. `VACUUM FULL` does, takes an exclusive lock, and
  needs headroom. Any automatic cleanup is a TWO-STEP and must run outside a
  wave — and "outside a wave" is hard to predict while cron delay runs 3-5h
  (item 21).
- 3.3 **ARCHIVE BEFORE DELETE, unconditionally.** CSV out to a GitHub release
  (not counted against Supabase) before any row is removed. If the archive
  fails, the delete does not run.
- 3.4 **RAW ARTICLES AND ARTICLE REFS MUST SHARE A WINDOW.** `articles_used`
  ids resolve against `lens_raw_articles`; a ref pointing at a deleted article
  is D-020's failure mode arriving by another road.
- 3.5 **HISTORICAL SHRINK STILL OWED.** August 16 onward was left fat on
  purpose during the outage; `lens_reports` is 98 MB of which 93 MB is TOAST.
  Re-running the LENS-038 UPDATE pattern on those rows should return ~33 MB.
  Preserve the double-encoding shape and put the idempotency guard in the
  WHERE clause (LR-159).
- 3.6 **NEW, MEASURED 2026-09-11:** all five rows of one wave carry a
  BYTE-IDENTICAL `articles_used` (14,086 bytes each, identical prefix), because
  `article_ids` is computed once and written by each lens. Normalising to one
  wave-level row would cut what remains by ~80%. Low priority while growth is
  ~1.6 MB/day; recorded so it is not rediscovered.

**4. NO SIZE MONITORING** [R9, R7] — was 1.6, and now the only unaddressed half
of R9.
- 4.1 `pg_database_size()` is not reachable through PostgREST. A Postgres
  function called via `sb.rpc()` is required — that is the implementation, not
  a detail to discover later.
- 4.2 **THRESHOLDS (D-022):** WARN at 400 MB, cleanup trigger at 450 MB,
  cleanup target 350 MB. 400 MB is 80% of quota, the top of ISO/IEC 27002
  Control 8.6's stated band; a single 450 threshold would make the trigger and
  the alarm the same event, leaving no voice if cleanup fails.
- 4.3 **A FREE DETECTOR ALREADY EXISTS AND IS UNREAD.** Across 16 dead waves and
  22 live ones the separation is absolute: database-dead manage-analyze runs
  last 19-27 SECONDS, healthy ones 21-32 MINUTES. Any run under 60s means the
  database was unreachable. No code needed to observe it; a line of code to
  alarm on it.

**5. THE WRITE GUARD IS NEVER CALLED** [R2] — NEW.
`lens_write_guard.py` is 253 lines documented as "Layer 4 of 5". A repo-wide
grep for `validate_write` finds it in the module itself and in two docstrings
in `lens_audit_guard.py:10` and `lens_preflight_guard.py:10`. **Zero call
sites.** Two pieces of evidence that it has never run: `required` includes
`generated_at`, which neither S1 record dict sets, so every S1 write would
ABORT; and `types` declares `"articles_used": int` when the value is a JSON
string. Decide: wire it (and fix the schema first), or delete it and stop
claiming five layers.

**6. SYSTEM 1 IS ONE SURVIVOR QUADRUPLED** [R2, R3, R4] — was 3
- 6.1 `lens_orchestrator.py:375` passes `--single-lens`; `analyze_lens_multi.py`
  has no `sys.argv`, no `argparse`, no `LENS_ID`, so every invocation runs all
  four lenses. Parent loops 1..4, so **every wave runs 16 lens analyses, not
  4**, and healing spawns more.
- 6.2 **CANARY THREE-ARM GAS-MASK TEST BEFORE ANY EDIT HERE.** Arm 2 is the
  live risk: the Collection import chain is S1's air supply.
- 6.3 lens3 and lens4 remain on dead Cerebras. Wire AFTER 6.1; lens3's leg is
  groq/gpt-oss-20b — see item 7.
- 6.4 S1 burn is UNMEASURED. `subprocess.run(capture_output=True)` swallows the
  child's stdout.
- 6.5 **THE STORAGE ARGUMENT IS WEAKER NOW, NOT GONE.** CC-59 cut the per-row
  cost by 94.7%, so 16 analyses now cost ~0.9 MB/day instead of ~13. The burn
  and diversity arguments are untouched. Re-ranked DOWN accordingly, honestly:
  this item was raised partly on storage grounds that CC-59 removed.

**7. THE REGISTRY CONTRADICTS A RULING IT WAS SUPPOSED TO IMPLEMENT**
[R1, R2, R7] — was 4
- 7.1 D-015 ruled ALL fallbacks become mistral-small, uniform. Six roles still
  carry groq/gpt-oss-20b legs: lens3, ai5_watchdog, s2gap, entity_extract,
  s3a_patterns, s3d_longterm. **Read item 2.3 before acting here** — the two
  items now pull in opposite directions and must be ruled together.
- 7.2 **S3-A specifically.** Its registry note says the leg "inherits max_out
  and hits the same 939-token ceiling — it is broken and unprobed" (CC-12,
  2026-08-01), still true. Registry edit plus an LR-106 probe.
- 7.3 RULING NEEDED: do the dead Cerebras PRIMARIES stay in front of the new
  legs, or get repointed? Lean KEEP was the old position; item 2.1 now prices
  it — two wasted attempts and ~65 seconds per position per wave, on every
  wave, forever. **Lean now REPOINT.**
- 7.4 RULING NEEDED: three fallback shapes exist — S2-A's hardcoded dual-path
  `model=None` (CC-14), MA/S2-E/S2-D's `_call_fallback_leg` copies, and nothing
  elsewhere. Extract a shared helper. CC-58's cert is now DONE, so the
  precondition is cleared.

**8. THE RECORD CANNOT ATTRIBUTE** [R6, R3] — was 5
- 8.1 **CC-57 — the saved row does not say which leg produced it.** Design
  settled (`parsed["_wire"]` on both legs, copied into `evidence` at save).
  BLOCKER: `lens_mission_analyst.py:487` does
  `truncate(json.dumps(evidence), MAX_S2_CHARS)`, so any key added to
  `evidence` enters MA's prompt. Fork: (A) accept and size it; (B) a real
  column or side table; (C) log-only. Not (D) overloading `source_id`.
  Option (B) keeps the weight it gained at LENS-038.
- 8.2 `lens_reports.domain_focus` is the literal "ALL" on all rows, from
  `analyze_lens_multi.py:1206`. Identity IS recoverable from `summary`'s
  `[<Lens Name> — <perspective>]` prefix or from `prompt_version`.
- 8.3 `cycle` is the literal "manual" on every row; wave scoping must come
  from `run_id` or a `generated_at` recency floor.
- 8.4 Arrival counts ROWS, not distinct lens identity.
- 8.5 Zero of 243 macro reports have ever cited a named lens; 236 cite S2 only.
- 8.6 The `S2-E FALLBACK` log lines read "for unknown" — confirmed again on
  `34505961936` (`S2-E failed after 2 attempts for unknown`, four times). The
  lens name is absent exactly where item 1.1's four FAILED sub-results would
  need naming. Kin to 8.2, and now load-bearing for item 1.

**9. CERTIFICATION MEASURES MECHANICS, NOT BEHAVIOUR** [R8] — was 6
- 9.1 Make a calibration band part of every migration cert.
- 9.2 **RETRO OWED, NOW UNBLOCKED:** MA, S3-A, S3-D, lens3, lens4 and
  s2f_primary all moved to Cerebras on 2026-07-28 and none has been measured
  across that boundary. The database is back; the evidence is reachable.
- 9.3 `probe_lens_models.py` (at the REPO ROOT, not `code/`) has no mistral
  caller, so `--candidate fallback` cannot exercise any mistral leg. **Item 2
  makes this urgent** — the fallback path is now the ONLY path, and it is the
  one path no probe can exercise.
- 9.4 **CC-58's `⏭ SKIP` and `⚠️ DEGRADED` paths remain UNEXERCISED live.**
  Five waves produced OK and FAIL only. A path with no live evidence is not
  certified, and CC-58's cert above is explicitly a two-label cert.

**10. NOTHING WATCHES PROVIDER LIFECYCLE** [R7, R1] — was 7
Cerebras announced 2026-07-17, died 2026-08-17, took five positions and is
still burning two attempts per position per wave. Supabase warns at 20% of a
limit and that mail was not read. Known ahead: **gemini-2.5-flash dies
2026-10-16** (lens2, S2-B, S3-B) — and item 1.2 shows gemini-2.0-flash is
ALREADY referenced somewhere live despite dying in June, so the 16 Oct date
will not announce itself either.

**11. THE OUTAGE POST-MORTEM: TWO BANKED CLAIMS FAILED** [R3, R7] — NEW
- 11.1 **"OFFLINE UNTIL 11 SEPTEMBER" WAS WRONG BY ELEVEN DAYS.** Measured from
  the run list: last healthy wave #288 Aug 22 13:51Z, first dead #289 Aug 23
  02:43Z, last dead #304 Aug 30 17:40Z, **first recovered #305 Aug 31 07:18Z**.
  The outage lasted 8 days 4 hours, 16 dead waves. The claim came from repeating
  a vendor banner as if it were a measurement. **Pruning DID lift the
  restriction early** — the "average daily size" model cannot explain Aug 31
  (the cycle average was still ~2x quota), so the actual mechanism is UNKNOWN.
  Do not replace one vendor story with another; measure. Now LR-160.
- 11.2 **THE DISABLE LEAN WAS WRONG.** The red crons were the ONLY thing that
  announced the recovery. Had they been disabled, eleven further days of
  intelligence would have been lost with no signal to say otherwise. A failing
  scheduled job is a free liveness detector for an external dependency. Now
  LR-161, and item 4.3 turns it into an instrument.

**12. PROMPT PACKING AND VALUE ORDER** [R3] — was 8
MA's S1 allotment oscillates near the ceiling (96.0% then 95.5%) rather than
climbing. S2 admissions 3 of 27. `S1_PARTIAL_ARRIVAL` has still never fired
live. **Stale in a new way:** MA has not completed a synthesis in five waves,
so no new packing data exists since Sep 8. Blocked behind item 2.

**13. GROQ TPD REFILL LAW** [R3] — was 9. Measured 2026-08-10, now **32 days
stale** and downstream of item 6.

**14. INPUT-QUALITY CLUSTER** [R3] — was 10. BUG-001: batch 1 held 44
articles, the 9,000-char cap fired at 21, and the user message still states the
pre-truncation count.

**15. CI PROVES COMPILATION, NOT BEHAVIOUR** [R2, R8] — was 11.

**16. DEAD-SYMBOL GATE** [R2] — was 12. `check_groq_tpm` is defined twice
byte-identically in `lens_s2_orchestrator.py` (:38-67, :69-98) AND in
`lens_s3_orchestrator.py` (:27-56, :58-87).

**17. THE MAP IS STALE IN MORE PLACES THAN THE TERRITORY** [R6] — was 13.
`lens_s2_orchestrator.py:15` still says `llama-3.3-70b / GROQ_S2E_API_KEY`;
`lens_s2e_legitimacy.py:49` still says max_out 10,000 against the registry's
16,000.

**18. REGISTER AND ROUTING HYGIENE** — was 14
- 18.1 **FORMAT NORMALISATION, still open.** `grep -c "^## LR-"` under-reports
  by seven because LR-117..123 sit in a compact block. Precedent `1bbb6f3`.
- 18.2 CC-24 appears in NO decision record.
- 18.3 **GREW AGAIN — now TEN.** The eight from LENS-038 plus this session's
  `patch_cc59_v3.py` and `probe402.py`. `s2_excel_offline.py` (item 19.2)
  deserves a tracked home rather than the same bin. `_s39/` holds five wave
  logs and is also untracked.

**19. WAVE SEQUENCING AND CROSS-POSITION SPACING** [R3] — was 15.
Cross-position spacing cannot live in a per-position TPMGuard (LR-112).
TPMGuard still paces against the PRIMARY's TPM on a fallback path — which,
per item 2, is now every path.
- 19.1 **NEW, MEASURED 2026-09-11.** Collect and manage-analyze are declared 28
  minutes apart but queue independently. Measured gaps between actual starts
  over five waves: 12m34s, 35m54s, **26 SECONDS**, 36m50s, 11m13s. On Sep 9
  evening the ordering nearly inverted. If manage-analyze ever starts before
  collect finishes, MA analyses the PREVIOUS wave's articles and reports
  success. Cron delay itself is GitHub queue depth (James, 2026-09-11) and is
  not ours to fix; the ordering assumption is.

**20. THE REFERENCE EXPORT IS PARTLY DECORATIVE** [R6, R2] — was 16
- 20.1 **`source_tier` and `also_s1_pool` are meaningless in every S2 Excel
  ever sent.** `lens_article_refs` is selected without `source_id`
  (`lens_ref_system.py:102`, `:146`) while `:296` and `:558` look up
  `tier_map[source_id]`, so every article row takes the `TIER2` default and
  Sheet 3's "By Tier" breakdown is a single bar. Verifiable by opening any past
  workbook. Fix = join `source_id` in from `lens_raw_articles`.
- 20.2 `s2_excel_offline.py` was built during the outage and is untracked. It
  imports `lens_ref_system` and calls that module's own `get_s2_selected()` and
  `build_excel()` against three CSVs. Decide whether it becomes a supported
  tool or is deleted now that the pipeline is restored.
- 20.3 That tool reproduces 20.1's tier defect ON PURPOSE so its output matches
  the workbooks James is used to; a `--fix-tier` flag produces a DIFFERENT
  workbook. Whoever fixes 20.1 must fix both together or the two stop being
  comparable.
- 20.4 **RELEVANT TO ITEM 1:** this export is how a human would have seen S2-E
  producing nothing. It has been partly decorative the whole time.

**21. TIMESTAMP ENCODING DEFECT** [R3] — NEW, live and logged.
`[SB] GET lens_pipeline_runs HTTP 400 {"code":"22007"}` and the same on
`lens_reports`: the `+` in `2026-09-10T00:00:00+00:00` reaches the server as a
space. Two call sites, both silently returning `[]`. The logger says
`-- returning [] (NOT 'no rows')`, which is exactly right and is why this was
findable at all — note it as a pattern worth copying.

### OTHERS

**22. MEASUREMENT ODDITIES, BANKED** — was 17
- `actual_prompt - counted_total = 149` on twenty-two consecutive waves.
- chars/token on `mistral-small-2603`: 4.738 (MA), 4.19 (S2-E), 3.80 (S2-D) —
  CONTENT-dependent, never a model constant.
- `lens_raw_articles` is efficient (~1.3 KB/row); the duplication was entirely
  in `lens_reports`.
- **NEW:** cron delay ran 3h36m to 4h53m across five waves (Sep 8-10), against
  33-71 min measured on Aug 19. James attributes it to GitHub queue load.
  Banked, not acted on.
- **NEW:** `reltuples` in `pg_class` read 3,815 for `lens_reports` while the
  live count was 3,949 — it is the last-ANALYZE snapshot, i.e. a banked number
  wearing a live-looking column name.

## WATCH ITEMS
- **16 OCTOBER 2026** — gemini-2.5-flash dies (lens2, S2-B, S3-B).
- **~LATE OCTOBER 2026** — at the measured post-CC-59 rate of ~1.6 MB/day from
  374 MB, the 450 MB trigger arrives in about 47 days. Item 3.5's shrink pushes
  it further. This is a PROJECTION from an 11-day sample, not a measurement.
- Billing cycle is now **11 Sep – 11 Oct**. Quota is average daily size, 500 MB.
- The LENS-037 calibration-band prediction still holds and still needs a longer
  series.
- GNI: `grep -rn "\.delete()"` across GNI_Autonomous returns ZERO. R9 and D-022
  belong in the next Lens-to-GNI transfer packet.

## STANDING BLOCKER
$0/month. D-019 stands.

## DECISIONS RECORDED THIS SESSION
- **D-022 — the retention policy.** `lens_reports`: never delete a row (item
  3.1); shrink rows older than 30 days instead. `lens_raw_articles`: 120 days.
  `lens_article_refs`: 120 days, locked to raw_articles. `injection_reports`:
  180 days. The other twenty tables: NO retention rule — together they are
  ~24 MB, and twenty rules would be cost without benefit; revisit if any one
  passes 10 MB. Every deletion archives to CSV first or does not run.
  Watermarks: WARN 400 MB, trigger 450 MB, target 350 MB — 350 rather than 400
  because it makes VACUUM FULL a ~3-weekly event rather than a ~10-day one and
  leaves 100 MB of runway for growth we have not predicted.
- **D-023 — `articles_used` stores `{id, url}`, not id alone.** id-only is 2.6%
  smaller but blanks the no-ref fallback rows at `lens_ref_system.py:228-236`,
  whose frequency is unmeasured. Keeping `url` preserves `ref_by_url` matching
  and leaves those rows a working link. Paying 2.6% to avoid spending a wave
  measuring a risk.
- **D-024 — item 1.3 was shipped BEFORE item 1.4 despite ranking below it.**
  The ranking (a retention policy outranks one column's write side) is not
  disputed and did not change. Shipping order is a separate axis: 1.3 was two
  reversible line changes removing 68% of growth; retention is irreversible
  deletion needing an archive mechanism first. Reversible before irreversible.

## CHANGED THIS REGENERATION
- CLOSED: old 1.2 (restoration), old 1.3 (CC-59 `2a9639c`, certified), old 1.5
  (answered by events), old item 2 (CC-58 certified on five waves).
- NEW ROOT PROPOSED: **R10** — a position's declared status is not derived from
  its own work. JAMES RULES.
- NEW at the TOP: item 1 (silent failure in S2-C and S2-E). It is the only open
  item that breaches the target's absolute rule.
- NEW: item 2 (fallback convergence on one Mistral key) — the proximate cause
  of three positions failing on every wave.
- NEW: item 5 (the write guard has no call sites), item 11 (the outage
  post-mortem and its two failed claims), item 20 (the timestamp encoding
  defect), 19.1 (measured wave-ordering margin of 26 seconds).
- RE-RANKED DOWN, stated honestly: old item 3 (System 1 quadrupled) becomes
  item 6, partly because CC-59 removed the storage argument it had gained.
- RE-RANKED: old 4->7, 5->8, 6->9, 7->10, 8->12, 9->13, 10->14, 11->15, 12->16,
  13->17, 14->18, 15->19, 16->20, 17->22.
- **DROP CAUGHT AND REPAIRED DURING THIS REGENERATION:** old item 16 (the
  reference export's dead tier columns and the offline tool) was omitted from
  the first pass of this file and restored as item 20, with 20.3 carrying the
  LENS-038 deliberate non-fix and 20.4 noting its bearing on item 1. Recorded
  rather than silently fixed: a regeneration that drops an item is exactly the
  failure this section exists to catch.
- DECISIONS: D-022..D-024 recorded.
- ITEM NUMBERS: 1..22, asserted unique, no gaps.

## NEXT SESSION'S MISSION
**Item 1 — make S2-E and S2-C stop lying, then audit every other position for
the same shape.** It is the only open item that breaches the target's absolute
rule, and it has been live and unread for five weeks.

Item 2 is the proximate cause of the failures those positions are hiding, and
2.3 is a ruling James owes; expect to need it in the same session, because a
fixed S2-E will simply report FAILED until the fallback has capacity. Take the
ruling first, ship item 1, and let the next wave certify both at once against
the prediction banked at 1.5.
