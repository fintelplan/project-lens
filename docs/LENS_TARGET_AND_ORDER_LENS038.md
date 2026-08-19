# LENS TARGET AND WORKING ORDER — generated at the LENS-037 close (2026-08-19)
**Regenerated 2026-08-19 at the LENS-037 close. SUPERSEDES the 2026-08-18 version.**
Ranks are TARGET-RELATIVE. Freshness confers no priority. This file holds the
target instance and the path; LENS_CONTRACT.md holds the mission and the method.

**THE FILENAME IS FIXED ON PURPOSE.** The OPEN prompt names
`docs/LENS_TARGET_AND_ORDER.md` literally, so the live order must always sit
at that path or a session opens on a stale list. The SESSION lives in the H1
above and in `git log -p`, never in the filename. Only an ARCHIVED order gets
a named copy, under `docs/archive/`, and only at a phase transition
(LENS_CONTRACT.md).

## CURRENT TARGET (declared 2026-08-06, unchanged)
Every scheduled wave produces valid intelligence, unattended, with **NO SILENT
FAILURE**. Not "no defects". A system with logged, ordered, non-silent defects
is done. A system with one silent failure is not.

## ROOTS
- **R1 — Deprecation is weather, and we have no forecast.** Models and whole
  free tiers die on someone else's schedule.
- **R2 — Written but never wired.** Code that declares a capability no call
  site reads. Now demonstrated at scale: every role declared a fallback leg
  and, until 2026-08-18, not one call site read one.
- **R3 — Numbers set once, never re-derived against reality.**
- **R4 — Epistemic diversity is assumed, not verified.**
- **R6 — The record cannot attribute.** Outputs do not carry enough
  provenance to reconstruct which instrument produced them.
- **R7 — Nothing watches our external dependencies' lifecycle.** PROPOSED at
  LENS-036, JAMES RULES.
- **R8 — Certification measures mechanics, not behaviour.** PROPOSED at
  LENS-037, JAMES RULES. We verify that a position RUNS; we have never
  verified that it BEHAVES the same after a change. Evidence: D-016 moved
  S2-E and S2-D to Cerebras on 2026-07-28 and both changed substantially
  (S2-E actors/row 4.00 -> 8.50; S2-D claims/row 8.8 -> 26.6) while both
  certs passed on finish_reason and budget_used. Three weeks unnoticed.
  Distinct from R3: R3 is stale constants, R8 is a cert that never asked.

## CLOSED THIS SESSION
- **Old item 1.1 — wire the declared fallback legs.** S2-E shipped as
  `02831ee` (CC-55), S2-D as `ad083fe` (CC-56). Both probed before wiring on
  mechanics AND calibration. S3-A was RECLASSIFIED OUT of this item rather
  than skipped — see item 3.2.

## WORKING ORDER

### URGENT

**1. THE ORCHESTRATOR PRINTS A SUCCESS TICK FOR FAILED POSITIONS** [R2, R6]
Promoted to the top. It is the instrument through which every other item on
this list will be certified, and it hid a full-day outage on 2026-08-18.
**CONFIRMED LIVE ON THE CERT WAVE ITSELF (run 32209212733, 2026-08-19):**
`[S2-ORC] S2-B (tick) status=ANALYSIS_FAILED`, and the run ends "All
positions complete." A failed position wearing a green tick, printed on the
same log that certified the fixes below it. S2-B is gemini — a different
failure from the Cerebras 402s, so this is not one provider's problem.
Fixing the truth-telling before more surgery is the higher-leverage order.
- 1.1 `_run`'s failure test is a DENYLIST (`status not in ("SAVE_FAILED",
  "ERROR", "NO_REPORTS")`). Statuses that therefore print a tick across the
  whole S2 tier: `ANALYSIS_FAILED`, `QUOTA_SKIP`, `NO_S1_REPORTS`,
  `NO_S1_DATA`, `NO_ARTICLES`, `NO_RAW_ARTICLES`, `INSUFFICIENT_ARTICLES`,
  `SKIP`, and now `S1_PARTIAL_ARRIVAL` / `S1_ZERO_ARRIVAL`. An ALLOWLIST
  (`status in ("COMPLETE", "OK")`) is the right shape.
- 1.2 **SPLIT THE AXES FIRST.** `failed` currently conflates "did not
  complete" with "must not deliver": a non-empty `failed` list skips the
  `else` branch containing `send_s2_intelligence()` and
  `run_s2_report(run_id=RUN_ID)`. Flipping statuses to failed without
  splitting would SUPPRESS the S2 Telegram report and step report on exactly
  the waves where something went wrong.
- 1.3 S2 and S3 orchestrators disagree on the same string: S3 prints an
  alarm for `ANALYSIS_FAILED`, S2 prints a tick.
- **1.1 and 1.2 MUST SHIP TOGETHER.** 1.1 alone removes information from the
  human at the moment it matters most.

**2. SYSTEM 1 IS ONE SURVIVOR QUADRUPLED** [R2, R3, R4]
Merges the old item 1.2 (lens3/lens4 on a dead provider) with the old item 3
(16 analyses per wave). They are one wave of damage and the dispatch defect
is UPSTREAM of the leg wiring — wiring legs first would multiply the burn.
- 2.1 `lens_orchestrator.py:375` passes `--single-lens`; `analyze_lens_multi.py`
  has no `sys.argv`, no `argparse`, no `LENS_ID`, so every invocation runs all
  four lenses. Parent loops 1..4, so **every wave runs 16 lens analyses, not
  4**, and healing spawns more. Documented at the call site since CC-24 and in
  no decision record.
- 2.2 **CANARY THREE-ARM GAS-MASK TEST BEFORE ANY EDIT HERE.** Arm 2 is the
  live risk: the Collection import chain is S1's air supply.
- 2.3 lens3 and lens4 remain on dead Cerebras. Wire AFTER 2.1, and note lens3's
  leg is groq/gpt-oss-20b — see item 3.
- 2.4 S1 burn is UNMEASURED. `subprocess.run(capture_output=True)` swallows the
  child's stdout, so no S1 provider traffic appears in the parent log. Measure
  provider-side or instrument the child; do not count POSTs in the parent.

**3. THE REGISTRY CONTRADICTS A RULING IT WAS SUPPOSED TO IMPLEMENT** [R1, R2, R7]
- 3.1 **D-015 (2026-07-28) ruled ALL fallbacks become mistral-small, uniform,
  with the explicit reason that "gpt-oss-20b is NOT insurance because it shares
  the reasoning-starvation failure mode, proven on s2e."** Six roles still
  carry groq/gpt-oss-20b legs at HEAD: lens3, ai5_watchdog, s2gap,
  entity_extract, s3a_patterns, s3d_longterm.
- 3.2 **S3-A specifically.** Its own registry note says the leg "inherits
  max_out and hits the same 939-token ceiling — it is broken and unprobed",
  recorded at CC-12 on 2026-08-01 and still true. This is a registry edit plus
  an LR-106 probe, NOT a wiring commit. That is why it left item 1.1.
- 3.3 RULING NEEDED: do the dead Cerebras PRIMARIES stay in front of the new
  legs, or get repointed? Keeping them costs a failed call and a retry per
  position per wave; repointing loses the automatic return if Cerebras is ever
  restored. Lean KEEP, but it is now a measurable cost, not a free option.
- 3.4 RULING NEEDED: three fallback shapes now exist in the repo — S2-A's
  hardcoded dual-path `model=None` (CC-14), MA/S2-E/S2-D's `_call_fallback_leg`
  copies, and nothing at all elsewhere. Extract a shared helper AFTER the
  01:28 cert, not before: generalising an uncertified pattern was the reason
  it was not done at LENS-037.

**4. THE RECORD CANNOT ATTRIBUTE** [R6, R3]
- 4.1 **CC-57 — the saved row does not say which leg produced it.** Design
  settled: stamp `parsed["_wire"] = provider/model` on BOTH legs and copy it
  into `evidence` at save. Rejected: a module-scope global; a tuple return
  (CC-53's lesson). **BLOCKER FOUND AND UNRESOLVED:**
  `lens_mission_analyst.py:487` does `truncate(json.dumps(evidence),
  MAX_S2_CHARS)`, so ANY key added to `evidence` enters MA's prompt and
  consumes S2 budget inside a truncate that already binds. This is a PROMPT
  CHANGE, not metadata. Fork: (A) accept it deliberately and size it — it
  would also let MA cite the producing leg; (B) a real column or side table;
  (C) log-only, relying on run_id and the exact transition timestamp.
  Not (D) overloading `source_id` — one column with two meanings is how S2-D
  died. S2-D batches independently, so a row can be genuinely mixed: store the
  DISTINCT set of wires per wave, never a single winner.
- 4.2 `lens_reports.domain_focus` is the literal "ALL" on all 3,783 rows, from
  `analyze_lens_multi.py:1206`. Every S1 entry in every synthesis prompt is
  headed `--- ALL ---`, so the model cannot name a lens. Identity IS
  recoverable from `summary`'s `[<Lens Name> — <perspective>]` prefix or
  `prompt_version` (`v2.0-LENS004-<LensNameNoSpaces>`). No schema change needed.
- 4.3 `cycle` is the literal "manual" on every row, so the old prescribed
  cycle-filter remedy CANNOT work. Wave scoping must come from `run_id` or a
  `generated_at` recency floor.
- 4.4 Arrival counts ROWS, not distinct lens identity. Four copies of one lens
  satisfy `4/4` in the budget log and in CC-53's guard.
- 4.5 Zero of 243 macro reports have ever cited a named lens; 236 cite S2 only.

**5. CERTIFICATION MEASURES MECHANICS, NOT BEHAVIOUR** [R8 — PROPOSED ROOT]
- 5.1 Make a calibration band part of every migration cert, not an optional
  extra. Mechanics (HTTP, finish_reason, budget_used, schema) prove a position
  RUNS. A band from the position's own stored history proves it still BEHAVES.
- 5.2 **RETRO OWED: the other D-016 positions were never checked.** MA, S3-A,
  S3-D, lens3, lens4 and s2f_primary all moved to Cerebras on 2026-07-28 and
  none has been measured across that boundary. Two of two checked so far
  showed a substantial shift. Free — the evidence is already in the DB.
- 5.3 `probe_lens_models.py` has NO mistral caller (zero matches for
  "mistral"), so `--candidate fallback` cannot exercise any mistral leg. Both
  LENS-037 probes were purpose-built scripts. Extending the pack is an
  instrument change and needs its own commit.

### IMPORTANT

**6. NOTHING WATCHES PROVIDER LIFECYCLE** [R7, R1]
Cerebras announced its free-tier end on 2026-07-17 by email, died 2026-08-17,
took five positions, and nobody knew until a query counted rows. Record EOL
dates in the registry note and as dated watch items at announcement.
Known: gemini-2.5-flash dies 2026-10-16 (lens2, and S2-B/S3-B on flash-lite).

**7. PROMPT PACKING AND VALUE ORDER** [R3]
MA's S1 allotment hit **96.0% (24,001 / 25,000) on 2026-08-19** — the
highest ever, past the 94.5% and 94.1% of the two waves before it.
`S1_PARTIAL_ARRIVAL` has never fired live, is the reachable branch, and is
now within one long lens report of firing. S2 admissions fell to 3 of 25 as
corrections grew to 11,214.

**8. GROQ TPD REFILL LAW** [R3] — measured 2026-08-10, now nine days stale and
downstream of item 2.

**9. INPUT-QUALITY CLUSTER** [R3] — BUG-001 now has a number: batch 1 held 44
articles, the 9,000-char cap fired at 21 (8,929 chars), and the user message
still states the pre-truncation count. 52% of the batch never reached the model.

**10. CI PROVES COMPILATION, NOT BEHAVIOUR** [R2, R8] — related to item 5;
"Lens CI success" means py_compile plus the registry self-test plus one script.

**11. DEAD-SYMBOL GATE** [R2] — `check_groq_tpm` is defined twice,
byte-identical, at `lens_s2_orchestrator.py:38-67` and `:69-98`.

**12. THE MAP IS STALE IN MORE PLACES THAN THE TERRITORY** [R6]
Three sources describe S2-E's wiring and two are wrong:
`lens_s2_orchestrator.py:15` still says `llama-3.3-70b / GROQ_S2E_API_KEY`
(model dead since 2026-08-16, key env not in the registry), and
`lens_s2e_legitimacy.py:49` still says `max_out 10,000` where the registry says
16,000. A generated map beats a hand-maintained one.

**13. REGISTER AND ROUTING HYGIENE**
- 13.1 **THE REGISTER HAS TWO FORMATS AND ONLY ONE IS COUNTABLE.** LR-090..141
  are ALL present — 45 under `## LR-NNN` headings, plus LR-117..123 at lines
  507-520 in a compact `LR-NNN  text` block under one `## LENS-030 ... earned
  rules` header. 45 + 7 = 52, no gaps, exactly as banked. A count is only as
  true as its grep: `grep -c "^## LR-"` returns 45 and looks like seven
  missing rules. Normalise the LENS-030 block to the register's own
  convention — precedent is `1bbb6f3`, which did exactly that for
  LR-127..133. A rule no count can see is a rule nobody finds: LR-119 was
  live the whole time and got re-derived from scratch at LENS-037.
  LR-142..146 (LENS-036) and LR-147..151 (LENS-037) still need appending.
- 13.2 CC-24 appears in NO decision record — a live architectural defect
  documented only in a code comment.
- 13.3 **PROMOTED FROM RETIRE CANDIDATE:** patch and probe scripts accumulate
  untracked in the repo root. LENS-037 added five (`patch_cc55.py`,
  `patch_cc55b.py`, `patch_cc56.py`, `probe_s2e_fallback.py`,
  `probe_s2d_fallback.py`, plus `s2e_baseline.py`). Promoted rather than
  retired because it grew this session and the two probe scripts are
  REUSABLE INSTRUMENTS that deserve a tracked home, not deletion.

**14. WAVE SEQUENCING AND CROSS-POSITION SPACING** [R3]
Cross-position spacing cannot live in a per-position TPMGuard (LR-112).
Also: TPMGuard still paces against the PRIMARY's TPM on a fallback path —
advisory only, but the meter is wrong once a leg is live.

### OTHERS
**15. MEASUREMENT ODDITIES, BANKED**
- `actual_prompt - counted_total = 149` on twenty consecutive waves, no exception.
- chars/token on ONE model, `mistral-small-2603`, measured at 4.738 (MA),
  4.19 (S2-E) and 3.80 (S2-D). It is CONTENT-dependent. No measured
  chars/token value is a model constant, and every Cerebras-derived sizing
  constant must be re-derived per leg, never carried across.

## WATCH ITEMS
- **PREDICTION HELD — KEEP WATCHING THE TREND.** The 02:37 UTC wave of
  2026-08-19 produced the first Mistral rows: S2-D `claims=6 consistency=0.85`
  and `claims=6 consistency=0.7` against a pre-wiring prediction of 6-7 claims
  at 0.70-0.85; S2-E 8/7/5/7 actors and 5/5/4/4 LOW against a predicted 8/7/8
  and 5/3/5. A band measured BEFORE shipping predicted live behaviour to the
  digit — the strongest evidence yet for item 5 / R8. Three trials are not a
  trend, and S2-D's confidence_score step down will still bend any series
  crossing this date.
- MA's S1 allotment saturation (item 7).

## STANDING BLOCKER
$0/month. Cerebras' $5 credit behind a payment method is not a solution.

## CHANGED THIS REGENERATION
- CLOSED: old item 1.1 (fallback legs) — S2-E `02831ee`, S2-D `ad083fe`.
- RECLASSIFIED: S3-A moved OUT of the wiring item into new item 3.2 — its leg
  was ruled out as a class by D-015 and recorded broken at CC-12.
- PROMOTED: old item 2 (orchestrator denylist) to item 1 — it is the
  instrument every other item is certified through.
- MERGED: old item 1.2 (lens3/lens4) with old item 3 (16 analyses) into item 2
  — one wave of S1 damage, dispatch defect upstream.
- NEW: item 3 (registry contradicts D-015, six surviving groq-20b legs).
- NEW: item 5 and PROPOSED ROOT R8 (certification measures mechanics only).
- NEW EVIDENCE: item 4.1 gains the `json.dumps(evidence)` blocker — CC-57 is a
  prompt change, not metadata.
- RETIRE CANDIDATES RESOLVED at generation 3 of 3, as the clause requires:
  S3-B's 600-char summary cap CLOSED AS ACCEPTED (four months of records, no
  defect traced to it); patch-script clutter PROMOTED to item 13.3 with reason.
- CORRECTED AFTER FIRST DRAFT: item 13.1 claimed the register ended at LR-141
  with seven rules missing. It does not. They are present in a second format;
  the claim came from comparing two different greps, which is LR-121's exact
  failure. The item now names the format split instead.
- ITEM NUMBERS ASSERTED UNIQUE: 1..15, no duplicates, no gaps.

## NEXT SESSION'S MISSION
**Order item 1** — the orchestrator allowlist, with 1.1 and 1.2 shipping
together. The cert is DONE and GREEN (run 32209212733, headSha ad083fe):
all four commits are certified live, so nothing is pending on a wave at open.
