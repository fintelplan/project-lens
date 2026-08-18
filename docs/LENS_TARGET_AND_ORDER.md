# LENS - CURRENT TARGET AND WORKING ORDER
Regenerated 2026-08-18 at LENS-036 close. SUPERSEDES the 2026-08-18 LENS-035 version.
Governed by LENS_CONTRACT.md (MISSION AND SCOPE + DISCOVERY POLICY).

## CURRENT TARGET  (unchanged)
**Lens produces valid intelligence, unattended, with no silent failure.**
Declared 2026-08-06. All ranks below are relative to THIS target.

## ROOTS
R1  No source of truth about what the system IS.
R2  Written-but-never-wired code, with no detector. **EVIDENCE NOW
    OVERWHELMING.** Every role in lens_models.py declares a fallback leg;
    a grep for fb_provider/fb_model/fb_key_env OUTSIDE the registry returns
    NOTHING. `fallback()` was dead code from the cliff migration until
    2026-08-18. Separately, lens_orchestrator passes `--single-lens N` to a
    child that has no argv parsing at all. R2 is no longer a class of bug;
    it is the mechanism by which this system fails.
R3  Numbers set once, never re-derived against reality.
R4  Input quality never verified.
R5  The tests do not run. META-ROOT.
R6  STILL PROPOSED, JAMES RULES: no position verifies that its INPUTS
    ARRIVED. Partially instrumented this session (CC-53) -- MA now reports
    S1_ZERO_ARRIVAL / S1_PARTIAL_ARRIVAL. Nothing else does.
R7  **NEW, PROPOSED, JAMES RULES: nothing watches our external dependencies'
    lifecycle.** Cerebras announced the end of its free tier on 2026-07-17,
    by email, with the date in the body. It died 2026-08-17 and took five
    positions with it. Nobody knew until a query counted rows. D-014 says
    deprecation is weather -- but weather has forecasts and we have none.
    Distinct from R1 (what the system IS) because the fact lives OUTSIDE
    the system entirely. Precedent: gemini-2.5-flash dies 2026-10-16 and is
    tracked ONLY as a code comment and one order line.

## CLOSED THIS SESSION
- ITEM 1.1 SHIPPED (d44efa5). RULED **WARN, NOT RAISE**, on evidence: five
  consumers (lens_telegram, lens_s2_step_report, lens_s3_step_report,
  lens_compendium, lens_s1_report) read the latest macro row with no date
  floor, so writing NO row publishes yesterday's synthesis as today's.
  Staleness is worse than absence. build_synthesis_prompt now fills a stats
  out-parameter; MA reports S1_ZERO_ARRIVAL and S1_PARTIAL_ARRIVAL.
  Bite-tested at three budgets: 4/4 COMPLETE, 2/4 PARTIAL, 0/4 ZERO.
- ITEM 1.5 ANSWERED IN FULL. All 243 macro reports, 2026-04-14 to
  2026-08-17. **236 of 243 cite S2 ONLY. Zero cite a named lens.** The seven
  exceptions use the generic strings "S1 lens" / "S1 lenses". Chronic is
  measured across the whole record; the pre-instrument period is settled.
- ITEM 1.4 ANSWERED, AND ITS PREMISE WAS FALSE. There is no "which lens" to
  name: `lens_reports.domain_focus` is the literal "ALL" on **all 3,783
  rows** since 2026-04-12, hardcoded at analyze_lens_multi.py:1206.
- CC-54 SHIPPED (260f18e). MA's declared fallback leg is wired and Lens
  produces intelligence again.

## WORKING ORDER

### URGENT
1  FIVE POSITIONS RUN ON A DEAD PROVIDER  [R2 + R7]
   Cerebras returns HTTP 402 payment_required on every call, verified live.
   MA is covered by CC-54. These are not: **S2-D, S2-E, S3-A, lens3, lens4.**
   Each has a declared fallback leg in lens_models.py that no call site reads.
   1.1  WIRE S2-E, S2-D, S3-A on the CC-54 pattern (plain requests,
        assert_model_known first, schema validator, None when both legs are
        down). One position per commit. S2-E first -- four calls per wave,
        the largest single contributor to MA's corrections block.
   1.2  LENS3 AND LENS4 ARE SYSTEM 1. Run the THREE-ARM GAS-MASK TEST before
        touching them. lens3's declared leg is groq/gpt-oss-20b, lens4's is
        mistral. Arm 3 applies: the goal is an honest lens or an honest
        absence, never a manufactured one.
   1.3  RULING NEEDED: do the Cerebras PRIMARIES stay? Keep = the position
        self-heals if Cerebras ever returns, and costs ~20s of failed
        attempts per wave. Repoint = faster waves, and a manual edit back if
        it returns. Lean KEEP, because the fallback path is now the tested
        one and the cost is bounded.
   1.4  A SHARED HELPER IS TEMPTING AND IS A DUAL-SOURCE HAZARD. Four call
        sites with four copies of the same 40 lines is how S2-D died. Decide
        deliberately: one helper in lens_models.py, or four copies with a
        test that asserts they agree.

2  THE ORCHESTRATOR REPORTS SUCCESS FOR FAILED POSITIONS  [R6/R2]
   `_run` at lens_s2_orchestrator.py:107 computes
   `ok = status not in ("SAVE_FAILED", "ERROR", "NO_REPORTS")`. A DENYLIST.
   Statuses returned across the tier that therefore print a green tick:
   ANALYSIS_FAILED (S2-B, S2-GAP, MA), QUOTA_SKIP (four positions),
   NO_S1_REPORTS, NO_S1_DATA, NO_ARTICLES, NO_RAW_ARTICLES,
   INSUFFICIENT_ARTICLES, SKIP. **This is what hid a full day of outage:
   `[S2-ORC] Mission Analyst OK status=ANALYSIS_FAILED`.**
   2.1  ALLOWLIST, not denylist: `status in ("COMPLETE", "OK")`.
   2.2  **SPLIT THE AXES FIRST.** In main(), a non-empty `failed` list skips
        the else branch, which contains send_s2_intelligence() AND
        run_s2_report(). Flipping statuses without splitting would SUPPRESS
        the S2 report on exactly the waves that degraded. `degraded` and
        `failed` must be separate, with delivery gated on `failed` only.
   2.3  S3's orchestrator prints a warning symbol for the same status string
        that S2's prints a tick for. Two orchestrators, one vocabulary,
        opposite verdicts.
   MUST SHIP TOGETHER: 2.1 without 2.2 is an information blackout.

3  EVERY WAVE RUNS 16 LENS ANALYSES INSTEAD OF 4  [R2/R3/R4]
   lens_orchestrator.py:375 runs `analyze_lens_multi.py --single-lens N` once
   per lens. The child has **no sys.argv, no argparse, no LENS_ID** -- the
   flag is ignored and its main() fires all four lenses every invocation.
   Four invocations x four lenses = sixteen. Healing repairs spawn more.
   3.1  KNOWN SINCE CC-24 AND NEVER PRICED. lens_orchestrator.py:339 states
        the defect in a comment; CC-24 fixed the quality PARSE and left the
        multiplication. CC-24 appears in NO decision record.
   3.2  IT ALSO EXPLAINS THE DUPLICATE ROWS. Each subprocess saves whichever
        lenses survived; with only lens 2 alive under the 402, four
        subprocesses wrote four identical PhysicalReality rows -- which is
        what "4/4 reports" counted.
   3.3  SYSTEM 1 -- GAS-MASK TEST FIRST. Note arm 3: the accidental
        redundancy currently MASKS lens failures. Removing it means a failed
        lens produces no row, which is more honest and less forgiving.
   3.4  BURN IS UNMEASURED. `subprocess.run(capture_output=True)` swallows the
        child's output, so S1 traffic appears in NO parent log. Measure
        provider-side or instrument the child. Do not infer it from run logs.

4  ARRIVAL IS INSTRUMENTED AT ONE POSITION ONLY  [R6 + R3]
   4.1  SURFACE IT WHERE A HUMAN LOOKS. CC-53 sets a status; the status is
        read by the orchestrator this order's item 2 is about to fix, and by
        nothing else. Candidate: one line in the pre-flight Telegram block,
        the mechanism that made REGISTRY MISALIGNMENT visible.
   4.2  THE S1 FETCH HAS NO WAVE SCOPE -- **AND `cycle` CANNOT PROVIDE IT.**
        `cycle` is the literal "manual" on all 3,783 rows, so the written
        `.eq("cycle", cycle).limit(8)` branch would scope nothing even if it
        were reached. Wave scope must come from `run_id` or a `generated_at`
        recency floor. (This corrects the previous order, which prescribed a
        cycle filter.)
   4.3  COUNTING ROWS IS NOT COUNTING PERSPECTIVES. `_s1_in` counts entries;
        four copies of one lens returns COMPLETE. Count DISTINCT identity.
        Identity is available today from `prompt_version`
        (`v2.0-LENS004-<Name>`) or the `summary` prefix `[<Lens Name> -- ...]`.
   4.4  THE RECORD CANNOT ATTRIBUTE. domain_focus is "ALL" everywhere, so MA
        cannot cite a lens and no audit can reconstruct who spoke. Same fault
        as BUG-002 (S2-E reads report["lens_name"], which does not exist).
        Fix at the writer; historical rows are recoverable from the two
        columns above.

### IMPORTANT
5  PROVIDER LIFECYCLE IS UNWATCHED  [R7]
   5.1  Record every known end-of-life date in the registry note AND as a
        dated watch item, at the moment of announcement.
   5.2  **gemini-2.5-flash dies 2026-10-16.** lens2 runs on it AND it is the
        fallback nothing can reach. Act ~Oct 10.
   5.3  A monthly liveness probe across all configured providers is ~10
        calls and would have caught Cerebras on Aug 18 at 00:01 instead of
        by accident. The probe script from this session is the prototype.

6  PROMPT PACKING AND VALUE ORDER  [R3]
   Both tier loops use `break`, so ONE oversized entry abandons the whole
   tier. Measured: S2 quit with 2,971 chars unspent on MA #264.
   6.1  `continue` instead of `break`, BOTH loops.
   6.2  SORT s2_reports BY VALUE before iterating.
   MUST SHIP TOGETHER.

7  GROQ TPD SATURATION  [R3/R4]
   Measurement CLOSED at LENS-034; the burn is not. UNAUDITED since Aug 10.
   Item 3 is upstream of this: 16 lens analyses per wave is a burn multiplier
   nobody counted. Re-measure AFTER item 3.
   7.1  TPD SCOPE STILL UNPROVEN (per-key vs per-org).

8  INPUT-QUALITY CLUSTER  [R4] -- one problem, three faces, in this order
   8.1  RT full-text fetch. Owner is fetch_text.py.
   8.2  U1a entity_extract visible-text gate -- ONLY after 8.1.
   8.3  TPD burn falls out of 8.2: eligible articles 116 -> 55.

9  I5 -- CI  [R5 meta-root]
   Rebuild the 2 stale response-guard fixtures from PRODUCTION output, THEN
   wire pytest into CI. Local: 2 failed / 182 passed, unchanged all session.
   CI runs none of it.

10 DEAD-SYMBOL / UNWIRED-WRITE CI GATE  [R2]
   Promoted in importance by this session's evidence: a gate that flagged
   "declared in the registry, read by no call site" would have caught the
   fallback legs, `--single-lens`, `_FORCE_PROVIDER` and DET-DEAD.
   10.1  CC-44 -- get_llm_client() must honour _FORCE_PROVIDER.
   10.2  I6 DAILY_BUDGET ruling -- lean C.

11 GENERATED SYSTEM MAP  [R1]
   11.1  Stale docstrings. probe_lens_models.py:319 still cites "the 0.6 S1
         cap", removed by CC-51.
   11.2  SYMBOL COLLISION: `MAX_TOTAL_CHARS` is 56,000 / 800,000 / 9,000 in
         three modules. Verified INDEPENDENT. Rename anyway.
   11.3  `check_groq_tpm` is DEFINED TWICE in lens_s2_orchestrator.py,
         byte-identical, :38-67 and :69-98, second shadows first. **VERIFIED
         AT HEAD THIS SESSION** (was banked since LENS-034).

12 REGISTER AND ROUTING  [R1/R5]
   12.1  REGISTER-INTEGRITY GATE in tests/. Register at 52 rules,
         LR-090..141, gaps none as of Aug 9. LR-142..146 minted this session.
   12.2  AMEND LR-078 -- never pipe a heredoc into python; never place a
         literal backtick in a heredoc body; build fences with chr(96).
   12.3  ADD THE ROUTING RULE to LENS_CONTRACT.md: a finding is routed ONCE
         by what it is. **CC-24 is the proof this is needed: a live
         architectural defect documented ONLY in a code comment, in no
         decision record, for weeks.**
   12.4  MIRROR-AND-LOG THE GNI TRANSFER -- register half outstanding.
         **NEW, HIGH VALUE TO GNI: the unreachable-fallback finding.** GNI's
         MAD topology assumes provider redundancy; the same question applies
         there and it is one grep to answer.

13 WAVE SEQUENCING -- NO ORDERING GUARANTEE  [R3/R6]
   Collect and MA are independently scheduled AND independently lagged.

### OTHERS
14 `total_chars` UNDERCOUNTS the prompt by 149 chars -- holds on all **20**
   measured waves - TWO GUARDS, TWO MARGINS (fit_max_tokens 8% -> usable
   27,600; TPMGuard 15% -> 25,500; the tighter binds) - TPMGuard NEVER
   BLOCKS, it is advisory and the provider's 429 is the real enforcement -
   **chars/token is 4.738 on Mistral vs 3.435-4.064 on Cerebras, so every
   sizing constant in lens_mission_analyst.py is CEREBRAS-DERIVED and must be
   re-derived before it is trusted on the fallback path** - `GROQ_S2F_API_KEY`
   referenced in code, absent from local env (lead) - B1/B2/B3 provenance -
   analyze_lens_multi.py (fresh session only).

15 RETIRE CANDIDATES -- **generation 3 of 3. CLOSE OR PROMOTE AT THE NEXT
   REGENERATION; carrying them a fourth time is the archive behaviour the
   clause forbids.** S3-B's 600-char summary cap. LR-093's 23 patch scripts
   in repo root.

## WATCH ITEMS  (not work, but read them on any wave)
- **CEREBRAS IS DEAD AND WILL NOT SELF-HEAL.** Free tier ended 2026-08-17;
  restoring it requires a payment method for $5 of credits, against the
  $0/month constraint. Live-probed 402 at 2026-08-18 23:30 ICT.
- **SAMBANOVA HAS BEEN DEAD SINCE 2026-07-28** (402, balance_units 0) and
  the registry already says so at three call-site notes. It is not a
  migration target. Re-probed 402 this session.
- ALIVE as of 2026-08-18 23:45 ICT: groq gpt-oss-120b and gpt-oss-20b,
  mistral-small-2603, gemini-2.5-flash, cohere command-r-plus-08-2024.
- **S1 hit 23,628 of its 25,000 allotment on #279 and 23,524 on #280** --
  94.5% and 94.1%. Two of the last four waves within 6% of the ceiling. The
  PARTIAL branch shipped this session may fire within days.
- Corrections keep drifting DOWN unprompted: 12,198 on #280 versus
  13,600-14,894 on #277-#279 and 14,485-15,144 a week earlier.
- `fallback()` returns **None** for a role with no declared leg. A
  module-scope unpack would crash at import. Guard it at every new site.

## STANDING BLOCKER
CC-1d must not ship. `CHARS_PER_TOKEN = 3` OVER-estimates against every
measured ratio, which is the CONSERVATIVE direction. 3 -> 4 would make every
estimator under-count. Re-derive against measured ratios, never assume.

## CHANGED THIS REGENERATION
- **R7 PROPOSED** (external dependency lifecycle unwatched), from a live
  outage caused by a deprecation announced a month in advance.
- R2's evidence upgraded from "a class of bug" to "the mechanism by which
  this system fails" -- every declared fallback leg is unreachable.
- OLD ITEM 1 (arrival) SPLIT: what shipped is closed; what remains is item 4,
  re-classified and joined by two NEW sub-items (4.3 distinct-identity
  counting, 4.4 the record cannot attribute).
- **OLD ITEM 1.3's PRESCRIBED FIX WAS WRONG AND IS CORRECTED at 4.2**: a
  cycle filter cannot scope a wave because `cycle` is "manual" on every row.
- **OLD ITEM 1.4's PREMISE WAS FALSE AND IS RETIRED**: the S1 break line
  cannot name a dropped lens; domain_focus is "ALL" on all 3,783 rows.
- NEW ITEMS 1, 2, 3, 5 -- all four from this session, all four ranked above
  the previous urgent tier because they are live or system-scale.
- Old 2 -> 6, 3 -> 7, 4 -> 8, 5 -> 9, 6 -> 10, 7 -> 11, 9 -> 12, 10 -> 13,
  11 -> 14, 12 -> 15. Old item 8 (lifecycle) FOLDED into new item 5.
  Numbering verified unique 1..15.
- Item 11.3 promoted from banked to VERIFIED AT HEAD.
- Retire candidates advance to generation 3 of 3 -- next regeneration must
  close or promote them.

## NEXT SESSION'S MISSION
Item 1.1 -- wire the fallback leg at S2-E, then S2-D, then S3-A, on the
CC-54 pattern, one position per commit. Read 260f18e first; it is the
template. Item 2 is the second urgent and its two halves must ship together.
Do NOT start item 3 without running the three-arm gas-mask test.
