# LENS - CURRENT TARGET AND WORKING ORDER
Regenerated 2026-08-18 at LENS-035 close. SUPERSEDES the 2026-08-09 version.
Governed by LENS_CONTRACT.md (MISSION AND SCOPE + DISCOVERY POLICY).

## CURRENT TARGET  (unchanged)
**Lens produces valid intelligence, unattended, with no silent failure.**
Declared 2026-08-06. All ranks below are relative to THIS target.

## ROOTS
R1  No source of truth about what the system IS.
R2  Written-but-never-wired code, with no detector.
R3  Numbers set once, never re-derived against reality.
R4  Input quality never verified.
R5  The tests do not run. META-ROOT.
R6  STILL PROPOSED, JAMES RULES: no position verifies that its INPUTS
    ARRIVED. Distinct from R2 -- the code is wired and runs; its inputs
    vanish between fetch and use. Evidence has STRENGTHENED since it was
    proposed: MA's guard tested the FETCHED S1 list while the prompt got
    zero for 16 waves; 26 of 30 S2 reports still vanish unremarked; and
    GNI's arbitrator was found to have the identical gap on 2026-08-17.
    A cross-project failure class is a root, not an incident.

## CLOSED THIS SESSION
- ITEM 1.2 SHIPPED AND CERTIFIED. Absolute per-tier allotments (93a30c3).
  **s1 = 4/4 reports on THREE consecutive waves (#277 #278 #279)** against
  sixteen prior waves that never exceeded 1 of 4. The S1 break never fired,
  all sums exact, actual-counted = 149 on all three.
- CC-50 SHIPPED (b957d6c). RETRY_SLEEP 10 -> 65 moves a retry into a fresh
  TPM window. No cert wave applies -- it fires only on a failure path.
  Its proof is arithmetic, and it was the ENABLER: while a retry shared the
  window a call had to fit twice, which made 1.2's allocation impossible.
- THE fit_max_tokens GATE IS CLOSED. usable = 30,000 - max(200, 8%) =
  27,600. At the 56,000 cap, max_tokens stays a full 5,000 with 2,557
  tokens of slack, and TPMGuard sits at 25,043 of 25,500. Measured, not
  assumed.
- OLD ITEM 1.1 (CC-49 clip) CERT CLOSED AS **PARTIAL**. The clip worked
  mechanically (687 -> 541 chars per correction) but its stated purpose
  failed: the freed space went entirely to S2, and s1 stayed 0. It remains
  a stopgap under 1. A stopgap never closes a root.

## WORKING ORDER

### URGENT
1  ARRIVAL IS STILL NOT VERIFIED  [R6 + R3]
   1.2 restored the canary's readings to the synthesis prompt. It did NOT
   make their absence detectable. If the same class recurs -- a tier
   silently emptied -- nothing fails, nothing alerts, and the run reports
   SUCCESS exactly as it did for sixteen waves. Under the declared target
   that is the live gap, not the allocation.
   1.1  THE GUARD AT :659 TESTS THE FETCHED LIST. `if not s1_reports ->
        NO_S1_REPORTS` passes whenever four rows come back, regardless of
        what reached the prompt. NEEDS A RULING BEFORE A PATCH: does
        zero-inclusion WARN or RAISE? A raise means no daily brief at all;
        a warning repeats the "log line nobody read" failure. Precedent
        available: GNI ruled print-only WARNING on the identical question
        on 2026-08-17, deliberately as a half-step.
   1.2  SURFACE IT WHERE A HUMAN LOOKS. The budget line already prints
        (4/4). The S2-E exclusion was equally visible for weeks and simply
        never read. Candidate: one line in the pre-flight Telegram block,
        the same mechanism that made REGISTRY MISALIGNMENT visible.
   1.3  THE S1 FETCH HAS NO CYCLE FILTER. `order(generated_at desc)
        .limit(4)` takes the four newest rows GLOBALLY. If a lens fails to
        produce on a wave, MA silently fills the four from a PREVIOUS wave
        and still logs "Fetched 4 S1 reports". A count is not provenance.
   1.4  WHICH LENS WAS THE ONE? For sixteen waves at most one lens reached
        synthesis, and `generated_at DESC` means it was whichever finished
        LAST -- probably the same one every time. If so, three specific
        perspectives were absent from every macro report in the record.
        The S1 break now names a dropped lens, but only when it fires.
   1.5  RETRO TEST, NO WAVE NEEDED. Macro reports carry `evidence_sources`
        naming the supporting S1 lens or S2 analyst. Query
        `lens_macro_reports` back over weeks and count rows citing any S1
        lens. Zero across history = chronic, MEASURED, and settles both
        1.4 and the pre-instrument period at once.

### IMPORTANT
2  PROMPT PACKING AND VALUE ORDER  [R3]
   Both tier loops use `break`, so ONE oversized entry abandons the whole
   tier -- including smaller entries after it that would have fit. Measured:
   S2 quit with 2,971 chars of its budget unspent on MA #264.
   2.1  `continue` instead of `break`, BOTH loops.
   2.2  SORT s2_reports BY VALUE before iterating. Today the 3-4 of ~30
        that reach MA are simply the ones that arrived FIRST.
        `apply_s2_corrections` in the same file already sorts by
        (mandatory, injection_score desc); the report list is not sorted at
        all.
   MUST SHIP TOGETHER: `continue` without sorting cherry-picks small
   low-value entries; sorting without `continue` still abandons the tier.

3  GROQ TPD SATURATION  [R3/R4]
   Measurement CLOSED at LENS-034; the burn is not. Every Collection wave
   for 72+ hours hit the 200,000 ceiling (#248 199742 · #249 199632 ·
   #250 199458 · #251 199439). 429s log as `[ENTITY] WARNING` and never
   touch exit status. UNAUDITED since Aug 10 -- re-read before acting.
   THE FIX IS DOWNSTREAM OF ITEM 4.
   3.1  TPD SCOPE STILL UNPROVEN. The 429 names an organization, but that
        same wording appears on buckets measured PER-KEY (918 vs 999
        remaining requests on two keys in the same second). Test: one
        ~700-token call on a second key while the entity key is blocked.

4  INPUT-QUALITY CLUSTER  [R4] -- one problem, three faces, in this order
   4.1  RT full-text fetch. Owner is fetch_text.py.
   4.2  U1a entity_extract visible-text gate -- ONLY after 4.1.
   4.3  TPD burn falls out of 4.2: eligible articles 116 -> 55.

5  I5 -- CI  [R5 meta-root]
   Rebuild the 2 stale response-guard fixtures from PRODUCTION output,
   THEN wire pytest into CI. Local: 2 failed / 182 passed. CI runs none.

6  DEAD-SYMBOL / UNWIRED-WRITE CI GATE  [R2]
   6.1  CC-44 -- get_llm_client() must honour _FORCE_PROVIDER.
   6.2  I6 DAILY_BUDGET ruling -- lean C, then A or B once A3 lands.

7  GENERATED SYSTEM MAP  [R1]
   7.1  Stale docstrings. MA's module docstring still claims
        "Input: lens_reports (S1) + injection_reports (S2)" -- true again
        as of 93a30c3, but it was false for months and nothing caught it.
   7.2  SYMBOL COLLISION: `MAX_TOTAL_CHARS` is 56,000 in
        lens_mission_analyst, 800,000 in lens_s2b_coordination, 9,000 in
        lens_s2d_adversary. Verified INDEPENDENT (no cross-import), so this
        is a readability hazard, not a coupling. Rename anyway.
   7.3  `check_groq_tpm` is DEFINED TWICE in lens_s2_orchestrator.py,
        byte-identical, second shadows first; four near-copies exist across
        the s2/s3 orchestrators.

8  LIFECYCLE
   A3 -- delete the Gemini legs on s2b/s3b. Recovers ~7.5 min per wave.
   I7 -- **gemini-2.5-flash dies Oct 16 and lens2 runs on it. Act ~Oct 10.**

9  REGISTER AND ROUTING  [R1/R5]
   9.1  REGISTER-INTEGRITY GATE in tests/, counting rule DEFINITIONS across
        both formats and diffing against the expected unbroken sequence.
        Register is at 52 rules, LR-090..141, GAPS none as of Aug 9.
   9.2  AMEND LR-078 -- never pipe a heredoc into python; never place a
        literal backtick in a heredoc body; build fences with chr(96).
   9.3  ADD THE ROUTING RULE to LENS_CONTRACT.md: a finding is routed ONCE
        by what it is. Then promote durable hazards out of the briefs.
   9.4  MIRROR-AND-LOG THE GNI TRANSFER. Two packets were delivered to GNI
        (Aug 10 loop-hole notes, Aug 17 eight findings) and GNI adopted
        them into its CONTRACT v4/v5, its rules register and a shipped
        arrival instrument. LENS_CONTRACT.md's own rule says a mirrored
        rule is logged on BOTH sides. Lens has not logged its half.
        Reciprocal items GNI earned that Lens lacks: a WRONGNESS LEDGER
        step at close (Lens has it in the protocol, GNI now has it too),
        and GNI's ruling that **a close is a CHECKPOINT, not a hard stop,
        amendable only if the order is regenerated again** -- which
        answers Lens's own open question, twice demonstrated.
        STATUS: the checkpoint ruling is ADOPTED (contract v3, 4c35347)
        and the protocol drift is FIXED. What REMAINS of 9.4 is the
        register half -- LR entries for the rules earned this session, and
        the reciprocal wrongness-ledger step GNI now carries.

10 WAVE SEQUENCING -- NO ORDERING GUARANTEE  [R3/R6]
   Collect and MA are independently scheduled AND independently lagged. If
   MA starts first it analyses the PREVIOUS wave and reports SUCCESS. Same
   R6 family as item 1 -- if R6 is ruled a root, this promotes.

### OTHERS
11 `total_chars` UNDERCOUNTS the prompt by 149 chars (section headers and
   the corrections newline are never counted) -- holds on all 19 measured
   waves - TWO GUARDS, TWO MARGINS on one ceiling: fit_max_tokens reserves
   8% (usable 27,600), TPMGuard reserves 15% (25,500), so the tighter one
   binds first and they can disagree - TPMGuard NEVER BLOCKS: over-limit
   logs an error and PROCEEDS, so it is advisory and the provider's 429 is
   the real enforcement - `GROQ_S2F_API_KEY` referenced in code, absent
   from local env (lead, not a verdict) - B1/B2/B3 provenance - two
   hardcoded LR ranges in the protocol - analyze_lens_multi.py (fresh
   session only).

12 RETIRE CANDIDATES -- **generation 2 of 3.** At the next regeneration
   each is closed as accepted or promoted with a written reason.
   S3-B's 600-char summary cap. LR-093's 23 patch scripts in repo root.

## WATCH ITEMS  (not work, but read them on any wave)
- **S1 hit 23,628 of its 25,000 allotment on #279 -- 94.5%.** Four entries
  at the 6,060 truncation ceiling is 24,240, so the ceiling case is real
  and close. One larger wave drops a lens. It will NOT be silent: the S1
  break names the dropped lens by domain_focus. Raise the allotment when it
  fires, not before -- the number should follow a measurement.
- **chars/token is now 4.064** (43,647 / 10,740 on #277), above the entire
  prior 3.435-3.713 range, because S1 prose displaced dense S2 JSON. The
  ratio moves with the MIX. Keep sizing against the worst observed (3.435);
  never treat any single value as the constant.
- Corrections have drifted DOWN unprompted: 13,600-14,894 at 25-27, versus
  14,485-15,144 at 27-28 a week earlier. Upstream S2 volume varies on its
  own; the knife-edge we removed was riding on a moving number.

## STANDING BLOCKER
CC-1d must not ship. `CHARS_PER_TOKEN = 3` OVER-estimates against the
measured 3.435-4.064, which is the CONSERVATIVE direction. Changing 3 -> 4
would make every estimator under-count and start overshooting the real
limit. Re-derive against measured ratios, never assume.

## AMENDED AFTER THIS REGENERATION  (per LENS_CONTRACT.md v3)
2026-08-18, same day, logged rather than silent:
- Contract v3 (4c35347): a close is a CHECKPOINT, not a hard stop.
  Mirrored from GNI's S82 ruling; logged on both sides.
- Protocol v2: only the OPEN is pasted now. The OPEN reads
  LENS_SESSION_PROTOCOL.md, so the CLOSE is invoked by name and read from
  the repo. Cause: the pasted close prompt had already lost two clauses
  present in the file -- a dual source of truth inside the document
  written to prevent one. OPEN also gains the canary gate, ls-remote as
  the only HEAD source, and an elapsed-time/wave-count step. CLOSE gains
  item-number uniqueness, hazards-promote-or-expire, and the LR-138 grep.
- Item 9.4 half-closed, above.
- NOTED FOR THE NEXT CLOSE: contract v3 says an amendment requires the
  order be "regenerated again". Taken literally that means regenerating a
  20-minute-old order to log a protocol fix. The clause's PURPOSE is that
  the order must never describe a state that has passed, which this
  section satisfies. The wording is heavier than the purpose -- refine it.

## CHANGED THIS REGENERATION
- Item 1 REPLACED, not closed. The allocation defect is fixed and
  certified; the ARRIVAL defect it exposed is not, so item 1 is now about
  detection rather than allocation. Its old sub-items 1.3-1.6 survive
  re-classified, never inherited.
- CC-50 and 1.2 CLOSED with cert evidence. CC-49 closed as PARTIAL.
- R6's evidence strengthened by a cross-project sighting; still unruled.
- Packing + value-order PROMOTED from a sub-item to item 2, and its two
  halves bound together as must-ship-as-one.
- Old item 2 (TPD) -> 3, 3 -> 4, 4 -> 5, 5 -> 6, 6 -> 7, 7 -> 8, 8 -> 9,
  9 -> 10, 10 -> 11, 11 -> 12. Numbering verified unique 1..12.
- NEW item 9.4: log the Lens half of the GNI mirror, and adopt GNI's
  close-is-a-checkpoint ruling, which answers our own open question.
- The fit_max_tokens gate, the two-margins finding and the
  TPMGuard-is-advisory finding are all NEW this session and placed.
- Retire candidates advance to generation 2 of 3.

## NEXT SESSION'S MISSION
Item 1.1 -- RULE on the arrival guard (warn vs raise), then ship it.
Read the :659 guard and the macro-report write path first. Item 1.5 (the
retro query) is free and can bank evidence in the same session.
