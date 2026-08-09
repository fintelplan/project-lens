# LENS - CURRENT TARGET AND WORKING ORDER
Regenerated 2026-08-09 at LENS-034 close. SUPERSEDES the 2026-08-07 version.
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
R6  PROPOSED, JAMES RULES: no position verifies that its INPUTS ARRIVED.
    Distinct from R2 -- the code is wired and runs; its inputs vanish
    between fetch and use. Evidence: MA's guard tests the FETCHED S1 list
    while the prompt received zero; 27 of 30 S2 reports vanish unremarked.
--  NOT A ROOT WE CONTROL: provider lifecycle (D-014 weather).

## CLOSED THIS SESSION
- ITEM 1 (Groq TPD) MEASUREMENT COMPLETE. No -day header exists on any Groq
  response. Groq uses CONTINUOUS LEAKY-BUCKET REFILL at Limit/86400 per sec,
  verified to the millisecond on 7 readings across 2 buckets. TPD refill =
  2.3148 tokens/sec = 8,333/hour. THERE IS NO RESET BOUNDARY TO FIND.
- 6d -- CC-48 shipped (edfe708) and CERTIFIED on MA #261: the budget line
  sums exactly (18560+0+9341+975=28876) and the predicted 149-char header
  undercount was confirmed.
- RIDER "CC-47 cert" CLOSED on MA #260: the S2 break and the S3 add fired on
  the same wave.
- RIDER "S1 side-effect" ANSWERED: S1 was consuming ZERO, not eating S2's
  budget. The corrections block was.
- OLD ITEM 1b SUBSUMED into new item 1 -- S2-E exclusion is one symptom of a
  larger starvation (3 of 30 S2 reports reach MA, not 29 of 30).

## WORKING ORDER

### URGENT
1  MISSION ANALYST SYNTHESISES WITHOUT ITS INPUTS  [R3 + R6]
   MEASURED on MA #261: corrections=18560 s1=0 (0/4 reports) s2=9341 (3/30).
   All four S1 lenses and 27 of 30 S2 analysts were excluded, and the run
   reported SUCCESS. Silent live failure: absolute rule.
   CANARY DOCTRINE: S1 is the canary. Its readings never reach synthesis,
   while the IMMUNE SYSTEM's output (S2 corrections) fills the prompt. MA
   synthesises from S2's verdicts about evidence it cannot see. Epistemic
   diversity at the synthesis position is ZERO -- structurally worse than
   S1-001, where the denominator merely lied.
   PERVERSITY: more S2 findings -> more corrections -> S1 more completely
   excluded. Failure scales with upstream health, which is why it never
   looked like failure.
   1.1  CC-49 (7b06437) SHIPPED AS A STOPGAP -- mandatory reason clipped to
        240. AWAITING CERT on the next wave. A stopgap NEVER closes a root.
   1.2  ROOT FIX: replace MAX_TOTAL_CHARS * 0.6 with an ABSOLUTE S1
        allotment, sized from CC-49's post-clip measurement, so S1's
        presence stops depending on how large the corrections block is.
        Do NOT ship the floor alone: at 18560 corrections an 8000 floor
        leaves ~1440 for S2 and starves it instead.
   1.3  The :659 guard tests the FETCHED list, not the prompt. Add an
        arrival check -- this is what makes the failure silent.
   1.4  injection_goal is also unclipped; next candidate if 1.1's measured
        corrections figure is still large.

2  GROQ TPD SATURATION IS CHRONIC  [R3/R4]
   Measurement CLOSED; the burn is not. Every Collection wave for 72+ hours
   hit TPD: #248 Used 199742, #249 199632, #250 199458, #251 199439. The org
   sits pinned at the 200,000 ceiling consuming refill as fast as it arrives.
   429s log as ENTITY WARNING and never touch exit status.
   THE FIX IS DOWNSTREAM OF ITEM 3 -- only cutting call volume stops it.
   2.1  TPD SCOPE STILL UNPROVEN. The 429 names an organization, but the
        SAME wording appears on buckets we measured as PER-KEY (918 vs 999
        remaining-requests). Test: one ~700-token call on a second key while
        the entity key is TPD-blocked mid-Collection. 200 => per-key.

### IMPORTANT
3  INPUT-QUALITY CLUSTER  [R4] -- one problem, three faces, in this order
   3.1  RT full-text fetch. Owner is fetch_text.py.
   3.2  U1a entity_extract visible-text gate -- ONLY after 3.1.
   3.3  TPD burn falls out of 3.2: eligible articles 116 -> 55.

4  I5 -- CI  [R5 meta-root]
   Rebuild the 2 stale response-guard fixtures from PRODUCTION output, THEN
   wire pytest into CI. Local: 2 failed / 182 passed. CI runs none.

5  DEAD-SYMBOL / UNWIRED-WRITE CI GATE  [R2]
   5.1  CC-44 -- get_llm_client() must honour _FORCE_PROVIDER.
   5.2  I6 DAILY_BUDGET ruling -- lean C, then A or B once A3 lands.

6  GENERATED SYSTEM MAP  [R1]
   6.1  The ten stale docstrings. NOTE: MA's module docstring still claims
        "Input: lens_reports (S1) + injection_reports (S2)" -- item 1 proves
        that false. A docstring that lies about INPUTS is not cosmetic.
   6.2  SYMBOL COLLISION: MAX_TOTAL_CHARS is 32,000 in lens_mission_analyst
        and 800,000 in the S2-B coordinator. Same name, two values. Any
        cross-module reasoning about "the cap" is unsafe until renamed.

7  LIFECYCLE
   A3 -- delete the Gemini legs on s2b/s3b. Recovers ~7.5 min per wave.
   I7 -- gemini-2.5-flash dies Oct 16; lens2 runs on it. Act ~Oct 10.

8  REGISTER AND ROUTING  [R1/R5]
   8.1  REGISTER-INTEGRITY GATE. Counts rule DEFINITIONS across BOTH
        formats, excluding prose cross-references, diffed against the
        expected unbroken sequence. Lives in tests/ so CI enforces it.
   8.2  MINT LR-138 -- an artifact is not verified by its own consistency.
        Register is lens-DOC-002_rules.md in the repo ROOT, CRLF 402 /
        bare_LF 248 -- append onto raw bytes, assert bare-LF unchanged.
   8.3  AMEND LR-078 -- never pipe a heredoc into python; never place a
        literal backtick in a heredoc body; build fences with chr(96).
   8.4  ADD THE ROUTING RULE to LENS_CONTRACT.md: a finding is routed ONCE
        by what it is. Then promote durable hazards out of the briefs.
   8.5  NEW LRs EARNED THIS SESSION, to mint with 8.2:
        - A guard whose expected value is HAND-DERIVED is the same R3 defect
          the guard exists to catch. Derive it from the edit list.
        - Never put a rollback command in the same message as the apply
          command. A rollback in a paste block is a rollback that gets run.
        - Assert line endings RELATIVE to the file's state before the patch,
          never absolutely: autocrlf will flip this repo's LF files on the
          next Windows checkout.

9  WAVE SEQUENCING -- NO ORDERING GUARANTEE  [R3]
   Collect and MA are independently scheduled AND independently lagged. MA
   is nominally 28 min behind Collect; Collect has run 27m15s. If MA starts
   first it analyses the PREVIOUS wave and reports SUCCESS. Same R6 family.

### OTHERS
10 total_chars UNDERCOUNTS the prompt by 149 chars (section headers and the
   corrections newline are never counted) - check_groq_tpm is DEFINED TWICE
   in lens_s2_orchestrator.py, byte-identical, second shadows first - four
   near-duplicate copies of that helper across s2/s3 orchestrators -
   GROQ_S2F_API_KEY referenced in code, absent from local env (lead, not a
   verdict: local env is not Actions secrets) - B1/B2/B3 provenance - two
   hardcoded LR ranges in the protocol - analyze_lens_multi.py (fresh
   session only).

11 RETIRE CANDIDATES -- decide at next regeneration
   S3-B's 600-char summary cap. LR-093's 23 patch scripts in repo root.

## RIDERS ON CLOSED ITEMS  (one grep each, not work items)
- CC-49 cert: "MA prompt budget:" on the next wave -- corrections well below
  18,560 and s1 NON-ZERO for the first time ever measured.
- CC-48 remains certified; its numbers must keep summing to counted_total.

## STANDING BLOCKER
CC-1d must not ship. CHARS_PER_TOKEN 3 -> 4 over-estimates capacity.
Re-derive against measured ratios, never assume.

## CHANGED THIS REGENERATION
- Item 1 (Groq TPD) CLOSED as measured; its unfinished burn became item 2.
- Item 1b SUBSUMED into the new item 1: it was one symptom of a wider
  starvation, and its proposed 45,000 cap would NOT have fixed the cause.
- 6d CLOSED (CC-48 shipped and certified); it produced the new item 1.
- NEW item 1 opened at URGENT -- silent live failure, absolute rule.
- R6 PROPOSED as a new root; James rules. It would re-rank item 9 upward.
- Old 2->3, 3->4, 4->5, 5->6, 6->7, 6b->8, 6c->9, 7->10, 8->11. Numbering
  verified unique: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11.
- Both riders from the previous order CLOSED, not carried.

## NEXT SESSION'S MISSION
Item 1.1 -- CERT CC-49 from the live wave. Then item 1.2, the root fix.
