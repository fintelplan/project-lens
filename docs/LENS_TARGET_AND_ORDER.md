# LENS - CURRENT TARGET AND WORKING ORDER
Regenerated 2026-08-07 at LENS-033 close. SUPERSEDES the 2026-08-06 version.
Governed by LENS_CONTRACT.md (MISSION AND SCOPE + DISCOVERY POLICY).

## CURRENT TARGET  (unchanged)
**Lens produces valid intelligence, unattended, with no silent failure.**
Declared 2026-08-06. All ranks below are relative to THIS target.

## ROOTS  (unchanged; no new root opened this session)
R1  No source of truth about what the system IS.
R2  Written-but-never-wired code, with no detector.
R3  Numbers set once, never re-derived against reality.
R4  Input quality never verified -- the collector does not check what it
    collected.
R5  The tests do not run. META-ROOT.
--  NOT A ROOT WE CONTROL: provider lifecycle (D-014 weather).

## CLOSED THIS SESSION
- U1b -- S2-B strips markup. CERTIFIED on MA #257: 42,953 -> 18,352 prompt
  tokens, 87.5% -> 38.7% of TPM, 200/200 articles kept, S2-B still 4 findings.
- CC-47 -- S3 context reserved slot. CERTIFIED on MA 660da99 (14:31 UTC):
  total_chars=27664 at the S2 break plus an 876-char S3 section = 28540,
  which EXCEEDS the old 28000 cap -- without the reserve S3 would have
  been dropped on that wave. S2's budget stayed at 28000.
- CC-43 -- regular_report Groq leg 3 removed. CERTIFIED on Aug-7 morning wave.

## WORKING ORDER

### URGENT
1  GROQ TPD, ORG-SCOPED  [R3]
   Limit 200000 / Used 199747, scoped to the organization -- LR-094 per-key
   isolation does NOT cover TPD. Collection's entity storm can starve MA's
   Groq positions, and it degraded entity enrichment INSIDE a run that
   reported success. Absolute rule: silent live failure.
   FIRST STEP IS A MEASUREMENT, NOT A FIX: read
   x-ratelimit-remaining-tokens-day from a live Groq response and establish
   the reset boundary. It is NOT derivable from what we hold.
   The fix is downstream of item 2.

1b S2-E EXCLUDED FROM MISSION ANALYST, EVERY WAVE  [R3]
   "Prompt cap reached at S2 entry for S2-E" fires in BOTH slots:
   total_chars=27664 evening 2026-08-07, 27465 morning 2026-08-08 against
   an s2_budget of 28000. S2 runs at ~98% of budget CHRONICALLY, so the
   break is systematic, not occasional. The loop breaks AT the S2-E entry,
   so S2-E's evidence -- and anything ordered after it -- never reaches MA's
   synthesis, while S2-E still writes 4 findings to the DB every wave.
   Same intelligence-validity class as the dropped S3 circuit: the synthesis
   position is not seeing its inputs. NOT strictly silent -- the log line
   existed before CC-47 without the numbers and was simply never read.
   HEADROOM EXISTS: MA total=10461 is ~35% of the Cerebras 30,000 ceiling;
   a 45,000-char cap would sit near 58%.
   BLOCKED ON 6d: raising MAX_TOTAL_CHARS again also moves the S1 loop's
   * 0.6 fraction again -- the exact assumption that was wrong in CC-47.
   Log S1's share FIRST, then size the cap with all three consumers visible.

### IMPORTANT
2  INPUT-QUALITY CLUSTER  [R4] -- one problem, three faces, in this order
   2.1  RT full-text fetch. RT is stored as RSS teasers ending
        "Read Full Article at RT.com" while S2-F attributes influence
        operations to it. Owner is fetch_text.py (NOT lens_fetch_tierc.py,
        which is IMF/World Bank/UN Comtrade data).
   2.2  U1a entity_extract visible-text gate -- ONLY after 2.1. As scoped it
        drops 15 RT articles; after 2.1 it drops only the 45 Google News
        link stubs, whose entire body is a base64 tracking URL.
   2.3  TPD burn falls out of 2.2 -- eligible articles 116 -> 55 halves the
        entity_extract call volume that produced item 1.

3  I5 -- CI  [R5 meta-root]
   Rebuild the 2 stale response-guard fixtures from PRODUCTION output
   (both positions write to injection_reports; real shapes live in the
   evidence column), THEN wire pytest into CI. That order matters: wiring
   first turns CI red on a known-benign failure and trains everyone to
   ignore it. Local today: 2 failed / 182 passed. CI runs none of them.

4  DEAD-SYMBOL / UNWIRED-WRITE CI GATE  [R2]
   Retires the class in both repos. Catches 4.1 and 4.2 mechanically.
   4.1  CC-44 -- make get_llm_client() honour _FORCE_PROVIDER so Regular
        Report has a real leg 2. Needs a Cerebras leg-2 probe at the real
        71,286-char prompt first.
   4.2  I6 DAILY_BUDGET ruling -- lean C (fix the timestamp, arm nothing,
        relabel the counter honestly), then A or B once A3 has landed.

5  GENERATED SYSTEM MAP  [R1]
   5.1  The ten stale docstrings -- nearly free after 5, and they rot again
        before it.

6  LIFECYCLE
   A3 -- delete the Gemini legs on s2b/s3b. NOW UNBLOCKED: S2-B has 30,648
   tokens of headroom. Recovers ~7.5 min of every wave.
   I7 -- gemini-2.5-flash dies Oct 16 and lens2 runs on it. Act ~Oct 10.

### IMPORTANT (continued)
6b REGISTER AND ROUTING  [R1/R5]
   6b.1 REGISTER-INTEGRITY GATE (was B7 in the LENS-032 brief). It counts
        rule DEFINITIONS across BOTH formats -- "^## LR-" headings AND the
        compact "^LR-\d{3}\s" entries -- excluding prose cross-references,
        then diffs against the expected unbroken sequence. Both gates in use
        today pass on a register that has silently lost a rule; this is the
        only thing that would have caught LR-124..126 vanishing. Lives in
        tests/ so CI enforces it.
        DECLARED: this item was DROPPED from the 2026-08-07 regeneration
        without a retire decision. That was a violation of the RETIRE clause
        in its first application. Restored here.
   6b.2 MINT LR-138 -- an artifact is not verified by its own consistency.
        Before committing a change AGREED in conversation, grep one
        distinguishing phrase per agreed element and report the hits.
        git log --stat proves message-vs-contents; it cannot prove
        contents-vs-agreement. Evidence: 657f5cf described 3 changes and
        shipped 2; fcde3ad shipped 1 of 6 agreed prompt changes and its
        message was ACCURATE. Register is lens-DOC-002_rules.md in the repo
        ROOT, CRLF 402 / bare_LF 248 -- append onto raw bytes, assert the
        bare-LF count unchanged either side.
   6b.3 AMEND LR-078 -- never pipe a heredoc into python (python - << EOF);
        always cat > file << EOF then python file. Never place a literal
        backtick in a heredoc body; build fences with chr(96). Evidence:
        two stalls in one session, one of which produced 657f5cf.
   6b.4 ADD THE ROUTING RULE to LENS_CONTRACT.md: a finding is routed ONCE
        by what it is -- do (order) / learned (LR) / chosen (D) / true now
        (brief). A finding in two homes is a routing error. Then promote the
        durable hazards out of the briefs into the register: the HAZARDS
        section has been acting as a rules register nobody promotes from,
        which is why LENS-032 carried eleven hazards and the register has
        not gained a rule since LR-137.

6c WAVE SEQUENCING -- NO ORDERING GUARANTEE  [R3]
   Collect and Manage+Analyze are independently scheduled AND independently
   lagged. MA is nominally 28 min behind Collect; Collect has measured
   16m08s, 23m35s and 27m15s. Lag order is NOT guaranteed -- on Aug 5, S2-F
   (cron 30) started BEFORE MA (cron 28). If MA starts before Collect
   finishes it analyses the PREVIOUS wave's articles and reports SUCCESS,
   which is a silent failure under the declared target. Also unexamined:
   GDELT fires ~17 min into MA's ~27 min run, so enrichment writes land
   mid-analysis; Compendium and Ref Export both fire at 30 2.

6d S1 BREAK LOG -- the missing half of the CC-47 side-effect check  [R3]
   The S1 loop uses MAX_TOTAL_CHARS * 0.6 and has NO break log, so S1's
   share of total_chars is invisible. CC-47 raised the cap, moving S1's
   allotment 16800 -> 19200; whether S1 consumes it cannot be answered
   without this log. Tonight's total_chars=27664 is the FIRST measurement
   of the S2 break, so there is no before-figure to compare against.
   Promoted from OTHERS: it is the only way to settle whether CC-47 cost
   S2 content.

### OTHERS
7  B1/B2/B3 provenance (the 1M-context stamp, S2-B's prompt claiming 1M to a
   128k model, S3-B's doubled system prompt) - the S1 loop has no break log,
   so S1 truncation is invisible - LR-078 amendment (never pipe a heredoc to
   python; never put a literal backtick in a heredoc body) - two hardcoded LR
   ranges in the protocol, unverified against the current register -
   analyze_lens_multi.py (fresh session only).

8  RETIRE CANDIDATES -- decide at next regeneration
   S3-B's 600-char summary cap (~5x unused headroom). This is an
   IMPROVEMENT, not a defect; under the declared target it will never rank.
   LR-093's 23 patch scripts in repo root -- now with a measured cost, they
   contaminate the cliff ledger. Either that cost promotes it, or it closes.

## RIDERS ON CLOSED ITEMS  (one grep each, not work items)
- CC-47 cert: "S3 context added to MA prompt" must appear on a wave where S2
  fills its budget.
- S1 SIDE-EFFECT CHECK: the S1 loop uses MAX_TOTAL_CHARS * 0.6, so CC-47
  moved S1's allotment 16,800 -> 19,200. I asserted S1 would not use it
  WITHOUT MEASURING. If "Prompt cap reached at S2 entry" fires with
  total_chars near 19,200, S1 is eating S2's budget and the S1 allotment
  must become an absolute constant.

## STANDING BLOCKER
CC-1d must not ship. CHARS_PER_TOKEN 3 -> 4 over-estimates capacity.
Weakened but not retired: S2-B now sits at 38.7%, not 95.4%. Re-derive
against measured ratios, never assume.

## NEXT SESSION'S MISSION
Item 1 -- the Groq TPD MEASUREMENT. Not a fix.
