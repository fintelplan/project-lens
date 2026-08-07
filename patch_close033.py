from pathlib import Path

order = """# LENS - CURRENT TARGET AND WORKING ORDER
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
- CC-47 -- S3 context now has a reserved 4,000-char slot. Awaiting cert.
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
"""

brief = """# NEXT SESSION BRIEF -- LENS-034
Written 2026-08-07 at the LENS-033 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER.md. This brief references
order items BY NUMBER and never restates them.

## HEAD
d1acc5d. CI green. Working tree clean at close.

## WHAT SHIPPED (LENS-033)
| SHA | What |
| --- | --- |
| a05f3b3 | llama-3.3-70b baselines banked for entity_extract + ai5_watchdog (D-008, unrepeatable after Aug 16) |
| fb3fb00 | CC-43 regular_report Groq leg 3 removed -- CERTIFIED Aug-7 morning wave |
| 7039cc1 | CC-45 MA logs s3_chars and overflow when S3 context is dropped |
| ebabb79 | second entity_extract baseline, also null (6 trials, 2 pools, all empty) |
| 670b61d | CC-46 S2-B strips markup -- CERTIFIED MA #257 |
| 657f5cf | MISSION AND SCOPE + DISCOVERY POLICY + target file (shipped 2 of 3 it claimed) |
| abe75ee | completed what 657f5cf's message already claimed |
| d1acc5d | CC-47 S3 context reserved slot -- AWAITING CERT |

## IN FLIGHT
Order item 1 is the declared next mission. The two riders at the end of the
order file are one grep each on the next wave.

## HAZARDS FOUND THIS SESSION
- A run can fail with ZERO application logs. MA #256 was 35 lines: GitHub
  Actions incident 15:22-16:33 UTC, not our code. CHECK THE LINE COUNT
  BEFORE READING ANY LOG -- headSha alone is not enough.
- Literal backticks inside a heredoc body stall this shell. Two incidents;
  one produced a commit whose message described work that never shipped.
  Build fences with chr(96). And never pipe a heredoc into python.
- Line endings are PER FILE, not per directory. lens_mission_analyst.py and
  lens-manage-analyze.yml are LF; lens_s2b_coordination.py and
  requirements.txt are CRLF. The "code/ is CRLF" rule is folklore.
- git log --stat after every commit: 657f5cf's message and its contents
  disagreed and nobody noticed until the next session.
- refusal_flag fires on any response under 200 chars, so a correct empty
  extraction always trips it. It is a FLAG, never a verdict.
- The S3 report line reports DATA PRESENCE, not execution. S3-C=no means no
  recent report, not a failure. S3-A can read yes while skipping.

## LIVE (verified this session by bytes or logs)
- S2-B: 59,065 chars / 18,352 prompt tokens / 19,352 total = 38.7% of the
  50,000 TPM. Extractor is lxml. 200/200 articles. 4 findings.
- S3-B: 30,803 chars / 7,264 tokens. Fixed-size by construction
  (MAX_REPORTS 28, summaries clipped to 600) -- it cannot grow.
- MA: prompt 7,741 / completion 2,606 / total 10,347 = ~34% of the Cerebras
  30,000 ceiling. S3 section measured at 949 chars.
- Groq TPD 200,000, ORG-SCOPED, hit 199,747 during Collection #247.
- Cron map read from bytes; the pipeline has NO explicit sequencing, and MA
  is nominally only 28 minutes behind Collect, which has run 27m15s.
- Aug-16 migration COMPLETE. Honest ledger 19 hits, none able to fire.

## BANKED (not verified this session)
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- supplied Aug 5, not re-audited.
- lens_orchestrator.py FALLBACKS "no delivery path to the child" -- this is
  the same reasoning shape that failed on leg 3. Re-verify before trusting.
- The TPD reset boundary. Do not assume midnight Pacific.
"""

Path("docs/LENS_TARGET_AND_ORDER.md").write_bytes(order.encode("utf-8"))
Path("docs/NEXT_SESSION_BRIEF_LENS034.md").write_bytes(brief.encode("utf-8"))
for f in ("docs/LENS_TARGET_AND_ORDER.md", "docs/NEXT_SESSION_BRIEF_LENS034.md"):
    d = Path(f).read_bytes()
    print(f, len(d), "bytes | CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))
print("DONE")
