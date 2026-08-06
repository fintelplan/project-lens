# LENS - CURRENT TARGET AND WORKING ORDER
Regenerated 2026-08-06 at LENS-033. Supersedes any earlier dated version.
Governed by LENS_CONTRACT.md (MISSION AND SCOPE + DISCOVERY POLICY).

## CURRENT TARGET
**Lens produces valid intelligence, unattended, with no silent failure.**

Declared 2026-08-06. The previous target -- "survive the Aug-16
llama-3.3-70b cliff" -- was ACHIEVED AND CERTIFIED (CC-43 live on fb3fb00,
Aug-6 morning wave green on all seven scheduled workflows). It was never
formally closed, and the working target drifted to this larger one
undeclared. That drift is why the item list grew from 13 to 15 during a
single session.

All ranks below are relative to THIS target. If the target changes,
regenerate the whole order and say why.

## ROOTS
R1  No source of truth about what the system IS. Descriptions are written
    true and go false when code moves; only executing artefacts stay honest.
R2  Written-but-never-wired code, with no detector.
R3  Numbers set once, never re-derived against reality.
R4  Input quality never verified -- the collector does not check what it
    collected.
R5  The tests do not run. 182 tests, zero in CI. META-ROOT: every other root
    could have been caught by a test that ran.
--  NOT A ROOT WE CONTROL: provider lifecycle. Weather (D-014); the registry
    (D-001) is already the mitigation.

## WORKING ORDER

0  IN FLIGHT, wave-blocked -- complete before opening anything new
   0.1  U1b cert (CC-46 live on 670b61d) -- read the evening wave
   0.2  CC-47 size MA MAX_TOTAL_CHARS -- needs CC-45 overflow=   [R3 stopgap]

1  I5 -- rebuild 2 response-guard fixtures from PRODUCTION output, THEN wire
      pytest into CI. In that order: wiring first turns CI red on a known
      benign failure and trains everyone to ignore it.   [R5 meta-root]

2  RT full-text fetch   [R4]
      The canary reads RSS teasers from the source S2-F cites most. Owner is
      fetch_text.py (NOT lens_fetch_tierc.py, which is economic data).
   2.1  U1a entity_extract visible-text gate -- ONLY after 2, never before.

3  Dead-symbol / unwired-write CI gate   [R2] -- retires the class in both repos
   3.1  CC-44 _FORCE_PROVIDER -- Regular Report's real leg 2
   3.2  I6 DAILY_BUDGET ruling -- lean C (fix format, arm nothing, relabel
        honestly), then A or B once A3 has landed

4  Generated system map   [R1]
   4.1  The ten stale docstrings -- nearly free after 4, and they rot again
        before it

5  Lifecycle -- A3 (delete Gemini legs, after 0.1 re-measures); I7 Oct-16 lens2

6  Remainder -- B1/B2/B3 provenance; S3-B 600-char cap (~5x headroom);
      LR-093 (22 patch scripts, and they contaminate the cliff ledger);
      analyze_lens_multi.py (fresh session only)

## STANDING BLOCKER
CC-1d must not ship. CHARS_PER_TOKEN 3 -> 4 would over-estimate S2-B capacity
on a position measured at 87.5-95.4% of TPM.

## NEXT SESSION'S MISSION
Item 0.1 + 0.2 if the wave has landed; otherwise item 1.
