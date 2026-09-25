# LENS-047 POST-CLOSE ADDENDUM (2026-09-25, after `c02bfc0`)

Project Lens — Bro Alpha. Read at the LENS-048 open **with** `NEXT_SESSION_BRIEF_LENS048.md`; it corrects
that brief's expected HEAD and adds what happened after the close commit.

## 1. Expected HEAD at the LENS-048 open

The commit that carries this file, on top of `81689bb` (CC-127b) -> `2324ad8` (CC-127) -> `c02bfc0` (close).

## 2. Morning wave, Sep 25 -- order item 1 read (head `c02bfc0`, all certs carried)

| Cert | Result | Bytes |
| --- | --- | --- |
| CC-120 second half | **certified** -- L3.4 byte row met on two runs | S2-F run 36102521904: `S2F_DELIVERY new=0 changed=0 cleared=0 quiet=4 failed=0`; ledger still 1 row |
| CC-122 | certified again | `29 sources -> 8 picked`, `7 already scored` excluded |
| CC-125 | **certified** | `S2F_COVERAGE scored 14 of 24 attempted scorings (58%)` |
| CC-126 breaker | **certified** | collect 36099018817: one `ENTITY_EXTRACT_TPD_BREAKER`; Groq 429 x83 (was 233) |
| CC-119 + CC-126 column | **certified** | S3-A row: first_domino 248 chars, "confirmed if ... disconfirmed if ..." |
| CC-124 cycle + presentation | **certified** | `lens_reports.cycle = 2of1`; canary and S2 messages clean (screenshots) |
| CC-126 markings | **certified** | S2 docx: 0 CLASSIFIED/DISSEMINATE |

## 3. Two live failures in the same wave -- found, fixed, shipped

- **No macro report, no Daily Brief, silently.** M+A run 36102604108: `MA fallback failed: Expecting ','
  delimiter` -> ANALYSIS_FAILED; the S2 docx read `THREAT: UNKNOWN`; the step stayed green. The fallback
  leg (the only live MA leg) had no retry on invalid JSON.
- **No S3 message.** `Telegram error 400 ... Unsupported start tag "sign"`: CC-119's template
  `confirmed if: <sign>` was copied as tags, and CC-126 put first_domino into the message for the first
  time; model text reached Telegram HTML unescaped. Claude's error (two of its own changes interacting).
- **CC-127 `2324ad8`:** `_presentable` escapes a bare `&` and any `<` that does not open a Telegram tag;
  S3-A strips `<sign>` tags before storing; the MA fallback asks once more on invalid JSON; a failed
  analysis sends "MISSION ANALYST FAILED this wave ... no Daily Brief". SQL: today's `<sign>` tags
  removed from `lens_system3_reports.first_domino` and `lens_predictions.prediction` (0 left).
- **CC-127b `81689bb`:** CC-127's gate imported the Mission Analyst in CI, where no provider SDK is
  installed (`No module named 'cerebras'`); CI went red while production was fine. The gate now puts in
  placeholders for absent SDKs; proved with the SDKs hidden before the commit. CI green (36129616236).

## 4. Certs owed from this addendum (add to order item 1)

- CC-127: the next M+A wave sends the S3 message (`can't parse entities` = 0); a Daily Brief arrives, or
  "MISSION ANALYST FAILED" does; `asking once more` appears only when the first answer was invalid JSON.
- Completion-test effect: the morning's missing Brief without a FAILED line was an **L1.2 miss**; with
  CC-127 the miss would speak. L3.4 is now fully certified (two runs).

## 5. Also for LENS-048

- The S3-A prompt template still writes `<sign>`; CC-127 strips it. Changing the template is a prompt
  change (probe first) -- a small item beside order item 12.
- Docx senders still carry markdown: Regular Report `**` x874, S1 docx `**` x36 and `#` x5 (L2.5 covered
  only the Telegram sender). Joins order item 12.
- Two rules appended to the register: LR-285, LR-286.
