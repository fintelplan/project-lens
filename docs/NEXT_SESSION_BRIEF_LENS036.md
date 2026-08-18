# NEXT SESSION BRIEF -- LENS-036
Written 2026-08-18 at the LENS-035 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER.md. This brief references
order items BY NUMBER and never restates them.

## HEAD -- VERIFY, DO NOT TRUST THIS LINE
Last code commit: 93a30c3. This close adds ONE docs commit on top of it, so
HEAD at your open is that docs commit. `git ls-remote origin refs/heads/main`
is the only truth (LR-104). Two briefs in a row have named a stale HEAD --
a brief cannot name its own commit, so this one does not try.

## WHAT SHIPPED (LENS-035)
| SHA | What |
| --- | --- |
| b957d6c | CC-50 -- RETRY_SLEEP 10 -> 65, retry lands in a fresh TPM window |
| 93a30c3 | CC-51 -- absolute per-tier allotments. **CERTIFIED s1=4/4 on #277 #278 #279** |
| (this commit) | LENS-035 close: order regenerated, brief |

Both code commits were preceded by a read of the gate they depended on, and
both messages were checked against `git log --stat`.

## IN FLIGHT
Order item 1.1 is the declared next mission and it needs a RULING before a
patch. Items 1.5 (retro query) and 2 are ready to work. Nothing is blocked
on a wave.

## HAZARDS FOUND THIS SESSION
- **Paste mangling struck four-plus times despite the ritual, and the file
  was intact EVERY time.** The terminal echo is display-only. Verify by grep
  and byte count, never by what appeared on screen.
- **Multiple command blocks in one message get PARTIALLY pasted.** A commit
  block ran while its patch block did not, committing nothing and printing
  "working tree clean" -- which looks exactly like success. One load-bearing
  block per message.
- **Never put a rollback command in the same message as an apply.** One did,
  it got pasted, and it reverted a good patch. (Now LR-140.)
- **Placeholder commands get pasted literally.** `MA=<databaseId from above>`
  produced a bash syntax error. Derive IDs in the command; never ask for a
  substitution. This is the second time in this project's history.
- **autocrlf will flip this repo's LF files to CRLF on the next Windows
  checkout**, so absolute `b"\r\n" not in raw` asserts start failing on files
  nobody touched. Assert RELATIVE to the file's state at read time.
- **A guard whose expected value is hand-derived is the defect it exists to
  catch.** A patch asserted a hand-counted line delta of 13; the real delta
  was 18. It failed safe, but the number was a banked estimate living inside
  a tool built to stop banked estimates. (Now LR-139.)
- A `grep -c` uniqueness checklist passed two SHAs that legitimately recur.
  The distinguishing phrase must be unique BY CONSTRUCTION.

## LIVE (verified this session by bytes or logs)
- MA #277/#278/#279, all headSha 93a30c3:
  `s1=18283 (4/4)` / `19838 (4/4)` / `23628 (4/4)`;
  corrections 14894 / 13600 / 13731; s2 9425 (3/30) / 9370 (3/29) /
  11293 (4/27); counted 43498 / 43746 / 49566; actual-counted = 149 on all.
  Usage: prompt 10740 / 10678, total 13273 / 12913 = ~44% of the ceiling.
- Live constants: MAX_S1_TOTAL_CHARS 25000 · MAX_S2_TOTAL_CHARS 12000 ·
  MAX_TOTAL_CHARS 56000 · MAX_S1_CHARS 6000 UNCHANGED · S3_RESERVE_CHARS
  4000 UNCHANGED · RETRY_SLEEP 65 · MAX_RETRIES 2.
  `MAX_TOTAL_CHARS * 0.6` is asserted ABSENT from the file.
- `fit_max_tokens` = `max(768, min(cap, usable - prompt_chars//3))`, usable =
  ceiling - max(MARGIN_FLOOR 200, 8% of ceiling) = 27,600 for Cerebras.
  SYSTEM_PROMPT is 4,024 chars, so prompt_chars ~= actual_prompt + 4,129.
- Cerebras gpt-oss-120b: RPM 5 · RPD 2,400 · TPM 30,000 · TPD 1,000,000 ·
  CTX 131,000 · MAX_COMPLETION 40,000. `request_ceiling` = min(CTX, TPM) =
  30,000, so TPM binds, not context.
- Groq refills CONTINUOUSLY at Limit/86400 per second. No reset boundary
  exists. No `-day` header exists; a live 200 carries exactly six
  `x-ratelimit-*` headers, all per-minute or per-day-requests.
- `timeout-minutes: 35` on lens-manage-analyze against a ~22 min run, and at
  most ONE retry sleep ever occurs.

## BANKED (not verified this session)
- Everything in item 3 (TPD saturation) is from Aug 10 and eight days stale.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- not re-audited since Aug 5.
- The register at 52 rules / LR-090..141 / no gaps -- last counted Aug 9.
- `GROQ_S2F_API_KEY` absent from local env. Local env is NOT Actions
  secrets, so this is a lead, not a verdict.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- Predicted `s1=0` a fourth time on MA #264. It was 1/4 -- the first
  non-zero ever measured, and it happened without any change from us.
- Called the #264 result a zero-sum S1-for-S2 trade. The bytes said the
  whole prompt SHRANK by 2,702 chars: a `break` had abandoned a tier with
  2,971 chars of budget unused. Part of the "shortage" was waste.
- Back-solved `usable` as 25,500 assuming a 15% margin. It is 27,600 at 8%.
  The waves could not distinguish it because nothing sits near the boundary.
- Sized MAX_TOTAL_CHARS at 57,500, which was 129 chars past the point where
  TPMGuard starts logging an over-limit error. Corrected to 56,000 before
  shipping.
- Added SYSTEM_PROMPT tokens on top of a chars/token ratio that already
  included them, overstating the token estimate.
- Predicted counted_total ~46,700 and prompt ~13,600 tokens at 4/4. Actual:
  43,498-49,566 and 10,678-10,740. Over-predicted tokens by ~26% because the
  mix shifted toward prose (ratio 4.064, above the whole prior range).
- Used the Cerebras 30,000 while calling it a context window. It is TPM.
  Right number, wrong reason -- and James caught it.
- ESTIMATES THAT HELD: the 149-char undercount, predicted at "~150+" before
  measuring, exact on all 19 waves; and the S1 entry-size bound derived by
  exclusion, which put four lenses at 16,000-20,800 against an actual
  18,283-23,628.
