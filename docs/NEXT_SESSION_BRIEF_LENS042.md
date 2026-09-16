# NEXT SESSION BRIEF — LENS-042
Written 2026-09-16 at the LENS-041 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS042.md. This brief
references order items BY NUMBER and never restates them.

## HEAD — VERIFY, DO NOT TRUST THIS LINE
Last code commit: `0191362`. This close adds docs commits on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104). A brief
cannot name its own commit, so this line is stale BY CONSTRUCTION.
Read `git log --oneline -20`.

## READ THIS BEFORE ANYTHING ELSE
**THE PIPELINE WORKS. IT WORKS ON ONE MODEL.**

`[S2-ORC] All positions complete.` on wave `35063596665` — first time in
five weeks. The Regular Report delivered to Telegram at 07:37Z — first time
since 2026-09-03. Both are real and both were certified by reading the log,
not by trusting a green tick.

**And every position that came back except S2-A and lens1 is running on
`ministral-8b-2512`.** Cerebras is 402 account-wide with a `$0.00` balance.
Gemini `2.0-flash` has been 404 for three and a half months.
`mistral-small*` has 429'd for twelve days. That is ROOT 12 and order
item 1, and it is the mission.

**Do NOT open this session hunting broken positions.** LENS-041 fixed
eleven of them. The question now is what happens when the one live leg
fails.

## WHAT SHIPPED (LENS-041) — eighteen commits
| SHA | What |
| --- | --- |
| `018693b` | Bank six ministral-8b json-mode probe lines from LENS-040 |
| `8a7e899` | LENS-040 close docs that had never been committed |
| `b689893` | LENS-040 probe bodies + Mistral model list as evidence |
| `74a6189` | **CC-63** `response_format` on eight Mistral JSON legs |
| `2f1eb11` | **CC-64** nine registry fallback legs -> ministral-8b-2512 |
| `73d2a84` | **CC-65** Regular Report logs usage + finish_reason |
| `fe9d733` | **CC-66** PART 4 built in code, not by the model |
| `eabb261` | Bank the CC-66 measurement (5/5 length at 4096) |
| `d0bc391` | **CC-67** Regular Report cap 4096 -> 8192, on measurement |
| `ee6ca35` | **CC-68** Regular Report primary -> ministral-8b-2512 |
| `9e281b8` | **CC-69** docx heading test matched 0 of 41 real headings |
| `40c562f` | Empty the repo root — 22 loose files archived/banked/deleted |
| `49fec6a` | **CC-70** S2-B's 404 branch skipped its own fallback; S3-B model |
| `f602edc` | **CC-71** s2c + s3f primaries (single-leg, no fallback at all) |
| `7aac065` | **CC-72** T25 asserted a corpse for the second time |
| `0191362` | **CC-73** S3-D and S2-A wires caught up with the registry |

`f602edc` turned Lens CI **RED** for eight minutes. `7aac065` fixed it. See
"claims that were wrong" below — that one is a method failure, not a code
failure, and it has an LR.

## THE SITUATION AT CLOSE
- **Cerebras is gone.** Order item 1, the mission. Four positions' primary.
- **Cloudflare sits in the registry, wired to nothing.** Item 1.2. The
  closest thing to a replacement and it has never been probed.
- **S3-A fails with no error text at all.** Item 2. Not the 402.
- **Three prose positions still on the dead class** — deliberately. Item 4.
  Instrument first, swap second.
- **R10 is still owed.** Lean unchanged: RULE OUT.

## IN FLIGHT
Nothing is half-shipped. Every commit is complete, gated, and either
certified or explicitly banked as evidence. The only unfinished work is
work that was never started.

## HAZARDS FOUND THIS SESSION (these are now LRs)
- **Post-processing does not reduce generation.** LR-176.
- **A censored measurement cannot calibrate a cap.** LR-177.
- **Moving input invalidates a cross-run comparison.** LR-178.
- **Derive a test fixture from the POSITION, not from a constant.** LR-179.
- **Run all three CI gates locally before every push.** LR-180.
- **`json_object` guarantees VALID JSON, not COMPLETE JSON.** LR-181.
- **Providers say whether to retry; we never listen.** LR-182.
- **`grep -c` counts your own comments as code.** LR-183.

## LIVE (verified this session by bytes, logs or a live call)
- **HEAD `0191362`; local == remote; Lens CI green.**
- **Wave `35063596665` (2026-09-16 06:24Z):** `[S2-ORC] All positions
  complete.` · `[S3-ORC] 1 failed: ['S3-A']` · S2-C `COMPLETE | 4 reports |
  total steps=2 | 55.7s` (was `ANALYSIS_FAILED | 161.9s`) · S2-B Mistral
  fallback `6 findings, score=0.85` · S3-B fallback OK at 10,135 chars on
  attempt 2 · zero JSON parse failures.
- **Wave `34936864688` (2026-09-15 06:25Z):** `2 position(s) did not
  complete: ['S2-B','S2-C']` (was five), S2-E `COMPLETE` with
  `reports_saved: 4`, MA saved a macro report `contamination_depth=DEEP`.
- **Regular Report `35069334184` (2026-09-16 07:35Z):** `REGULAR usage:
  mistral/ministral-8b-2512 prompt=19082 completion=5682 total=24764
  budget_used=69% finish_reason=stop` · `Citation validation: attempted=38
  valid=38 stripped=0` · `PART 4 built in code: 38 of 38` · `sent=True` ·
  1,806 words · 62.5s.
- **Cerebras cold, 2026-09-16:** `gpt-oss-120b` **402**, `qwen-3.8-27b`
  **402**, `GET /v1/models` **200** (2 models), model card **200**,
  `x-should-retry: false`. Console: balance `$0.00`, tokens last month
  `0.0`, API calls last month `0.0`. **Key fingerprint matches the console
  account** — same account, not a key mix-up.
- **Regular Report probe, ministral-8b, real prompt:** at 4096 — 5/5
  `finish_reason=length`, `completion_tokens` exactly 4096 every time, PART
  3 reached 2/5, PART 4 1/5. At 8192 — 4/5 `stop`, completions 3,686 to
  8,192. At 16384 — 3/5 lost to ReadTimeout, survivors 28% and 27%.
- **chars/token on the Regular Report:** 2.64, 2.91, 2.97, 3.07, 3.30.
- **Heading census:** 41 PART headings across 15 probe bodies; the old docx
  test caught **0**; the new one catches **41**.
- **Prompt size moves daily and within a day:** 64,968 (09-14) -> 63,964
  (09-15 10:31) -> 68,604 (09-15 13:39) -> 63,305 (09-15 13:52) -> 64,109
  (09-16 07:36). No figure here is a constant.
- **Registry:** 24 roles, 10 wire pairs, 5 limit rows. Nine fallback legs
  and two primaries on `ministral-8b-2512`.
- **Local test gate:** `python tests/test_lens_quota_guard.py` -> 35
  passed, 0 failed.

## BANKED (not verified this session)
- `pg_database_size` — NOT re-measured since 2026-09-11. 374 MB and the
  late-October projection are from an 11-day sample.
- Groq TPD (200,000) — from the registry, VERIFIED-console 2026-07-28.
  **Fifty days old, and Cerebras's row of the same vintage is now false.**
- The `mistral-small` calibration band — still from the source comment at
  `lens_s2e_legitimacy.py:443-446`, still unmeasurable live.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 — not re-audited since Aug 5.
- Why the `mistral-small*` class 429s. UNKNOWN. Keep it written as unknown.
- Why the Supabase restriction lifted on Aug 31. UNKNOWN.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **Pushed fifteen times without running the one test file CI executes.**
  `f602edc` turned main red. The three CI gates take two seconds locally
  and memory already recorded, from 2026-08-02, exactly which three steps
  CI runs. Knowing it and doing it were different things. LR-180.
- **Predicted "Mistral 429 zero" on the CC-63/64 cert wave.** There were
  three — all `[S1-RPT]`, a position I had myself decided not to touch and
  then left out of my own prediction.
- **Predicted Cerebras would answer 200** on the cold probe, reasoning from
  that morning's successes. It was 402, and the successes had all been
  fallbacks.
- **Predicted `fb_max_tokens` would rise** after the ceiling moved
  50,000 -> 262,144. It did not: `fit_max_tokens` returned the cap before
  and after. A changed number that changes no behaviour.
- **Guessed `PROVIDER_LIMITS` was keyed by provider** and that the new row
  had overwritten the old one. It is keyed by pair; the real cause was the
  RPD axis filter. Right conclusion, wrong mechanism, stated before reading.
- **Said "E first, then A"** on the Regular Report fork, then withdrew it
  one turn later: stripping PART 4 after generation cannot recover tokens
  already spent. Reversed before any code was written.
- **Counted `_log_completion` as 4 sites (it was 3), `valid_ids` as 2 via a
  substring grep (6), alias literals as 11 then 2 then 3 (16, then 5, then
  4).** Four count errors in one session, all from `grep -c` counting
  comments, substrings, or my own new text. LR-183.
- **Emitted a rollback command inside a runnable block**, and it ran: a
  clean, fully gated CC-64 was reverted by my own `git checkout`. Re-run
  cost one round trip. Conditional commands do not belong in paste blocks.
- **Twice handed Windows Python a MINGW path** (`/c/school/lens/...`),
  twice got FileNotFoundError, and the traceback printed the correct
  `C:\school\lens\...` both times before I read it.
- **Predicted `<ID>` substitution would work** and pasted a literal
  placeholder into a runnable block. Same class as LENS-028's `<RUN_ID>`.
- **ESTIMATES THAT HELD:** that item 3 was 10 legs and not 12, found by
  reading every `json.loads` instead of trusting the census proxy; that the
  two zero-JSON-word files were prose and the json_object hazard was
  therefore moot; that the docx heading test caught zero of the real
  headings (`catches=0 misses=41`); that the new one catches all 41; that
  S2-E would pass the cert gate; that MA would not truncate; that the CC-70
  signature string would appear verbatim; that S2-B's 82,007-char prompt
  would survive on an unprobed model; that CI's failure was in the test
  file and not in the code.

## WHAT I DELIBERATELY DID NOT DO
Did not swap the three prose positions off the dead model. They log no
`finish_reason` and ask for PART-structured prose — the exact shape that
returned `length` 5/5 at 4096. Swapping first and instrumenting later is
how a loud failure becomes a silent one. Item 4 says instrument first.

Did not touch `gemini-2.0-flash`. CC-70 made S2-B reach a live fallback, so
the position works; changing the primary means choosing between a model
that dies in thirty days and one that has never been probed, inside a
commit about something else. Item 3.

Did not add a retry gate for `x-should-retry`. It is correct and it is
cheap, but it arrived at hour twenty-two after main had already been red
once, and a guard written tired is a guard written twice. Item 5.

Did not probe Cloudflare. It is the most valuable unopened box in the repo
and it deserves a session that starts with it, not one that ends with it.
Item 1.2.
