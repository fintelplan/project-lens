# NEXT SESSION BRIEF -- LENS-041
Written 2026-09-14 at the LENS-040 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS041.md. This brief
references order items BY NUMBER and never restates them.

## HEAD -- VERIFY, DO NOT TRUST THIS LINE
Last code commit: `7e4f453`. This close adds docs commits on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104). A brief
cannot name its own commit, so this line is stale BY CONSTRUCTION.
Read `git log --oneline -8`.

## READ THIS BEFORE ANYTHING ELSE
**THE SILENT FAILURE IS GONE. THE SYSTEM IS HONEST AND NOT WORKING.**
CC-60 is certified. S2-C and S2-E now say `ANALYSIS_FAILED` when they save
nothing. Do NOT open this session hunting a lying position -- that was
LENS-040's mission and it is finished.

**FIVE OF SEVEN S2 POSITIONS FAIL ON EVERY WAVE, AND THE DAILY REPORT HAS NOT
BEEN DELIVERED SINCE 3 SEPTEMBER.** Order items 1, 2 and 3. Item 3 is the
mission.

**AND THE LENS-040 ORDER'S DIAGNOSIS OF ITEM 2 WAS WRONG.** It said every
fallback converges on one Mistral key and bursts it. Measurement disproved that
in three independent ways. The rewritten item 1 carries the real finding. If
any document still says "burst" or "convergence", it predates 2026-09-14.

## WHAT SHIPPED (LENS-040)
| SHA | What |
| --- | --- |
| `522ff8c` | Mistral added to the probe pack's `PROVIDER_ENDPOINTS`. Old order item 9.3 |
| `863ca8c` | First ministral-8b probe banked (3 trials) |
| `f5809e1` | Probe gains `--save-output` and `--json-mode` |
| `7e4f453` | Nine more probe lines banked (8 trials + one 429) |
| (this close) | order regenerated as LENS041, brief, LR-167..175 appended |

Gates on the code commits: nine anchors asserted unique before any write;
CRLF 1218 -> 1224 -> 1247 -> 1270 with bare_LF 0 at every step; `py_compile`
green each time; dry-run byte-identical on the default path after each change;
zero deletions. Lens CI green on both pushes -- **and see order item 18.2
before treating that green as evidence.**

`7e4f453` was `--amend`ed before pushing: its first message said "eleven
trials" while the commit contained nine. Caught by reading `--stat` against the
message.

## THE SITUATION AT CLOSE
- **Mistral's `mistral-small*` class refuses everything, and it is not us.**
  Order item 1. Seven positions affected, zero Mistral successes across two
  consecutive waves.
- **`ministral-8b-2512` is probed and ready** -- but only with the JSON
  constraint of item 3. Without it, 4/6.
- **The Regular Report is red and a model swap would make it worse.** Item 2,
  D-027.
- **`response_format` count across all of `code/` is zero.** Item 3, root R11.
- **The registry's Mistral row is wrong in every field.** Item 4.
- **Two rulings owed: R10 and R11.** Claude's lean on R10 is RULE OUT, recorded
  at LENS-039 and unchanged. R11 is new this session.

## IN FLIGHT
Item 2.2 is a fork Bro Alpha rules. Item 10.3 and 10.4 are rulings. Item 5 is
designed (D-022) with no code. Item 7 is a decide-then-do. Nothing is
half-shipped: every commit this session is complete and certified or explicitly
banked as evidence.

## HAZARDS FOUND THIS SESSION (these are now LRs)
- **A 429 can be scoped to a model NAME, not to usage.** Two ids absent from
  the account's own model list still returned 429. LR-167.
- **A vendor's Limits page states the ceiling, not what is enforced.** LR-168.
- **Three trials is not a certification.** 3/3 then 1/3 on byte-identical
  input. LR-169.
- **An instrument needs its own census.** The probe pack could not reach the
  provider eight roles fall back to, for 47 days. LR-170.
- **Never accept a provider's defaults where it offers a guarantee.** LR-171.
- **A dry-run that validates preparation but not feasibility grants false
  permission.** LR-172.
- **Saving 400 characters of a body proves it answered, not what it said.**
  LR-173.
- **A red scheduled workflow is a report nobody reads.** Eleven days. LR-174.
- **`finish_reason=length` is a silent failure wearing an HTTP 200.** LR-175.

## LIVE (verified this session by bytes, logs or a live call)
- **HEAD `7e4f453`; local == remote, `ls-remote` checked after each push.**
- **CC-60 CERTIFIED** on `34770305528` (09-13 17:32Z) and re-confirmed on
  `34814620057` (09-14 07:07Z). Both waves: `5 position(s) did not complete:
  ['S2-B','S2-C','S2-D','S2-E','Mission Analyst']`, `All positions complete`
  = 0, `=== S2-E COMPLETE` count 0.
- **Mistral cold calls, one key, 2026-09-13/14:** `mistral-small-2603` 429,
  `mistral-medium-latest` 429, `mistral-small-2506` 429, `mistral-small-2503`
  429, `mistral-small-latest` 429, `glm-5-2` 403, `mistral-large-2512` 403,
  `ministral-14b-2512` 200, `ministral-8b-2512` 200, `ministral-3b-2512` 200,
  `codestral-2508` 200. `GET /v1/models` 200, 46 models.
- **Groq `openai/gpt-oss-120b` 200 cold. Cohere `command-r-plus-08-2024` 200
  cold.** Both live as of 09-13 16:45Z.
- **Mistral account:** Free plan, $0.16 of $10 monthly allowance, 17 days to
  reset, $0.00 billing, no payment method. Console TPM/RPS per model read
  directly.
- **Model cards:** `ministral-8b-2512` ctx 262,144; `ministral-14b-2512`
  262,144; `ministral-3b-2512` 131,072; `mistral-small-2603` 262,144. All four
  `deprecation: None`.
- **ministral-8b on S2-E's real 9,457-char prompt:** without `--json-mode`
  3/3 then 1/3 (both failures an unescaped quote on `IMF "caved"`); with it,
  5/5 parse, actors 7/6/8/7/8, low 4/4/4/4/4, budget 15-20%, finish=stop.
- **Regular Report probe:** prompt 64,968 chars; `ministral-14b` and
  `ministral-8b`, 3 trials each, **6/6 `finish_reason=length`, 6/6 budget 100%,
  `tokens=23479` on all six**, every body cut mid-PART-2.
- **Regular Report failure boundary:** `33726430126` Sep 3 07:07Z green,
  `33847533497` Sep 4 07:10:58Z first 429 -> `All 3 attempts failed for
  mistral` -> `Fallback also failed` -> `{"status":"FAILED"}` -> exit 1.
- **`response_format` in `code/*.py` = 0.** Fourteen files call
  `api.mistral.ai`; twelve `json.loads`.
- **All twelve JSON legs DO strip code fences** (eleven via `startswith`, one
  via `re.sub` in `lens_s3f_countercheck.py:324`). No leg is fence-unsafe.
- **Row counts 2026-09-14:** `lens_reports` 3,978; `lens_raw_articles`
  124,571.
- **Lens CI:** 35 passed / 0 failed; registry self-test 24 roles / 9 wire pairs
  / 5 limit rows.

## BANKED (not verified this session)
- `pg_database_size` -- NOT re-measured. 374 MB and the late-October date are
  from 2026-09-11 and are a projection from an 11-day sample.
- The `mistral-small` calibration band (actors 8/7/8, low 5/3/5) comes from the
  comment at `lens_s2e_legitimacy.py:443-446`. It could not be re-measured live
  because the model 429s.
- Item 16 (Groq TPD) is from Aug 10 and 35 days stale.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- not re-audited since Aug 5.
- Why the Supabase restriction lifted on Aug 31. UNKNOWN, and it should stay
  written as unknown.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **Ruled the 2.3 fork toward "repoint" while still believing the burst
  theory.** The ruling survived; its stated reasoning did not. Corrected the
  same session by measurement, but the ruling was issued before the decisive
  cold call was made, and it should not have been.
- **Called the 4/6 parse rate "randomness".** It is content-triggered -- both
  failures on the same token. A wrong mechanism stated with a number attached.
- **Predicted the CC-62 patch at +33 lines; it was +23.** Arithmetic, caught by
  `git diff --stat`.
- **Said `/tmp` had been cleared.** It had not -- my own `ls | head` cut the
  listing before the file in question. A conclusion drawn from a truncated
  instrument.
- **Ran a zero-match grep and read it as absence FOUR times:** `chr(96)`
  fence-stripping in `lens_s2e_legitimacy.py`, the `re.sub` form in
  `lens_s3f_countercheck.py`, `^PART ` at line start, and the probe jsonl's
  field names. LR-113 is mine and I broke it four times in one session.
- **Warned that `fit_max_tokens`'s hardcoded Groq 7500 would distort Mistral
  budgets.** It does not -- the ceiling is read from the registry. Read a help
  string as if it were code.
- **Predicted the probe would borrow a neighbouring registry row's ceiling for
  an unregistered model.** It does not; it logs UNRESOLVED and returns the cap.
- **Wrote a commit message naming a count the commit did not contain.** Caught
  before push, amended.
- **ESTIMATES THAT HELD:** that the CC-60 cert would show exactly five failing
  positions with that exact name list (it did, both waves); that the cold
  Mistral call would 429 and kill the burst theory (it did); that `--json-mode`
  would take the parse rate to 5/5 (it did); that the band would hold (it did);
  that Sep 12's X and Sep 13's X were infra cancels unrelated to CC-60 (they
  were); that no commit existed at the Sep 3/4 boundary (none did).

## WHAT I DELIBERATELY DID NOT DO
Did not wire `ministral-8b` anywhere. It is probed, not deployed, and deploying
it without item 3 ships a position that fails a third of the time.

Did not touch `lens_regular_report.py`. A model swap there is the one change
that would turn a loud failure into a silent one, and the session that removed
the last silent failure is not the session to add one back. D-027.

Did not edit `code/lens_models.py`. Item 4 is understood, measured and written
down; it was reached at hour fifteen and a registry edit made tired is a
registry edit made twice.

Did not chase the `response_format` sweep after counting it. The census is the
deliverable (item 3 lists all twelve); the sweep is the next session's mission.
LENS-038 ran a sweep of this exact shape and missed two strings, and that cost
five weeks.
