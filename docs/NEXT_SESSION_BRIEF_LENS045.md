# NEXT SESSION BRIEF — LENS-045
Written 2026-09-21 at the LENS-044 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS045.md. This brief
references order items BY NUMBER and never restates them.

## HEAD — VERIFY, DO NOT TRUST THIS LINE
Last code commit: `b1e28b7`. This close adds a docs commit on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104).
Read `git log --oneline -15`.

## READ THIS BEFORE ANYTHING ELSE
**Certs still owed: Thursday's S3-D run, and CC-91.**
- **The 2026-09-21 evening S2-F wave was READ before close** (run
  `35640858699`, 18:49Z, 17m23s): CC-95 and CC-92's Cloudflare path are
  certified, and the refusal mechanism is now known. CC-91 is still
  unexercised.
- **Thursday 2026-09-24, S3-D's first 90-day run on the CC-93/94/96 code** —
  order item 4. The 90-day prompt WAS probed before close (order item 3,
  done): `stop`, 48% of 16000, 121s against a 240s timeout.

The session ran from Sunday evening into Monday night (UTC+7) and shipped ten
commits. It spent four Mistral calls (Lens 4's key, reason written in
`LENS_ALLOW_CANARY_AIR`, no wave running) and no other canary air.

**What is new and must not be undone:**
1. `lens_s2f_helpers.lens_filter_from_argv()` is the ONE parser of the lens
   argument for all three aggregators. CC-88 had fixed a hand-copied expression
   in Clarity only; Watch and Verification kept reading `--dry` as a lens.
2. Every S2-F aggregator counts rejected writes and exits non-zero on them.
3. The S2-F workflow runs Watch, Clarity, Verification and Direction B with
   `if: ${{ !cancelled() }}`. Stage 1 is deliberately NOT guarded.
4. A provider refusal is logged with its own reason, redacted against the
   real `.env` VALUES (`lens_framing_rubrics._redact`) — not against patterns.
5. S3-D samples its whole window evenly IN TIME, in two passes (ids first,
   then the chosen rows); an empty stretch of time lends its place to the
   nearest real row, so the sample stays at k and nothing is invented. It
   labels every window by `window_days`, takes model and budget from the
   registry at 16000, timeout 240, and logs "N in the newest third".
6. `ENSEMBLE_LEGS = [cloudflare]`. Cerebras is gone from S2-F. Each provider
   has its own pacing guard (`_guard_for`).
7. Lens CI has NINE gates.

## WHAT SHIPPED (LENS-044) — ten commits
| SHA | What |
| --- | --- |
| `688c567` | **CC-89** Watch/Verification count rejected writes; one `--dry` parser; Verification's silent no-client return now fails. Gate 7 |
| `628a6d7` | **CC-90** Verification's decorative Mistral env block deleted (order LENS044 item 6); four-schedule comment fixed (10.3) |
| `387877d` | **CC-91** a failed Stage 1 no longer skips the four steps that read existing rows |
| `a6ff408` | **CC-92** the provider's own refusal reason is logged, redacted. Gate 8. `.gitignore` gains `_s4*` |
| `125c359` | probe: S3-D fixture built by capturing `run_s3d()`'s own request; three trials on the record |
| `f84fa9b` | **CC-93** S3-D even window sample, truthful labels, registry wiring, 16000 / 240s. Gate 9 |
| `1831ac0` | **CC-94** S3-D sampling pages past PostgREST's 1000-row cap |
| `dc1297a` | **CC-95** Cerebras leg removed; one guard per provider; env restored symmetrically. Gate 5 extended |
| `d047b72` | probe: S3-D probed at 90 days (`S3D_PROBE_WINDOW_DAYS`); harness timeout 180 -> 300, it had been stricter than production's 240 |
| `b1e28b7` | **CC-96** S3-D samples evenly in TIME, not by row count; a gap lends its place. Gate 9 extended |

Every gate was proven to BITE before commit: the defect was re-injected and
the gate went RED. Lens CI green on every push; `ls-remote` matched each time.

## CERTIFIED THIS SESSION
| What | Evidence |
| --- | --- |
| CC-85 exit code | 2026-09-20 evening: `scored=0 skipped=0 failed=24` -> `wrote ZERO rows ... exiting 1`. First time a dead run went RED |
| CC-85 rows (LENS044 item 3) | Monday wave: 24 rows, all `provider='cloudflare'`, `ensemble_mode=false` |
| CC-87 (LENS044 item 4) | `Clarity writes: 7 ok`, 0 rejected; five rows with `sample_size` 0 or 1; log and DB reconcile 9 = 9; constraint `CHECK ((sample_size >= 0))` |
| CC-89 | production: `Watch writes: 1 ok`, `Verification writes: 1 ok` |
| CC-92, 402 path | 24 lines: `Error code: 402 ... 'code': 'payment_required'`; leak scan 0 |
| CC-95 | evening 2026-09-21: `api.cerebras.ai` 0 lines; `Running leg 1/1: cloudflare` x24; guard labels `-cloudflare` x37, `-cerebras` 0 |
| CC-92, Cloudflare path | evening 2026-09-21: nine refusals in the provider's own words, `code 4006`, "you have used up your daily free allocation of 10,000 neurons"; account ID masked; leak scan 0 |

**Not certified:** S3-D (LENS044 item 2) FAILED Monday at `finish_reason=length`
on 8000 — which led to CC-93/94/96. CC-91 has not been exercised: no Stage 1
has exited 1 since it shipped.

## THE SITUATION AT CLOSE
- **The S2-F evening wave has had no working detector on at least two of the
  last four days** (Sep 18 and Sep 20: 0 of 72 Cloudflare calls answered;
  Sep 18 morning: 15 ok, 18 refused). Every one showed green until CC-85.
- **Cloudflare cannot carry S2-F alone, by arithmetic**: 7.02k neurons for 24
  calls = ~292 per call; 10,000 a day = ~34 calls; S2-F asks 48.
- **The refusal is the daily 10,000-neuron allocation** — CC-92 printed
  Cloudflare's own `code 4006` on the Sep 21 evening wave. On Sep 21 UTC
  Cloudflare served 24 + 15 = 39 calls, then refused (~256 neurons a call).
  Still unexplained: on Sep 20 the first evening call was refused while the
  dashboard showed 7.02k used. The rolling-24h guess is no longer needed.
- **A second leg does not relieve Cloudflare** — every article calls every
  leg. This is why order item 1 needs a design ruling before a matrix.
- **S3-D writes 14 fields and stores 6.** Order item 6.

## IN FLIGHT
Nothing is half-shipped. `probe_out_s3d*` (three S3-D probe bodies) are
untracked on purpose; `probe_results.jsonl` holds their permanent record.

## RULINGS TAKEN THIS SESSION
- James delegated the openers ("your call"). Claude chose: hoist the `--dry`
  parser into one helper rather than copy it twice more; DELETE Verification's
  Mistral env block rather than wire it (charter: arithmetic, no LLM; the key
  was Lens 4's); ship CC-91 at once because CC-85 had caused the regression.
- **James ruled A + C at LENS-044: remove the Cerebras leg and split the guard
  now.** This overrides Claude's lean (B, docs first) and LENS-043's recorded
  "removing it is the same decision as choosing its replacement". Grounds: the
  order's definition of done removes Cerebras in both branches, and the
  structural profile had been absent for a month either way.
- Not a ruling, a lean: item 1.1 -> Mistral `ministral-8b-2512`. Order item 1.

## LIVE (verified this session by bytes, logs, console or a passing test)
- HEAD `dc1297a` at the last push; `ls-remote` matched.
- Cloudflare dashboard, Sep 20 UTC: 7.02k / 10k neurons, all `@cf/openai/gpt-oss-120b`,
  resets 00:00 UTC.
- S2-F census Sep 18-21 (seven waves): see THE SITUATION AT CLOSE.
- S2-F evening 2026-09-21: `scored=15 skipped=0 failed=9`, job GREEN (the
  partial-failure gap, order item 9). Cloudflare 42 POSTs = 15 x 200 +
  27 x 429 — the SDK retried each refusal twice. Direction B reported
  "5 findings delivered" on both Monday waves; whether it re-sent the same
  five was NOT checked.
- Mistral console, Sep 21: Plan **Free**; included API allowance **$10/month**,
  $0.44 used Sep 1-20 (323 requests); no payment method; pay-as-you-go NOT
  enabled -> **$0/month holds.** Limits are PER MODEL, rate only, no RPD and no
  monthly request cap: `ministral-8b-2512` TPM 625,000 / RPS 3.13;
  `mistral-small-2603` TPM **20,000** / RPS 1.00; `mistral-large-2512` 250,000 / 1.00.
- Gemini console, Sep 21: Free tier, 2.5 Flash RPM 3/5, TPM 28.83K/250K,
  **RPD 16/20**, "nearing a rate limit". With "All models" ON the table lists
  2.5 Flash, Antigravity and Deep Research Pro Preview — **no 2.5-flash-lite row.**
  Google's public free-tier table does NOT apply to this project.
- `lens_reports`: 239 rows in the last 30 days. `injection_reports`: 1,169.
- S3-D probe (ministral-8b, JSON mode, 26,616-26,707 chars in):
  t1 8000 -> `length`, key 13 of 14, not a loop; t2 16000 -> `stop`, 6,571 tokens,
  41%, 55s; t3 16000 on the CC-93 prompt -> `stop`, 8,522 tokens, 53%, 128s,
  18 distinct September evidence dates (t2: zero).
- Capture fixture = production prompt: 26,616 chars both.
- S3-D 90-day probe (CC-93/94 prompt, before CC-96): 70,435 chars / 18,077
  tokens in, 7,682 out, 48%, 121s, `stop`, 14 of 14 keys. The model wrote
  "90 days" 19 times and "30 days" never — the window line works. It cites
  dates in words ("June 2026"), so an ISO-date count reads zero.
- 90-day density: S1 1,803 rows, 1,564 before 2026-08-22 (~26/day then, ~8/day
  now — the pre-CC-75 duplicates); S2 5,326. By row count the newest 30 days
  drew ~12 of 90; by time (CC-96) they draw 30.
- **No S1 rows between 2026-08-22 ~14:00Z and 2026-08-31.** Found when the
  30-day span moved from 08-22 to 08-31 across five hours. Consistent with the
  late-August outage; not verified.

## BANKED (not verified this session)
- `pg_database_size` 374 MB — 2026-09-11, now ten days stale.
- Gemini project ID: the console said "Default Gemini Project"; whether that is
  `gen-lang-client-0818484204` was NOT confirmed.
- Groq TPD refill law and per-key vs per-org — unproven.
- Collection's Groq consumption — unmeasured (order item 7).
- LENS_LCLIFF_DECISIONS D-001..D-017 — not re-audited since Aug 5.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **"The morning wave ate the whole neuron pool."** The dashboard said 7.02k of 10k.
- **"Mistral is costing money ($0.44)."** It is usage against an included allowance.
- **"The evening 429s are a burst the dead Cerebras leg caused."** The first
  call of the run was refused, with no traffic before it.
- **"The morning wave had seven 429s."** `grep -c "429"` counted non-Cloudflare
  lines; the Cloudflare lines were 24 x 200.
- **"S3-D reads the oldest ~4 days."** It was 11 (Aug 22-Sep 2): outage-era volume.
- **"JSON mode may be looping S3-D."** All nine Mistral legs use JSON mode,
  including S3-A, which finishes at ~4K on the same model.
- **"Items 3 and 4 will certify tonight."** Stage 1 failed and GitHub skipped the
  aggregators — which exposed CC-85's own regression (CC-91).
- **Three greps built from memory**, two returning nothing: a Cloudflare
  timestamp pattern expecting two quote pairs, and `api.mistral.ai` (S3-D uses
  `requests`, not httpx, so no request line exists to match).
- **A verification dry run that proved nothing**: it crashed on `✅` under
  cp1252 before reaching its summary.
- **A date gate a projection could pass**: "latest date >= Sep 15" was met by
  an "expected 2026-11-04" signal. Replaced by counting past evidence dates.
- **CC-93's test double was more generous than production**: its fake returned
  every row, so it could not see PostgREST's 1000-row cap. CC-94's fake enforces it.
- **CC-96's first version let an empty stretch of time contribute nothing, and
  its live threshold was set on gap-free synthetic data.** The live dry run
  caught both: the Monday sample fell from 30 to 23 and the gate stopped the
  commit. The second version lends the gap's place to the nearest real row.
- **"Zero evidence dates in the 90-day body."** The regex looked for ISO dates;
  the model wrote "June 2026". The zero indicted the pattern, as it should.
- **CC-93's even sample was even by row count, not by time**, and the pipeline's
  own duplicate rows made those different by 3x at 90 days (CC-96).
- **ESTIMATES THAT HELD:** the 90-day prompt at ~65K chars (actual 70,435); evening S2-F lands ~16:35-17:10Z (actual 16:53);
  CC-85's exit code fires on `scored=0`; order item 7's shape exactly;
  Verification's env block decorative; the cron has two schedules, not four;
  the S3-D truncation was a long answer, not a loop; the capture fixture
  equals production byte-count for byte-count.

## WHAT I DELIBERATELY DID NOT DO
- **No S2-F second-leg probe.** Item 1 needs its leg-calling ruling first, or
  the matrix measures the wrong question.
- **No Gemini probe.** RPD 20 is per project and Lens 2 already sits at 16/20.
- **No circuit breaker** for daily-quota refusals. When it was proposed, the
  only "4006" in the repo was one I had written into a test fixture. The real
  body arrived on the last wave read before close; the breaker is now
  designable against it and is order item 5.
- **No change to S3-D's system prompt or schema**, and no DB column for its
  discarded fields. Charter and schema: James's ruling (order item 6).
- **Did not close any Watch rows** (order item 8).
- **Did not commit the probe bodies.**

## LEAK LOG
Two local scratch files held `SUPABASE_URL` (httpx logs full request URLs at
INFO; Actions masks them, a local run does not): `_s43_cc87.log` and
`_s44_cc94_dry.txt` (deleted). Neither was committed; `_s4*` is gitignored.
No credential leaked. The URL's weight depends on RLS still holding —
order item 10.
