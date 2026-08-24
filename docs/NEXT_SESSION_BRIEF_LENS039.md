# NEXT SESSION BRIEF -- LENS-039
Written 2026-08-24 at the LENS-038 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS039.md. This brief
references order items BY NUMBER and never restates them.

## HEAD -- VERIFY, DO NOT TRUST THIS LINE
Last code commit: `d10708a`. This close adds docs commits on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104). A brief
cannot name its own commit, so this line is stale BY CONSTRUCTION.
Read `git log --oneline -8`.

## 🔴 READ THIS BEFORE ANYTHING ELSE
**LENS IS OFFLINE AND WILL STAY OFFLINE UNTIL 11 SEPTEMBER 2026.**
Every Supabase request returns 402. Order item 1 holds the full account.
Do not open this session expecting to read a wave, probe a model, or query a
table -- none of it works. Items 2, 6.2, and 1.2/1.3 are all BLOCKED on the
same date. What CAN be done is design work: item 1.5 is a ruling and item 1.4
is a design that the 2026-08-24 measurements already support.

## WHAT SHIPPED (LENS-038)
| SHA | What |
| --- | --- |
| d10708a | CC-58 + CC-58b -- orchestrator allowlist and the delivery split. Old order item 1.1 + 1.2 |
| (no commit) | The Supabase shrink -- five SQL UPDATEs plus VACUUM FULL, run in the dashboard. Order item 1.1 |
| (this close) | order regenerated as LENS039, brief, LR-152..159 appended |

`d10708a` receipts: 24 anchors asserted count==1, LR-147 delta table asserted
before write_bytes, CRLF 217 -> 269 with 0 bare LF, py_compile OK, registry
self-test 24 roles / 9 wire pairs / 5 limit rows, pytest 2 failed / 182 passed
= the unchanged baseline, nine LR-138 phrase counts all matching values stated
in advance, ls-remote verified, `git log --stat` checked against the message.

## THE SITUATION AT CLOSE
- **The outage is LOUD, not silent.** 423-line logs against a healthy ~1,100,
  six `402` hits, red workflows. The system reported honestly. Under last
  week's code this would have shown green.
- **The shrink is DONE and verified: 1353 MB -> 321 MB, `lens_reports`
  1094 MB -> 62 MB, zero rows deleted.** April's pilot was verified before the
  other months ran: `still_fat=0`, 91,165 ids retained, 2,753 distinct ids all
  resolving, `broken=0`. The August rows from the 16th onward were left fat on
  purpose.
- **`d10708a` has never run.** The outage began before its first cron. Item 2
  carries the falsifiable prediction; read it BEFORE reading the first wave,
  not after, or the prediction is worthless.
- **The scheduled workflows were NOT disabled** -- the block was written and
  James did not run it. Roughly thirteen runs a day are still firing into 402.
  Item 1.5.

## IN FLIGHT
Item 1.5 is a ruling James owes; item 1.4 is the declared next mission.
Items 4.3 and 4.4 are also rulings, not build work. Item 5.1 (CC-57) stays
BLOCKED on the prompt-change finding and has gained a new argument for option
(B). Item 1.3 is designed in prose but cannot be written or tested yet.

## HAZARDS FOUND THIS SESSION (promote or drop -- these are now LRs)
- **A clock reading is a banked number the moment it is read.** A step-0
  `date -u` was reused hours later to contradict James about whether a wave had
  run. He was right. Now LR-152.
- **A measurement that lumps two quantities under one label cannot indict
  either.** `total - relation_size` reported as "idx_toast" could not
  distinguish a 1 GB index from a 1 GB TOAST. Now LR-153.
- **A regex quantifier silently excluded the value being hunted** --
  `"[A-Z][A-Z0-9_]{3,}"` cannot match `"OK"`, and `OK` is a live status on
  every wave. Now LR-154.
- **A sample never authorises a destructive write.** Now LR-158.
- **Preserve the ENCODING shape when rewriting a stored value**, and put the
  idempotency guard in the WHERE clause. Now LR-159.

## LIVE (verified this session by bytes, logs or a live call)
- **HEAD `d10708a`; local == remote at the time of the code push.**
- **Supabase: 402 on every table. DB 321 MB after the shrink, quota 0.5 GB,
  `default_transaction_read_only = off` (writes ARE accepted via the SQL
  Editor, which stays available during the restriction).**
- **`lens_reports`: 3,815 rows. `articles_used` is jsonb holding a JSON
  STRING (double-encoded) -- `jsonb_typeof` = 'string' for all of them.**
  Post-shrink monthly sizes: Apr 4288 kB, May 3785 kB, Jun 4274 kB,
  Jul 4131 kB, Aug 26 MB with 88 rows deliberately unshrunk.
- **`lens_raw_articles`: 113,263 rows, 2026-04-11 to 2026-08-22, 139 MB.
  All 14,234 referenced ids resolve; `missing = 0` on the FULL check, not a
  sample.**
- **Wave `32261985640` (Aug 19 14:05Z, headSha 763aabb, 1,122 lines):** S2-A
  COMPLETE, S2-B ANALYSIS_FAILED, S2-C COMPLETE, S2-D COMPLETE, S2-GAP **OK**,
  S2-E COMPLETE, MA COMPLETE -- every one of those classifies correctly under
  CC-58. S3 on the same log: SKIPPED_CADENCE, SKIPPED, SKIPPED_CI -- three
  strings that exist nowhere in System 2 (item 4-adjacent; the CC-58 comment
  names them). MA budget `s1=23868 (4/4) s2=9339 (3/27) counted 45735 actual
  45884`. 19 `payment_required`. All three wired legs fired.
- **`get_s1_selected()` and `lens_s1_report.py` read a SIX-HOUR window only**
  -- which is why rewriting historical `articles_used` was safe.
- **GNI is ALIVE** (runs green through 2026-08-24 01:13Z) and in a DIFFERENT
  Supabase org. `grep -rn "\.delete()"` across GNI_Autonomous returns ZERO --
  it has no retention policy either. The 365-day cleanup James remembered is
  in the GNI_Myanmar repo, frozen at `9d1a6e5`.

## BANKED (not verified this session)
- Item 9 (Groq TPD) is from Aug 10 and fourteen days stale.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- not re-audited since Aug 5.
- Everything about how the 11 Sep reset will actually behave. Supabase says
  there may be a short delay after the reset; that is their word, not measured.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **Told James the run list contradicted him about the evening wave.** It did
  not. I read `date -u` once at step 0 and kept reasoning from it hours later.
  The commit stamp settled it: the session ran ~16 hours and the 14:05Z wave
  had landed. He was right; I was wrong and said so. Now LR-152.
- **Quoted the quota as 1.1 GB from the email.** The dashboard says **0.5 GB**
  and 287%. I repeated a vendor number instead of reading the meter.
- **Predicted the cron delay at 2.5-3.7h, then re-derived it as 33-71 min,
  then discarded the model entirely.** The second version was built on the
  same bad clock reading as the first.
- **Built a status-universe grep that could not match `"OK"`** -- `{3,}` after
  a leading capital -- and scoped it to `lens_s2*.py`, missing S3 entirely.
  Both halves of the instrument were wrong and it was the input to an
  allowlist. Now LR-154.
- **Reported "idx_toast" as one number** and reasoned about bloat from it. It
  could not distinguish index from TOAST. Now LR-153.
- **Hoped aloud that the 1090 MB was VACUUM-recoverable bloat.** `n_dead_tup`
  was 0 and indexes totalled 384 kB. It was all real data.
- **Warned that the UPDATE would temporarily double the database.** Wrong in
  magnitude -- the replacement values are tiny, so the peak was about +60 MB.
- **Wrote the first CC-58 patch with three labels**, mapping the MA arrival
  statuses to FAIL. Caught and rewritten to four labels BEFORE James ran
  anything; the three-label script was discarded, not shipped.
- **Said GNI's low run frequency made it safe** -- heartbeat and selfcheck are
  both `*/30`, i.e. 48 runs a day each. Only pipeline and MAD are the sacred 2.
- **Asserted the register's byte delta did not reconcile** and treated it as a
  loose thread. The banked 36,771 was the weak term; the gap check settled it
  in one line.
- **ESTIMATES THAT HELD:** that one column would dominate the database (it was
  76%); that the ids would resolve against `lens_raw_articles` (14,234 of
  14,234); that shrinking to ids alone would clear the quota (321 MB against
  500); that the delivery gate was worse than the order stated (it was -- no
  path at all from `if failed:` to the delivery calls).

## WHAT I DELIBERATELY DID NOT DO
Did not fix item 16.1's tier defect while inside `lens_ref_system.py`. The
offline Excel tool reproduces the bug on purpose so its output matches the
file James is used to; a `--fix-tier` flag exists and is documented as
producing a DIFFERENT workbook. Fixing it silently would have made two days'
exports incomparable during an outage, which is the worst possible moment.
