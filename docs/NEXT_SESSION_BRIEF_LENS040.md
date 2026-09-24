# NEXT SESSION BRIEF -- LENS-040
Written 2026-09-11 at the LENS-039 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS040.md. This brief
references order items BY NUMBER and never restates them.

## HEAD -- VERIFY, DO NOT TRUST THIS LINE
Last code commit: `2a9639c`. This close adds docs commits on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104). A brief
cannot name its own commit, so this line is stale BY CONSTRUCTION.
Read `git log --oneline -8`.

## READ THIS BEFORE ANYTHING ELSE
**LENS IS ONLINE. THE OUTAGE ENDED 2026-08-31, NOT 11 SEPTEMBER.**
Waves are green and running normally. Do NOT open this session expecting a
dead database -- that was the LENS-039 brief's opening line and it was wrong
by eleven days. Every table answers. The SQL Editor answers. The crons fire.

**AND THE TARGET IS BREACHED.** Two positions report success while doing
nothing. Order item 1. That is the mission.

## WHAT SHIPPED (LENS-039)
| SHA | What |
| --- | --- |
| 2a9639c | CC-59 -- `articles_used` stores `{id,url}` only; the second list is gone. Old order item 1.3 |
| (this close) | order regenerated as LENS040, brief, LR-160..166 appended |

`2a9639c` receipts: four anchors asserted count==1; symbol delta table asserted
before write_bytes with three load-bearing zeros; the byte-identical block
inside `fetch_all_article_links()` asserted SURVIVING at 2 -> 1; CRLF pure both
ends, bare_LF 0; py_compile OK on both files; registry self-test 24 roles /
9 wire pairs / 5 limit rows; pytest 2 failed / 182 passed = the unchanged
baseline; five LR-138 grep counts all matching values stated in advance;
ls-remote verified `b708613..2a9639c`; `git log --stat` checked against the
message.

## THE SITUATION AT CLOSE
- **Item 1 is live, five waves deep, and five weeks old.** S2-E returns
  top-level `COMPLETE` next to `reports_saved: 0` and four `FAILED`
  sub-results. S2-C prints a green tick after failing four times on a model
  that died in June. The LENS-038 sweep saw both strings and passed over them.
- **CC-58 is CERTIFIED and it worked exactly as designed.** Default-deny caught
  MA returning `ANALYSIS_FAILED`, a string that is NOT in the banked MA status
  universe. Without D-018's four-label allowlist, MA would have ticked green.
- **CC-59 is CERTIFIED, same session, against a prediction banked before the
  wave.** 14,086 bytes measured against ~14 KB predicted; full-table check (not
  a sample) returned 0 rows containing `title`, `domain` or the second list.
- **Three positions fail on every wave for one reason.** Cerebras 402, then
  Mistral 429. Seven fallback attempts, seven 429s, in four minutes. Item 2.
- **Nothing still measures database size.** Item 4 is the last unaddressed half
  of R9.

## IN FLIGHT
Item 2.3 is a ruling Bro Alpha owes and item 1 will need it in the same session --
a fixed S2-E will report FAILED honestly until the fallback has capacity.
Items 7.3 and 7.4 are also rulings. R10 is PROPOSED and Bro Alpha rules.
Item 3 is designed (D-022) with no code written. Item 5 is a decide-then-do.

## HAZARDS FOUND THIS SESSION (these are now LRs)
- **A vendor's stated restriction window is not a measurement.** Supabase said
  11 Sep; the run list says 31 Aug. Now LR-160.
- **A failing scheduled job is a free liveness detector.** Disabling it deletes
  the only signal that would announce recovery. Now LR-161.
- **A removal is proved only by a zero** -- and a replacement comment that names
  the removed token keeps the count above zero. Now LR-162.
- **An anchor read from one `sed` window is not unique in the file.** The same
  three lines appeared twice in `analyze_lens.py`. Now LR-163.
- **When every primary fails over to the same model, that model's rate limit
  becomes a single point of failure.** Now LR-164.
- **A sweep that classifies a string as "sub-result, not top-level status" must
  ask whether the top level aggregates it.** Now LR-165.
- **A `+` in a URL query value arrives as a space.** Now LR-166.

## LIVE (verified this session by bytes, logs or a live call)
- **HEAD `2a9639c`; local == remote at the time of the code push.**
- **Supabase ANSWERS.** Live SELECTs on `lens_reports`, `lens_raw_articles`,
  `injection_reports`, `lens_macro_reports` all returned rows, 02:41Z.
- **Database 374 MB.** `lens_reports` 3,949 rows / 98 MB (93 MB of it TOAST),
  `lens_raw_articles` 122,577 rows / 149 MB, `lens_article_refs` 107,296 rows /
  71 MB, `injection_reports` 9,399 rows / 20 MB. The other twenty tables total
  ~24 MB. Three are completely empty: `lens_framing_scores`, `lens_feedback`,
  `lens_sources`.
- **Growth 4.75 MB/day BEFORE CC-59**, of which `lens_reports` was 3.3. The
  four biggest tables account for +52 MB of the +53 MB the database gained --
  the ledger closes, nothing unexplained.
- **Post-CC-59 rows measure 14,086 bytes each.** All five rows of one wave are
  byte-identical (item 3.6).
- **Outage window, from run numbers:** #288 Aug 22 13:51Z last healthy, #289
  Aug 23 02:43Z first dead, #304 Aug 30 17:40Z last dead, #305 Aug 31 07:18Z
  first recovered. 8 days 4 hours, 16 dead waves.
- **Dead waves run 19-27 SECONDS; healthy waves 21-32 MINUTES.** Absolute
  separation across 38 runs (item 4.3).
- **Wave `34505961936`** (Sep 10 17:27Z): S2-A COMPLETE, S2-B ANALYSIS_FAILED,
  S2-C COMPLETE **(lying)**, S2-D ANALYSIS_FAILED, S2-GAP OK, S2-E COMPLETE
  **(lying)**, MA ANALYSIS_FAILED, S4-E OK. `All positions complete` = 0.
- **Five waves, identical failure list:** 34256456580, 34318718752,
  34381537666, 34444756075, 34505961936 -- all
  `['S2-B', 'S2-D', 'Mission Analyst']`.
- **Consumer read-windows, from the constants:** raw_articles 48h
  (`analyze_lens_multi.py:95`), reports 30d (`s3b`/`s3c`/`s3d`),
  injection_reports 45d (`VERIFICATION_WINDOW_DAYS`), article_refs 3d
  (`lens_regular_report.py:159`), `assign_refs` 6h.
- **`lens_s4_upgrade_monitor.py:72` counts `lens_reports` from DAY_1_UTC with no
  upper bound** -- the S4-E maturity counter. Item 3.1.
- **`lens_ref_system.py:110` is SAFE** -- it scopes to `REF-{today}-%`, so
  deleting old refs cannot cause sequence reuse. This corrects a hazard raised
  and then withdrawn during this session.
- **`validate_write` has zero call sites.** Item 5.
- **`lens_sources` is empty**; sources live in `data/lens-SRC-001_sources.json`.

## BANKED (not verified this session)
- Item 13 (Groq TPD) is from Aug 10 and 32 days stale.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- not re-audited since Aug 5.
- The ~1.6 MB/day post-CC-59 growth rate is a PROJECTION from one wave plus an
  11-day pre-fix sample. Re-measure before trusting the late-October date.
- Why the restriction actually lifted on Aug 31. The average-daily model does
  not explain it. UNKNOWN, and it should stay written as unknown.
- `reltuples` in `pg_class` is a last-ANALYZE snapshot, not a live count.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **Said "about 25 days" to the next quota breach.** Computed against live size
  when the quota scores AVERAGE DAILY size. Re-derived the same session to
  mid-November. The first number was the confident one.
- **Reported `grep -c "402"` = 46 on a wave log as if it bore on Supabase.**
  Those are Cerebras EOL constants. LR-153's exact disease, committed again
  four days after writing LR-153.
- **Predicted that deleting old `lens_article_refs` would break REF sequence
  numbering.** Wrong -- `:110` scopes to today. Flagged a hazard from a function
  name without reading its bytes.
- **Called the first post-restore wave "perishable" evidence for CC-58.** It was
  not: `d10708a` was committed Aug 19 20:12Z and the order's own last-good waves
  are Aug 22, so CC-58 had already run five or six times. Both close documents
  said "`d10708a` has never run" and both were wrong.
- **Opened the session repeating "offline until 11 September".** It was in both
  LENS-039 documents in bold, and both were wrong by eleven days. A vendor
  banner was banked as a measurement with no step anywhere in the order to
  re-check it.
- **Wrote patch v1 with an anchor that matched twice**, and v2 with a comment
  that kept the removed token alive at 1. Both aborted and wrote nothing, which
  is the gates working -- but two of three attempts were mine to get right.
- **Left a trailing space in the `[save]` print line** because the replacement
  string dropped a character the anchor had not included. Caught by reading the
  diff, fixed before commit.
- **The regenerated order dropped old item 16 on the first pass.** Caught while
  writing the CHANGED section and restored as item 20.
- **ESTIMATES THAT HELD:** that the duplicate anchor block would be inside
  `fetch_all_article_links()` (it was); that the outage ended around Aug 31,
  derived from row counts before the run list was seen (Aug 31 07:18Z); that
  CC-59 would land near 14 KB/row and about -95% (14,086 bytes, -94.7%); that
  growth was ~4.75 MB/day (measured 4.9); that the four biggest tables would
  account for essentially all the growth (52 of 53 MB).

## WHAT I DELIBERATELY DID NOT DO
Did not touch `fetch_all_article_links()` while inside `analyze_lens.py`. After
CC-59 its return value feeds only a `len()` in a print -- it fetches every
article and builds full dicts to produce one number. Trimming it is item 18's
kind of work, not item 1.3's, and doing it silently inside a storage commit
would have made the diff say more than the message.

Did not fix item 1 after finding it. Reading the logs was the agreed bound for
that step; the fix touches two files' return paths and needs item 2.3's ruling
first. It is the LENS-040 mission instead.
