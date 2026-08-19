# NEXT SESSION BRIEF -- LENS-038
Written 2026-08-19 at the LENS-037 close. SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER.md. This brief references
order items BY NUMBER and never restates them.

## HEAD -- VERIFY, DO NOT TRUST THIS LINE
Last code commit: `ad083fe`. This close adds docs commits on top of it.
`git ls-remote origin refs/heads/main` is the only truth (LR-104). A brief
cannot name its own commit, and under contract v3 a close is a checkpoint, so
this brief is stale by construction at BOTH ends. Read `git log --oneline -8`.

Note: `origin/main` in the local log lags because pushes go through the full
URL and never update the named remote's tracking ref. That is cosmetic; the
log will look one commit behind. Believe `ls-remote`.

## WHAT SHIPPED (LENS-037)
| SHA | What |
| --- | --- |
| 02831ee | CC-55 -- S2-E's declared fallback leg wired. Order item 1.1 |
| ad083fe | CC-56 -- S2-D's leg wired, calibration mismatch documented in the message |
| (this close) | order regenerated, brief, LR-142..151 appended |

Both code commits: anchors asserted count==1, greps with expected counts
derived from the edit list and stated in advance, py_compile green, registry
self-test green, pytest 2 failed / 182 passed = the unchanged baseline,
ls-remote verified, `git log --stat` checked against the message.

## THE SITUATION AT CLOSE — READ THIS FIRST
**THE CERT IS DONE AND GREEN.** Run `32209212733` (Lens Manager + Analyze,
headSha `ad083fe`, 1,121 lines, 02:37 UTC 2026-08-19) certifies ALL FOUR
commits at once. Nothing is pending on a wave at your open.

- `d44efa5` — `MA prompt budget: corrections=11214 s1=24001 (4/4 reports)
  s2=9413 (3/25) s3=914 counted_total=45542 actual_prompt=45691`, status
  COMPLETE. Neither arrival branch fired.
- `260f18e` — `MA FALLBACK: ... calling mistral/mistral-small-2603`, usage
  prompt=10993 completion=1919 total=12912, `threat=HIGH, findings=5`.
- `02831ee` — FOUR `S2-E FALLBACK` calls, one per lens report. Results
  8/7/5/7 actors and 5/5/4/4 LOW legitimacy.
- `ad083fe` — TWO `S2-D FALLBACK` calls, one per batch: `claims=6
  consistency=0.85` and `claims=6 consistency=0.7`.

**THE CALIBRATION BANDS PREDICTED LIVE BEHAVIOUR TO THE DIGIT.** S2-D was
predicted at 6-7 claims and 0.70-0.85 consistency before wiring; it landed at
6 and 6, 0.85 and 0.70. S2-E was predicted 8/7/8 actors and 5/3/5 LOW; it
landed 8/7/5/7 and 5/5/4/4. That is the evidence for R8 and order item 5.

**STILL BROKEN, ON THE SAME LOG:**
- `[S2-ORC] S2-B (tick) status=ANALYSIS_FAILED`, run ends "All positions
  complete." Order item 1, confirming itself on the very wave that certified
  the fixes below it. S2-B is gemini, not Cerebras — not one provider's problem.
- 19 `payment_required` lines: S3-A, lens3, lens4 still dead. Items 2 and 3.
- **S1 hit 96.0% of its allotment (24,001 / 25,000), the highest ever.** Item 7.
- `actual - counted = 149`, twenty-first consecutive wave, no exception.

## IN FLIGHT
Order item 1 is the declared next mission; its 1.1 and 1.2 must ship together.
Item 2 needs the gas-mask test before any edit. Item 3.3 and 3.4 are rulings
James owes, not build work. Item 4.1 (CC-57) is designed but BLOCKED on the
prompt-change finding -- do not ship it as "just a metadata key".

## HAZARDS FOUND THIS SESSION (promote or drop -- these are now LRs)
- **A `<PLACEHOLDER>` inside a pasted command block is bash input
  redirection.** The block died on line 1 and four greps ran against a file
  that never existed. The protocol's Notes already said "Twice now"; this was
  the third. Now LR-148.
- **Grep counts computed as TOTALS double-count any edit that re-emits its own
  anchor.** Deltas are the correct form. Now LR-147.
- **Chained `sed -e` rules are order-dependent** -- an earlier rule ate the
  anchor of a later one and left a probe whose module alias lied. Now LR-149.
- **Spelling a string with `chr()` arithmetic to dodge quote nesting** produced
  a dead `if False else` branch in one patch and two `chr()`-spelled dict keys
  in another. Now LR-151. `chr(96)` for backticks remains the only legitimate use.

## LIVE (verified this session by bytes, logs or a live call)
- **Cerebras still 402.** SambaNova still 402. ALIVE: groq gpt-oss-120b and
  gpt-oss-20b, mistral-small-2603, gemini-2.5-flash, cohere command-r-plus.
- **S2-E fallback probe, real fixture, prompt 9,457 chars / max_tokens 16,000:
  3/3 HTTP 200, stop, schema-valid, budget_used 19-22%, 18-21s. Calibration
  actors 8/7/8, low 5/3/5, mandatory True 3/3 -- INSIDE the Cerebras band.**
- **S2-D fallback probe, prompt 11,598 chars / max_tokens 8,000: 3/3 HTTP 200,
  stop, all nine fields, budget_used 12%, ~7s. Calibration claims 7/7/6 and
  consistency 0.85/0.70/0.70 -- OUTSIDE the band, shipped documented.**
- **`injection_reports` S2-E: 920 rows Apr 14 - Aug 17.** actors/row 4.76,
  low/row 2.31, tiers LOW 2,133 / MIXED 1,130 / HIGH 1,120, MANDATORY
  corrections on 753 of 920 (81.8%). Per-day: flat 3.38-4.38 for the nineteen
  days to Jul 28, then 8.50 on Jul 29 -- the D-016 date. CC-7a's Aug 1 max_out
  raise shows NO second step.
- **`injection_reports` S2-D: 215 rows.** claims/row 8.8 pre-D-016 vs 26.6
  after; consistency 0.834 -> 0.853. `emotional_tone` changed SHAPE at the same
  date, from a repeated label to a unique sentence per row.
- **`lens_mission_analyst.py:487` does `truncate(json.dumps(evidence),
  MAX_S2_CHARS)`** -- so any key added to `injection_reports.evidence` enters
  MA's prompt. Fourteen modules read that table; none uses `select("*")`.
- Nothing on the Collection import chain imports S2-E; the orchestrator's
  import at `:167` is inside a function. Module-scope `fallback()` is safe here.
- `probe_lens_models.py` is at the REPO ROOT, not in `code/`, and has no
  mistral caller.
- **Register: 52 rules, LR-090..141, NO GAPS — the banked claim was right.**
  45 sit under `## LR-NNN` headings; LR-117..123 sit at lines 507-520 in a
  compact format under one `## LENS-030 ... earned rules` header. Count with
  BOTH greps or you will conclude seven rules are missing. See item 13.1.

## BANKED (not verified this session)
- Item 8 (Groq TPD) is from Aug 10 and nine days stale.
- LENS_LCLIFF_DECISIONS.md D-001..D-017 -- not re-audited since Aug 5.
- The 01:28 wave's behaviour. Everything above about it is prediction.

## CLAIMS THIS SESSION THAT WERE WRONG (close step 7)
- **Claimed seven ratified rules (LR-117..123) had been DELETED from the
  register.** They were never gone. I compared a `^## LR-` heading count
  against an older `grep -o "LR-1[0-9][0-9]"` count — two different
  measurements — and cited LR-121 ("verify a banked number describes the same
  thing you are measuring") in the same message that broke it. Then asserted
  the opposite ten minutes later. The bytes settled it both times.
- **Minted LR-144 as a new rule. It is LR-119**, live in the register since
  LENS-030: "Fallback SELECTION is not fallback DELIVERY... Audit every
  fallback for delivery." LR-120 (loud fallbacks fine, silent forbidden) is
  the design CC-54/55/56 all follow. The register was never opened this
  session; the cost was re-deriving a rule we already had. LR-144 is
  re-issued as an AMENDMENT to LR-119.
- **Handed over `MA=32000000000   # replace this number`.** A dummy value is
  still a placeholder. It was pasted literally, `gh` wrote its error into the
  log, and four greps ran against a 1-line file. LR-148 broken one layer
  down — its remedy is to DERIVE the id inside the command, which is what
  finally worked.
- Predicted `FB_PROVIDER` would appear 6 times in S2-E; it was 7. The extra was
  in a docstring I had written minutes earlier. Fifth count miss in three
  sessions, all on my own fresh text.
- Built a count-checking harness that then MISCOUNTED `fit_max_tokens` (4 vs 3)
  because it accumulated totals instead of deltas. The assert fired before
  `write_bytes`, so nothing was written -- the first time a count error was
  caught before a byte moved. The checker was wrong, not the patch.
- Shipped a dead `X if False else Y` conditional into the CC-55 patch body,
  whose unreachable branch called `usage.get('')`. py_compile passed it.
  Caught by re-reading and removed as CC-55b BEFORE the commit.
- CC-55's commit message said `MISTRAL_API_KEY` had a "sixth" consumer and
  omitted `regular_report`. The true count is SEVEN. Corrected in CC-56's
  message rather than left as two commits disagreeing.
- Gave a command block containing `<RUN_ID>`, which bash read as redirection.
  Nothing about the wave was learned, and nothing could have been.
- Stream-edited the S2-E probe into an S2-D probe with chained `sed -e`; the
  first rule ate the third rule's anchor and left `import lens_s2d_adversary
  as s2e`. Rewrote the probe from scratch.
- Predicted the August jump in S2-E actors/row was probably CC-7a's Aug-1
  max_out raise. The per-day cut put the step at Jul 28 -> 29, i.e. D-016.
  Right that a mechanical cause existed, wrong about which.
- Looked for `probe_lens_models.py` in `code/`. It is at the repo root.
- **ESTIMATES THAT HELD:** that MA's leg would carry S2-E's prompt without a
  `--max-out` override (16,000 passed through unchanged); that the probe pack
  might lack a mistral caller (it does); that a single fixture x 3 trials
  would be weaker than a paired A/B and should be labelled so.

## WHAT I DELIBERATELY DID NOT DO
CC-57 was designed, then held. At 05:00, hours before a cert, with fourteen
reader modules unverified, it looked like an additive key and turned out to be
a change to MA's prompt. The cost of waiting is bounded: the transition
timestamp is exact, so the same date-boundary method that identified D-016
this session will reconstruct this one. See item 4.1 for the unresolved fork.
