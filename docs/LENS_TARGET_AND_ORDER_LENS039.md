# LENS TARGET AND WORKING ORDER — generated at the LENS-038 close (2026-08-24)
**Regenerated 2026-08-24 at the LENS-038 close. SUPERSEDES the 2026-08-19 version.**
Ranks are TARGET-RELATIVE. Freshness confers no priority. This file holds the
target instance and the path; LENS_CONTRACT.md holds the mission and the method.

**THE FILENAME CARRIES THE CONSUMING SESSION** (CONTRACT v4, James's ruling at
the LENS-037 close): this file is `docs/LENS_TARGET_AND_ORDER_LENS039.md` and
the OPEN prompt resolves the LATEST by glob. The previous generation's header
argued the opposite — that a fixed `docs/LENS_TARGET_AND_ORDER.md` was
deliberate — because it was written minutes before the ruling and never
updated. That text is now corrected, and the fixed path no longer exists on
disk (verified at the LENS-038 open).

## CURRENT TARGET (declared 2026-08-06, unchanged)
Every scheduled wave produces valid intelligence, unattended, with **NO SILENT
FAILURE**. Not "no defects". A system with logged, ordered, non-silent defects
is done. A system with one silent failure is not.

**Note on the target's status right now:** Lens produces NOTHING and has since
2026-08-23. That is not a target failure — the outage is LOUD (red workflows,
402 in the logs, a provider email). The target survives; the order changes.

## ROOTS
- **R1 — Deprecation is weather, and we have no forecast.** Models and whole
  free tiers die on someone else's schedule.
- **R2 — Written but never wired.** Code that declares a capability no call
  site reads.
- **R3 — Numbers set once, never re-derived against reality.**
- **R4 — Epistemic diversity is assumed, not verified.**
- **R6 — The record cannot attribute.**
- **R7 — Nothing watches our external dependencies' lifecycle.** PROPOSED at
  LENS-036, JAMES RULES. Now breached a second time: the Cerebras EOL email of
  2026-07-17 and the Supabase quota mail of 2026-08-23 are the same class.
- **R8 — Certification measures mechanics, not behaviour.** PROPOSED at
  LENS-037, JAMES RULES. Strengthened this session: the LENS-037 calibration
  bands predicted live behaviour to the digit.
- **R9 — Free-tier resources are CONSUMED, not merely rate-limited, and
  nothing meters what we accumulate.** PROPOSED at LENS-038, JAMES RULES.
  Every quota discipline in this repo is about tokens per minute or per day —
  a flow. Storage is a STOCK: it only grows, no wave resets it, and no code
  anywhere in Lens deletes a row or measures a table. Distinct from R7: R7 is
  someone else changing the deal, R9 is us filling a bucket we never watched.
  Evidence: `lens_reports.articles_used` reached 1,037 MB against a 500 MB
  quota and took the whole system offline for at least nineteen days.

## CLOSED THIS SESSION
- **Old item 1 (1.1 + 1.2) — the orchestrator success tick and the delivery
  axis.** Shipped as `d10708a` (CC-58 + CC-58b): four-label allowlist
  OK/SKIP/DEGRADED/FAIL with default-deny, delivery made unconditional,
  seven `ok_x` locals renamed `st_x`. UNCERTIFIED — see item 2.
- **Old item 13.1, second half.** LR-142..151 were found already appended at
  the LENS-038 open; the register is 55 headings + 7 compact = 62 = LR-090..151
  contiguous, verified by a gap check. The FORMAT NORMALISATION remains open —
  see item 14.1.

## WORKING ORDER

### URGENT

**1. LENS IS OFFLINE. SUPABASE FAIR USE, UNTIL 11 SEPTEMBER 2026** [R9, R7]
The organisation `fintelplan's Org` exceeded its Free-Plan database quota and
every PostgREST request returns **402**. Confirmed by bytes on 2026-08-24, not
from the email: plain SELECTs on `lens_reports`, `injection_reports` and
`lens_macro_reports` all returned `code: 402`. Database size was 1.434 GB
against a **0.5 GB** quota (287%). Billing cycle 11 Aug – 11 Sep.
Last good waves: `32546589024` (Aug 22 02:33) and `32576995824` (Aug 22 13:51).
First dead waves: `32613557186` (Aug 23 02:43) and `32643748485` (Aug 23 13:53).
- 1.1 **DONE 2026-08-24 — the database is back under quota.** Five monthly
  `UPDATE`s rewrote `articles_used` to ids only, then `vacuum (full, analyze)`.
  **1353 MB -> 321 MB; `lens_reports` 1094 MB -> 62 MB. Zero rows deleted.**
  Aug 16 onward left untouched on purpose. This does NOT lift the restriction
  (the quota is the AVERAGE DAILY size over the billing period, so only the
  11 Sep cycle reset lifts it) — it ensures the reset HOLDS. Supabase grants
  no second grace period.
- 1.2 **RESTORATION CHECKLIST, 11 SEPTEMBER.** Confirm the 402 is gone with a
  live SELECT before trusting anything; re-enable whatever was disabled under
  1.5; then read the first wave in full. There may be a short delay after the
  reset before restrictions clear.
- 1.3 **THE WRITE SIDE IS UNFIXED AND THIS WILL RECUR.**
  `analyze_lens.py:343` and `analyze_lens_multi.py:1210` both store
  `json.dumps(article_ids)` with the full `{id,url,title,domain}` objects.
  14,234 distinct articles were stored as ~519,000 entries — the same article
  re-serialised ~36 times. Growth was ~230 MB/month; unfixed, the quota is
  breached again around December. Fix = serialise ids only at write time.
  **BLOCKED until 11 Sep** — it cannot be tested against a dead database.
- 1.4 **NO RETENTION POLICY EXISTS ANYWHERE IN LENS.** Not one `.delete()`,
  not one age cutoff, in any table, since April. This is R9's structural form
  and it outranks 1.3: fixing the write side stops one column growing, a
  retention policy stops the next one.
- 1.5 **RULING NEEDED: disable the scheduled workflows until 11 Sep?**
  Seven workflows carry crons — collect (01:00/13:00), manage-analyze
  (01:28/13:28), s2f-scoring (01:30/13:30), ref-export (02:30/14:30),
  regular-report (02:10), compendium (02:30), gdelt — roughly thirteen runs a
  day against a 402 database. Disabling costs nothing extra: collection cannot
  write either way, so the coverage is already lost. Lean DISABLE, because two
  red runs a day for nineteen days is how a real red stops being read. The
  counter-risk is real and must be answered in writing: a paused cron nobody
  un-pauses is how a system quietly dies, so the re-enable belongs in 1.2 as a
  dated step, never in memory.
- 1.6 **NO SIZE MONITORING.** Nothing in Lens measures database size or warns
  before a quota. Supabase notifies at 20% from a limit; that mail was not
  read. A cheap meter in the wave (one `pg_database_size` equivalent plus a
  threshold line in the Telegram pre-flight) closes the whole class.

**2. CC-58 IS SHIPPED AND UNCERTIFIED** [R8]
`d10708a` changed how every S2 position is judged and how delivery is
triggered, and no wave has run under it — the outage began before the next
cron. The cert is owed the moment waves resume.
- 2.1 **FALSIFIABLE PREDICTION, BANKED 2026-08-20.** If S2-B still returns
  ANALYSIS_FAILED the log MUST show `⚠️  status=ANALYSIS_FAILED` in `_run`,
  `❌ S2-B` in the summary, `1 position(s) did not complete: ['S2-B']`,
  **NO "All positions complete."**, and the Telegram intelligence report plus
  step report still delivered. Exit stays 0, so System 3 still runs.
  "All positions complete." appearing anyway means the commit did not take;
  a missing Telegram means 1.2 of the old order was wrong.
- 2.2 The `⏭ SKIP` and `⚠️ DEGRADED` paths are UNEXERCISED live. Neither
  fired on `32261985640`. A path with no live evidence is not certified.

**3. SYSTEM 1 IS ONE SURVIVOR QUADRUPLED** [R2, R3, R4] — was item 2
- 3.1 `lens_orchestrator.py:375` passes `--single-lens`; `analyze_lens_multi.py`
  has no `sys.argv`, no `argparse`, no `LENS_ID`, so every invocation runs all
  four lenses. Parent loops 1..4, so **every wave runs 16 lens analyses, not
  4**, and healing spawns more.
- 3.2 **CANARY THREE-ARM GAS-MASK TEST BEFORE ANY EDIT HERE.** Arm 2 is the
  live risk: the Collection import chain is S1's air supply.
- 3.3 lens3 and lens4 remain on dead Cerebras. Wire AFTER 3.1; lens3's leg is
  groq/gpt-oss-20b — see item 4.
- 3.4 S1 burn is UNMEASURED. `subprocess.run(capture_output=True)` swallows the
  child's stdout.
- 3.5 **NEW ANGLE FROM ITEM 1:** 16 analyses per wave is also 4x the write
  volume into `lens_reports`. Item 3 is now a STORAGE item as well as a burn
  item, which raises its value against R9.

**4. THE REGISTRY CONTRADICTS A RULING IT WAS SUPPOSED TO IMPLEMENT** [R1, R2, R7]
— was item 3
- 4.1 D-015 ruled ALL fallbacks become mistral-small, uniform, because
  gpt-oss-20b shares the reasoning-starvation failure mode. Six roles still
  carry groq/gpt-oss-20b legs: lens3, ai5_watchdog, s2gap, entity_extract,
  s3a_patterns, s3d_longterm.
- 4.2 **S3-A specifically.** Its registry note says the leg "inherits max_out
  and hits the same 939-token ceiling — it is broken and unprobed" (CC-12,
  2026-08-01), still true. Registry edit plus an LR-106 probe.
- 4.3 RULING NEEDED: do the dead Cerebras PRIMARIES stay in front of the new
  legs, or get repointed? Lean KEEP; it is a measurable cost, not free.
- 4.4 RULING NEEDED: three fallback shapes exist — S2-A's hardcoded dual-path
  `model=None` (CC-14), MA/S2-E/S2-D's `_call_fallback_leg` copies, and nothing
  elsewhere. Extract a shared helper AFTER CC-58's cert, not before.

**5. THE RECORD CANNOT ATTRIBUTE** [R6, R3] — was item 4
- 5.1 **CC-57 — the saved row does not say which leg produced it.** Design
  settled (`parsed["_wire"]` on both legs, copied into `evidence` at save).
  BLOCKER: `lens_mission_analyst.py:487` does
  `truncate(json.dumps(evidence), MAX_S2_CHARS)`, so any key added to
  `evidence` enters MA's prompt. Fork: (A) accept and size it; (B) a real
  column or side table; (C) log-only. Not (D) overloading `source_id`.
  **NEW WEIGHT FOR (B):** item 1 proved `evidence`-adjacent columns are also a
  STORAGE decision, and (B) is the only option that does not grow a TOASTed
  jsonb column on every row.
- 5.2 `lens_reports.domain_focus` is the literal "ALL" on all rows, from
  `analyze_lens_multi.py:1206`. Identity IS recoverable from `summary`'s
  `[<Lens Name> — <perspective>]` prefix or from `prompt_version`.
- 5.3 `cycle` is the literal "manual" on every row; wave scoping must come
  from `run_id` or a `generated_at` recency floor.
- 5.4 Arrival counts ROWS, not distinct lens identity.
- 5.5 Zero of 243 macro reports have ever cited a named lens; 236 cite S2 only.
- 5.6 **NEW:** the four `S2-E FALLBACK` log lines on `32261985620` read
  "for unknown" — the lens name is literally absent in that log line. Likely
  kin to 5.2.

**6. CERTIFICATION MEASURES MECHANICS, NOT BEHAVIOUR** [R8] — was item 5
- 6.1 Make a calibration band part of every migration cert.
- 6.2 **RETRO OWED:** MA, S3-A, S3-D, lens3, lens4 and s2f_primary all moved to
  Cerebras on 2026-07-28 and none has been measured across that boundary. Two
  of two checked showed a substantial shift. The evidence is in the DB — but
  it is now BLOCKED until 11 Sep along with everything else.
- 6.3 `probe_lens_models.py` (at the REPO ROOT, not `code/`) has no mistral
  caller, so `--candidate fallback` cannot exercise any mistral leg.

### IMPORTANT

**7. NOTHING WATCHES PROVIDER LIFECYCLE** [R7, R1] — was item 6
Cerebras announced 2026-07-17, died 2026-08-17, took five positions. Supabase
warns at 20% of a limit and that mail was not read either. Known ahead:
gemini-2.5-flash dies 2026-10-16 (lens2, S2-B, S3-B). Item 1.6 is this item's
storage-shaped twin; solving one should solve both.

**8. PROMPT PACKING AND VALUE ORDER** [R3] — was item 7
MA's S1 allotment: 96.0% (24,001/25,000) on Aug 19, then **95.5%
(23,868/25,000) on Aug 19 evening** — it OSCILLATES near the ceiling rather
than climbing monotonically. S2 admissions 3 of 27; corrections 11,614.
`S1_PARTIAL_ARRIVAL` has still never fired live, and CC-58 now classifies it
as DEGRADED rather than as a failure.

**9. GROQ TPD REFILL LAW** [R3] — was item 8. Measured 2026-08-10, now
fourteen days stale and downstream of item 3.

**10. INPUT-QUALITY CLUSTER** [R3] — was item 9. BUG-001: batch 1 held 44
articles, the 9,000-char cap fired at 21, and the user message still states the
pre-truncation count.

**11. CI PROVES COMPILATION, NOT BEHAVIOUR** [R2, R8] — was item 10.

**12. DEAD-SYMBOL GATE** [R2] — was item 11. **NOW TWO FILES, NOT ONE:**
`check_groq_tpm` is defined twice byte-identically in
`lens_s2_orchestrator.py` (:38-67, :69-98) AND in `lens_s3_orchestrator.py`
(:27-56, :58-87).

**13. THE MAP IS STALE IN MORE PLACES THAN THE TERRITORY** [R6] — was item 12.
`lens_s2_orchestrator.py:15` still says `llama-3.3-70b / GROQ_S2E_API_KEY`;
`lens_s2e_legitimacy.py:49` still says max_out 10,000 against the registry's
16,000.

**14. REGISTER AND ROUTING HYGIENE** — was item 13
- 14.1 **FORMAT NORMALISATION, still open.** The register is 62 rules,
  LR-090..151, contiguous — but 55 sit under `## LR-NNN` headings and 7
  (LR-117..123) sit in a compact block, so `grep -c "^## LR-"` under-reports by
  seven. Normalise; precedent `1bbb6f3`.
- 14.2 CC-24 appears in NO decision record.
- 14.3 **GREW AGAIN.** Untracked scripts in the repo root now number EIGHT:
  `patch_cc55.py`, `patch_cc55b.py`, `patch_cc56.py`, `patch_cc58.py`,
  `patch_cc58b.py`, `probe_s2e_fallback.py`, `probe_s2d_fallback.py`,
  `s2e_baseline.py` — plus `s2_excel_offline.py` (item 16.2), which is a
  deliberate tool and deserves a tracked home rather than the same bin.

**15. WAVE SEQUENCING AND CROSS-POSITION SPACING** [R3] — was item 14.
Cross-position spacing cannot live in a per-position TPMGuard (LR-112).
TPMGuard still paces against the PRIMARY's TPM on a fallback path.

**16. THE REFERENCE EXPORT IS PARTLY DECORATIVE** [R6, R2] — NEW
- 16.1 **`source_tier` and `also_s1_pool` are meaningless in every S2 Excel
  ever sent.** `lens_article_refs` is selected without `source_id`
  (`lens_ref_system.py:102`, `:146`) while `:296` and `:558` look up
  `tier_map[source_id]`, so every article row takes the `TIER2` default and
  Sheet 3's "By Tier" breakdown is a single bar. Verifiable by opening any
  past workbook. Fix = join `source_id` in from `lens_raw_articles`.
- 16.2 `s2_excel_offline.py` was built during the outage: it imports
  `lens_ref_system` and calls that module's own `get_s2_selected()` and
  `build_excel()` against three CSVs, so the usual workbook can still be made
  by hand. Untracked. Decide whether it becomes a supported tool or is deleted
  once 11 Sep restores the pipeline.

### OTHERS

**17. MEASUREMENT ODDITIES, BANKED** — was item 15
- `actual_prompt - counted_total = 149` on **twenty-two** consecutive waves,
  no exception.
- chars/token on `mistral-small-2603` measured at 4.738 (MA), 4.19 (S2-E),
  3.80 (S2-D) — CONTENT-dependent, never a model constant.
- **NEW, from item 1's forensics:** `lens_raw_articles` holds 113,263 rows
  spanning 2026-04-11 to 2026-08-22 at only 139 MB (~1.3 KB/row) and every one
  of 14,234 referenced article ids resolves against it. The raw-article store
  is efficient; the duplication was entirely in `lens_reports`.

## WATCH ITEMS
- **11 SEPTEMBER 2026** — cycle reset. Items 1.2, 1.3, 2, 6.2 all unblock.
- **16 OCTOBER 2026** — gemini-2.5-flash dies (lens2, S2-B, S3-B).
- Prediction from LENS-037 still holds and still needs a longer series: the
  pre-wiring calibration bands predicted live Mistral behaviour to the digit.
- MA's S1 allotment oscillation (item 8).
- GNI: verified ALIVE on 2026-08-24 and in a DIFFERENT Supabase org, so it did
  not share this blast radius — but `grep -rn "\.delete()"` across
  GNI_Autonomous returns ZERO. It has no retention policy either. Lens's R9
  belongs in the next Lens-to-GNI transfer packet.

## STANDING BLOCKER
$0/month. The Supabase Pro plan at $25/month would have lifted this
restriction immediately and was REJECTED — see DECISIONS.

## DECISIONS RECORDED THIS SESSION
- **D-018 — CC-58 is a four-label allowlist, not two.** OK / SKIP / DEGRADED /
  FAIL, default-deny. DEGRADED exists because `lens_mission_analyst.py:849`
  returns the arrival statuses only AFTER `save_macro_report()` succeeded, so
  the report exists; and because S1 sits near 96% of its allotment, a two-value
  allowlist would have alarmed on nearly every wave. Rejected the two-state
  form the order proposed, and rejected shipping 1.2 alone.
- **D-019 — do not buy the Supabase Pro plan.** $25/month lifts the
  restriction today but does not fix the write side or the absent retention
  policy, and the same wall arrives at the next tier. Nineteen days of outage
  accepted instead. James's call, taken 2026-08-24.
- **D-020 — shrink `articles_used`, do not delete `lens_raw_articles`.**
  James proposed deleting the raw articles. Rejected on two grounds: the whole
  table is 139 MB, so deleting all of it still leaves 2.4x the quota; and the
  shrink is only safe BECAUSE the ids resolve against that table, so the two
  plans destroy each other.
- **D-021 — the malformed three rows are skipped, not migrated.** Three
  `lens_reports` rows lack the `selected` wrapper. Combined size trivial, and
  `parsed.get("selected", [])` already returns empty for them.

## CHANGED THIS REGENERATION
- CLOSED: old item 1 (orchestrator tick + delivery axis) — `d10708a`.
- CLOSED: the appending half of old item 13.1 — LR-142..151 were already in
  the register at the LENS-038 open; the format split remains as 14.1.
- NEW ROOT PROPOSED: **R9** — free-tier resources are consumed, not merely
  rate-limited, and nothing meters what we accumulate. JAMES RULES.
- NEW: item 1 (the Supabase outage and its five sub-items) at the TOP. It is
  not merely urgent; nothing else can be worked or certified while it stands.
- NEW: item 2 (CC-58 shipped and uncertified) — a shipped change with no live
  evidence is an open item, not a closed one.
- NEW: item 16 (the reference export's dead tier columns, and the offline tool).
- RE-RANKED: everything from old item 2 downward shifts by one (old 2->3,
  3->4, 4->5, 5->6, 6->7, 7->8, 8->9, 9->10, 10->11, 11->12, 12->13, 13->14,
  14->15, 15->17).
- NEW EVIDENCE: item 12 is two files, not one (S3 duplicates it too);
  item 3 gains 3.5, the storage argument; item 5.1 gains new weight for
  option (B); item 8's saturation now shown to oscillate, not climb.
- HEADER CORRECTED: the previous generation argued for a FIXED filename,
  contradicting CONTRACT v4, which was ruled the same day. Corrected above.
- DECISIONS: D-018..D-021 recorded, first use of a DECISIONS section in a
  Lens order.
- ITEM NUMBERS ASSERTED UNIQUE: 1..17, no duplicates, no gaps.

## NEXT SESSION'S MISSION
**Order item 1.5 first (a ruling, minutes), then item 1.4 — design the
retention policy.** 1.3 and 1.2 are BLOCKED until 11 September and 1.1 is
done. A retention policy can be DESIGNED against a dead database: the table
inventory, the growth rates and the consumer read-windows were all measured on
2026-08-24 and are in the LENS-039 brief. Ship the design as a decision record
now and the code when the database returns. If James rules R9 out as a root,
re-rank before working anything.
