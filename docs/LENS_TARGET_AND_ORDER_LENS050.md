# LENS TARGET AND ORDER — for LENS-050 (regenerated at the LENS-049 close, 2026-10-03)

Project Lens — Bro Alpha. The live order is the file with the highest session number. Supersedes
`LENS_TARGET_AND_ORDER_LENS049.md`. Read with `LENS_COMPLETION_TEST_LENS049.md` (row statuses, this close),
`LENS_RULES_REGISTER_LENS049.md` (the one register), `LENS_ITEM3_DESIGN_LENS048.md`, `LENS_REVIEW2_PREP_LENS048.md`,
`LENS_FOUNDATIONS_LENS046.md` (canon, section 0 = WHY).

---

## 0. WHY LENS EXISTS (Bro Alpha, LENS-046, verbatim)

> Cognitive Warfare ကြောင့် ဝေဝါးသွားတဲ့ hidden pattern and invisible linking insights တွေကို
> အကြောင်းအကျိုး ခွဲခြမ်းစိတ်ဖြာ နားလည်နေဖို့ Freedom from Fear ပိုမိုခိုင်မာလာစေဖို့

*Translation (not the original):* to understand, by cause-and-effect analysis, the hidden patterns and
invisible linking insights that Cognitive Warfare blurs — so that Freedom from Fear grows stronger.

## 1. TARGET (D1 = A, LENS-046 — replaces the LENS-036 operational line)

**Lens earns its public voice — PHI-004 Phase 2 (Direction A) — declared only by Bro Alpha.**
Readiness = all of: PHI-004's three Phase 1 gates over at least six weeks (arc observed end to end;
operator-calibrated Watch false-positive rate under 50%; zero PHI-003 vocabulary violations) + every
Layer 1 row green for 14 consecutive waves + every Layer 2/3 row passing on 2 consecutive weekly
reviews + Bro Alpha's explicit go-live decision. The six weeks count from Review #1 (2026-09-24):
**earliest go-live 2026-11-05**, only if every gate is met. Rows and status: the completion-test file.

Target unchanged (D1 = A). Layer 1 streak **16** at the Oct 3 morning wave -- the 14-wave window is met.

## 2. CHANGED THIS REGENERATION

- CLOSED: order item 1 of LENS049 (every cert read); item 2's budget (CC-134b) and its three side defects.
- SHIPPED (eight commits, CI green on each):
  `f68c277` CC-134b (S2-F cap 17 -> 10: a run and the two before it share one rolling 24 h window);
  `2d692f0` CC-135 (item 3 Phase A, selection: no reader ranks, caps or maxes the six-measure column; registry
  `code/lens_s2_measures.py`; the Brief shows the most frequent finding);
  `fe69c49` CC-136 (item 3 Phase A, labels: twelve sites name each analyst's measure; S3-C names the analyst;
  the Compendium's dominant method prefers a finding to NONE);
  `d907028` CC-137 (S2-F scores an article whole or leaves it whole; one retry on invalid JSON inside the cap;
  the coverage line stops saying "the exit stays green");
  `d85d2ba` CC-138 (the Brief says when Cloudflare's daily quota cut a scoring run);
  `d5b2f5c` CC-139 + CC-140 (Regular Report docx in Word structure, true model subtitle, no model-written date;
  `_redact` masks a secret URL's host and is one implementation);
  `1cb8850` CC-141 (the Brief's SYSTEM 3 says when its S3-A row was written).
- CERTIFIED at LENS-049: CC-134b (10 of 10 on two runs, BREAKER 0); CC-135 (Brief line; Compendium S2-E 3 rows,
  59 findings, no "max conf"); Layer 1 window (16 waves).
- SETTLED: Cloudflare enforces a **rolling 24 h window** (~11-12k neurons); the documented 00:00 UTC reset was
  falsified by the Oct 2 07:16Z refusal on a fresh UTC day with nothing spent since midnight.
- **CORRECTED (Claude's error):** the commit messages of CC-135 and CC-136 call `code/lens_forensic_report.py`
  "the Regular Report". It is the **Forensic Report**, a separate workflow (`lens-forensic-report.yml`),
  manual-only since May (last run 2026-05-08, workflow_dispatch; it calls a paid API). Those edits reach no
  reader and cannot be field-certified. The Regular Report is `code/lens_regular_report.py` (CC-139).
- NEW: items 16-18. Item 3 narrowed to Phases B-D. Item 11 narrowed.
- ITEM NUMBERS: 1..18, unique, no gaps.

## 3. ORDER (root to sub; every item against the target)

1. **Certs owed** (no build; first job at the open). Pattern: `lens049_certs.sh` -- every run since a commit,
   prove it carries the commit, strip the job/step columns, count by step name, not by a shared count string.
   - CC-136 `fe69c49`: S2 Telegram message "(NN% model-stated confidence)"; Compendium "[S2-E] share of
     low-legitimacy actors 0.50" (docx); MA / S2 report / S3 outputs read once for drift (LR-268: inputs changed).
   - CC-137 `d907028`: `S2F_BUDGET deferred=N ... retries=M of 1` on every S2-F run; calls <= 10; any
     `S2F_DEFER` / "asked once more" lines are conditional.
   - CC-138 `d85d2ba`: no cut line on a day without a cut (correct silence); the line itself only on a cut day.
   - CC-139/140 `d5b2f5c`: Regular Report docx -- 0 `**`, Word tables, subtitle `ministral-8b-2512` (or
     "model not recorded" -- then `_log_completion` is not on the answering path), no `Date:` line.
     CC-140's field cert only on a day a flush times out (the gate proves the path).
   - CC-141 `1cb8850`: Brief "Patterns: N detected -- S3-A row written YYYY-MM-DD HH:MM UTC"; compare with
     the Brief's own time to see whether the row is this wave's.
   - Layer 1 streak continues (16 at Oct 3 morning).
2. **D6 -- the RED threshold on S2F_COVERAGE** from a week of cap-10 runs (`S2F_COVERAGE` + `S2F_BUDGET`; LR-245).
   With cap 10 a 0-scored run is a real signal. The S2-F code default is still 24 when `S2F_MAX_CALLS` is
   unset (env-only safety).
3. **L2.1 -- confidence, Phases B-D** (`LENS_ITEM3_DESIGN_LENS048.md`; Phase A shipped). Phase B re-designed
   from the LENS-049 bytes: the evidence fields differ by analyst (`sources_involved` S2-B, `sources_analyzed`
   S2-D, `source_id` S2-A, `lens_report_id` S2-C/E), so "distinct sources" needs a per-analyst rule first.
   S2-F's `lens_operation_detections.confidence` (model-stated, 0.95 on every pair in the LENS-048 probe) is
   in scope from Phase B. The design's four questions still wait for Bro Alpha.
4. **L3.2 / L2.6 -- the threat level**, after item 3 Phase B. CC-123 stays held. **Review #2 decides** whether
   the Brief keeps a five-level headline until then.
5. **L3.5 = D5 -- the Watch -> Clarity -> Verification arc**, acknowledgement path on `lens_s2f_deliveries`.
   Includes L3.7 / L3.8 (PHI-004 Phase 1 gates).
6. **Teaser-only sources are never scored** (TASS, Al Jazeera, NDTV, BBC, FT, CGTN, The Hindu ...).
7. **MA sees 3 of 30 S2 reports** (`s2=9421 (3/30)`).
8. **L3.9 -- PHI-004 Appendix A** for S2-F Verification messages.
9. **Verification aggregator loops** without grouping by voice x lens.
10. **L3.6 / ruling 4 -- S4**: unchecked archive; falsifiable hypotheses checked in the weekly review.
11. **Label and presentation truths still open (L2.4, L2.5):** two cycle conventions in one wave (`2of1`
    canary / `1of2` xlsx, `lens_ref_system.py:63`); S1 docx `**` x36; S3-A's template writes `<sign>` (prompt:
    probe first); S2-B stores `"context": "1M"`; Compendium "PREDICTION TRACKER"; S3-B stores a cut JSON wrapper;
    the S2 report prompt names "GCSP educators" as the audience in Phase 1; S2-F's `conf=` lines in the
    Compendium and Forensic Report (item 3). (Closed by CC-139: the Regular Report docx markdown, subtitle and
    date; by CC-141: the First Domino's missing time.)
12. **L3.1 sibling -- unmarked certainty in S3's other sections** ("are structural repeats of the Roman Empire").
13. **Registry drift and hygiene.** `_GOVERNING_AXIS["mistral"] = "RPD"`; registry rows still name
    `mistral-small*` (the regular_report row too); `mistral-small-2603` LIMITS row wrong; `check_groq_tpm`
    defined twice; L1.5 should also fail a primary known dead for > 7 days.
14. **Mistral concentration** -- availability, not budget (Sep 30 console: $0.83 of $10). S2-C / S3-F have no
    fallback; S3-D `ANALYSIS_FAILED` and S3-F `PARSE_FAILED` on Oct 1 evening.
15. **Title-case lens rows** (64; not rewritten; Review #2 decides).
16. **NEW -- provider events are lost on a flush timeout.** `lens_provider_refusal.flush()` posts once with a
    10 s timeout and no retry; on Oct 2 07:16Z the S2-F cut was never stored, so PROVIDER HEALTH and CC-138
    could not see it. An insert is not idempotent (a timeout may have stored the row): add a key
    (e.g. `run_id + source + class + model`, unique) and an upsert, then one retry. **SQL change: Bro Alpha rules.**
17. **NEW -- Layer 1 does not see a failed S3 position** inside a COMPLETE S3 report (Oct 1 evening: S3-D
    `ANALYSIS_FAILED`, S3-F `PARSE_FAILED`). Proposal: an L1 row counting S3 position statuses per wave
    (S3-E's `SKIPPED_CI` / `SKIPPED_CADENCE` are by design). **Row change: Bro Alpha rules.**
18. **NEW -- dormant code that edits reach but readers do not:** the Forensic Report (manual since May; a paid
    API) and S3-E (local-only by PHI-002, never in CI). Retire, revive at $0, or mark as dormant in the code
    so a fix is not mistaken for a shipped change. **Bro Alpha rules.**

## 4. MISSION FOR LENS-050 (proposal -- Bro Alpha declares it)

Item 1 at the open (six certs). Then **Review #2** as its own sitting -- it is overdue (from 2026-09-28) and
item 4, item 15 and the three NEW rulings (16-18) wait on it. Engineering after it: item 3 Phase B design, or
item 16 if Bro Alpha rules the key.

## 5. IDENTITY SEPARATION (ruling B, Bro Alpha, LENS-046)

The public tree is de-identified (`1c52063`): nothing but "Bro Alpha" appears for the operator, and the
partner project is named only by pseudonym. The remaining steps are tracked **off-repo** in a private
note, on purpose.

## 6. WEEKLY REVIEW (ruling 3)

**Review #2 is overdue (from 2026-09-28).** Prepared: `LENS_REVIEW2_PREP_LENS048.md`. Add from LENS-049:
- the Brief's S2 line changed from "Top injection ... conf=0.95" to "Most frequent finding ... n of N" (CC-135);
- the S2-F cut line (CC-138) and the SYSTEM 3 row time (CC-141) -- judge whether they help the reader;
- the Regular Report docx after CC-139 (bring the Oct 4 docx);
- the new rulings: items 16, 17, 18.
Output `LENS_REVIEW_<session>.md`; Bro Alpha gives the Layer 2/3 verdicts.

## 7. DECISIONS RECORDED AT LENS-049 (delegated to Claude unless marked)

- Mission: item 1, then item 2 (CC-134b) first because the bytes met the order's own condition (S2-F kept
  starving), then item 3 Phase A -- "your call" from Bro Alpha.
- CC-134b: 10 calls a run (3 x 10 x ~325 = 9,750, under the documented 10k) over adaptive budgeting (needs a spend
  ledger; revisit if 10 cuts relevant pairs). Guideline: Google SRE client-side throttling.
- Item 3 Phase A split into CC-135 (selection) and CC-136 (labels), CC-136 committed only after CC-135's cert.
  Guideline: ISO/IEC 11179 (one data element, one concept, one value domain). Phase A ranks by count, not by
  distinct sources (the evidence fields differ by analyst -- re-design for Phase B).
- Item 2 B (Bro Alpha: "B"): CC-137 (whole articles, one retry inside the cap; SRE retry budget) and CC-138
  (the cut line, read from existing events; ISA-18.2: a line only when a response is needed). CC-138 shipped with
  its known limit written in the message; item 16 carries the fix.
- A + B (Bro Alpha): CC-139 (WCAG 1.3.1) and CC-140 (logging guidance: mask connection strings and internal
  network names) in one commit; CC-141 after the Forensic Report finding. The cycle-label convention left open.
- Close: Bro Alpha, after A + B.

## 8. EARNED THIS SESSION

`LENS_RULES_REGISTER_LENS049.md`: LR-305 .. LR-317. Recurrences (not re-numbered): LR-246 x2, LR-236, LR-258 x2,
LR-289, LR-260.

## 9. CLOSE CHECK (session protocol)

**1) Declared mission completed?** Item 1: yes. Item 2: yes except the RED threshold (needs a week of data).
Item 3: Phase A shipped and half certified. Beyond it (Bro Alpha's B, then A + B): CC-137 .. CC-141.

**2-3) Findings and roots:** items 2-18. New roots: an order on a mixed column ahead of a limit selects rows
(S2-E 0 of 8); a cap checked inside the lens loop cut articles in half for good; a flush with no retry loses
the alarm it carries; one `_redact` copy drifted.

**7) Claims this session that were wrong (what the bytes said):**
- "CC-135/136 touch the Regular Report" -> `lens_forensic_report.py` is the dormant Forensic Report (LR-309).
- "CC-135's S3-E change shows in the next wave" -> S3-E never runs in CI (`SKIPPED_CI`; LR-307, LR-289 again).
- "PROVIDER HEALTH is built in lens_models.py" -> `head -1` picked the first match; it is `lens_provider_health.py`.
- "lens_provider_events does not exist" -> the SQL Editor was on another project (LR-311).
- "Every CC-136 mutant bites" (first run) -> the gate was already red on S2-F's `conf=` lines (LR-308, LR-246).
- "RESULT: 12 passed proves CC-137 in CI" -> three tests print that count; the check now reads the step name.
- Tooling: a stray replace wrote `XX` into a cert script; two window starts went negative; a dry run counted CRs
  with grep; a timestamp slice tripped CC-124's ratchet (`test_presentation`) before the commit.
All but the first two were caught by a dry run or a gate before they reached the repo.

**Deliberately not done:** the RED threshold (data); the flush retry (needs a key: item 16); the cycle-label
convention; Phase B (design first); prompts of MA / S2 / S3 while CC-136's cert was pending; Title-case rows.

## 10. CARRY-OVER

Every open item of `LENS_TARGET_AND_ORDER_LENS049.md` not closed above is folded into items 1-18.
