# LENS TARGET AND ORDER — for LENS-048 (regenerated at the LENS-047 close, 2026-09-25)

Project Lens — Bro Alpha. The live order is the file with the highest session number. Supersedes
`LENS_TARGET_AND_ORDER_LENS047.md`. Read with `LENS_COMPLETION_TEST_LENS047.md` (row statuses, this close),
`LENS_RULES_REGISTER_LENS047.md` (the one register), `LENS_ROOT_CAUSE_LENS046.md`,
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

Target unchanged (D1 = A). Count at this close: see `LENS_COMPLETION_TEST_LENS047.md` section 8.

## 2. CHANGED THIS REGENERATION

- CLOSED: L3.4 send-once (CC-120, half certified); S2-F Stage 1 starvation (CC-122, certified);
  D6 coverage half (CC-125: the line ships, the RED threshold waits for D2); D7 breaker (CC-126);
  L2.7 rating columns (SQL); header comments naming retired models, buffered S2 print, silent
  MODERATE default (CC-125).
- FIXED, CERT OWED: L2.3 (CC-121), L2.4 and L2.5 (CC-124), S3-A first_domino column (CC-126).
- NEW ROOT: S2 confidences saturate (0.80-0.95 on every wave) -- L2.1 now blocks L3.2 (item 3).
- NEW: teaser-only sources are never scored (item 6); MA sees 3 of 30 S2 reports (item 7).
- HELD: CC-123 (threat scale + reason gate) -- probe showed invented reasons; waits on item 3.
- RE-RANKED: D2 rises to item 2 -- S2-F now picks fairly but still scores 8 of ~480 long articles,
  and 5 of the last 20 runs scored nothing (provider refusals).
- RETIRE CHECK: nothing has sat three regenerations unworked. The register debt that had (four
  unnumbered lists, LENS-043..046) is closed by `LENS_RULES_REGISTER_LENS047.md`.
- ITEM NUMBERS: 1..12, unique, no gaps.

## 3. ORDER (root to sub; every item against the target)

1. **Certs owed** (no build; first job at the open):
   - CC-119 `9cabfa7` + CC-126 `1508328`: the next S3-A row (morning wave) has a non-empty
     `lens_system3_reports.first_domino` worded "confirmed if ... disconfirmed if ...".
   - CC-120 `e6a0f33`, second half: the next S2-F run logs `S2F_DELIVERY new=0 ... quiet=4` and sends
     no Direction B message (the L3.4 byte row).
   - CC-124 `636d76e`: the canary message says `2of1`/`2of2`, not `manual`; no `**`/`##`/JSON;
     cuts end in an ellipsis at a word.
   - CC-125 `83be03b`: `S2F_COVERAGE scored X of Y` in the S2-F log.
   - CC-126 `1508328`: at most one `ENTITY_EXTRACT_TPD_BREAKER` line in the collect log, far fewer
     Groq 429s (Sep 24: 233); the next S2 docx carries no CLASSIFIED/DISSEMINATE heading.
   - Carried: CC-108 (S3-C weekly), CC-114 (only when a lens fails).
2. **D2 -- the S2-F scorer's provider and capacity** (Layer 1 air). Ruled at LENS-045: Mistral
   `ministral-8b-2512` primary, Cloudflare best-effort. LR-106 probe on S2-F's real prompt first, then
   size `S2F_MAX_ARTICLES` from measured throughput (CC-122 made the pick fair; it is still 8 of ~480
   long articles a window), then D6's RED threshold on `S2F_COVERAGE`.
3. **L2.1 -- calibrate the S2 confidences** (the root under L3.2). Bytes, LENS-047: over 19 waves every
   S2 position reported 0.80-0.95 max confidence on nearly every wave; a count-and-confidence ceiling
   was CRITICAL on 18 of 19. Find the guideline first (ICD 203 on uncertainty; calibration practice).
4. **L3.2 / L2.6 -- the threat level**, after item 3. CC-123's draft (module, patch, probe results) is
   in the LENS-047 record: the reason gate works, but the analyst invented a previous basis and read
   adversary news as "amplification". **Review #2 decides** whether the Brief keeps a five-level
   headline until item 3 lands (Claude's lean: show "what changed since the last wave" instead).
5. **L3.5 = D5 -- the Watch -> Clarity -> Verification arc**, with an acknowledgement path that can
   attach to the `lens_s2f_deliveries` ledger (283 HIGH, 644 MEDIUM, 828 LOW rows unreviewed).
   Includes L3.7 / L3.8 (PHI-004 Phase 1 gates).
6. **Teaser-only sources are never scored.** TASS 159/159, Al Jazeera 158/158, NDTV, BBC, FT, CGTN,
   The Hindu and others store RSS teasers under 400 characters. Collector full-text fetch, or say it.
7. **MA sees 3 of 30 S2 reports** (`s2=9421 (3/30)` in the MA budget line). Same family as the
   Aug 8 "S2-E never reaches MA" finding.
8. **L3.9 -- PHI-004 Appendix A** for S2-F Verification messages (alternative hypotheses, Cui Bono,
   legitimacy category, next update date).
9. **Verification aggregator loops 644 MEDIUM rows** without grouping by voice x lens (261 identical
   checks for one write per run). Output-identical fix, diff guard.
10. **Source-bytes tests that break on a CRLF working copy.** `test_alert_on_change` fails locally on
    a CRLF checkout and passes in CI (LF). Make such checks line-ending blind (as `test_cc125`/`cc126`).
11. **L3.6 / ruling 4 -- S4**: unchecked archive; falsifiable hypotheses checked in the weekly review.
12. **Siblings, small:** Compendium "PREDICTION TRACKER" section; S3-B stores a cut JSON wrapper
    (CC-124 unwraps it for readers; the stored value is still cut); the S2 report prompt names
    "GCSP educators" as the audience in Phase 1.

## 4. MISSION FOR LENS-048 (proposal -- Bro Alpha declares it)

Item 1 at the open, then **item 2 (D2)**: it is Layer 1 air -- S2-F feeds every Verification finding,
and a fair sample of 8 is still a small one. Item 3 next.

## 4. IDENTITY SEPARATION (ruling B, Bro Alpha, LENS-046)

The public tree is de-identified (`1c52063`): nothing but "Bro Alpha" appears for the operator, and the
partner project is named only by pseudonym. The remaining steps are tracked **off-repo** in a private
note, on purpose: describing them here would point readers at what they are meant to remove.

## 6. WEEKLY REVIEW (ruling 3)

**Review #2 due in the week of 2026-09-28.** Agenda: the first week of Direction B under CC-120
(one NEW, then silence unless something changes); the headline threat level question (item 4); the
sample widening after CC-122 (Watch alerts on new voices; RT findings may CLEAR -- the sample, not
RT, changed). Output `LENS_REVIEW_<session>.md`; Bro Alpha gives the Layer 2/3 verdicts.

## 7. DECISIONS RECORDED AT LENS-047 (delegated to Claude unless marked)

- Mission L3.4 (declared by Bro Alpha at the open).
- L3.4 design: finding = voice x lens; NEW at once; CHANGED after 3 daily checks in or out;
  CLEARED after 7 aggregator days; dark days do not count; append-only ledger; no backfill.
- CC-121 scope; S2-F Stage 1 as item 1 before L3.2 (Layer 1 first).
- CC-122: filter first, one article per source per round, oldest turn first (not count), lookback 24 h.
- L3.2: CC-123 held; L3.2 re-ranked behind L2.1.
- CC-124 / CC-125 / CC-126 scopes; S3-A rows not backfilled (pre-CC-119 text reads as a forecast).
- Register: one file, new numbers from LR-234 (above every id seen in the tree).
- Session not closed at midnight (Bro Alpha): continue; finish every LENS-046 item that could be finished.

## 8. EARNED THIS SESSION

Numbered in `LENS_RULES_REGISTER_LENS047.md`: LENS-047 as LR-269 .. LR-284 (the four older lists as LR-234 .. LR-268).

## 9. CLOSE CHECK (session protocol)

**1) Declared mission completed?** Yes. L3.4 shipped (CC-120) and is half certified. Beyond it, every
LENS-046 handover item that did not need a design, a probe or an outside event was done:
L2.3, L2.4, L2.5, L2.7, D6 (half), D7, the siblings; plus a Layer 1 starvation found on the way (CC-122).
Not done, with reasons: D2, L2.1 -> L3.2, L3.5 (design and probe work, items 2-5).

**2-3) Findings and roots:** see items 2-12. One new root: uncalibrated S2 confidence (item 3).

**7) Claims this session that were wrong (what the bytes said):**
- A raw-set "change" would chatter daily -> 9 messages in 30 days.
- A stateless "earlier row = sent" design -> withdrawn: a failed send would be lost silently.
- Dry run `quiet=2` -> `quiet=3` (RT x xi in the 90-day lookback). "Gates 31 -> 32" -> test steps 29 -> 30.
- The 6 h lookback misses half of each cycle -> batches fit in 6 h; the real risk is a run that starts
  before collection finishes (the lookback went to 24 h for that reason).
- The cap was for Cloudflare's budget -> `b98b814` says "fits 30-min cron window".
- RT is teaser-only -> RT's median is 439 characters; it passes the 400 gate, the teasers are TASS & co.
- Least-scored-first -> would have starved RT for ten days (fixed to oldest turn first before shipping).
- A defined scale + reason gate fixes L3.2 -> the evidence is saturated; reasons were invented (CC-123 held).
- The register was deferred twice -> four times (LENS-043..046 lists).
- "23 articles scored" (CC-121's own wording) -> 8 articles x 3 lenses.
- Tooling: `load_dotenv()` without a path; a CI grep cut at 10 lines; a grep for `scor` matching the
  job name; a stale bytecode mutant; an awk range ending on its own start line; `/tmp` invisible to
  Windows Python; `import os` placed above `from __future__`; a patch assuming LF after an autocrlf
  checkout; four lines inside an except block that a CC-104 test reads by bytes.

**Deliberately not done:** D2, L2.1, L3.5 (items 2-5); CC-123 (held); no backfill of the ledger or of
S3-A's first_domino; the completion test's rows and rulings unchanged (statuses only).

## 10. CARRY-OVER

Every open item of `LENS_TARGET_AND_ORDER_LENS047.md` not closed above is folded into items 1-12.
