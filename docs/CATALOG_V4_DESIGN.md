# Catalog v4 Design Document
# Project Lens | LENS-020 | Apr 29 2026

## Problem analysis

### Problem 1 — Stenographic article blindspot
Short wire reports (<2000 chars) score 1-3 ops vs 7-11 on opinion/investigative.
Root cause: 5 ops require multi-sentence evidence chains that don't exist in steno genre.
Affected: OP-006, OP-009, OP-018, OP-022, OP-023

The evidence patterns for these ops need LONG context:
- OP-006 needs "multiple decisions described" — steno has only one
- OP-009 needs "pattern correlates with source type" — steno has 1-2 quotes
- OP-018 needs specific counterparty actions to compare — steno omits all
- OP-022 needs "sentences in article's own voice making claims" — steno has few
- OP-023 needs "no companion sentence asking..." — steno has no space for companion

Fix: Add steno-genre variants (OP-030 to OP-034) with shorter evidence patterns
designed specifically for <3000 char articles.

### Problem 2 — OP-016 conflation
OP-016 "Personal-attribution framing of structural-national outcomes" fires on
BOTH legitimate cases (correct action attribution) and pretense cases (structural
outcome attribution). Evidence pattern says "DO NOT FIRE on simple factual
attribution" but models still conflate.

Fix: Keep OP-016 as outcome-attribution only. Add OP-035 for the subtle
conflation pattern where action-attribution is extended to imply outcome-causation.

---

## New operations: OP-030 to OP-035

### OP-030 — Absent counterparty in stenographic report
**Genre**: stenographic (wire/brief)
**Stage**: early_warning
**Primary lens**: 1 (Rights trajectory)
**Description**: In a short wire report about a bilateral interaction, only one
side's actions and statements are represented. The other party is referenced by
name or country only, with no specific actions quoted.
**Evidence pattern**: Article <3000 chars reports an interaction between two
actors. Actor A has specific quotes, actions, or stated positions. Actor B is
mentioned by name only with no specific quote, action, or position stated.
**Why this matters**: Steno genre legitimizes single-source framing by format —
"it's just a brief." But systematic single-sourcing across a beat is pretense
infrastructure even when individual articles look like legitimate constraints.
**Differs from OP-018**: OP-018 requires counterparty mentioned at strategic
level; OP-030 fires even when counterparty is simply absent.

### OP-031 — Unsourced state claim in brief format
**Genre**: stenographic
**Stage**: early_warning
**Primary lens**: 4 (Sovereignty Check)
**Description**: A short wire report makes a strategic-level claim about a
state actor's intent, goals, or posture in the article's own narrative voice,
with no attribution to any analyst, official, or document.
**Evidence pattern**: Article <3000 chars contains a sentence NOT in a quote
block that asserts what a state actor "seeks to," "aims to," "is attempting to,"
"wants to" or "is positioning for" — without "according to," "analysts say,"
or similar attribution.
**Why this matters**: In full-length articles, narrator voice is visible in
context. In steno, one unattributed sentence can be the entire framing of the
article.
**Differs from OP-022**: OP-022 requires longer context to detect the pattern;
OP-031 fires on a single sentence in brief format.

### OP-032 — Spokesperson quote as sole evidence in brief
**Genre**: stenographic
**Stage**: early_warning
**Primary lens**: 4 (Sovereignty Check)
**Description**: A short wire report's only substantive content is a single
official spokesperson quote, with no independent verification, no analyst
comment, and no observable evidence cited.
**Evidence pattern**: Article <3000 chars contains exactly one or two quotes,
both from official spokesperson(s) of the same side. No independent source,
no observable fact cited to verify the quote's claim.
**Why this matters**: Steno format makes pure spokesperson amplification look
like news. A 500-char wire that is 70% official quote is not reporting — it is
free distribution of official messaging.
**Differs from OP-023**: OP-023 requires identifying what follow-up is missing;
OP-032 fires on the structural pattern of spokesperson-only content.

### OP-033 — Label-only framing in headline-brief
**Genre**: stenographic
**Stage**: early_warning
**Primary lens**: 4 (Sovereignty Check)
**Description**: A very short wire report or brief uses a loaded evaluative
label (threat, aggression, provocation, escalation, stability, etc.) as its
primary framing device, with no cause-chain supporting the label in the body.
**Evidence pattern**: Article <2000 chars. Title or first sentence contains an
evaluative label. Body of article contains no explanation of what actions
constitute the labeled behavior, no evidence chain, no comparison to baseline.
**Why this matters**: In steno format, label-only framing IS the article. There
is no body text to provide context. The label becomes the fact.
**Differs from OP-006**: OP-006 requires "multiple decisions described" — in
steno there is only one, and the label IS the article.

### OP-034 — Single-source verb asymmetry in brief
**Genre**: stenographic
**Stage**: early_warning
**Primary lens**: 1 (Rights trajectory)
**Description**: A short report with only 2-3 quoted sources assigns reactive
or defensive introduction verbs to one side and neutral or active verbs to the
other, creating asymmetric framing with very limited evidence.
**Evidence pattern**: Article <3000 chars. One quoted source introduced with
active verb (said, stated, announced, declared). Different quoted source from
opposing side introduced with reactive verb (responded, pushed back, denied,
defended, rejected).
**Why this matters**: In full articles, verb asymmetry may reflect genuine
press conference dynamics. In steno, the writer has chosen 2-3 quotes and
their verb choices ARE the framing — there is no other context.
**Differs from OP-009**: OP-009 pattern requires "correlation with source type"
across multiple instances; OP-034 fires on the pattern in minimum-context articles.

### OP-035 — Action-to-outcome causation extension
**Stage**: early_warning
**Primary lens**: 1 (Rights trajectory)
**Description**: An article correctly attributes a DECISION to a named actor,
then extends that attribution to claim that the actor CAUSED a structural
outcome — conflating decision-making with structural causation.
**Evidence pattern**: Sentence 1 correctly attributes: "[Actor] decided/launched/
signed/ordered X." Sentence 2 then attributes structural outcome: "[Actor]'s X
caused/produced/resulted in [large structural outcome]." The causal chain between
decision and structural outcome is not explained.
**Why this matters**: "Trump signed the tariff order" (correct) → "Trump's
tariffs caused the recession" (structural conflation). The second sentence hides
all the intervening factors (markets, supply chains, corporate decisions, Fed
policy) behind one actor's name.
**Differs from OP-016**: OP-016 fires when structural outcomes are attributed
to personal agency with NO prior action citation. OP-035 fires specifically on
the extension pattern where legitimate action attribution is used to smuggle in
illegitimate outcome attribution.

---

## Schema changes for v4

New field: `genre_context` — "universal" | "stenographic" | "opinion" | "investigative"
- universal: applies to all article types (current ops OP-001 to OP-029)
- stenographic: designed for <3000 char wire/brief articles (OP-030 to OP-034)
- opinion: higher signal threshold (already handled by calibration)
- investigative: lower early_warning threshold (already handled)

New field: `min_article_chars` — optional minimum article length for the op to apply
- OP-030 to OP-034: max_article_chars = 3000 (only fire on short articles)
- All others: no limit

New field: `related_ops` — list of related operation IDs for ensemble context

---

## Backward compatibility

All 29 existing ops (OP-001 to OP-029) UNCHANGED.
v4 adds 6 new ops (OP-030 to OP-035) = 35 total.
detection_stage for new ops: all early_warning.
Catalog version: v4.0
Catalog ID: lens-OPS-001

---

## Implementation plan

1. Update lens-OPS-001_catalog_v3_1.json → lens-OPS-001_catalog_v4_0.json
2. Add genre_context + min/max_article_chars fields to schema
3. Update _build_catalog_block() in lens_framing_rubrics.py to filter by
   genre_context based on article length
4. Add article_chars parameter to detect_operations_in_article()
5. Re-run calibration on Articles 1, 3, 6, 7 with v4 catalog
6. Compare v3.1 vs v4 coverage on Article 6 (steno) specifically

Priority: Article 6 (1955 chars) should go from 1-5 ops → 5-9 ops with v4.

---

## Session: LENS-020 | Draft: Apr 29 2026 | Status: DESIGN LOCKED, pending implementation
