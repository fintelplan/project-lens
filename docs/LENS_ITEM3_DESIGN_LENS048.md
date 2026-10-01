# LENS ITEM 3 (L2.1) — DESIGN DRAFT: what Lens calls "confidence" (LENS-048, 2026-09-28)

Project Lens — Bro Alpha. Design only; nothing here is built. Order item 3 in
`LENS_TARGET_AND_ORDER_LENS049.md`. It is the root under L3.2 / L2.6 (the headline threat level) and
CC-123 (held). Everything in section 1 is bytes read at LENS-048; section 2 is the guideline; sections
3-6 are proposals for Bro Alpha to rule on.

---

## 1. What the bytes say (LENS-048)

**1.1 One column, five measures.** `injection_reports.confidence_score` is written by six analysts,
and it means something different for five of them:

| Analyst | What the column holds | Written at |
| --- | --- | --- |
| S2-A | the model's own stated confidence (prompt asks "0.0-1.0"; "only flag if >= 0.5") | `lens_s2a_injection.py:410` |
| S2-A (pre-sanitized phrases) | **a code constant 0.7** — 49 rows in 30 days | `lens_s2a_injection.py:266` |
| S2-B | the model's own stated confidence | `lens_s2b_coordination.py:439` |
| S2-C | `manipulation_score` (model-stated, a different quantity) | `lens_s2c_emotion.py:334` |
| S2-D | `narrative_consistency_score` | `lens_s2d_adversary.py:601` |
| S2-E | share of low-legitimacy actors, `len(low)/len(actors)` (a ratio, not a confidence) | `lens_s2e_legitimacy.py:550` |
| S2-GAP | `quality_score` | `lens_s2_gap.py:237` |

**1.2 The spread is wide; the per-wave maximum is not.** 30 days, 1,582 rows:

| Analyst | n | min | max | mean | most common |
| --- | --- | --- | --- | --- | --- |
| S2-A | 1,097 | 0.00 | 0.96 | 0.73 | 0.70 x142, 0.60 x100, 0.90 x71 |
| S2-B | 141 | 0.60 | 0.95 | 0.80 | 0.80 x35, 0.90 x34 |
| S2-C | 128 | 0.00 | 0.90 | 0.55 | 0.70 x34, 0.90 x29, 0.00 x28 |
| S2-D | 33 | 0.60 | 0.90 | 0.81 | 0.85 x14, 0.80 x12 |
| S2-E | 128 | 0.00 | 1.00 | 0.53 | 0.50 x26, 0.60 x18 |
| S2-GAP | 55 | 0.78 | 0.88 | 0.85 | 0.87 x21 |

**1.3 The readers take the max across the five measures:**
- Daily Brief "Top injection": orders `injection_reports` by `confidence_score` desc and prints
  `conf=0.92` (`lens_telegram.py:66, 127`).
- S2 report prompt: "Confidence: 90%" per row (`lens_s2_step_report.py:122, 133, 142`).
- Mission Analyst prompt: `confidence=` per row (`lens_mission_analyst.py:483-491`).
- Compendium: orders by `confidence_score` (`lens_compendium.py:101-145, 408`).

**1.4 Conclusion from the bytes.** "S2 confidences saturate at 0.80-0.95" (LENS-047) is first a
structural fact — a label that says confidence over five measures, and a max over ~50 mixed rows a
wave — and only second a question of model over-confidence. Both are real.

**1.5 Related, from the LENS-048 probe (S2-F, ministral-8b):** confidence 0.95 on every pair with
operations, including a pair where the model confused the actors. Model-stated confidence did not move
with the truth of the answer.

## 2. The guideline

**ICD 203 (US Intelligence Community Directive 203, Analytic Standards), standard (a)(2):** products
state the uncertainty of major judgments as two separate things — the **likelihood** of the event, and
the analyst's **confidence in the basis** for the judgment. Confidence may rest on the logic and the
evidentiary base: the **quantity and quality of source material** and understanding of the topic.
Products should note the causes of uncertainty (type, currency and amount of information, gaps) and,
as appropriate, **indicators that would change** the level of uncertainty.

**ICD 203 expressions of likelihood** (verify against the DNI text when building):
almost no chance / remote 01-05% · very unlikely 05-20% · unlikely 20-45% · roughly even chance 45-55% ·
likely 55-80% · very likely 80-95% · almost certain 95-99%.

**Presentation research:** readers match the intended meaning of these words far better when the
numeric range is printed in brackets beside the word (66% correspondence vs 32% for words alone, in an
experiment with 924 people).

**Research on model-stated confidence:** verbalized confidence from LLMs runs high and does not signal
error well; weaker models are worse. It is a claim, not a measurement.

## 3. Design principles (proposed)

1. **One name, one quantity.** Every stored number says what it measures. Nothing compares, ranks or
   takes a max across different kinds.
2. **Confidence is computed, not asked for.** Lens computes confidence in code from the evidence
   base (ICD 203's "quantity and quality of source material"). A model's stated number may be kept
   for audit, never shown as the confidence.
3. **Likelihood and confidence are separate fields** and are shown separately.
4. **Words with brackets for the reader** (e.g., "likely (55-80%)"), plus what the judgment rests on
   and what would change it.
5. **No constants posing as uncertainty** (the 0.7 at `s2a:266`).

## 4. Proposed phases (each its own CC, each with a gate)

**Phase A — name the measures (no behaviour change for the model; readers change).**
- A registry of what each analyst's number is (`S2-A: model-stated confidence`, `S2-C: manipulation
  score`, ...), in code, next to the readers.
- Readers stop ranking across analysts. The Brief's "Top injection" picks by **evidence count**
  (distinct sources behind the finding), shows the measure by its real name.
- The S2 report and MA prompts print each number under its real name.
- Gate: a test that fails if any reader orders `injection_reports` by `confidence_score` across
  analysts, or prints it as "confidence" for S2-C/D/E/GAP.

**Phase B — computed confidence (new field, not a rename).**
- For each finding: `evidence_sources` (distinct sources), `verbatim_share` (evidence found in the
  text; the LENS-048 title+body instrument), `agreement` (the same pattern flagged by >= 2 positions
  this wave), `recency`.
- `computed_confidence` in three ICD-style levels (low / moderate / high) from explicit rules — a
  table in code that anyone can read, not a model.
- The 0.7 constant becomes a `source: vocabulary_list` finding whose confidence comes from the same
  rules.
- Gate: replay on 30 days of rows (LR-270): the level must MOVE with the evidence (no single level
  above ~60% of rows), and the per-wave maximum must not sit at the top level on most waves.

**Phase C — the reader sees words with brackets.**
- Brief, S2 report and MA headline use ICD terms with ranges, the basis, and "what would change this".
- Only after Phase B, so the words rest on a number that moves.

**Phase D — the headline threat level (item 4, L3.2 / L2.6).**
- Decided after Review #2: keep a five-level headline driven by Phase B evidence, or replace it
  with "what changed since the last wave". CC-123's reason gate is re-probed on Phase B inputs.

## 5. What this does NOT do

- It does not re-train or re-prompt the models to be "less confident" — that would treat the symptom.
- It does not rewrite stored rows (history keeps its meaning; new fields are added).
- It does not touch S2-F's `lens_operation_detections.confidence` in Phase A (a separate reader set;
  the same principles apply later).

## 6. Questions for Bro Alpha

1. Show numbers to the reader at all, or ICD words with brackets only?
2. Keep the model-stated number in storage (audit) or drop it?
3. Phase A first as its own mission (smallest change, stops the false max), or A+B together?
4. For Phase D: is "what changed since the last wave" acceptable as the Brief's headline during
   Phase 1 (before public voice)?

## 7. References

- ODNI, *Intelligence Community Directive 203: Analytic Standards* (2015), standard (a)(2).
- Estimative-language table: ICD 203, as encoded in the MISP "estimative-language" taxonomy.
- Research on bracketed numeric guidelines for ICD 203 terms (experiment, n = 924).
- Research on LLM verbalized confidence and overconfidence (see the LENS-048 chat for the sources).
