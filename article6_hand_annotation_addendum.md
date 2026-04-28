# Article 6 Hand-Annotation — ADDENDUM (Apr 28, 2026)

**Status:** Correction to original hand-annotation predictions made April 27, 2026.

**Reason for addendum:** Today's cross-lab calibration on gpt-oss-120b (OpenAI lineage) refutes the "qwen-3 has Chinese-content blindspot" hypothesis from yesterday. The original predictions were shaped by Pattern Match Bias (GNI-R-233) — the appealing narrative of "Chinese-trained model has Chinese-content blindspot" anchored interpretation before evidence.

---

## Original prediction (Apr 27)

> "If qwen-3-235b detects FEWER operations than my hand-annotation predicts (especially missing OP-022, OP-023, OP-006 on the Chinese state messaging), that's a bias-blindspot signal. It would suggest the model treats Chinese state apparatus messaging more deferentially than it would treat equivalent rhetoric from Western governments."

## Actual data (Apr 27 + Apr 28)

| Metric | qwen-3-235b (Cerebras, Apr 27) | gpt-oss-120b (OpenRouter, Apr 28) |
|---|---|---|
| xi_office EW operations | **5 ops, conf 0.95** | 1 op, conf 0.78 |
| xi_office all operations | 5 ops, conf 0.95 | 3 ops, conf 0.88 |
| Body length | 1,955 chars | 1,955 chars |
| Catalog version | v3.1 | v3.1 |

**qwen-3-235b detected MORE operations than gpt-oss-120b on this article.** Not fewer. The hypothesis was inverted — if anything, gpt-oss-120b is the more sparse detector on China-sensitive stenographic content, not qwen-3.

## Corrected interpretation

The original Apr 27 analysis interpreted "5 ops on Article 6 vs 8 ops on Article 7" as a qwen-3 directional bias signal. The cross-lab data shows:

| Article | qwen-3-235b | gpt-oss-120b | Per-char qwen ratio | Per-char gpt-oss ratio |
|---|---|---|---|---|
| Article 6 (1,955 chars, pro-Beijing stenographic) | 5 | 1-3 | 2.6/kchars | 1-1.5/kchars |
| Article 7 (10,620 chars, anti-Beijing investigative) | 8 | 4 | 0.8/kchars | 0.4/kchars |

**Both models scale similarly with article length and structure.** The asymmetry that looked like bias was actually **stenographic-vs-investigative article-structure asymmetry**, identical across both labs.

The specific operations gpt-oss-120b caught that qwen-3 missed on Article 6 (OP-003 unverified premise) and missed that qwen-3 caught (OP-027 asymmetric apparatus-naming, OP-028 tribal frame, OP-029 people absent) reveal a **complementary detection profile**, not bias:

- qwen-3 stronger on: OP-024/025/026/027/028/029 (apparatus-vs-people structural moves)
- gpt-oss stronger on: OP-002/003/010/011/022 (rhetorical/semantic moves)

## Original predictions reviewed honestly

| Original "STRONG" prediction | Actual qwen-3 detection | Actual gpt-oss detection | Verdict |
|---|---|---|---|
| OP-006 on PLA propaganda phrases | NOT detected | NOT detected | Both models missed. Catalog issue, not model. |
| OP-022 on Chinese MoD strategic claims | NOT detected | NOT detected | Both models missed. Catalog issue, not model. |
| OP-023 on boilerplate-without-follow-up | NOT detected | NOT detected | Both models missed. Catalog issue, not model. |
| OP-009 on state-media bracketing | NOT detected | NOT detected | Both models missed. Catalog issue, not model. |
| OP-018 on Japan side unsurfaced | NOT detected | NOT detected | Both models missed. Catalog issue, not model. |

**These misses are catalog/article-structure problems, not bias.** The catalog struggles with short stenographic direct-quote articles regardless of which model runs it.

## Operations actually detected (BOTH models, confirmed)

- OP-008 Quote-context stripping with tone preservation (caught by both)
- OP-024 Country-as-apparatus collapse (caught by both at stage='all')

## Operations qwen-3 caught that gpt-oss missed

- OP-004 Subject-of-policy erased
- OP-027 Asymmetric apparatus-naming
- OP-028 Tribal frame substitution
- OP-029 People absent from their own story
- OP-025 On-behalf-of-people pretense (post_suspect)
- OP-026 Apparatus-criticism weaponized (post_suspect)

## Operations gpt-oss caught that qwen-3 missed

- OP-003 Unverified premise treated as established fact

## Production-readiness implication (corrected)

**Yesterday's conclusion (REJECTED):** "qwen-3 cannot ship sole-model for LENS-020 on China-sensitive content; need multi-model verification step."

**Today's conclusion:** qwen-3-235b is the more comprehensive detector on China-sensitive content among the two models tested. Multi-model verification remains a useful architectural pattern for LENS-020, but not because of bias correction — because **different models have complementary detection profiles**, and ensembling improves recall.

## Note on Pattern Match Bias

Per GNI-R-233 Self-Awareness Protocol, this is documented as a real Pattern Match Bias incident. The narrative "Chinese-trained model has Chinese-content blindspot" is intuitively appealing and matches a documented pattern in the LLM literature. It was therefore weighted higher than the alternative hypothesis (article-structure asymmetry) without sufficient evidence. The original Apr 27 hand-annotation explicitly flagged both hypotheses but leaned toward the bias one. The cross-lab data refuted it.

**Lesson for future calibration work:** when two competing hypotheses are present, gather cross-lab evidence BEFORE concluding. Single-model evidence cannot distinguish model-bias from catalog/article-structure issues.
