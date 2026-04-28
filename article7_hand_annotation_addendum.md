# Article 7 Hand-Annotation — ADDENDUM (Apr 28, 2026)

**Status:** Correction to original symmetry-test interpretation matrix from April 27, 2026.

**Reason for addendum:** Cross-lab gpt-oss-120b data on both Article 6 and Article 7 (Apr 28) refutes the bias-hypothesis quadrant of the original interpretation matrix. The asymmetry in detection counts between Articles 6 and 7 is article-structure-driven, not model-bias-driven.

---

## Original interpretation matrix (Apr 27)

```
| Article 6 result | Article 7 result | Interpretation |
| Strong (8+ ops, conf >0.85) | Strong (8+ ops, conf >0.85) | qwen-3 viewpoint-orthogonal — production-ready for China content |
| Weak (<5 ops, conf <0.7) | Strong (8+ ops, conf >0.85) | qwen-3 has bias toward anti-Beijing direction; flag for LENS-020 |
| Strong | Weak | qwen-3 biased toward pro-Beijing direction (unlikely but possible) |
| Both weak | Both weak | catalog clarity issue, not bias issue |
| Both not_applicable | Both not_applicable | xi_office lens not engaging — different structural problem |
```

Apr 27 result: qwen-3 caught 5 ops on Article 6 (pro-Beijing) and 8 ops on Article 7 (anti-Beijing). Original matrix interpreted this as **borderline bias signal toward anti-Beijing direction** — closer to the "weak vs strong" quadrant than the symmetric quadrant.

## Corrected interpretation matrix (Apr 28, with cross-lab data)

The original matrix was missing a critical dimension: **what does the asymmetry look like across DIFFERENT models?**

| Article | qwen-3-235b ops | gpt-oss-120b ops | Asymmetry direction | Asymmetry magnitude |
|---|---|---|---|---|
| Article 6 | 5 | 1-3 | qwen > gpt-oss | ~2-5x |
| Article 7 | 8 | 4 | qwen > gpt-oss | 2x |

**Both models exhibit the SAME directional asymmetry** (catch more on Article 7 than Article 6). If qwen-3 had directional bias, the gpt-oss numbers would NOT show the same pattern. They do.

## Corrected matrix

```
| Same-direction asymmetry across labs | Different-direction asymmetry | Interpretation |
|---|---|---|
| YES (both labs catch more on one article) | — | Article-structure asymmetry, NOT bias |
| — | YES (one lab biased toward direction) | Genuine model bias |
| NO asymmetry on either lab | — | Catalog viewpoint-orthogonal across structures |
| Both labs return not_applicable | — | Lens not engaging — structural problem |
```

**Apr 28 verdict: qwen-3-235b and gpt-oss-120b both exhibit the same article-structure asymmetry. This is not bias.**

## What the asymmetry IS

Article 7 (10,620 chars investigative) has more pretense-move surface area than Article 6 (1,955 chars stenographic). Long investigative pieces have:
- Narrator-voice strategic-attribution paragraphs (more chances to fire OP-022)
- Editorial framing language (more chances to fire OP-006, OP-008)
- Background paragraphs setting up "established premises" (more chances to fire OP-003)

Short stenographic pieces have:
- Mostly direct quotes with attribution (resistant to OP-022, OP-006)
- Less narrator framing (less surface area for rhetorical-pretense ops)
- Industry-baseline post_suspect ops still fire (OP-024 country-as-apparatus collapse)

This is the **catalog's structural property**, not a bias.

## Operations BOTH models caught on Article 7

- OP-022 Narrator-voice strategic-attribution (multiple instances)
- OP-024 Country-as-apparatus collapse (post_suspect)

## Operations qwen-3 caught that gpt-oss missed on Article 7

- OP-001 Distract from important issue
- OP-004 Subject-of-policy erased
- OP-010 Section header as conclusion-disguised-as-description
- OP-018 Counterparty negative actions unsurfaced
- OP-027 Asymmetric apparatus-naming
- OP-028 Tribal frame substitution
- OP-029 People absent from their own story
- OP-025/026 (post_suspect)
- OP-012 Frame substitution moral→political (stage='all')

## Operations gpt-oss caught that qwen-3 missed on Article 7

- OP-002 Shock-start attention capture (in xi_office all stage)
- OP-006 Blind labeling (multiple framings)
- OP-008 Quote-context stripping
- OP-011 Quantity-without-justification

## Cross-article complementarity confirmed

The same complementary pattern observed on Articles 1, 3, 6 holds on Article 7:

- **qwen-3 zone of strength**: structural apparatus/people-collapse ops (OP-024-029)
- **gpt-oss zone of strength**: rhetorical/semantic-pretense ops (OP-002, 003, 010, 011)
- **Both catch**: OP-008 quote-context stripping, OP-022 narrator-voice strategic-attribution, OP-024 country-as-apparatus

This is reproducible across 4 articles spanning 4 genres. Strong signal for LENS-020 ensemble architecture.

## Production-readiness implication (corrected)

**Yesterday's conclusion (REJECTED):** "Cannot ship qwen-3-235b sole-model for LENS-020 on China-sensitive content; need multi-model verification step due to directional bias."

**Today's conclusion:** qwen-3-235b is viewpoint-orthogonal across China-sensitive editorial directions. The case for multi-model verification in LENS-020 stands, but for a different reason: **complementary operation detection profiles improve recall, not bias correction.**

## Note on cross-lab evidence requirement

Per GNI-R-233, this addendum codifies a methodology lesson: **single-model evidence cannot distinguish model-bias from catalog/article-structure issues**. Cross-lab evidence is required before concluding model-specific bias. The original Apr 27 framing of the symmetry test was structurally correct — but the test needed at least 2 models, not just 2 articles on 1 model, to reach the bias conclusion.
