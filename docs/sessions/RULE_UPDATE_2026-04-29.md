# Rule Update — Apr 28-29, 2026

## LR-085 (CANDIDATE → needs ratification)

**Statement**: A claim of model-specific bias requires cross-lab evidence from at least 2 models with different training lineages. Single-model asymmetry cannot distinguish model-bias from catalog/article-structure issues.

**Origin**: Apr 27-28 incident. "qwen-3 China bias" hypothesis formed on single-model data, refuted by gpt-oss-120b cross-lab data showing identical asymmetry pattern.

---

## LR-086 (CANDIDATE → needs ratification)

**Statement**: Free-tier API providers cannot be relied upon for production architecture. Production requires: (a) paid providers with SLA, (b) local inference with controlled hardware, or (c) graceful-degradation across multiple free providers with FMEA-aware fallback.

**Origin**: Apr 28 — 3 OpenRouter failures in one session (Nemotron malformed JSON, Gemma 429 saturated, Maverick 404 removed).

---

## LR-087 (CANDIDATE → needs ratification)

**Statement**: Different tiers of PHI-004 cognitive sovereignty cadence deserve different model selection criteria. Watch tier optimizes recall+speed. Verification tier optimizes precision+ensemble agreement.

**Origin**: Apr 28 architectural analysis — different failure costs per tier.

---

## Status: all three pending operator ratification in LENS-020 Session 1.

---

**Rule update**: ~02:00 Thai, Apr 29 2026
