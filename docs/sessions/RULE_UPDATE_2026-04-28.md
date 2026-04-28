# Rule Update — Apr 28, 2026

## Three new candidate rules from today's session

These are LR-085, LR-086, LR-087 candidates. Need operator review and ratification before adding to canonical rule set. (Per session record, GNI rules use LR-XXX numbering and require operator confirmation.)

---

## LR-085 (candidate) — Cross-lab evidence required for model-bias claims

**Statement**: A claim of model-specific bias (directional, content-based, or otherwise) requires cross-lab evidence from at least 2 models with different training lineages. Single-model asymmetry data cannot distinguish model-bias from catalog/article-structure issues.

**Origin**: Apr 27-28 incident. Apr 27 Claude formed "qwen-3 has Chinese-content blindspot" hypothesis based on single-model data (5 ops on pro-Beijing Article 6 vs 8 ops on anti-Beijing Article 7). Apr 28 cross-lab data (gpt-oss-120b on same articles) refuted the hypothesis — both models exhibit identical asymmetry pattern, which is article-structure-driven not bias-driven.

**Why this rule**: Pattern Match Bias makes the bias-narrative intuitively appealing. The structural alternative (article length/genre asymmetry creates more pretense-move surface area) is less narrative-rich but more often correct. Single-model evidence cannot rule out the structural explanation.

**Mechanism**:
- When asymmetry observed in single-model data, do NOT publish bias hypothesis
- Run same articles on at least 1 model from a different lab (different training lineage)
- If asymmetry persists across labs → structural property of catalog/articles, not bias
- If asymmetry differs across labs → genuine model-specific signal, candidate for bias claim

**Linked to**: GNI-R-233 (Self-Awareness Protocol — Pattern Match Bias)

---

## LR-086 (candidate) — Free-tier API providers are not production infrastructure

**Statement**: Free-tier API providers (OpenRouter free models, HuggingFace inference free, similar) cannot be relied upon for production architecture. Production architecture requires either (a) paid providers with SLA, (b) local inference with controlled hardware, or (c) graceful-degradation across multiple free providers with FMEA-aware fallback logic.

**Origin**: Apr 28 documented 3 distinct free-tier failure modes within a single 4-hour session:
- NVIDIA Nemotron 3 Super (OpenRouter): malformed JSON for our schema, functionally broken
- Google Gemma 4 31B (OpenRouter): consistent 429 upstream-saturation
- Meta Llama 4 Maverick (OpenRouter): removed from free tier without notice in Feb 2026

Plus historical:
- HuggingFace inference API replaced with Cerebras after persistent issues (S18)
- Groq llama-3.3-70b 413 Request Too Large on free tier for catalog v3.1 size (S22-equivalent for Lens)

**Why this rule**: Free tiers have no SLA, no upstream commitment, no notification of changes. They are useful for evaluation, prototyping, and low-stakes inference. They are not production-grade for systems where reliability is a feature.

**Mechanism**:
- For LENS-020 production architecture, do NOT default to free tier as primary
- Acceptable: free tier as evaluation/calibration infrastructure
- Acceptable: free tier as one of N parallel providers with graceful fallback
- Required for primary: either paid provider with SLA, or local Ollama with controlled hardware
- Document failure modes when discovered (today's data is the canonical record)

**Linked to**: GNI-R-220-225 (FMEA discipline), zero-cost architecture constraint (creates tension to manage)

---

## LR-087 (candidate) — Tier-specific model selection

**Statement**: Different stages of the cognitive sovereignty cadence (Watch / Clarity / Verification / Direction) deserve different model selection criteria. A single-model-fits-all architecture under-serves the system. Watch tier (LOW conf) can use higher-recall lower-precision detector. Verification tier (HIGH conf) requires highest-accuracy detector or ensemble.

**Origin**: Apr 28 architectural analysis identified that S2-F output feeds different downstream consumers with different reliability requirements:
- Watch tier flags possible-pretense at LOW confidence — false positives are tolerable, false negatives are not
- Verification tier produces HIGH confidence forensic reports — false positives create cry-wolf fatigue, must be minimized
- Direction A/B (Phase 2) consumes verified outputs — quality propagates downstream

**Why this rule**: PHI-004 cognitive sovereignty cadence is built on the principle that different stages have different stakes. Model selection should respect this. Forcing one model to serve all tiers either over-spends compute on Watch or under-detects on Verification.

**Mechanism**:
- Each tier has documented model selection criteria
- Watch tier: optimize for recall + speed + cost (faster/cheaper/wider net)
- Clarity tier: optimize for balanced precision/recall
- Verification tier: optimize for precision + ensemble agreement (most accurate single model OR ensemble)
- Documented per-tier model selection becomes part of LENS-020 architectural decision

**Linked to**: PHI-004 (cognitive sovereignty cadence), GNI-R-220-225 (FMEA — different failure cost profiles per tier)

---

## Notes on rule-promotion process

These are CANDIDATES, not yet ratified. Operator review needed. Per session record, GNI uses two patterns:
- Numbered rule documents (e.g., GNI-R-232 Visual Fix Protocol)
- LR-XXX (Lens Rules) for Project Lens specifically

These three should be reviewed with operator on Apr 29 or later session and either:
- Ratified as LR-085/086/087 with formal documentation, OR
- Reframed as patterns-of-practice without rule status, OR
- Rejected if operator disagrees

---

## Existing rules confirmed in active use today

- **GNI-R-037** (Bird-eye view first): used when operator asked for "deep analysis" — produced bird-eye-view document
- **GNI-R-076** (Read before patch): not directly invoked but principle held
- **GNI-R-080** (Write-then-verify): used in smoke_test_ollama after patch_add_ollama_provider
- **GNI-R-083** (Investigation not research-paper): held — kept work focused on production-readiness frame
- **GNI-R-220-225** (FMEA): explicitly invoked when discussing free-tier reliability
- **GNI-R-233** (Self-Awareness Protocol — Pattern Match Bias): central to today's hand-annotation correction work
- **PHI-004** (Cognitive sovereignty cadence): architectural framing for LENS-020 decision

---

**Rule update**: 15:40 Thai, Apr 28 2026
