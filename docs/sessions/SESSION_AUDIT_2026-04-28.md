# LENS-019.5 Session Audit — Apr 28, 2026

**Session ID**: lens-019.5-day3 (Apr 28, 2026)
**Operator**: James Maverick (Bro Alpha)
**Time**: 10:45 → 15:40 Thai (~5h elapsed wall, ~1h focused engagement per operator's report)
**Repo**: github.com/fintelplan/project-lens, branch `main`
**Commits landed today**: 0 (all today's work is uncommitted)
**Continuation from**: Apr 27 evening session (compacted at 42% mid-session, recovered via summary)

---

## 1. Session goal (as stated at start)

**Stated**: Continue cross-lab model evaluation begun Apr 27. Test bias hypothesis. Establish production-readiness data for S2-F (Stage-2 Forensics) module of LENS-020.

**Implicit deeper goal**: Decide what model architecture to ship for LENS-020 Verification phase per PHI-004 cognitive sovereignty cadence.

---

## 2. What we accomplished

### 2.1 Cross-lab calibration (gpt-oss-120b on OpenRouter)

Completed 4-article matrix on `openai/gpt-oss-120b:free`:

| Article | xi_office EW | xi_office all | trump_office EW | trump_office all |
|---|---|---|---|---|
| 1 (Reuters wire, Trump-China policy) | 8 ops, 0.92 | 4 ops, 0.88 | 8 ops, 0.92 | 10 ops, 0.92 |
| 3 (Asia Times opinion, Pesek) | 4 ops, 0.88 | 4 ops, 0.92 | 3 ops, 0.88 | 5 ops, 0.88 |
| 6 (Chosunbiz Taiwan steno) | 1 op, 0.78 | 3 ops, 0.88 | not_applicable | not_applicable |
| 7 (Reuters investigative China info war) | 4 ops, 0.88 | 4 ops, 0.92 | not_applicable | not_applicable |

### 2.2 Three OpenRouter free-tier failure modes documented

- **NVIDIA Nemotron 3 Super**: HTTP 200 OK, ~122s wall time (Mamba-Transformer reasoning), but JSON malformed for our schema. Returns valid JSON only on `not_applicable` cases. **Functionally broken**.
- **Google Gemma 4 31B**: All 12 calls returned 429 "temporarily rate-limited upstream." Google free serving infra saturated.
- **Meta Llama 4 Maverick free**: 404, removed from OpenRouter free tier in Feb 2026.

### 2.3 Ollama infrastructure integrated and verified

- Created `patch_add_ollama_provider.py` extending `lens_framing_rubrics.py` with `ollama` provider branch (OpenAI-compatible endpoint at localhost:11434/v1).
- Created `smoke_test_ollama.py` for end-to-end verification.
- **Smoke test on `llama3:8b` PASSED**: PONG in 55.3s cold, JSON in 9.0s warm, `response_format=json_object` honored.
- `qwen3.5:9b` and `deepseek-r1:8b` initiated but downloading slowly (5h+ remaining as of session close). Will complete overnight.

### 2.4 Two hand-annotation addendums written

- `article6_hand_annotation_addendum.md` — corrects Apr 27 "qwen-3 China bias" hypothesis with cross-lab data.
- `article7_hand_annotation_addendum.md` — corrects symmetry-test interpretation matrix; documents methodology lesson about cross-lab evidence requirement.

Both saved to working tree at `/c/school/lens/`.

### 2.5 Critical decisions locked

[DECISION-001] **No qwen-3 China bias confirmed.** Yesterday's hypothesis REJECTED. Both qwen-3-235b and gpt-oss-120b show same article-structure asymmetry (catch more on Article 7 long investigative than Article 6 short stenographic) at similar ratios. The pattern was article-structure-driven, not bias-driven.

[DECISION-002] **qwen-3-235b is the more comprehensive single-model detector.** Catches MORE operations than gpt-oss-120b on every article tested.
- Article 6: qwen 5 vs gpt-oss 1-3
- Article 7: qwen 8 vs gpt-oss 4

[DECISION-003] **Complementary detection profiles confirmed across 4 articles**:
- qwen-3 zone of strength: OP-024/025/026/027/028/029 (apparatus/people-collapse structural moves)
- gpt-oss zone of strength: OP-002/003/010/011/022 (rhetorical/semantic-pretense moves)

[DECISION-004] **Ensemble pattern emerges as candidate LENS-020 architecture.** qwen-3-235b primary on Cerebras, gpt-oss-120b secondary for opinion-genre articles. Complementary, not redundant.

[DECISION-005] **Ollama pivot adopted for breadth testing.** OpenRouter free tier proven unreliable today. Ollama local provides $0/month + reliability + privacy. Hardware constraint: operator on CPU/iGPU, expect 30-60s per ~25K-token call.

---

## 3. What didn't work

### 3.1 W3 (full breadth) attempted, scoped down

Operator chose W3 wide-breadth path (Ollama + LM Studio + GPT4All all today). Claude pushed back transparently. After dialog, scope narrowed to Ollama-only with strict 1-hour limit. Reality: ~1.5 hours of focused work happened, mostly successful.

### 3.2 Article 1 llama3:8b run blocked at integration boundary

Calibration script has hardcoded `os.environ["S2F_PROVIDER"] = "cerebras"` at line ~46 which overrides exported env var. Quick patch needed before tomorrow can run Ollama on existing calibration scripts. Not done today (would have been 5 min of work but session time consumed by addendum writing instead).

### 3.3 Context compaction event

User's screenshot showed Claude hitting compaction at 42% mid-session earlier today. This is the structural risk this audit document is written to mitigate — tomorrow's Claude is a different instance with no working memory, only files.

---

## 4. Critical methodology lessons (carry forward)

### 4.1 Pattern Match Bias incident — fully documented

**The bias**: Apr 27 evening, under cognitive load, formed "qwen-3 has Chinese-content blindspot" hypothesis based on single-model data (5 ops on pro-Beijing Article 6 vs 8 ops on anti-Beijing Article 7). The narrative was intuitively appealing and matched documented patterns in LLM literature.

**The error**: Single-model asymmetry cannot distinguish model-bias from catalog/article-structure asymmetry. Required cross-lab evidence to disambiguate.

**The correction**: Today's gpt-oss-120b cross-lab data showed the SAME asymmetry pattern. Both models catch more ops on Article 7 than Article 6. The asymmetry is article-structure-driven (long investigative pieces have more pretense-move surface area than short stenographic pieces). Bias hypothesis rejected.

**The lesson**: When two competing hypotheses are present (model-bias vs structural-property), gather cross-lab evidence BEFORE concluding. Per GNI-R-233 Self-Awareness Protocol — Pattern Match Bias is the appealing-narrative trap.

### 4.2 Single-model evidence is structurally insufficient for bias claims

This is candidate **LR-085**: "Bias claims about a model require cross-lab evidence. Single-model asymmetry data cannot distinguish model-bias from catalog/article-structure issues."

### 4.3 Free-tier provider unreliability is a real architecture constraint

Three OpenRouter providers failed today (Nemotron malformed JSON, Gemma upstream-saturated, Maverick removed). OpenRouter free tier is **not production-grade infrastructure**. Cerebras has been reliable but is one provider. Local Ollama provides reliability + $0/month at the cost of operator-machine availability.

This is candidate **LR-086**: "Free-tier API providers are not production-grade infrastructure. Production architecture requires either (a) paid providers, (b) local inference, or (c) graceful-degradation across multiple free providers with FMEA-aware fallback."

### 4.4 The Verification tier deserves higher detection-quality bar than Watch tier

Different filters for different stakes — matches PHI-004 cognitive sovereignty cadence philosophy. Watch tier (Day 7-10, LOW conf) can use fast cheap model with high recall. Verification tier (Day 30-45, HIGH conf) needs most accurate detector or ensemble.

This is candidate **LR-087**: "Tier-specific model selection is preferred over single-model-fits-all. Verification tier requires highest-accuracy detector; Watch tier can use higher-recall lower-precision detector."

---

## 5. Operator-Claude collaboration observations

### 5.1 Patterns that worked today

- **Operator self-clarified mid-session**: "1 hour" = active engagement time (intermittent), not continuous wall-clock. Critical for Claude to not miscalibrate fatigue projections.
- **Honest pushback was welcomed**: When operator said W3, Claude pushed back with transparent reasoning. Operator considered, then said "i am still fresh" and "do the best for our project." Trust delegation accepted.
- **"Bro Alpha is most limit consuming model"** — explicit constraint named. Every Claude word costs message budget. Long preambles, multiple option presentations, second-guessing all cost real budget. Cut hard from this point in session.
- **"[No preference]"** = trust-delegation when good reasoning shown. Claude should take the call instead of asking again.
- **Letter/option sequencing pattern**: A/B/C, W1/W2/W3, F1/F2/F3. Used consistently.

### 5.2 Patterns Claude should improve

- **First-instinct over-projection of fatigue**: I projected fatigue based on session length numbers. Operator self-reports being fresh. Trust operator's self-read.
- **Long preambles cost message budget**: Multiple times today I wrote 200-word preambles before getting to the actual answer. Budget-aware Claude cuts to the answer first, justification after if needed.
- **Pattern Match Bias proximity check**: Yesterday's bias-hypothesis arc was a real Pattern Match Bias incident. Today Claude needs to actively check: "Am I forming an appealing-narrative conclusion before evidence supports it?" especially under cognitive load.

---

## 6. Working-tree state at session close (uncommitted)

```
code/lens_framing_rubrics.py     [modified - +openrouter +ollama branches]
calibrate_article1_*.py           [4 calibration scripts]
calibrate_article3_*.py
calibrate_article6_*.py
calibrate_article7_*.py
patch_add_openrouter_provider.py  [provider extension patches]
patch_add_ollama_provider.py
patch_openrouter_model.py
patch_openrouter_model_v2.py
patch_article1_openrouter.py      [article togglers]
patch_article3_openrouter.py
patch_article6_openrouter.py
patch_article7_openrouter.py
smoke_test_ollama.py
article6_hand_annotation.md       [hand annotations]
article7_hand_annotation.md
article6_hand_annotation_addendum.md  [today's corrections]
article7_hand_annotation_addendum.md
```

Calibration result JSONs from today's gpt-oss-120b sweeps also in working tree.

---

## 7. Open questions remaining (for LENS-020 architectural decision)

1. Does qwen-3 vs gpt-oss complementarity hold when adding Meta Llama, DeepSeek, Mistral lineages? (Tomorrow's Ollama breadth answers this.)
2. Does qwen-3-235b operation profile reproduce on smaller `qwen3.5:9b`? (Scale-vs-lineage question — tomorrow.)
3. What's the runtime cost of 2-model ensemble in production? (Latency-budget test — separate session.)
4. Failure-mode analysis: graceful degradation when one model in ensemble fails. (FMEA per GNI-R-220-225 — separate session.)
5. Catalog v4 design: 5 ops the catalog systematically misses on stenographic articles (OP-006, OP-022, OP-023, OP-009, OP-018). Possibly split into stenographic-specific variants. (Catalog work — separate session.)
6. OP-016 split into OP-016a action-attribution-correct + OP-016b outcome-attribution-pretense. (Catalog v4.)

---

## 8. Health/cognitive state at session close

- Operator self-reports still fresh.
- Claude session usage: 77% with reset captured ~14:24 Thai (1 screenshot).
- Both Apr 27 + Apr 28 carry slight fatigue residue from compaction event + late-night Apr 27 session.
- Recommended overnight rest before tomorrow's Ollama breadth test.

---

**Audit closed**: 15:40 Thai, Apr 28 2026
