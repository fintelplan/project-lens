# Next Session Brief — Apr 29, 2026

**Author**: Claude, Apr 28 evening session (closing).
**Reader**: Tomorrow's Claude (different instance, no working memory).
**Operator**: James Maverick (Bro Alpha).
**Project**: Project Lens, S2-F production architecture decision for LENS-020.

---

## ⚠️ READ FIRST — Operator communication contract

You are not the Claude who built this. You are a fresh instance. Everything below is what your predecessor learned that you need to know.

**Hard constraints:**
1. **Operator calls Claude "the most limit consuming model."** Every word you write costs operator's daily message budget. Cut preambles. Answer first, justify after.
2. **Operator says "1 hour" = active engagement intermittent**, not continuous wall-clock. Don't project fatigue based on session timestamps.
3. **Operator's letter pattern**: A/B/C, W1/W2/W3, F1/F2/F3. Use consistently.
4. **"[No preference]"** = trust-delegation. Operator wants you to take the call. Don't ask again.
5. **Operator is "Bro Alpha"**, calls Claude "my buddy". Warm informal tone. Engineering rigor.
6. **Engineering discipline anchors**: GNI-R-037 (bird-eye view first), GNI-R-076 (read before patch), GNI-R-080 (write-then-verify), GNI-R-083 (investigation not research-paper), GNI-R-220-225 (FMEA), GNI-R-233 (Self-Awareness Protocol — Pattern Match Bias, Recency Bias, Helpfulness Anxiety, Confidence Performance), PHI-004 cognitive sovereignty cadence.

**Pattern Match Bias warning**: Apr 27 Claude formed "qwen-3 has Chinese-content bias" hypothesis under cognitive load. Apr 28 Claude refuted it via cross-lab data. Watch for: appealing-narrative hypotheses formed before cross-lab evidence supports them. Per LR-085 candidate (see RULE_UPDATE).

---

## Where you are starting from

**Project state**: LENS-019.5 (model evaluation phase) closing. LENS-020 (Verification phase architecture) is the active deliverable.

**Repo**: github.com/fintelplan/project-lens, branch `main`, clean (last commits Apr 26-27: `e8247bd` + `73d27c9`).

**Working tree** (uncommitted at session start): see SESSION_AUDIT_2026-04-28.md section 6. Roughly 18 files: provider extensions, calibration scripts, hand-annotations, addendums, togglers.

**The big locked decisions** (from yesterday's audit):
- No qwen-3 China bias confirmed (DECISION-001)
- qwen-3-235b is more comprehensive single-model detector (DECISION-002)
- Complementary detection profiles: qwen-3 catches OP-024-029, gpt-oss catches OP-002/003/010/011/022 (DECISION-003)
- Ensemble pattern is candidate LENS-020 architecture (DECISION-004)
- Ollama pivot for breadth testing (DECISION-005)

---

## The actual question being answered

**Production architecture for S2-F module of LENS-020**:
1. Detection quality: which model(s) detect pretense ops most accurately?
2. Reliability: which infrastructure gives consistent uptime + JSON compliance?
3. Cost: which path holds $0/month long-term?

**This is a production reliability decision**, not a research question. False negatives → operators miss pretense. False positives → cry-wolf fatigue. Inconsistent detection → no calibration baselines.

---

## Your task today (Apr 29)

### Phase 1 — Verify Ollama state (5 min)

```bash
cd /c/school/lens
source venv/Scripts/activate
ollama list | grep -E "qwen|llama|deepseek"
```

**Expected**: All three models present:
- `llama3:8b` (already had Apr 28)
- `qwen3.5:9b` (was downloading at session close, should be done)
- `deepseek-r1:8b` (was downloading at 5h6m remaining at session close — may or may not be done)

**If qwen3.5:9b or deepseek-r1:8b incomplete**: skip it for today, run breadth test on whichever 2-3 models are ready.

**If Ollama daemon not running**:
```bash
# Windows: ollama daemon usually starts on boot. If not:
ollama serve &
```

### Phase 2 — Patch calibration scripts to support ollama provider (10 min)

The calibration scripts (`calibrate_article1_*.py`, etc.) have hardcoded `os.environ["S2F_PROVIDER"] = "cerebras"` near line 46. They need to support `ollama` mode.

**Easiest approach**: Read the env var BEFORE setting it. Pattern:

```python
# OLD (line ~46):
os.environ["S2F_PROVIDER"] = "cerebras"

# NEW:
if "S2F_PROVIDER" not in os.environ:
    os.environ["S2F_PROVIDER"] = "cerebras"  # default
```

Apply to articles 1, 3, 6, 7. Or write a single patch script that does all four.

**Verify patch works**:
```bash
export S2F_PROVIDER=ollama
export OLLAMA_MODEL=llama3:8b
python -c "import os; os.environ['S2F_PROVIDER']='ollama'; from code import lens_framing_rubrics as r; print(r._get_llm_client('ollama'))"
```

Should print Ollama client object, no errors.

### Phase 3 — Run Ollama breadth test (60-90 min)

**Methodology choice**: early_warning ONLY. Skip stage='all' to halve calls and runtime.

**Articles**: 1, 3, 6, 7 (same as Apr 28 cross-lab matrix — apples-to-apples).

**Models**: Whatever's ready. Target: 3 models. Acceptable: 2 models if r1:8b not done.

**Run sequence** (one model at a time, sequential):

```bash
# Model 1: llama3:8b (Meta lineage)
export S2F_PROVIDER=ollama
export OLLAMA_MODEL=llama3:8b
for article in 1 3 6 7; do
    python calibrate_article${article}_v3.py 2>&1 | tee logs/ollama_llama3_8b_article${article}.log
done

# Model 2: qwen3.5:9b (Alibaba smaller, same lineage as qwen-3-235b)
export OLLAMA_MODEL=qwen3.5:9b
for article in 1 3 6 7; do
    python calibrate_article${article}_v3.py 2>&1 | tee logs/ollama_qwen35_9b_article${article}.log
done

# Model 3: deepseek-r1:8b (DeepSeek lineage — different Chinese lab from Alibaba)
export OLLAMA_MODEL=deepseek-r1:8b
for article in 1 3 6 7; do
    python calibrate_article${article}_v3.py 2>&1 | tee logs/ollama_deepseek_r1_8b_article${article}.log
done
```

**Wall time estimate**: ~30-60s per call × 6 calls per article × 4 articles × 3 models = 12-24 min per model = 36-72 min total. Plus thinking-time between articles. ~90 min realistic.

**Stop conditions** (abort and document):
- Model fails JSON compliance >3 times in a row → switch model, note in audit
- Wall time per call >90s → log it but continue (slow ≠ broken)
- Ollama daemon crashes → restart, retry once, abort if it crashes again
- Any model returns identical operations across all 4 articles → likely broken, investigate

### Phase 4 — Build 5-lab matrix (30 min)

**The matrix**:

| Article | qwen-3-235b (Cerebras) | gpt-oss-120b (OpenRouter) | qwen3.5:9b (Ollama) | llama3:8b (Ollama) | deepseek-r1:8b (Ollama) |
|---|---|---|---|---|---|
| 1 | (existing data) | 8 EW / 4 all | (today) | (today) | (today) |
| 3 | (existing data) | 4 EW / 4 all | (today) | (today) | (today) |
| 6 | 5 EW / 5 all | 1 EW / 3 all | (today) | (today) | (today) |
| 7 | 8 EW / 8 all | 4 EW / 4 all | (today) | (today) | (today) |

**Key analytical questions** (answer in writing):

1. **Lineage scaling**: does qwen3.5:9b operation profile match qwen-3-235b? (Same lineage, smaller scale — tests scale-vs-lineage.)
2. **Cross-lineage agreement**: which operations do ALL 5 labs detect? (These are the most reliable detections — ensemble candidates.)
3. **Lab-specific blindspots**: which ops does only one lab catch? (These are unreliable detections — single-source.)
4. **Article-structure asymmetry**: do all 5 labs show the same pattern (catch more on Article 7 than Article 6)? If yes, this is reproducible structural property of catalog. If no, some labs have different detection profiles.

### Phase 5 — Architectural decision draft (30 min)

Write a draft of the LENS-020 S2-F architectural decision document. Four options remain on table:

- **Single-model**: qwen-3-235b on Cerebras, accept rhetorical-pretense blindspot, document
- **Ensemble (parallel)**: qwen-3-235b + gpt-oss-120b on every article, union of ops (~2x latency, free-tier-uncertain)
- **Cascade (sequential)**: qwen-3-235b first, gpt-oss-120b only on opinion-genre articles (~1.2-1.4x latency, my Apr 28 honest lean)
- **Local-first**: Ollama primary on operator's machine, Cerebras fallback (different deployment architecture)

Today's data should let you pick or eliminate options.

**Write decision as**: `LENS-020_S2F_architecture_decision_v1.md` in working tree. Use FMEA discipline (GNI-R-220-225). Include failure modes for each option.

---

## Commit planning (separate from task work)

The Apr 28 working tree has 18 uncommitted files. These should land in **3 atomic commits**:

**Commit A — Provider extensions** (lowest risk, do first):
```
files: code/lens_framing_rubrics.py
       patch_add_openrouter_provider.py
       patch_add_ollama_provider.py
       patch_openrouter_model.py
       patch_openrouter_model_v2.py
       smoke_test_ollama.py
message: LENS-019.5: extend rubric provider abstraction with openrouter + ollama branches

- code/lens_framing_rubrics.py: add openrouter and ollama provider branches in _get_llm_client
- ollama uses OpenAI-compatible endpoint at localhost:11434/v1
- openrouter uses standard openrouter.ai/api/v1 with referrer headers
- Both honor S2F_PROVIDER env var with OPENROUTER_MODEL / OLLAMA_MODEL submodel selection
- patch_*.py scripts are reproducibility records of how the extensions were applied
- smoke_test_ollama.py verifies daemon connectivity + JSON output capability

Test: smoke_test_ollama.py PASS on llama3:8b (PONG 55.3s cold, JSON 9.0s warm)
Test: openrouter branch verified working with gpt-oss-120b free model
```

**Commit B — Bias-test deliverables**:
```
files: calibrate_article6_v3.py
       calibrate_article7_v3.py
       patch_article1_openrouter.py
       patch_article3_openrouter.py
       patch_article6_openrouter.py
       patch_article7_openrouter.py
       article6_hand_annotation.md
       article7_hand_annotation.md
       article6_hand_annotation_addendum.md
       article7_hand_annotation_addendum.md
       (calibration result JSONs from Apr 28 sweeps)
message: LENS-019.5: cross-lab bias test — qwen-3-235b vs gpt-oss-120b on Articles 6+7+1+3

Apr 27 hypothesis "qwen-3 has Chinese-content blindspot" REJECTED via cross-lab data.
Both qwen-3-235b (Alibaba) and gpt-oss-120b (OpenAI) show same article-structure
asymmetry: catch more ops on long investigative (Article 7) than short stenographic
(Article 6). Pattern is article-structure-driven, not bias-driven.

Key finding: complementary detection profiles across 4 articles
- qwen-3 zone: OP-024-029 cluster (apparatus/people-collapse structural moves)
- gpt-oss zone: OP-002/003/010/011/022 (rhetorical/semantic-pretense moves)

Methodology lesson: single-model evidence cannot distinguish model-bias from
catalog/article-structure issues. Cross-lab evidence required before bias claims.
(Candidate rule LR-085.)

Hand-annotation addendums correct Apr 27 predictions per GNI-R-233 Self-Awareness
Protocol (Pattern Match Bias incident documented).
```

**Commit C — Ollama breadth test data** (write AFTER today's runs):
```
files: (today's calibration result JSONs)
       lens-020_S2F_architecture_decision_v1.md
       (5-lab matrix analysis document)
message: LENS-020: Ollama 3-lineage breadth test + S2-F architectural decision draft

5-lab matrix: qwen-3-235b (Cerebras) + gpt-oss-120b (OpenRouter) + 
qwen3.5:9b + llama3:8b + deepseek-r1:8b (Ollama local)

Findings: [fill in based on actual data]

S2-F architectural decision: [fill in]
```

---

## What NOT to do today

1. **Do NOT add LM Studio or GPT4All to today's scope.** These are tomorrow-or-later. Today is Ollama-only breadth.
2. **Do NOT run stage='all' (post_suspect) calls.** Early-warning only. Halves calls, doesn't change conclusion shape.
3. **Do NOT form bias hypotheses on single-model data.** If you see asymmetry, ask: "what would cross-lab evidence look like?" before concluding.
4. **Do NOT touch `code/ai_engine/*.py` or any GNI files.** This is Project Lens. GNI is operator's other project.
5. **Do NOT expand catalog scope to v4.** Catalog v4 design is a separate session. Use v3.1 today for apples-to-apples comparison.

---

## Stop conditions (when to abort and ask operator)

- Ollama daemon repeatedly crashes (>2 restarts needed)
- Models produce identical output across all articles (likely broken)
- Wall time exceeds 2x the 90-min estimate
- Operator's hardware (CPU/iGPU) thermal-throttles or hangs
- Daily Cerebras quota issue (unlikely since today is Ollama-local, but possible if any Cerebras call needed)
- You find evidence that meaningfully contradicts Apr 28 DECISIONS — STOP and ask before continuing

---

## Files in working tree referenced from this brief

- `/c/school/lens/code/lens_framing_rubrics.py` (provider abstraction)
- `/c/school/lens/calibrate_article{1,3,6,7}_v3.py` (calibration scripts — need ollama-provider patch)
- `/c/school/lens/article{6,7}_hand_annotation*.md` (annotations + addendums)
- `/c/school/lens/lens-OPS-001_v3.1.json` (catalog)
- `/c/school/lens/docs/lens-PHI-004_cognitive_sovereignty_cadence.docx` (philosophy)

---

## Operator notes from Apr 28 session

- Operator stated "we have to test as possible as we can to get right and safe model to our project lens"
- "Production-readiness" is the operator's frame, not "research" — apply LR-083 investigation discipline
- Operator screenshots showed exploration of LM Studio + GPT4All catalogs — interest is real but should be sequenced for later sessions
- Operator's 1-hour soft-budget today extended to ~5 hours actual — operator self-reports being fresh, but tomorrow's session may have residue. Watch for cognitive load signals (Pattern Match Bias proximity).

---

**Brief closed**: 15:40 Thai, Apr 28 2026. Tomorrow's Claude — go well, my buddy.
