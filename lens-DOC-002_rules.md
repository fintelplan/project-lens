
---

## LR-091 — LOCAL model testing protocol (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

When testing LOCAL models for S2-F rubric calibration or S3-E self-check:

1. **Provider setup**: `export S2F_PROVIDER=ollama && export OLLAMA_HOST=localhost:11434 && export OLLAMA_MODEL=<model_name>`
2. **LM Studio alternative**: use port 1234 instead of 11434 — same OpenAI-compatible API
3. **RAM constraint (James machine: 32GB)**: models >24GB will cause OOM. Safe limit: 20GB model size
4. **JSON quality gate**: run calibration script first. If `LLM_FAILED` or malformed JSON on 2+ runs — reject model regardless of benchmark claims
5. **Proven LOCAL models (Apr 2026)**: `ministral-3:8b` (6GB, best quality, MistralAI lineage) ✅
6. **Rejected LOCAL models**: `gemma4:e4b` — failed JSON quality gate ❌
7. **No rate limits locally** — can run full 6-lens calibration in one pass, unlike cloud providers
8. **S3-E air-gap requirement**: S3-E self-check MUST use LOCAL model (Ollama). Remote API defeats the epistemic independence purpose (W-010)



---

## LR-092 — Post-patch syntax verification on ALL affected files (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

After ANY patch that touches multiple files, run python -m py_compile on
ALL modified files before committing — not just the file you intended to fix.

Pattern that caused two production outages (S2-D + 3 S2F aggregators):
- Patch script found insertion point correctly in one file
- Same broken import pattern existed in sibling files (same LENS-021 origin)
- Only the primary file was syntax-checked
- All sibling files broke silently on next cron

Rule: After any patch, verify ALL modified Python files:
  python -m py_compile code/file1.py && echo OK
  python -m py_compile code/file2.py && echo OK
Never commit until ALL modified Python files pass py_compile.
