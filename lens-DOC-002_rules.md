
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


---

## LR-093 — Explicit cleanup tracking in session close docs (LENS-022)

**Type**: Process | **Added**: LENS-022 | **Status**: RATIFIED

Every session close doc must include a CLEANUP section listing:
- Test/smoke data to delete from production DB
- Stale references to remove from code/docs
- Temporary files to delete from repo root

These items are BLOCKING for the next session — not optional backlog.

Origin: Smoke test entity 'professor john smith' persisted in lens_entities
for 13 days (Apr 21 -> May 4) because it was noted but never added to
any explicit cleanup list. It appeared as 'most active entity' in every
Daily Brief during that period, polluting operator intelligence.


---

## LR-094 — Quota isolation for critical positions (LENS-022)

**Type**: Architecture | **Added**: LENS-022 | **Status**: RATIFIED

Any position designated CRITICAL (sys.exit(1) on failure) MUST have its own
dedicated API key from a separate provider account.

Origin: S2-A was designated critical but shared GROQ_S2_API_KEY with S2-GAP.
By second daily run, shared quota was depleted -> S2-A failed -> manage-analyze
exit(1) -> workflow_run for forensic report never fired -> 0 Opus docx for days.

Pattern: Same lesson as GNI S30 (LR-058), LENS-010 S2-A/E isolation, LENS-022
GEMINI_S2B_API_KEY. Same-account keys share quota. Critical positions need
guaranteed headroom = dedicated account.

Rule: Before designating any position as sys.exit(1) critical, verify:
1. It has its own API key from a separate account
2. No other position shares that key
3. The key is added to all relevant yml env sections
