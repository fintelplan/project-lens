# LENS L-CLIFF Build Plan (Claude Code task blocks)
**2026-07-27, LENS-028. Execute IN ORDER, one block = one commit purpose. Every block: BEV the target files first, LR-078 ship-to-file patches (binary rb/wb, ASCII anchors, assert count==1, derive newline from the file's own bytes), `python -m py_compile` every touched .py (LR-092), propose the patch BEFORE editing (L2 — James approves), push with the full-URL command, `git ls-remote` verify. Companion docs: LENS_LCLIFF_DECISIONS.md (rationale), code/lens_models.py (the registry, already written and self-tested).**

## CC-0 — Land the registry
Place `lens_models.py` at `code/lens_models.py` exactly as delivered. Run `python code/lens_models.py` (self-test prints roles/pairs/rows) + py_compile. Commit: `feat: lens_models.py registry -- L-CLIFF single source of truth (LENS-028 D-001)`.

## CC-1 — Guard rewire (code/lens_quota_guard.py, 778 lines — full read first)
1. `from lens_models import PROVIDER_LIMITS as REGISTRY_LIMITS, assert_model_known, limits_for` — replace the in-file PROVIDER_LIMITS dict (L62-70) with the registry export. Delete the dead `("groq","qwen3-32b")` row via the registry (it simply is not there).
2. POSITION_CONSUMPTION: re-key each position's (provider, model) tuple from `lens_models.ROLES` (positions map: S2-A->s2a_injection, S2-B->s2b_coordination, S2-D->s2d_adversary, S2-E->s2e_legitimacy, S2-GAP->s2gap, MA->mission_analyst, S3-*->s3*_..., etc.). Keep the numeric estimates AS-IS but add a `# PROVISIONAL until post-migration recalibration (D-009)` comment.
3. Add `assert_model_known(provider, model)` at the guard's request chokepoint(s) so any unregistered pair raises before an HTTP call is made.
4. Retune TPM governor constants for the 8K TPM reality (was 12K on 70b; S2-A's internal 6000 pacing value must not exceed 8000 and should leave headroom — propose 7000).
5. Update tests/test_lens_quota_guard.py expectations (24 model-string hits) to import from the registry instead of literals.
Receipts: grep zero remaining `llama-3.3-70b-versatile` / `qwen3-32b` literals in the guard + its test; py_compile both; guard unit tests green.

## CC-2 — 4-lens engine + orchestrator
Files: code/analyze_lens_multi.py, code/lens_orchestrator.py.
1. LENSES config: model/provider/api_key_env fields become `lens_models.wire("lens1")` etc. Lens-1 primary goes from dead qwen to gpt-oss-120b. Keep the `<think>`-stripping (L605) — it applies to gpt-oss reasoning output too; verify it strips gpt-oss's channel format, extend if needed.
2. FALLBACKS dict (L343-344): populate from `lens_models.fallback(role)` — kills the gemini-1.5-flash corpse and the dying 70b entries.
3. AI-5 watchdog call (L174-176): model from registry role `ai5_watchdog`, `max_tokens=300` -> `fit_max_tokens(prompt_chars, 1600)`; key env stays GROQ_MA_API_KEY in this block (key wiring moves in CC-6 to keep one purpose per commit).
Receipts: grep zero cliff literals in both files; py_compile.

## CC-3 — S2 family sweep
Files: lens_s2a_injection.py, lens_s2d_adversary.py, lens_s2e_legitimacy.py, lens_s2_gap.py, lens_mission_analyst.py, lens_s2_orchestrator.py (docstring table too), lens_s2b_coordination.py (gemini-2.5-flash-lite per D-006 + docstring line 4 fix), lens_entity_extract.py (MAX_TOKENS 600 -> registry 1600).
Each: model string + MAX_TOKENS from registry (`wire(role)` + `fit_max_tokens`); the S2-D `qwen/` call dies here and S2-D returns to life on gpt-oss-120b. Log labels print the registry model string — no more hand-written names (kills drift specimens #1-#4).
Receipts per file: grep zero old literals, py_compile, one commit per file or per tight pair.

## CC-4 — S3 + remaining call sites
Files: lens_s3a_patterns.py (L61 hardcode), lens_s3_orchestrator.py (docstring table), lens_s3b (2.5-flash-lite), lens_framing_rubrics.py (provider table comments), lens_manager.py, lens_compendium.py, lens_regular_report.py, analyze_lens.py (UNREFERENCED scratch — recommend `git rm` with the other three scratch files, James rules; if kept, sweep it too), lens-compendium.yml (1 hit).
Receipts: repo-wide grep `llama-3\.3-70b|qwen` in code/ + .github/ returns ONLY SambaNova-context lines and historical comments explicitly marked as history.

## CC-5 — Probe pack (BEFORE certs, AFTER CC-0..CC-4 compile)
Build `probe_lens_models.py` (repo root, read-only against production data):
- For each Groq/Gemini/SambaNova role: 3 trials on the role's OWN key (LR-094), fixture = the role's real latest prompt (pull from the same DB reads the position uses; R-S80-2 — fixture must match the real call shape, including max_tokens via fit_max_tokens).
- Score: content non-empty (>=200 chars), JSON-parse where the role requires it, HTTP status, finish_reason, usage tokens, latency; content-fitness scan for refusal/sanitizing markers ("I can't", "I'm unable", "as an AI", empty-on-war-content) on khamenei/trump/xi material.
- Append one JSON line per trial to `probe_results.jsonl` (committed — permanent record).
- FIRST run the llama-3.3-70b-versatile baseline (same fixtures) — unrepeatable after Aug 16.
- SambaNova probe also records max accepted prompt size (context check, D-005).
Gate: any role failing content-fitness on gpt-oss-120b escalates to James — options there are SambaNova-as-primary for that role or Cerebras routing; do NOT ship a censored position.

## CC-6 — Hygiene commit (one purpose: truth-in-config)
- S2-F: fix the "[ENSEMBLE] Running primary: qwen-3-235b" banner + any config carrying that string to match the wire (gpt-oss-120b on Cerebras).
- Wire `GROQ_MANAGER_API_KEY` where the orchestrator/manager watchdog reads its key (D-011) — restores LR-094 isolation from MA.
- Remove the duplicated COHERE_API_KEY line in .env (local, no commit needed — James, one line).
- Scratch files ruling from James: `git rm` patch_add_ollama_provider.py, patch_article4_provider.py, patch_cerebras_model.py, smoke_test_ollama.py (+ analyze_lens.py if ruled dead) — LR-093 pattern.
- s3f_dump.txt: confirm dead -> rm (carried Part 6 item).

## CC-7 — Cert wave (James + next crons; before Aug 12)
Per touched position, next scheduled cron, read logs (gh run view … --log | grep):
- wire model string == registry string; zero 404/413/429-storm/empty; content present and sane; no refusal language.
- LR-080 SELECT-verify any ledger writes.
- Two consecutive green scheduled runs per Groq position = CERTIFIED. Record cert lines in the session brief.
Watch item during certs: Groq TPM is 8K now — if 429 pacing appears, slow staggers before touching budgets.

## Rollback rule
Every commit is single-purpose so `git revert <sha>` is always clean. If a wave certs red, revert that wave only — the registry (CC-0) never needs reverting; values change, shape stays.
