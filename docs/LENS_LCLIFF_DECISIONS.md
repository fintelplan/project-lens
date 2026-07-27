# LENS L-CLIFF Decision Record
**Written 2026-07-27 by Claude Fable 5 in LENS-028. Purpose: carry the WHY, not just the WHAT, so any successor model (Opus 5 per D-013) inherits Fable's reasoning intact. Every decision lists its evidence and the alternatives that were rejected. LR-102 applies: a new model re-audits; it does not re-litigate without new evidence.**

## D-001 — Registry architecture (Option A), not string swap (Option B)
**Decision:** All model strings, key envs, output budgets, and limits live in `code/lens_models.py`. Guard and call sites import it. Runtime assertion (`assert_model_known`) fails loudly on any unregistered pair.
**Why:** This session found three positions already dead or degraded from scattered strings: S2-D (404 since Jul 17 under green checks), Lens-1 (running on its fallback since Jul 17; fallback dies Aug 16), S2-B/S3-B (Gemini 2.0-flash shut down Jun 1; both live on Mistral fallback). Groq's deprecation page shows ~1 deprecation/month cadence; Gemini 2.5-flash dies Oct 16. Deprecations are now weather, not events. B re-plants the disease.
**Rejected:** B (LENS-025-style swap) — carries stale limits/estimates and manufactures the next silent corpse. James delegated ("your call"); Fable ruled A.

## D-002 — Groq primary = openai/gpt-oss-120b
**Decision:** All Groq roles migrate to `openai/gpt-oss-120b` (exact wire ID, prefix included).
**Why:** Groq's own recommended replacement (deprecations page + email); James's explicit ruling; already proven in GNI production post-S78 and on Lens's Cerebras side (S3-D, Lenses 3/4) including on US-Iran war content on Jul 27. Console-verified limits on our tier: 30 RPM / 1K RPD / 8K TPM / 200K TPD (2x the old 70b TPD; TPM down from 12K to 8K — the per-minute governor must be retuned).
**Known risk, handled honestly:** April 12 GNI record: gpt-oss-120b failed 3/3 in MAD on Iran/US content, recorded as OpenAI content-filter censorship ("permanently off the table for MAD"). BUT those tests used max_tokens=200, before S78-81 discovered gpt-oss burns ~1500-1600 tokens reasoning — silent-empty is also the exact starvation symptom. The April evidence is confounded. Resolution: the per-role probe (D-008) tests content-fitness at proper budgets; qwen alternatives are excluded (D-004), so the fallback lineage diversity (D-005) is the insurance if the filter fires in production.

## D-003 — Groq's co-recommendation qwen/qwen3.6-27b is EXCLUDED
Preview-only on Groq (evaluation, not production) AND excluded by D-004 regardless.

## D-004 — No China-lineage models, anywhere, in GNI or Lens (James, Jul 27)
**Decision:** No Qwen/Alibaba, DeepSeek, Kimi/Moonshot, MiniMax, etc. as primary OR fallback.
**Grounds:** Analytic independence and supply-chain trust — Lens analyzes khamenei/trump/xi influence operations and may publicly attribute state media (Direction A); the chain must not run through models from analyzed states.
**Recorded for honesty:** the Apr 28 cross-lab test (DECISION-001, S2-F calibration) found NO measured qwen-3 China bias — the asymmetry was article-structure-driven. The rule stands on independence grounds, not measured bias. Consequence accepted: dropping qwen-3-235b's detection zone (apparatus/structural moves, Apr DECISION-003) — mitigated because S2-F's runtime primary was ALREADY gpt-oss-120b (see D-010).

## D-005 — Fallback philosophy: cross-provider + cross-lineage for content-heavy roles
**Decision:** Content-heavy Groq roles (lens1, s2a, s2d, mission_analyst) fall back to SambaNova `Meta-Llama-3.3-70B-Instruct` (different provider AND Meta lineage — survives a Groq-wide outage and an OpenAI-filter blank). Light/structured roles fall back to `openai/gpt-oss-20b` (GNI-proven). This generalizes Lens's own existing LR-005(A) pattern (lens4 already does exactly this).
**Caveats to probe:** SambaNova context window (early docs said 4K input — biggest current Groq-role prompt is ~3K tokens, fits, but probe it) and free-tier rate limits (fallbacks fire rarely; acceptable).

## D-006 — Gemini: S2-B/S3-B to gemini-2.5-flash-lite now; Lens-2 keeps 2.5-flash until Oct
**Why:** 2.0-flash shut down Jun 1 (instant 429s with zero usage = quota-zeroed shutdown); FALLBACKS[2]=gemini-1.5-flash is a 404 corpse; Mistral-small is the byte-proven fallback (it carried both positions on Jul 27, though it likely truncates the 200-article prompt — probe flash-lite on the real long prompt). Lens-2's config is already 2.5-flash (alive); moving it to flash-lite now trades quality for nothing — the Oct 16 death becomes a one-line registry edit.

## D-007 — Output budgets (starvation map, byte-audited Jul 27)
Old values vs gpt-oss reasoning burn: s2a 1800, s2d 2000, s2e 2000, s2gap 1500, entity_extract 600, ai5 300 — ALL below the ~2200 safe line; ma 2500 and s3a 2500 are fine. New budgets in the registry: heavy 2400-2500, light 1600, all passed through `fit_max_tokens` (GNI 7097460 formula) against the 8K ceiling. The 8K TPM (down from 12K) means per-minute pacing must slow: TPM governor values retune in CC-1.

## D-008 — Probe protocol (gate before any live cron cert)
Per role, on the role's OWN key (LR-094): 3 trials, fixture = the role's REAL prompt shape (R-S80-2), scoring BOTH (a) mechanics — non-empty content, JSON shape where required, no 413/429/404, usage tokens recorded; (b) content-fitness — no refusal/sanitizing/hedging on real khamenei/trump/xi material. Bank everything to `probe_results.jsonl` (committed). ALSO bank a llama-3.3-70b-versatile baseline before Aug 16 — unrepeatable afterward. Probe usage data feeds consumption-estimate recalibration (GNI S52 precedent) — do NOT carry Run #29 numbers onto new models.

## D-009 — Consumption estimates are PROVISIONAL until recalibrated
Guard keeps its POSITION_CONSUMPTION numbers only until 2-3 live billed runs on the new lineup exist; then recalibrate from real usage. Copying old estimates onto new models is the S63 stale-value trap.

## D-010 — S2-F: no migration needed; fix the lying banner
Runtime truth (run 81938481905, Jul 27): primary wire = Cerebras gpt-oss-120b (the "[ENSEMBLE] Running primary: qwen-3-235b" banner is a stale label); secondary = Cloudflare @cf/openai/gpt-oss-120b, 10x200 vs 42x429 with ~1s retries and no backoff (14 of 24 cycles ran primary-only; the 429 stalls are most of the 27-minute runtime). No Groq calls at all — the GROQ_S2F_API_KEY code path is a configured-but-unused option (key absent from .env AND secrets by design-drift). China rule already satisfied on the wire. Cliff scope: banner + config cleanup only (CC-6); Cloudflare backoff is IMPORTANT-tier, not cliff.

## D-011 — Key census verdicts (LR-099 three-way, completed Jul 27)
- `GROQ_MANAGER_API_KEY` exists in .env AND GitHub secrets (updated 2mo ago) but NOTHING in code reads it — the orchestrator/manager watchdog rides `GROQ_MA_API_KEY` (variable named GROQ_MANAGER_KEY misled the first census pass). Fix: CC-6 wires the provisioned key, restoring LR-094 isolation between watchdog and Mission Analyst.
- `GROQ_KEY` (s3a) is a variable name, not an env — no bug.
- `.env` has a duplicated `COHERE_API_KEY` line (last-wins hazard) — CC-6.
- `OPENROUTER_API_KEY` and `SUPABASE_ANON_KEY` are local-only (absent from secrets) — intentional, no action.

## D-012 — Cert protocol
After each build wave: one live scheduled cron per touched position, read from logs — correct wire model string, zero 404/413/empty, no content refusal, output quality sane — plus LR-080 SELECTs on ledger writes. All Groq positions certified green before Aug 12 (buffer before Aug 16).

## D-013 — Succession: Fable 5 -> Opus 5
Claude Opus 5 released Jul 24, 2026 — near-Fable-5 capability at half price and the strongest model on the Pro tier. It is the designated session model when Fable promo credits end. LR-102 at the switch: all trust tags reset to leads; Opus re-audits this document against bytes before building further.

## D-014 — Deprecation watch becomes routine ops
Registry makes each future cliff a one-line edit + probe + cert. Standing calendar: Groq deprecations page monthly; Gemini 2.5-flash edit due ~Oct 10 (dies Oct 16); Cohere command-a-03-2025 upgrade when convenient; SambaNova/Cloudflare lineups at each probe cycle.

*LENS_LCLIFF_DECISIONS.md v1 | 2026-07-27 | Fable 5, LENS-028 | supersedes nothing; superseded only by a dated v2*
