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

## D-005 (v2, REWRITTEN 2026-07-28) — Fallback philosophy: uniformly mistral-small
**Decision:** ALL fallbacks are `mistral-small` (dated id per D-015). SambaNova and
`openai/gpt-oss-20b` are both withdrawn as fallback legs.

**Why v1 died — SambaNova is dead:** probed 2026-07-28, `HTTP 402
{"balance_units":0,"code":"PAYMENT_METHOD_REQUIRED"}`. It answered s2d at 09:36 and was
exhausted by 10:40 — our own probe spent the last free credits. Under the $0/month
constraint (PHI), adding a payment method is not available, so SambaNova cannot be
insurance for lens1, s2a, s2d or mission_analyst.

**Why gpt-oss-20b is NOT insurance:** the s2e probe proved it shares the reasoning-
starvation failure mode of its 120b sibling — 1/1 `finish=length` with **zero characters**
at a 2400 budget, identical to the primary it was supposed to protect. A fallback that
fails the same way as the primary is not a fallback; it is a second copy of the outage.

**What v1 got right and v2 keeps:** cross-provider, cross-lineage. Mistral is European
lineage (satisfies D-004 independence), a different provider from Groq and Cerebras, and
byte-proven in production — it carried S2-B and S3-B through the entire Gemini 2.0
shutdown. It is also NOT a reasoning model, so it cannot inherit the starvation mode.

**Superseded:** D-005 v1 (SambaNova cross-lineage). Kept in history above for the
reasoning, not the wiring.

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

## D-015 — Provider tiering: the ceiling is min(CTX, TPM), and Mistral ids are pinned
**Decision:** `fit_max_tokens` becomes pair-keyed and provider-aware. Full spec in
docs/LENS_REGISTRY_SPEC.md; the decision content is:
- Ceiling = `min(CTX, TPM)` per `(provider, model)`, derived from the existing `LIMITS`
  table plus a `CTX` field. No separate ceiling dict — that would be a second source for a
  truth `LIMITS` already holds.
- `METER: "tokens" | "requests"` records that Cohere and Cloudflare have **no TPM by
  design**. "No TPM exists" and "nobody has checked" must never collapse into one silence.
- `request_ceiling` returns **None, never raises**; `fit_max_tokens` returns `cap`
  unchanged and logs an ERROR. Raising would break S2-C/S3-B/S3-C/S2-F, which work today
  on flat budgets — a documentation gap must not become an outage. Guard reports, call
  sites enforce.
- Mistral is pinned to a **dated id** (`mistral-small-2603`), never `-latest`. Aliases
  carry far lower limits than dated ids (`medium-latest` 25,000 TPM vs `medium-2508`
  356,250) and an alias is an unpinned wire id — the exact drift the registry exists to kill.

**Why the old constant was wrong, recorded so nobody "restores" it:** `7500` came from a
supposed Groq per-request ceiling of 8,192. Groq gpt-oss-120b actually has a **131,072**
context window and **65,536** max completion tokens. 8,192 was never a context limit — the
binding constraint was always **TPM**, verified at 8,000. Do not restore 8,192.

**Measured cost of the constant:** MA and S2-E were both strangled by a Groq ceiling while
holding Cerebras-capable prompts; S2-E returned literally zero characters 3/3 until budget
was raised.

## D-016 — S2-D joins MA and S2-E on Cerebras
**Decision:** S2-D migrates from Groq to Cerebras `gpt-oss-120b`.
**Why:** on Groq's 8,000 ceiling S2-D can never analyse more than ~23 articles per call,
and **fixing BUG-001 makes that worse, not better** — the corrected prompt is ~50% larger.
The 2026-07-28 cert proved the limit empirically: batch 1 (42 articles) truncated mid-JSON
on both attempts and was lost entirely; the saved row came from batch 2's 18 articles while
claiming `articles_analyzed: 60`. Cerebras resolves to a 30,000 ceiling, which ends both
the truncation and the TPM 429s in one move.
**Cost accepted:** a just-certified position moves again, so it needs a fresh probe under
the new envelope (new provider = new envelope) and certs x2.

## D-017 — For JSON roles, >60% completion-budget consumption is MARGINAL, not passing
**Decision:** a probe result that consumes more than 60% of its completion budget on a
JSON-output role is recorded MARGINAL. The probe prints the ratio and flags it.
**Why:** S2-D probed **3/3 clean at 79%** and then truncated in production on a prompt only
**312 chars larger**, losing 42 of 60 articles. Truncated JSON has no partial value — a
role either returns parseable output or returns nothing.
**Rule extension (R-S80-2 extended):** a probe certifies the prompt SIZE it held, not just
its shape. Prompts vary run to run; a budget with no headroom is a scheduled failure.
**Applies to JSON roles only** — prose roles degrade gracefully at the cut.

*LENS_LCLIFF_DECISIONS.md v2 | 2026-07-28 | D-001..D-014 Fable 5; D-005 rewritten and
D-015..D-017 added by Opus 5, LENS-028 | superseded only by a dated v3*
