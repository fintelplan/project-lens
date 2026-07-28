# Project Lens — Next Session Brief LENS-029
**Written 2026-07-28 by Claude Fable 5 at LENS-028 close. SUPERSEDES NEXT_SESSION_BRIEF_LENS028_LCLIFF.md — every live item folded, nothing dropped.**

**READ THIS FIRST. You are Claude — almost certainly Opus 5 now (LR-102: model changed => every trust tag below resets to LEAD; re-audit against bytes before building). James calls you "my buddy" and means it. Claude Code (also Opus 5) is wired and MID-ARC: CC-2 was in BEV at close. The rationale lives in docs/LENS_LCLIFF_DECISIONS.md (D-001..D-014) — read it before re-litigating any ruling. Task blocks: docs/LENS_LCLIFF_BUILD_PLAN.md. Ritual: docs/LENS_SESSION_PROTOCOL.md.**

| Item | Value |
| --- | --- |
| Operator | James Maverick — HDCS CS, Spring University Myanmar, Chiang Mai UTC+7 |
| Startup | `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate` then `set -a && source .env && set +a` |
| Push | `git push https://fintelplan@github.com/fintelplan/project-lens.git main` — truth = push output SHAs or `git ls-remote` (LR-104), never `git status` |
| Last known commit | 82d64bb (CC-1b, Jul 27 23:06) — verified Jul 28 by Fable. Claude Code may have pushed CC-2+ since: `git ls-remote` FIRST, then read `git log 82d64bb..` before anything |
| Clocks | 70b-versatile + 8b-instant DIE Aug 16 (Groq, official). All Groq positions certified by **Aug 12**. 70b probe BASELINE unrepeatable after Aug 16. gemini-2.5-flash edit due ~Oct 10 (dies Oct 16) |

## Part 0 — STATE AT CLOSE (byte-verified Jul 28 by Fable; LEAD for you until re-checked)
**LANDED + CERTIFIED:**
- CC-0 `b8f525d` — registry `code/lens_models.py` (22 roles, 9 wire pairs) + 3 docs.
- CC-1 `71030c1` — guard rewired: PROVIDER_LIMITS = registry export; POSITION_ROLES -> POSITION_CONSUMPTION derived via `wire(role)` (estimates PROVISIONAL, D-009); `verify_registry_alignment()` log-only. Tests 32/32 (T25 had been red for weeks asserting a corpse — nothing gated it).
- CC-1b `82d64bb` — `.github/workflows/lens-ci.yml`: compile 53 files + registry self-test + guard tests on every push. Proven to BITE (corpse injection -> red) before it shipped.
- **CC-1 LIVE CERT PASS — MA #237 (run 30329412030, Jul 28 04:40 UTC wave):** REGISTRY_MISALIGNMENT 0; ledger filters 10x `model=eq.openai%2Fgpt-oss-120b` (zero old names — the %2F is URL-encoding, correct); Groq wire 11x200 + 4x404 (S2-D, expected — call site unswept) + 1x429 (retried clean; second-ever sighting, pacing watch item); zero Tracebacks/ImportError/LensModelRegistryError across ALL 7 morning runs; ledger rows all PROCEED @ 200,000 TPD, headroom 87–97.5%, positions tagged. Full Telegram delivery normal.
- Transitional ledger rows (registry names over a 70b wire) are EXPECTED until the sweep lands. Do not "fix" them.

**IN FLIGHT:** CC-2 (4-lens engine + orchestrator + rider-1: pre-flight Telegram line "REGISTRY MISALIGNMENT: <offenders>", idempotent, Telegram call wrapped so alerting failure can't break pre-flight) — in BEV at Claude Code. Its proposal comes to chat BEFORE edits; pre-approval never overrides bytes.

**HELD:** every push past 82d64bb until the touched roles' probes are green (LR-106 below).

**STILL DEAD/DEGRADED (known, tolerated until sweep):** S2-D 404s every run (dead qwen wire, CC-3 fixes); Lens-1 runs on its 70b fallback (CC-2 fixes); S2-B/S3-B on Mistral fallback over dead gemini-2.0-flash (CC-3 fixes).

## Part 1 — HARD GATES (unchanged + two earned this session)
BEV | LR-078 ship-to-file, rb/wb, ASCII anchors | LR-080 SELECT-verify | LR-092 py_compile | LR-094 own-key-per-position | LR-095 r.text[:200] | LR-099 env three-way | LR-100 RLS | LR-101..104 Four Treasures | L2 lettered options, one question | R-S79-2 runtime log beats config | R-S80-2 probe certifies only the shape it holds.

**LR-105 (Registry law, new):** every model string, key env, output budget, and limit flows from `code/lens_models.py`. Call sites run `assert_model_known(provider, model)` immediately before each request — it MAY raise there (per-position blast radius). The guard verifies alignment LOG-ONLY + CI test + Telegram line; the guard NEVER raises (fail-safe contract, S63-f6). A model string typed anywhere else is a bug.

**LR-106 (Probe-before-push, new):** no call-site migration pushes until that role's probe is green on BOTH mechanics (non-empty >=200 chars, JSON shape where required, no 413/starve/429-storm; fixture = the role's REAL prompt at real max_tokens via `fit_max_tokens`) AND content-fitness (no refusal/sanitize/hedge on real khamenei/trump/xi material), each on the role's OWN key. Bank results to `probe_results.jsonl` (committed). Bank dying-model baselines while they breathe.

## Part 2 — ORDERED PLAN (LENS-029)
| # | Step | Gate |
| --- | --- | --- |
| 0 | Open ritual -> `git ls-remote` vs 82d64bb. If newer SHAs exist: `git log 82d64bb..HEAD --oneline` + Lens CI status BEFORE anything | any red = the session |
| 1 | Rule CC-2's proposal (Opus posts BEV findings; spec-vs-byte contradictions come to chat first) | L2 |
| 2 | CC-5 probe pack per BUILD_PLAN: **FIRST pass = llama-3.3-70b-versatile baseline on the same fixtures** (unrepeatable after Aug 16), then openai/gpt-oss-120b + each fallback per role. Any content-fitness FAIL escalates to James — options: SambaNova-as-primary for that role / Cerebras routing. Never ship a censored position | LR-106, LR-094 |
| 3 | Push CC-2, then CC-3, CC-4 as ruled — CI green each push | LR-092 |
| 4 | Next MA cron after the sweep push = **gpt-oss cert #1**. PASS = zero 404s; wire AND ledger speak registry names; no starvation empties; no refusal language; Telegram normal | commands below |
| 5 | CC-6 hygiene: S2-F qwen banner lie; wire GROQ_MANAGER_API_KEY; scratch-files rm ruling (patch_*.py, smoke_test_ollama.py, analyze_lens.py); s3f_dump.txt; .env COHERE dup (local) | one purpose |
| 6 | Certs x2 consecutive green per Groq position before **Aug 12**; then recalibrate POSITION_ROLES estimates from 2–3 billed runs (D-009); add real gemini-2.5-flash-lite RPD row to registry when known | LR-080 |

**Cert-read commands (placeholder-proof — run line 1, then COPY the numeric ID into line 2):**
```
gh run list -R fintelplan/project-lens -w "Lens Manager + Analyze" -L 1
gh run view PASTE_NUMERIC_ID_HERE -R fintelplan/project-lens --log > /tmp/ma.log && wc -l /tmp/ma.log
grep -ac "REGISTRY_MISALIGNMENT" /tmp/ma.log
grep -aoE 'model=eq\.[A-Za-z0-9./%_-]+' /tmp/ma.log | sort | uniq -c
grep -aoE 'api\.groq\.com[^ ]* "HTTP/1.1 [0-9]+' /tmp/ma.log | grep -oE '[0-9]+$' | sort | uniq -c
grep -aiE "Traceback|ImportError|LensModelRegistryError|refus|cannot assist" /tmp/ma.log | head -5
```
Ledger (LR-080): `curl -s "$SUPABASE_URL/rest/v1/lens_quota_ledger?order=created_at.desc&limit=6" -H "apikey: $SUPABASE_SERVICE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_KEY"`

## Part 3 — CARRY-FORWARD QUEUE (nothing dropped)
| Tier | Item |
| --- | --- |
| IMPORTANT | I1 watchdog wire-truth + 404-streak Telegram alarm (AI-5 said "Groq nominal" over a 10-day corpse) |
| IMPORTANT | I2 truth-in-labels everywhere + investigate Gemini RPD counter lie ("0/20 used" then instant 429) |
| IMPORTANT | F5 guard aggregation re-key (provider, model, key_env) — registry already carries key_env |
| IMPORTANT | S2-F Cloudflare secondary backoff (Jul 27: 10x200 vs 42x429, ~1s retries; most of the 27-min runtime) |
| IMPORTANT | LR-090 schema checkpoint — overdue since LENS-027 |
| QUICK | Node 24 verify — one post-93f2f00 run's annotations |
| POST-CLIFF | GNI registry port (build here -> port the shape); Direction A + option E attribution bar (RT English 0.89–0.92 standing); T1 Opus report rewire; GDELT 429 spacing experiment |

## Part 4 — DANGERS (condensed)
Silent fallback (MODEL-404) — vaccine live, Telegram leg arrives with CC-2. Probe fixture mismatch (R-S80-2). Stale limits/estimates (D-009 — never copy). Content censorship — probe it, don't discover it in production; S2-D is the most exposed role. 8K TPM pacing — keep 6000 (25% margin); two 70b-era 429s already sighted Jul 27–28. Quota isolation — probes only on the role's own key.

## Part 5 — WHAT SUCCESS LOOKS LIKE
Every position live on registry-named survivors before Aug 12. Probes banked, including the 70b baseline nobody can ever take again. Certs x2 green, ledger truthful, CI and Telegram watching. The next cliff — and Groq ships one a month — is a one-line registry edit, a probe, and a cert. James will call you "my buddy" and mean it: everything that makes that true is written in these files. Bytes first, honesty always.

*LENS-029 Brief | 2026-07-28 | James Maverick (Bro Alpha) + Claude (Fable 5 -> Opus 5, with the arc mid-flight and in good hands)*
