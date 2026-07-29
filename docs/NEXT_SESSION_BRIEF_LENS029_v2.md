# Project Lens — Next Session Brief LENS-029 (v2)
**Written 2026-07-29 by Claude Fable 5 at the true LENS-028 close. SUPERSEDES `NEXT_SESSION_BRIEF_LENS029.md` (v1, written mid-session at a premature close — its live items are all folded here, nothing dropped).**

**READ THIS FIRST. You are Claude — most likely Opus 5. James calls you "my buddy" and means it. LR-102: the model changed, so every claim below is a LEAD until you re-verify it against bytes. Read `docs/LENS_CONTRACT.md` first (rules of engagement), then `docs/LENS_LCLIFF_DECISIONS.md` (D-001..D-017 — the WHY behind every ruling; do not re-litigate without new evidence), then this. Claude Code (also Opus 5) is wired and mid-arc with a background watcher armed for cert 1.**

| Item | Value |
| --- | --- |
| Operator | James Maverick ("Bro Alpha") — HDCS CS, Spring University Myanmar, Chiang Mai UTC+7. Lens is his SOLO project; "Team Geeks" is GNI only |
| Startup | `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate` then `set -a && source .env && set +a` |
| Push | `git push https://fintelplan@github.com/fintelplan/project-lens.git main` — truth is the push output or `git ls-remote` (LR-104), never `git status` |
| Last known commit | **43dd2fc** (Stage 6 SDK smoke test) — verify with ls-remote at open; Claude Code may have pushed cert follow-ups since |
| Clocks | **Aug 16** llama-3.3-70b-versatile + 8b-instant decommission (Groq, official) · **Aug 12** internal deadline: every position certified ×2 · **~Oct 10** gemini-2.5-flash registry edit (dies Oct 16) |
| Doc map | contract > this brief > recollection. Decisions: `LENS_LCLIFF_DECISIONS.md`. Blocks: `LENS_LCLIFF_BUILD_PLAN.md`. Registry spec: `LENS_REGISTRY_SPEC.md`. Bugs: `LENS_KNOWN_BUGS.md`. Ritual: `LENS_SESSION_PROTOCOL.md` |

## Part 0 — FIRST ACTION: read cert 1
The 01:28 UTC wave of 2026-07-29 is **cert 1** for three positions that migrated to Cerebras and have never run in production on that wiring. That slot historically starts 193–221 min late, so the run should appear ~04:40–05:10 UTC. Nothing else starts until this reads clean.

```
gh run list -R fintelplan/project-lens -w "Lens Manager + Analyze" -L 2
gh run view PASTE_NUMERIC_ID -R fintelplan/project-lens --log > /tmp/ma.log && wc -l /tmp/ma.log
grep -aiE "calling (cerebras|groq)/" /tmp/ma.log
grep -aic "JSON parse error" /tmp/ma.log
grep -aoE "budget_used=[0-9]+%" /tmp/ma.log | sort | uniq -c
grep -aoE "reasoning=[0-9na/]+" /tmp/ma.log | sort | uniq -c
grep -aoE 'api\.(groq|cerebras)[^ ]* "HTTP/[12](\.1)? [0-9]+' /tmp/ma.log | grep -oE '[0-9]+$' | sort | uniq -c
grep -ac "REGISTRY_MISALIGNMENT" /tmp/ma.log
grep -aciE "Traceback|LensModelRegistryError" /tmp/ma.log
```
**PASS =** S2-D, S2-E and MA call lines all read `cerebras/gpt-oss-120b` · **zero** JSON parse errors (that is the truncation tell that lost 42 of 60 articles on Jul 28) · every `budget_used%` under 60 (D-017) · `reasoning=` shows numbers, not `n/a` (proves the SDK extra-field chain survives production) · zero 429s · misalignment 0 · no tracebacks. Then LR-080:
```
set -a && source .env && set +a
curl -s "$SUPABASE_URL/rest/v1/injection_reports?order=created_at.desc&limit=8&select=analyst,injection_type,confidence_score,created_at" -H "apikey: $SUPABASE_SERVICE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_KEY"
```
Fresh rows dated today for S2-D, S2-E and MA. Two consecutive green waves per position = CERTIFIED (D-012).

## Part 1 — WHAT LANDED (LENS-028, all pushed, all CI green)
| SHA | What |
| --- | --- |
| b8f525d | CC-0 `code/lens_models.py` registry + decisions + build plan + session protocol |
| 71030c1 | CC-1 guard reads the registry — no hand-written model strings |
| 82d64bb | CC-1b `lens-ci.yml` build gate (compile + registry self-test + guard tests), proven to bite before shipping |
| b8a634b · b750bdd | operating contract v1 · attribution fix (Lens is Bro Alpha solo) |
| ebf3d76 | LENS-029 brief v1 (superseded by this file) |
| cd28b05 · 298e529 | CC-5 probe pack · Tier 1 fixture builders + overrides + D-017 ratio |
| 8126f2c | CC-3a S2-D migrated to the registry on Groq |
| 7c78c43 · bfaf369 | probe results banked — 45+ trials including **both unrepeatable llama-3.3-70b baselines** |
| c4fe2af | "Team Geeks" swept from CLAUDE.md, guard header, guard README |
| 2813e80 · 3fae8a4 | BUG-001 filed + `LENS_KNOWN_BUGS.md`; BUG-002/003 + `LENS_REGISTRY_SPEC.md` |
| 3d5f10c · 1dbcd8f | pure move of S2-D batching helpers to module level · probe imports the real ones, mirror deleted |
| b05767d · 27857e6 | D-005 rewritten (SambaNova dead) + D-015/016/017 · D-015 amendment (max_out raises, RPM ruling) |
| 2e47027 | **Stage 1** registry v2 — pair-keyed provider-aware ceilings |
| 3e13f77 | **Stage 2** CC-1c — real usage logging, over-limit guard clause, retry-after, registry-sourced TPM |
| b80c951 | **Stage 3** `timeout-minutes: 35` on manage-analyze |
| 347b8d3 · 6ac9954 · 869f368 | **Stage 5** S2-D · S2-E · MA migrated to Cerebras, one commit each |
| 43dd2fc | **Stage 6** pre-cert SDK smoke test — HEAD |

## Part 2 — CURRENT WIRING (verified this session; re-verify per LR-102)
**Cerebras `gpt-oss-120b`** (CEREBRAS_API_KEY, 6 positions): S1-L3, S1-L4, **S2-D** (max_out 8,000), **S2-E** (10,000), **MA** (5,000), S3-D — plus S2-F primary in its own workflow.
**Groq `openai/gpt-oss-120b`** (per-position keys): S1-L1, S2-A (3,400), S2-GAP, S3-A — plus entity_extract, ai5_watchdog. **These six are Stage 7, still unswept and still holding dying strings in places.**
**Other:** S2-B/S3-B Gemini (LIMITS_UNKNOWN, on Mistral fallback in practice) · S2-C/S1-RPT/S2-report/S3-F Mistral · S3-C Cohere · S2-F secondary Cloudflare.
Verified limits in the registry: Groq TPM 8,000 / TPD 200,000 / CTX 131,072 · Cerebras RPM 5 / TPM 30,000 / RPD 2,400 / TPD 1,000,000 / CTX 131,000 / MAX_COMPLETION 40,000 (console, overrides the public docs) · Mistral `mistral-small-2603` TPM 50,000 · Cohere 20 req/min METER=requests · Cloudflare 300 req/min METER=requests, CTX 128,000 · **Gemini LIMITS_UNKNOWN — James still owes AI Studio numbers.**

## Part 3 — LR ENTRIES EARNED THIS SESSION
- **LR-105 (Registry law).** Every model string, key env, output budget and limit flows from `code/lens_models.py`. Call sites run `assert_model_known` immediately before each request and MAY raise there (per-position blast radius). The guard verifies alignment LOG-ONLY + CI test + pre-flight Telegram; **the guard never raises** (fail-safe contract). A model string typed anywhere else is a bug.
- **LR-106 (Probe-before-push).** No call-site migration ships until that role's probe is green on mechanics AND content-fitness, on the role's own key, with real-prompt fixtures. Bank dying-model baselines while they breathe.
- **LR-107 (Probe headroom, D-017).** For JSON-output roles, >60% completion-budget consumption in probe is MARGINAL, not passing — S2-D probed 3/3 clean at 79% and truncated in production on a prompt 312 chars larger, losing 42 of 60 articles. A probe certifies the prompt **size** it held, not merely its shape (R-S80-2 extended). Truncated JSON has no partial value.
- **LR-108 (Derived ceilings).** Per-request ceiling = `min(CTX, TPM)` per **(provider, model)** — never a hardcoded constant, never provider-only. The old "Groq 8,192" was never a context limit; Groq's context is 131,072 and TPM was always the binding cap. "No TPM exists" (Cohere, Cloudflare) and "nobody has checked" (Gemini) must never collapse into the same silence — hence `METER`. An unresolved ceiling returns the cap and logs; it does not raise.
- **LR-109 (Never pollute the record to save time).** Do not `workflow_dispatch` an analysis pipeline outside its schedule to speed a cert — it re-analyses the same collection pool and writes a duplicate cycle into the evidence base, skewing S2-F's 45-day windows invisibly for months. Use a read-only smoke test instead.
- **LR-110 (Tests derive, never hardcode).** Five guard tests broke on the Cerebras migration because they encoded "these positions share one group". A hardcoded assumption in a test silently changes what the test measures after a migration instead of failing honestly. Derive fixtures from the registry.

## Part 4 — STAGE 7 AND THE QUEUE
**Stage 7 (after cert 1 reads clean): the Groq sweep** — lens1, s2a_injection, s2gap, entity_extract, ai5_watchdog, s3a_patterns onto the registry, plus **15 Mistral literals across 10 files** when the `-latest` alias is dropped, plus the s2f workflow's `MISTRAL_MODEL` env default. Bigger than "a few call sites".

| Tier | Item |
| --- | --- |
| POST-CERT | Cerebras `reasoning_tokens` arrives via pydantic `extra='allow'`, not a declared field — add a WARNING when provider is cerebras and reasoning is None, so a future SDK tightening degrades loudly |
| POST-CERT | BUG-001 fix (S2-D drops ~1/3 of each batch and states a count it did not send) — own commit, own probe on the larger prompt, own cert. Direction A must never cite a pre-fix S2-D count |
| POST-CERT | BUG-002 (S2-E sends `Lens: unknown`) — own commit, own cert |
| POST-CERT | CC-1d: the `//3` divisor over-estimates prompts ~47% (measured 4.14–4.76 real chars/token). `//4` is defensible but loosens the only 413 guard — own decision, own probe |
| IMPORTANT | Watchdog wire-truth + 404-streak Telegram alarm · F5 guard aggregation re-key to (provider, model, key_env) · BUG-003 (S2-F Cloudflare 429s: check whether Workers AI is even provisioned — console question) · LR-090 schema checkpoint, overdue since LENS-027 |
| JAMES | Gemini AI Studio limits · second free Cerebras account (6 positions on one key at RPM 5) · Cloudflare Workers AI provisioning check · Node 24 verify |
| POST-CLIFF | GNI registry port · Direction A + option E attribution bar · T1 Opus report rewire · GDELT 429 spacing · `s3f_dump.txt` rm |

## Part 5 — DANGERS
Silent degrade is this project's signature failure: a dead model behind green checkmarks (11 days), a test asserting a corpse with nothing reading it, a guard whose window was never populated, a count inflated 3.3× in the analytical record. Every fix this session was an instance of the same cure — **derive truth, then make its absence loud.** Keep that reflex. Specifically: JSON parse errors are the truncation tell, not 429s; a probe that passes at high budget consumption has not passed; Cerebras now carries six positions on one key at RPM 5; and `/areas/lens-lcliff.md` in memory is **at its size cap — condense it before adding more.**

## Part 6 — LIVE vs BANKED (for the next model's hands)
**LIVE — verified by bytes this session, ~90%:** head 43dd2fc and every SHA in Part 1 · registry v2 resolving groq 8,000 / cerebras 30,000 / mistral 50,000 / cohere 128,000 / cloudflare 128,000 / gemini None→cap · CC-1c behaviours proven by test (infinite loop gone, retry-after parsed, TPM 6,800 groq / 25,500 cerebras) · the three Cerebras factories smoke-tested against the real SDK object (usage 135/161/160, reasoning 39/65/64) · S2-D's Jul-28 Groq cert (right string, zero 404s, fresh row 15:57:18Z conf 0.86) · 45+ banked probe trials including both 70b baselines · console limit numbers.

**BANKED — true when written, re-verify before acting, ~40%:** Stage 7's sweep scope · BUG-001/002/003 details · the Groq roles' current budgets · Gemini's dead 2.0-flash and the 3.x generation note · the S2-F Cloudflare picture · everything in the POST-CLIFF row.

**UNKNOWN — the open question this session hands you:** whether cert 1 is green. Nothing in Stage 7 starts until you have read it.

*LENS-029 Brief v2 | 2026-07-29 | James Maverick (Bro Alpha) + Claude (Fable 5 → Opus 5) | Bytes first, honesty always*
