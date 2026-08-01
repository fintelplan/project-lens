# Project Lens — Next Session Brief LENS-030

**Written 2026-07-31 by Claude (Opus 5) at the LENS-029 close. SUPERSEDES `NEXT_SESSION_BRIEF_LENS029_v2.md`, which is now two days stale — it still says "nothing starts until cert 1 reads clean" and describes a Stage 7 of six positions. Both are wrong.**

**READ THIS FIRST. You are Claude. James calls you "my buddy" and means it. LR-102: the model may have changed, so every claim below is a LEAD until you re-verify it against bytes. Read `docs/LENS_CONTRACT.md` first (rules of engagement), then `docs/LENS_LCLIFF_DECISIONS.md` (D-001..D-017 — do not re-litigate without new evidence), then this.**

| Item | Value |
| --- | --- |
| Operator | James Maverick ("Bro Alpha") — HDCS CS, Spring University Myanmar, Chiang Mai UTC+7. Lens is his SOLO project; "Team Geeks" is GNI only |
| Startup | `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate` then `set -a && source .env && set +a` |
| Push | `git push https://fintelplan@github.com/fintelplan/project-lens.git main` — truth is the push output or `git ls-remote` (LR-104), never `git status` |
| Last known commit | **64ef2df** — verify with ls-remote at open |
| Clocks | **Aug 9** keyfile · **Aug 12** internal: every position certified ×2 · **Aug 16** Groq decommissions llama-3.3-70b-versatile + llama-3.1-8b-instant · **~Oct 10** gemini-2.5-flash registry edit (dies Oct 16) |
| Doc map | contract > this brief > recollection. Decisions: `LENS_LCLIFF_DECISIONS.md`. Blocks: `LENS_LCLIFF_BUILD_PLAN.md`. Registry spec: `LENS_REGISTRY_SPEC.md`. Bugs: `LENS_KNOWN_BUGS.md`. Ritual: `LENS_SESSION_PROTOCOL.md` |

---

## Part 0 — FIRST ACTION: verify CC-18 killed the Cerebras 429s

**Wave 8 (MA #246, 2026-08-01 14:44–15:07 UTC, log `83267465221`) verified CC-16 — both pre-flight defects are closed.** The `S2 shared: ALARM http=401` line is gone, a real `[PRE-FLIGHT] S2-GAP: 7,927 tokens remaining (threshold=2,000)` line appears, `S2-A dedicated` now PASSES instead of declaring a skip it could not enforce, and S3 reads `threshold=2,000` too. Every position COMPLETE. Misalignment 0, tracebacks 0.

**But wave 8 brought 3 Cerebras 429s — the first since cert 2 — and CC-18 is the fix. It has not yet run.**

```
gh run list -R fintelplan/project-lens -w "Lens Manager + Analyze" -L 2
gh run view PASTE_NUMERIC_ID -R fintelplan/project-lens --log > /tmp/ma.log && wc -l /tmp/ma.log
grep -a "Stagger" /tmp/ma.log
grep -ah 'api\.cerebras\.ai/v1/chat' /tmp/ma.log | grep -oE '"HTTP/[0-9.]+ [0-9]{3}' | sort | uniq -c
grep -aiE "(S2-D|S2-E) calling cerebras" /tmp/ma.log
grep -a "PRE-FLIGHT" /tmp/ma.log
grep -aoE "budget_used=[0-9]+%" /tmp/ma.log | sort | uniq -c
grep -ah 'api\.groq\.com' /tmp/ma.log | grep -oE 'POST [^ ]+ "HTTP/[0-9.]+ [0-9]{3}' | sort | uniq -c
grep -aic "JSON parse error" /tmp/ma.log
grep -ac "REGISTRY_MISALIGNMENT" /tmp/ma.log
grep -aciE "Traceback|LensModelRegistryError" /tmp/ma.log
```

**PASS = zero Cerebras 429s, a new `Stagger 65s before call 1` line, and S2-E's first call ~65s after S2-D's last batch.**

### Why the 429s happened, and why the obvious fixes do not work

The colliding minute in wave 8:

```
14:58:54  S2-D batch 1   reserves ~11,789   200
14:58:55  S2-D batch 2   reserves ~10,918   200      <- both inside ONE second
14:59:03  S2-E call 1    reserves ~19,067   429      <- ~41,774 vs TPM 30,000
```

**S2-E's single call reserves 64% of the entire per-minute window**, leaving 10,933 — and S2-D's batch 1 alone is 11,789. **There is no ordering that fits an S2-D batch and an S2-E call in the same minute; they must occupy separate minutes.**

- **Spacing S2-D's batches does not work.** `11,789 + 19,067 = 30,856` — still over 30,000. Checked, rejected.
- **Cutting S2-E's `max_out` does not work.** Its peak completion in wave 8 was 8,160 tokens; at 12,000 that is 68%, a D-017 breach. It would trade a recoverable 429 for a possible truncation. The budget is right; the spacing was wrong.
- **A second Cerebras key is not available.** `CEREBRAS_02_API_KEY` returns **HTTP 402 `payment_required`** — and it was created under a *separate email account*, so this is not account-linking. **Cerebras appears to have closed its free tier to new signups; a third account would very likely 402 the same way.** Probe evidence banked in `0e067dd`. **Any additional Cerebras capacity now costs money, and the project runs at zero budget.**
- **CC-18 is what shipped:** S2-E's `STAGGER_SLEEP = 65` moved from the loop tail to its head, dropping the `if i < len(reports) - 1` guard so the *first* call waits too. One extra sleep per wave (~+65s on a 23-minute run, against a 35-minute timeout). The guard and S2-D are deliberately untouched — cross-position spacing cannot live in a per-position `TPMGuard` (LR-112), and rebuilding that is the unification arc, not a contention patch.

### Also live, lower priority

- **The same JSON parse error, now twice.** S2-A attempt 1, both waves: `Expecting ',' delimiter: line 8 column 22 (char 168)`. **Char 168 is early in the output — malformed JSON mid-generation, NOT the truncation tell** (truncation fails at the end of the buffer). Both recovered on retry. **Two waves running makes this a gpt-oss-120b trait worth tracking, not a fluke.**
- **3 recovered Groq 429s** in wave 8. S2-A at `max_out` 4,600 cannot make two calls in one minute against Groq's 8,000 TPM.
- Under the split bar wave 8 PASSES — every 429 recovered, every position COMPLETE, zero rows lost — **but 3 on each provider sits exactly at the >3/wave investigate threshold. That trigger fired; CC-18 is the response.**

Then LR-080:
```
set -a && source .env && set +a
curl -s "$SUPABASE_URL/rest/v1/injection_reports?order=created_at.desc&limit=8&select=analyst,injection_type,confidence_score,created_at" -H "apikey: $SUPABASE_SERVICE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_KEY"
```

---

## Part 1 — WHAT LANDED (LENS-029)

### Certification achieved
**S2-D, S2-E and Mission Analyst are CERTIFIED** on Cerebras `gpt-oss-120b` under D-012 — two consecutive green waves (Jul 30 15:44 UTC and Jul 31 05:09 UTC), both confirmed independently in the database. The gate that blocked Stage 7 since LENS-028 is open.

### Commits, in order

| SHA | What |
| --- | --- |
| `019124c` | CC-7a — S2-E `max_out` 10,000 → 16,000, probed 3/3 at 26-43% |
| `afef355` | CC-7b — S2-D gates on `prompt_chars//3 + max_tokens`, not article tokens |
| `e178efd` | baselines banked: s2a_injection 3/3, lens1 2/3 |
| `75348db` | CC-8 — pre-flight key-health probes read the registry, blind checks alarm |
| `d2b51c3` | CC-9 — fixture builders for s2gap and s3a_patterns |
| `e1e134c` | baselines banked: s2gap 3/3, s3a_patterns 3/3 |
| `86c89da` | CC-10a — s2gap `max_out` 2,400 → 4,000 |
| `352725e` | CC-10 — s2gap call site to registry, silent fallback deleted |
| `06f9a52` | CC-11 — s2a_injection `max_out` 2,400 → 4,600 |
| `67ee822` | CC-12 — s3a_patterns **groq → cerebras**, `max_out` 5,000 |
| `3333dd4` | CC-13 — s3a_patterns call site to Cerebras via registry |
| `044db89` | CC-14 — s2a_injection call site, dual-path budget |
| `59ee42a` | CC-14b — stale key-fallback comment corrected |
| `9de8a2c` | lens1 primary baseline banked |
| `1ab9b1c` | this brief |
| `7d1e413` | LR-105..115 appended to the register |
| `f1d188f` | this brief, updated at close |
| `03461b7` | CC-15 — s2gap key reverted after wave 6 failed |
| `2381803` | brief updated with the wave-6 incident |
| `949e8b8` | LR-116 appended to the register |
| `20c3b99` | CC-16 — pre-flight fixes |
| `188cf87` | brief through wave 7 |
| `0e067dd` | CEREBRAS_02 402 probe banked |
| `64ef2df` | **CC-18 — S2-E first-call stagger. HEAD** |

Working tree clean at close: fifteen scratch files removed (fourteen one-shot `patch_*.py` scripts plus `s3f_dump.txt`, a dead dump from 2026-05-26 that had been an LR-093 cleanup candidate twice).

### The discovery that changed everything

**`openai/gpt-oss-120b` is a reasoning model; `llama-3.3-70b-versatile` is not.** The reasoning channel costs roughly 4.5× the completion tokens, and **Groq folds it into `completion_tokens` with no separate `reasoning_tokens` field** — so it is invisible until you measure budget consumption.

Every Groq `max_out` set at LENS-028 was sized against a non-reasoning model. Measured consequences:

| Position | at old budget | verdict |
| --- | --- | --- |
| `s2gap` | 63/65/59% at 2,400 | MARGINAL → raised to 4,000 → 26/39/40% ✅ |
| `s2a_injection` | 64/66/57% at 2,400 | MARGINAL → raised to 4,600 → 32/37/37% ✅ |
| `s3a_patterns` | **100% ×3, `finish=length`, invalid JSON, one trial returned ZERO characters** | Groq is impossible → moved to Cerebras ✅ |
| `lens1` | 27/46/35% at 2,400 | **PASSES unchanged** |

**`lens1` breaks the pattern and that matters more than the pattern does.** It was projected to need ~2,900 and peaks at ~1,104. Its fixture shows `requires_json: False` — prose output, shorter, and LR-107's "truncated JSON has no partial value" does not apply to it. **Measure every position. Do not project from a ratio.**

---

## Part 2 — CURRENT WIRING (verified this session; re-verify per LR-102)

**Cerebras `gpt-oss-120b` (CEREBRAS_API_KEY — now EIGHT consumers at RPM 5):** S1-L3, S1-L4, **S2-D** (8,000), **S2-E** (16,000), **MA** (5,000), **S3-A** (5,000, new this session), S3-D (2,400), plus S2-F primary (2,000) in its own workflow.

**Groq `openai/gpt-oss-120b` (per-position keys):** **s2gap** (4,000, GROQ_S2_API_KEY, migrated), **s2a_injection** (4,600, GROQ_S2A_API_KEY, migrated), lens1 (2,400, GROQ_API_KEY, **call site NOT yet wired**), entity_extract (1,600, GROQ_API_KEY), ai5_watchdog (1,600, GROQ_MANAGER_API_KEY).

**Other:** S2-B/S3-B Gemini `gemini-2.5-flash-lite` in the registry but **both call sites still hardcode the dead `gemini-2.0-flash`** · S2-C/S1-RPT/S2-report/S3-F Mistral · S3-C Cohere · S2-F secondary Cloudflare.

**Verified limits.** Cerebras publishes `x-ratelimit-*` on every response: **RPM 5 · RPH 150 · RPD 2,400 · TPM 30,000 · TPH 1,000,000 · TPD 1,000,000.** Groq free tier: gpt-oss-120b TPM 8,000, llama-3.3-70b TPM 12,000, and **GROQ_API_KEY carries a 100,000 TPD that reset at midnight Pacific (07:00 UTC)**. Gemini: `planfintel` project, Free tier, 2.5 Flash at **RPM 5 / TPM 250,000 / RPD 20** — RPD is per PROJECT and Lens 2 alone peaks at 11 of the 20.

**`CEREBRAS_02_API_KEY` is now in GitHub secrets**, staged but not yet wired to any role. The intended split, when contention justifies it, is the S3 pair onto the second key.

---

## Part 3 — LR ENTRIES (all RATIFIED and banked at close)

**The register is `lens-DOC-002_rules.md` at the REPO ROOT — not under `docs/`.** It now holds 26 rules.

**A gap was found and closed at the LENS-029 close: LR-105 through LR-110, minted at the LENS-028 close and listed in the LENS-029 brief, had NEVER been appended to the register.** The file ended at LR-104. There is precedent — LR-095 to 098 were once recorded as "still pending addition at session close." **Rules were being minted in briefs and banked in a register, and the two drifted: the dual-source disease in the process docs themselves.** All eleven (105–115) landed in `7d1e413`.

**Check this at every close.** A rule that lives only in a brief is not a rule; it is a note.

Earned in LENS-029:

- **LR-111 (Derived logs only).** The truth order `runtime log > ledger > config > docstring` holds ONLY for log lines derived from the value they report. A hand-written log literal is a docstring wearing a log's clothes and ranks BELOW config. S2-B logs "calling gemini-1.5-flash" while its `MODEL` constant says `gemini-2.0-flash`; the log lied and cost a full turn. **Grep the f-string before trusting a log line as wire evidence.**
- **LR-112 (Mechanisms derive, never duplicate).** The code-side sibling of LR-110. `class TPMGuard` is defined **seven times** across the repo, all seven bodies different (22/26/33/35/49/61/75 lines). A duplicated mechanism is a hardcoded assumption with a heartbeat.
- **LR-113 (Never infer provider health from low consumption).** The pre-flight logged *"Gemini is OK with minimal RPD usage, which is a GO"* about a model dead since June 1 — failed calls consume no quota, so a corpse shows the LOWEST usage and reads HEALTHIEST. Absence of usage and absence of capability are indistinguishable on a usage meter. Read call outcomes.
- **LR-114 (Measure every position; never project from a ratio).** Two positions established a 4.5× reasoning-burn ratio and the third broke it. Projection would have raised `lens1` unnecessarily and would have missed that `s3a_patterns` needed a different provider entirely.
- **LR-115 (Perishable evidence commits immediately).** The llama-3.3-70b baselines are unrepeatable after Aug 16 and sat modified-but-unstaged after their probe run. Bank perishable evidence in its own commit the moment it exists.

---

## Part 3a — THE S2-GAP INCIDENT (wave 6, and the rule it earned)

**What happened.** CC-10 moved `s2gap` from `GROQ_S2DGCOM_API_KEY` to `GROQ_S2_API_KEY`. In wave 6 that position failed completely — `status=ANALYSIS_FAILED`, three attempts, three HTTP 401s — and the CC-8 alarm named it: `[PRE-FLIGHT] S2 shared: ALARM http=401 no quota header -- TPD check is BLIND`.

**Root cause.** `GROQ_S2_API_KEY` is valid in local `.env` — its probes passed 3/3 twice — and **invalid in GitHub Actions.** The Actions secret was last updated three months ago against DGCOM's two; it is stale or revoked. **Local `.env` and Actions secrets are independent stores that diverge silently.**

**How the wrong call was made.** The key was chosen by consensus of three *static* sources — the registry row, the file's line-15 docstring, and its `RuntimeError` message all said `GROQ_S2_API_KEY` — and then verified with a probe that runs **locally**. LR-094 says probe on the role's own key, which was done; but "the role's own key" locally is not the same secret as in CI.

**Fix shipped and VERIFIED:** `03461b7` reverted `key_env` to `GROQ_S2DGCOM_API_KEY`. Wave 7 confirmed it — `S2-GAP COMPLETE, saved=YES, first attempt, 6.8s`. CC-10's other gain, deletion of the silent `or GROQ_API_KEY` fallback, is preserved. **Incident closed.**

**Still open:** the naming is now inconsistent again (registry and code say DGCOM; the docstring and error message say GROQ_S2_API_KEY). Cleaning that up means refreshing the Actions secret first, then re-pointing — **and re-verifying in CI, not locally.**

- **LR-116 (A key verified locally is not verified in CI).** Local `.env` and CI secret stores are independent and diverge without warning. A probe proves a key works *where the probe ran*. Before moving a production position onto a different key, verify that key in the environment that will actually use it — or accept that the first live wave is the test, and watch it.

---

## Part 4 — STAGE 7: WHAT REMAINS

**Aug-16 string ledger: 16 live `llama-3.3-70b-versatile` strings originally. 8 resolved. 8 remain.**

Resolved: `lens_s2_gap.py:37` (CC-10) · `lens_s2_orchestrator.py:48,72` and `lens_s3_orchestrator.py:37,61` (CC-8) · `lens_s3a_patterns.py:61` (CC-13) · `lens_s2a_injection.py:44` (CC-14).

**Remaining, in recommended order:**

1. **`analyze_lens_multi.py` — lens1's call site, and the hardest file in the sweep.** One file serves all four lenses across THREE providers (lens1 Groq, lens2 Gemini, lens3/4 Cerebras), with its own `AsyncCerebras` client, its own 75-line `TPMGuard` copy, a provider-only `TPM_LIMITS` dict at line 777 with a `.get(provider, 10_000)` default (**LR-108 violation**), and `LENS_ARTICLE_BUDGETS` at line 103 deriving `"cerebras": 56800` from a 60,000 base when the real Cerebras TPM is 30,000. That last number is a CAP not a target, so it has never bound — a landmine with nobody standing on it. lens1's budget needs no change (2,400 passes).
2. **`lens_orchestrator.py:343-344` — the FALLBACKS map.** Slots 1, 3, 4 hold llama-3.3-70b; slot 2 holds already-dead gemini-1.5-flash. **A primary failure currently has no rescue.** The fix is derivation, not substitution: the registry already carries `fb_provider`/`fb_model`/`fb_key_env` per role, so FALLBACKS should be BUILT from it or it rots again at the next deprecation.
3. **`lens_manager.py:174` and `lens_orchestrator.py:174`** — the AI-5 verdict judge, duplicated.
4. **`entity_extract` and `ai5_watchdog`** — both **fixture-blocked**. `entity_extract`'s `_extract_experts_via_llm()` builds and calls in one function, so a faithful fixture needs a pure-move extraction first (the `3d5f10c` pattern). `ai5_watchdog` has no prompt builder anywhere. Both are support functions rather than analytical positions — **this is the defensible line to hold if time runs short.**
5. **Three files with live 3.3-70b and NO registry role at all:** `lens_compendium.py:506`, `lens_framing_rubrics.py:68`, `lens_regular_report.py:62`. Each needs a registry row created, then a fixture, then a probe. **This is the only part of the remaining work whose shape is genuinely unknown — open it early, not on Aug 8.**
6. **s3a's fallback is knowingly broken.** `fb_provider: groq` / `fb_model: gpt-oss-20b` inherits `max_out 5000` and hits the same 939-token ceiling that killed the primary on Groq. Recorded in the registry note so it cannot hide. Candidate replacement: `mistral-small-2603` (TPM 50,000, non-reasoning) — but it must be probed, not assumed.

**Certification plan.** D-012 certifies in **parallel** — waves 4 and 5 certified three positions simultaneously, and the same two waves would certify fifteen. The bottleneck is the sweep and its probes, not the certs. Genuine exception: **S3-D and S3-F run Mon+Thu**, so they need two waves of THEIR cadence and must migrate earliest.

**Realistic timeline:** all commits by ~Aug 3, two wave-days certify everything by ~Aug 5-6, eleven days before the cliff.

---

## Part 5 — QUEUE BEYOND THE CLIFF

| Tier | Item |
| --- | --- |
| IMPORTANT | **BUG-001** — root cause found and measured: `_split_batches` sizes in TOKENS (4,500) while `build_articles_prompt` re-truncates at `MAX_TOTAL_CHARS = 9000` CHARS ≈ 2,250 tokens, half the batch. Measured 44 batched → 23 sent. `articles_analyzed` reports `len(articles)` regardless — ~1.5× inflation on every S2-D row. Splits into "stop lying" (thread `included` up through three functions, cheap) and "stop dropping" (reconcile the budgets, needs a probe and likely an S2-D `max_out` raise) |
| IMPORTANT | **TPMGuard unification** — seven divergent copies, seven private windows modelling ONE shared 30,000 TPM budget. S2-A's hardcoded 6,000 cost a measured 50s stall in wave 4. Design: one shared module, state keyed by (provider, model, key_env), fed the true reservation and corrected from the `x-ratelimit-remaining-*` headers |
| IMPORTANT | **BUG-003 CONFIRMED TOTAL** — S2-F's Cloudflare secondary fails EVERY run (`Secondary failed — returning primary only`, 6+ times per run). The ensemble has never been an ensemble; every S2-F "HIGH confidence" finding for months was single-model. `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` both exist as secrets — the provisioning test has never been run |
| IMPORTANT | **The Gemini pair** — S2-B and S3-B both call the dead `gemini-2.0-flash` on one `GEMINI_API_KEY`, each burning a 30/60/90s ladder = 180s per position per wave. The Mistral fallback carrying them is itself now throwing `Service tier capacity exceeded`. Constrained by RPD 20 per project. **The AI Studio "All models" toggle has never been flipped**, so RPD for 3.1-flash-lite / 3.5-flash / 3.6-flash is unknown |
| IMPORTANT | **S3-E runs on SambaNova**, which is dead (D-005 rewrite). Nobody has looked at it |
| POST-CLIFF | CC-1d `//3` → `//4` divisor — now concrete: it fired a false "exceeds the 8K TPM ceiling" warning on a call that used 4,990 tokens, over-predicting by 79%. Measured ratio is 4.41 chars/token against the documented 4.42 |
| POST-CLIFF | `check_groq_tpd` defined twice per orchestrator file · `mistral-small-latest` alias at 12 sites (D-015 forbids aliases) · `framing_rubrics` defaulting to unregistered `mistral-medium-latest` · LR-090 schema checkpoint · watchdog wire-truth · `s3f_dump.txt` rm |

---

## Part 6 — DANGERS

Silent degrade remains this project's signature failure, and this session found four fresh instances of it: a log line that named a different model than the wire; a pre-flight that will report healthy forever once its model dies; an ensemble that has never been an ensemble; and a quota check that reads a corpse as the healthiest provider on the board.

**The specific cure that keeps working: derive truth, then make its absence loud.** Every fix this session was an instance of it.

**Things that will bite if forgotten:**

- **Probe before push is not a formality.** It caught a position that returned zero characters three times and would have shipped silently.
- **A probe fixture must rebuild the prompt from production's own functions.** The pack refuses hand-written prompts because "a hand-written prompt measures nothing" — and it is right.
- **The repo has MIXED line endings.** `lens_s2d_adversary.py` is LF, `lens_s2_orchestrator.py` is CRLF. Multi-line binary patch anchors must detect the ending per file. A pre-write assertion caught this before any bytes were written; keep it.
- **`assert_model_known` can raise on a fallback path.** s2a's `call_injection_tracer` is shared between Groq primary and Mistral fallback, and the registry holds `mistral-small-2603` while the file passes the alias `mistral-small-latest` — an unconditional assert would have killed the fallback it was meant to protect. Scope the vaccine to the primary path.
- **Cerebras now carries eight positions on one key at RPM 5**, and `CEREBRAS_02_API_KEY` is staged but unwired.

---

## Part 7 — LIVE vs BANKED

**LIVE — verified by bytes, ~90%:** head `949e8b8` and every SHA in Part 1 · certification of S2-D/S2-E/MA across waves 4 and 5 with LR-080 confirmation on both · every Groq position's real budget, measured on real prompts · the Cerebras meter table read from response headers · the Groq 404 fail-open behaviour, tested with a bogus model name · seven divergent TPMGuard copies with line counts · BUG-001's 44-batched-23-sent measurement · 27 rules in `lens-DOC-002_rules.md` · **wave 6: S3-A COMPLETE in 4.0s on Cerebras, S2-GAP failed on three 401s** · **wave 7: S2-GAP COMPLETE first attempt after CC-15** · **wave 8: CC-16 verified — no 401 alarm, real S2-GAP pre-flight line, S2-A passing; but 3 Cerebras + 3 Groq 429s, all recovered** · **`CEREBRAS_02_API_KEY` returns 402 payment_required 3/3, from a separate email account.**

**BANKED — true when written, re-verify before acting, ~40%:** the three role-less files' shape · entity_extract and ai5_watchdog's fixture difficulty · the Gemini 3.x model roster · the S2-F Cloudflare picture · everything in Part 5.

**UNKNOWN — the open question this session hands you:** whether the three migrated positions run clean in production. Nothing in Stage 7 continues until that wave reads.

---

*LENS-030 Brief | 2026-07-31 | James Maverick (Bro Alpha) + Claude (Opus 5) | Bytes first, honesty always*
