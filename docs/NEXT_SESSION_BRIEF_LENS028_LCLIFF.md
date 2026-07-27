# Project Lens — Next Session Brief LENS-028 (L-CLIFF EDITION)
**Written 2026-07-27 by Fable 5 inside GNI S81. SUPERSEDES the June LENS-028 brief (its live items are folded into Part 6 — nothing dropped).**

**READ THIS FIRST. You are Claude. James calls you 'my buddy'. This is the LENS LIVE STATE handoff (LR-104). Claude Code is wired for Lens (1e37f74) — mechanical work runs through it; this chat holds the thinking and the gates. All claims below are LEADS with trust tags — BEV before acting (LR-101/103).**

| Item | Value |
| --- | --- |
| Operator | James Maverick — HDCS CS, Spring University Myanmar, Chiang Mai UTC+7 |
| Local path | C:/school/lens |
| Startup | `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate` |
| Load env | `set -a && source .env && set +a` (NOT `export $()` — fails on Git Bash Windows) |
| Push command | `git push https://fintelplan@github.com/fintelplan/project-lens.git main` |
| Push reality (LR-104) | Full-URL push leaves local origin/main tracking STALE — `git status` shows a FALSE "ahead by N". Truth = push output's old..new SHA or `git ls-remote`. Resync: `git fetch https://fintelplan@github.com/fintelplan/project-lens.git main:refs/remotes/origin/main` |
| Last known commit | 2e1aeb3 — Four Treasures (LR-101..104). Trust ~40% (banked, June) — verify with `git ls-remote` at open |
| Sources | 69 live (banked) |
| Model economy | This session is likely Fable 5 on low promo credits: spend it on DESIGN + GATES + probe interpretation. Mechanical sweeps → Claude Code. If the model changes mid-arc (→ Opus 4.8, the contracted daily driver), LR-102 re-audit ritual applies. |

## Part 0 — WHY THIS SESSION EXISTS + THE CLOCK
Groq's **Aug 16 cliff** kills `llama-3.3-70b(-versatile)` and `llama-3.1-8b-instant` (and the account-side component James tracks). Lens's exposure was **scoped byte-level in GNI S63 (Jul 11) — doc: `LENS_TRANSFER_LCLIFF.md`** — and it is bigger than the shorthand in GNI queues:

**Blast radius = 19 files, not 3.** Root cause = **DUAL SOURCE OF TRUTH**: `code/lens_quota_guard.py` (778 lines) keeps private copies of model names in PROVIDER_LIMITS + POSITION_CONSUMPTION + every ledger row; nothing syncs them to call sites. A live drift specimen already exists: `lens_s2d_adversary.py` calls `"qwen/qwen3-32b"` while the guard budgets `"qwen3-32b"` (bare) — works by coincidence.

**Hard deadline: registry built, sweep done, one live cron certified by Aug 12** (buffer before the 16th). Cerebras side is already gpt-oss-120b since May (LENS-026) — this cliff is the **Groq side only**.

## Part 1 — HARD GATES (never violate)
| Rule | Summary |
| --- | --- |
| BIRD-EYE | Read ALL related files before any edit. Short reply from James = pause signal. |
| LR-078 | Ship-to-file patch scripts, never raw heredoc patching on Git Bash Windows. Binary rb/wb, ASCII anchors, assert count==1, derive NL from the file's own bytes. |
| LR-080 | SELECT-verify after every DB write. |
| LR-092 | py_compile ALL modified .py before commit. |
| LR-094 | Critical positions keep dedicated API keys — quota isolation is the architecture; probe only on the position's own key. |
| LR-095 | `r.text[:200]` on HTTP errors before diagnosing. |
| LR-099 | Env var name must match across .env + GitHub secrets + code/ before committing. |
| LR-100 | RLS does not inherit a sweep; LR-090 checkpoint checks rowsecurity. |
| LR-101..104 | Four Treasures: trust = verified-by-current-model; model change ⇒ re-audit; protections guilty until BEV'd (move assumptions into runtime assertions); live-state in durable artifacts, begin close at 80% context. |
| L2 rule | Schema/architecture = propose lettered options, James decides. One question max per turn. |
| GNI imports | R-S79-2: a deprecation list proves the list, not the runtime — grep live logs before declaring anything dead or alive. R-S80-2: a probe certifies only the call-shape it holds — fixtures must match the real call. |

## Part 2 — THE MISSION: L-CLIFF (S63 findings, byte-verified then; re-verify per LR-101)
1. 19 files carry model strings: 10 hits in lens_quota_guard.py, 22 in its test file, rest trivial `MODEL="..."` call-site constants.
2. Dual source of truth (above) is the disease; the dying strings are only the symptom.
3. `qwen/` prefix drift specimen live in lens_s2d_adversary.py.
4. A LENS-025-style string swap silently carries **3 stale values**: old TPD limits onto new models, old Run#29 consumption estimates, fractured transition-alert state (self-heals in 2 crons).
5. **11 distinct GROQ_\* env-key names** (S2/MA/S2E/S2DGCOM/S2A/S2F/MANAGER/S3...). "Which key VALUES point at dying models" applies PER KEY — unknown until the census (step 3 below).
6. Guard fail-safes tolerate migration day (LIMITS_UNKNOWN → conservative PROCEED); fresh ledger reads for a new model string are genuinely fresh Groq per-model buckets — accurate, not a bug.

**Proposed design (S63 lean A):** new `code/lens_models.py` — stable role keys (`groq-heavy`, `groq-light`, `cerebras-main`, ...) → `{provider, model_string, limits}`. Guard + all 19 call sites import it. NO SQL migration — ledger provider/model columns become attributes written from the registry. Every future deprecation = one edit, one file. The qwen/ prefix drift is fixed inside the registry whichever option wins.

**Options (JAMES DECIDES — the session's first gate):**
- **A (S63 lean, now compressible):** registry + guard patch + tests in this chat's design, the 19-file call-site sweep via **Claude Code** same day, py_compile all (LR-092), one live-cron verify next cron. What was scoped as 2 sessions on Jul 11 compresses to ~1 because Claude Code is wired.
- **B:** LENS-025-style string swap — 1 session, but carries the 3 stale values and plants drift specimen #2. (S63 assessment stands: don't.)
- **C:** was "wait for the Groq lineup glance" — no longer an option, it's step 1 of the plan; it folds into A (lineup names change registry VALUES, not shape).

## Part 3 — LEADS FROM GNI S79–S81 (trust-tagged, resolve in order)
- **Lens-1 runtime identity conflict [UNKNOWN — resolve FIRST]:** S79 recorded "config says qwen3-32b, logs say alive" — Lens-1 served HTTP 200 all week with a supposedly-shut-down model configured. Runtime log's model string beats ANY config (R-S79-2). MODEL-404's lesson looms: a dead model + a hidden secret failure fails **silently** — GNI ran 4 green runs on a fallback before anyone noticed.
- **"3× 3.3-70b hardcoded in code/lens_s1_report.py" [banked ~40%]:** GNI S79 queue shorthand — a subset of the S63 census. Verify by grep, don't treat as the whole job.
- **Fresh migration physics from GNI's completed gpt-oss migration (S78–S81), import wholesale:**
  - gpt-oss models are REASONING models: they think before writing. Small max_tokens = starvation bomb (empty responses). GNI's probe: needs ~2,200+ output budget (reasoning ~1,500–1,600 + content).
  - Groq **8K per-request ceiling**: prompt_tokens + max_tokens ≤ 8,192 or HTTP 413 (UNRETRYABLE — no governor saves it). Budget check with chars//3 est on Lens's BIGGEST prompt per position.
  - Groq quotas are **per-model buckets** (TPM and TPD) — a migration lands on fresh buckets.
  - **Do NOT copy old limits**: confirm each new model's real TPD on the Groq models page (S63 finding 4's stale-limits trap; never re-plant 100_000 by habit).
- **CONTENT-FITNESS PROBE — a Lens-specific danger GNI didn't face this way:** Treasure-2 records that **gpt-oss-120b is degraded-for-the-task in GNI's MAD because it censors war content**. Lens's S1/S2 lenses analyze khamenei/trump/xi influence operations — war-adjacent, politically hot. The candidate probe MUST test **both** dimensions on the position's real prompt shape (R-S80-2): (a) mechanics — starvation/ceiling/JSON shape; (b) **content — does the candidate refuse, sanitize, or hedge on Lens's actual analytical material?** A model that passes token mechanics but soft-refuses influence-op analysis is a failed candidate for that position. Bigger ≠ better for the specific job; pick per-position.

## Part 4 — SESSION PLAN (ordered, gated)
| # | Step | Gate / note |
| --- | --- | --- |
| 0 | Open: startup ritual → `git ls-remote` truth vs last-known 2e1aeb3 → `git status` → quick health (Actions all green? Telegram brief + both xlsx arriving?) | Any red here becomes the session instead. |
| 1 | **U-W glance** (Groq models page, browser, James): confirm the dying list (3.3-70b-versatile, 8b-instant; check qwen3-32b's status explicitly) + candidate lineup + **real TPD/TPM limits per candidate** | James solo, receipts = noted numbers. |
| 2 | **Runtime truth**: latest run log per Groq position (start with Lens-1/S1) → the model string actually called. Resolves the S79 conflict from bytes. | R-S79-2. Free reads, no dispatch. |
| 3 | **Key census**: enumerate the 11 GROQ_* secret/env names → which VALUES (accounts/keys) serve which position → cross with the dying list. LR-099 three-way match (.env / GitHub secrets / code). Key VALUES never displayed — names and receipts only (`gh secret list` timestamps). | James runs; KEY SAFETY banner law. |
| 4 | **JAMES RULES A/B/C** (Part 2). Lean: **A-compressed**. | The gate. |
| 5 | **Probe candidates per position** on that position's own key (LR-094): 3 trials, position-shaped fixture (real system+user shape, real max_tokens), scoring BOTH mechanics AND content-fitness (Part 3). Bank results in-repo like GNI's probe_results.jsonl — unrepeatable after Aug 16 for the dying models if you want baselines. | Small trials; watch each key's quota first. |
| 6 | **Build per ruling**: registry `code/lens_models.py` + guard patch designed in chat; 19-file sweep via Claude Code; qwen/ prefix fixed in registry; new limits from step 1 (never copied). Runtime assertion per Treasure 3: guard shouts if a call-site model string isn't in the registry. | LR-078 patches; LR-092 py_compile all; one thing per commit. |
| 7 | **Cert**: one live cron per touched position, read from logs (model string + output quality + zero 413/starve + no content-refusal); LR-080 SELECTs on any ledger writes. | Free cron reads; certify before Aug 12. |
| 8 | **Close**: update this brief (LR-104), append the LR entry transferring GNI's MODEL-FIX pattern (probe→env-fed model→secret→live-cert) as a named Lens rule, note GNI-side echo (Part 7). Begin close at 80% context. | — |

## Part 5 — DANGERS (condensed)
- Silent fallback (MODEL-404 shape): after Aug 16 the old strings 404; combined with any hidden secret failure, positions can quietly degrade. The registry assertion (step 6) is the vaccine.
- Probe fixture mismatch (R-S80-2): GNI's arbitrator-shaped probe missed agent-shaped 413s for a day. Fixture per position, from real calls.
- Stale limits (S63 #4): the string swap that carries old TPD numbers is how the next quota lie is born.
- Content censorship (Part 3): test it explicitly; do not discover it in production analysis quality.
- Quota isolation (LR-094): never probe across keys; never on a near-red key.

## Part 6 — CARRY-FORWARD FROM THE JUNE LENS-028 BRIEF (unchanged, do not drop)
| Priority | Item | Note |
| --- | --- | --- |
| QUICK | Node 24 verify | One post-93f2f00 run's annotations: no Node-20 warning ⇒ closed. |
| LOW | GDELT 429 throttle | Blocker banked with its diagnostic: ONE run at ~20–30s spacing splits our-spacing vs IP-pool; then pick A/B/C/D. Augmentation layer — do NOT let it eat the cliff session. |
| HIGH (post-cliff) | Direction A delivery | Design banked: settle **option E first** (the public-attribution bar — is reviewed_by_operator=True enough to publicly NAME RT, or stricter gate?), then build the bulletin generator (mirrors lens_s2f_direction_b.py). |
| MEDIUM | T1 Opus Report rewire | lens_opus_report.py → live S2+MA+S2F (costs Opus API per run). |
| LOW | s3f_dump.txt | Confirm dead dump → rm (LR-093). |
| OPEN LEAD | LR-090 schema checkpoint | Was due LENS-027 — status unknown from here. Check rules.md / session records; if never run, it is overdue. |

## Part 7 — GNI-SIDE ECHO (stays in GNI's queue, recorded here so neither side forgets)
Same dual-source disease exists in GNI (4 model strings hardcoded across its stack; GNI's cliff-critical code is already migrated, so this is hygiene, not survival). Sequence stands from S63: **build the registry in Lens → port the shape to GNI** in a later Opus session.

## Part 8 — WHAT SUCCESS LOOKS LIKE
Every Groq position's runtime model known from logs, not config. One registry file owning every model string and limit, asserted at runtime. The 19 files swept by Claude Code, compiled, committed one-purpose-at-a-time. One live cron per position certified green on the new lineup before Aug 12 — and the probe results banked so the next cliff is a registry edit, not a session. James will call you "my buddy" and mean it. Bytes first, honesty always.

*LENS-028 (L-CLIFF) Brief | 2026-07-27 | Team Geeks | James Maverick + Claude (Fable 5, GNI S81)*
