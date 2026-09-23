# NEXT SESSION BRIEF — LENS-046
DRAFT written 2026-09-22 during LENS-045 (session still open). SESSION STATE ONLY.
The item list lives in docs/LENS_TARGET_AND_ORDER_LENS046.md. This brief
references order items BY NUMBER and never restates them.

## HEAD — VERIFY, DO NOT TRUST THIS LINE
Last code commit: `7771891` (CC-115), pushed, `ls-remote`-matched, Lens CI green 2026-09-22.
`git ls-remote origin refs/heads/main` is the only truth (LR-104).

## READ THIS BEFORE ANYTHING ELSE
- **2026-09-22 EVENING: THE CANARY LOST LENS 1 AND ITS VOICE SAID 4/4.** Collection's
  entity extraction drew 160 `tokens per day` refusals on GROQ_API_KEY; Lens 1 (same
  key) failed `429_tpd_escalated` 3s after firing. The canary block, the S1 report
  and the Daily Brief each read `limit(4)`, filled the gap with the MORNING's
  Sovereignty Check, and said "4 lenses" / "4/4" (gas-mask arms 2 AND 3, in
  production). Fixed: CC-113 (air) and CC-114 (voice). **Cert: the next morning wave.**
- **Certs owed: order item 2** — run `lens045_certs.sh`; it refuses any run that
  does not carry the commit being certified.
- **S2-F scored nothing on the 2026-09-22 morning wave** (order item 1): Cloudflare
  refused the first call of the UTC day. The (iii) lean is withdrawn.
- **James's sequence (LENS-045):** handover -> urgent+important -> record OTHERS ->
  DISCUSS D1-D7 -> decide. No mission is declared until the discussion.

## WHAT SHIPPED (LENS-045) — nineteen commits, Lens CI green on each; gates 9 -> 27
| SHA | What |
| --- | --- |
| `b52f7f0` | **CC-97** S2-F daily-quota breaker: first `4006` trips it; SDK retries off, the loop owns them. Gate 10 on the real openai SDK |
| `04df48c` | **CC-98** China ban catches `zai` (two-sided bite) |
| `efbb7e1` | **CC-99** S3-A samples its 7-day window evenly in time; sampler moved unchanged to `lens_window_sample.py`. Gate 11 |
| `125c440` | **CC-100** S1 canary report repointed to `ministral-8b-2512` (probed first), exits non-zero, `|| true` removed, `finish_reason` logged, MAX_TOKENS 6000. Gate 12 |
| `fe19e4a` | **CC-101** five small truths (fallback labels, stale comments, PARSE_FAILED provenance, loud `already_scored`, Mistral default). Gate 13 |
| `7e9e577` | **CC-102** `lens_provider_refusal.py` + four sites write `PROVIDER_REFUSAL ...`. Gate 14 |
| `3b4629a` | **CC-103** events stored once per process in `lens_provider_events` (Actions only; never raises). Gate 15 |
| `bd9cd8b` | **CC-104** Groq (entity_extract, S2-A) + Cohere (S3-A) watched; identical events counted in `n`; CI installs `groq`. Gate 16 |
| `46916e8` | **CC-105** `lens_provider_health.py`, the last step of Manager + Analyze: one PROVIDER HEALTH message, apart from the canary; DOWN rule. Gate 17 |
| `adb062a` | **CC-106** S3-B stops retrying a `model_gone` primary (~90s a wave); S2-B's log counts its real attempts. Gate 18 |
| `97c28d3` | **CC-107** MA, S2-C, S2-D, S2-E, S2-GAP record refusals (primary and fallback legs); S2-C keeps the real HTTP status. Gate 19 |
| `a336d46` | **CC-108** **S3-C read the OLDEST 40 rows of its 30-day window** (~5 days of S1, ~1 day of S2); now sampled evenly in time, ids selected; Cohere refusals recorded. Gate 20 |
| `72924dd` | **CC-109** S3-F (Mistral, no fallback) and the Regular Report record refusals. Gate 21 |
| `4bfa018` | **CC-110** S3-D records refusals — nine added lines, zero removed, proven by a diff guard. Gate 22 |
| `7771891` | **CC-115** `RETIRED_PROVIDERS = {cerebras}` in the registry; S2-D, S2-E and MA skip their dead primary and go straight to the ministral-8b fallback that answered every wave. 26 lines added, 0 removed. Gate 27 |
| `b9dde9b` | **CC-114** the canary's voice counts THIS wave (`lens_canary_wave.py`): canary block, S1 report prompt and intro, Daily Brief say `3/4 -- MISSING: Foundation`; zero lenses is sent, not skipped. Live dry read on the Sep 22 evening rows: `3/4 lenses -- MISSING: Foundation`. Gate 26 |
| `184c09e` | **CC-113** entity extraction moves to GROQ_S3_API_KEY (idle; a separate org: 999/1000 daily requests left while Lens 1's org had spent 321+). Gate 25 = a ratchet: no NEW role may share a canary lens's key; Lens 1 carries no debt |
| `d04bd9e` | **CC-112** (D4 = A) S3-D stores its whole answer in `lens_system3_reports.analysis_full` (jsonb); one added line, diff guard OK. Gate 24 |
| `9f5672c` | **CC-111** the Compendium's zero-failure guard (CC-23) keeps shipping, logs that its intro is CANNED, and records the refusal. Gate 23 |

Every gate was proven to BITE before commit. Every patch was sha-checked,
binary, all-or-nothing; every sandbox hash matched James's machine.

## CERTIFIED THIS SESSION
| What | Evidence |
| --- | --- |
| CC-91 | run `35695267308`: Stage 1 `failure` (exit 1); Watch `1 ok`, Clarity `7 ok`, Verification `1 ok`, Direction B delivered — none skipped |
| **CC-113** | M+A `35826233294` (`b9dde9b`, Sep 23 morning): `[ORCH] Lens 1: ✅ quality=6.8` (2.3 the morning before, 429_tpd the evening before); Collection's Groq refusals: `rate_limit` x1, no TPD; GROQ_S3_API_KEY secret exists |
| **CC-114 (4/4 path)** | Telegram: canary block = Foundation, Physical Reality, Causal Chain, Sovereignty Check, all 06:23 UTC; S1 intro `4/4 lenses \| 428 articles`; Brief `Lenses: 4/4`. The MISSING path is not yet exercised |
| **CC-97** | S2-F `35826080294`: first real trip -- `refusals(4006)=1 breaker=1 cf_call_failed=1`, `quota_skipped=3`, `scored=14` |
| **CC-99** | `S1 window=7d: 52 reports in window, 20 sampled evenly in time (7 in the newest third), spanning 2026-09-16 to 2026-09-23`; S2 526 -> 15 (5 newest third) |
| **CC-105 (wave 2)** | `DOWN  cerebras/gpt-oss-120b  payment x14 in 2 runs`; `DOWN  google/gemini-2.0-flash  model_gone x4 in 2 runs` -- the first DOWN the detector ever said |
| CC-100 | M+A `35762354559` (`d04bd9e`): `[S1-RPT] 16171 chars generated, finish_reason=stop`, `S1 docx sent to Telegram` |
| CC-102/103/104 | same run: `PROVIDER_REFUSAL provider=google ... model_gone status=404`; `PROVIDER_EVENTS stored: 2 rows, 8 events` |
| CC-105 (wave 1) | a separate Telegram `PROVIDER HEALTH (operator, not the canary)`: `EXCEPTION (1 run, watching)` cerebras payment x7 and google model_gone x2; `WARN groq daily_quota x160`. Wave 2 must say DOWN |
| CC-106 | `Attempt 1 failed: 404` then `S3-B primary gemini-2.0-flash is gone -- not retrying`; no Attempt 2; `S2-B failed after 1 of 3 attempts` |
| Item 10 RLS | SQL: 24/24 `rls_on=true`, zero policies; anon `200 []` on 20 non-empty tables; anon schema `401` |

## RULINGS TAKEN THIS SESSION
- Item 1.2 delegated ("your call") -> Claude ruled (iii); **withdrawn the same day
  on evidence** (Sep 22 morning refusal). Reopened as D2.
- Item 11 delegated -> Claude ruled (a) passive, (b) not the canary's voice,
  visibly separate (order "settled"). Storage delegated -> Claude ruled a new
  table (ITIL 4 monitoring and event management: record, store, provide).
- **James ruled: S3-D is done NOW, by this session** ("the agent that knows it all
  now is cheaper than a later one"), overriding Claude's lean to wait until after
  Thursday's cert. The cost (touching code under cert) was met with a diff guard:
  0 removed lines, 9 added, all record lines; gate 9 unchanged at 30/30.
- Claude (reversible): the health message is its own script and step, exits 0
  always (a dead primary with a working fallback must not redden every wave).
- James: the target/completion-test question is NOT decided; discuss first.
- **D4 delegated -> Claude ruled A** (ICD 203: S3-D produced alternatives #4,
  relevance #5 and change #7 and stored none of them): a jsonb `analysis_full`.
  The promised dry insert was DROPPED (it would write a fake production row);
  the column was checked by SQL type and a service-key read instead.
- **D7 delegated -> Claude ruled B (measure first) -- then REOPENED the same night on
  evidence** (the canary lost Lens 1). Re-ruled A, refined by the key map: move entity
  extraction to an IDLE Groq org (GROQ_S3_API_KEY), not GROQ_S2DGCOM (that would
  have starved S2-GAP). Shipped CC-113.
- **D2 delegated -> Claude ruled A**: Mistral `ministral-8b-2512` primary on every
  article, Cloudflare a best-effort second leg (CC-97 makes a refusal cost one
  POST). Not wired: LR-106 probe on S2-F's real prompt first.
- Claude (reversible): CC-99 pure-moves the sampler rather than importing a
  position module; CC-100 probes before repointing; CC-102 ships in phases.

## LIVE (verified this session by bytes, logs, console or a passing test)
- **Detector's first catch:** S2-D (x2), S2-E (x4), MA (x1) still call their DEAD Cerebras
  primary every wave (402), then fall back to Mistral. `source` is the process
  (`lens_s2_orchestrator.py`), not the position -- the positions came from the log.
- Registry key map: GROQ_S3_API_KEY and GROQ_S2E_API_KEY had no Groq user. Canary keys
  shared: Lens 3 (Cohere) with S3-A/S3-C; Lens 4 (Mistral) with 13 roles (gate 25's debt).
- Direction B's "5 findings" = ONE trump_office Verification finding per day,
  Sep 18-22, all unreviewed, re-sent every wave (item 6).
- S3 Telegram shows raw JSON (`{"plain_english":...`) under "We have seen this before";
  canary rows carry cycle `manual` on scheduled waves; markdown `**` is not rendered.
- **Lens 1 quality 2.3 -> 6.8** on the first wave with its own air. Suggests the
  low quality was starvation; ONE wave -- a hypothesis until 3-4 more agree.
- Telegram, Sep 23: Direction B now sends SIX daily copies (Sep 18-23) of one finding;
  the CRITICAL ALERT fires every wave (7-day trend CRITICAL x3); the Brief says
  "Patterns: 5" while the S3 report says "3 patterns"; `**` is not rendered in the
  Brief, S2 message and Regular Report caption; S3-B's parallel came as plain text today
  (raw JSON yesterday -- intermittent).
- Cloudflare: 21/21 refused at 06:32Z (first call of the UTC day), then ~22 served at
  17:40Z. Consistent with a rolling window; still a hypothesis.
- S2-F morning Sep 22: 21 Cloudflare calls, 21 x `4006`, `scored=0 skipped=1 failed=21`, RED.
- Cloudflare refusal body read in full (run `35640858699`); openai 2.32.0
  reproduced: default 3 POSTs per refusal, `max_retries=0` -> 1.
- S1-RPT Sep 22: `mistral-small-latest` 429 x3 -> "Mistral failed to generate S1
  report" while the wave stayed green. Probe: `ministral-8b-2512` on production's
  own prompt (27,976 chars) -> 200, `stop`, 3,096 tokens, PART A-E, 0 bullets.
- S2-B/S3-B: Gemini `gemini-2.0-flash` 404 "no longer available"; fallback
  `ministral-8b-2512` works (S2-B 5 findings; S3-B 18,860 chars).
- Groq `GROQ_API_KEY` org, Collection morning: 321 POSTs, 188 ok, 133 x 429,
  58 TPD. Lens 1 2.3/10 kept (order item 4).
- `lens_provider_events`: service read `200`, anon INSERT `401`, `rls_on=true`,
  0 policies, column `n integer default 1`.
- **S3-C live dry read (CC-108, Supabase only, no Cohere call):** S1 242 in window,
  40 sampled, 13 in the newest third, 2026-08-31 -> 2026-09-21; S2 1,216 in window
  (past the 1000-row cap), 40 sampled, 13 in the newest third, same span; prompt
  30,855 chars. **S2 also has no rows 2026-08-22 -> 08-31** (the outage gap is not S1-only).
- `lens-forensic-report.yml` triggers on M+A `completed` with no conclusion
  filter: a red M+A does not skip it.

## CLAIMS THIS SESSION THAT WERE WRONG
- "Measure the entity bucket for two days, then cap" (D7 = B) -- too slow: the canary
  lost a lens that same evening.
- The first air-fix lean (GROQ_S2DGCOM) would have moved the starvation to S2-GAP;
  the registry key map showed it before anything shipped.
- The cert reader's `EXCEPTION \(1 run` pattern expected two spaces after it and missed
  the lines (Telegram showed them). The seventh substring slip.
- CC-113's first patch wrote an unterminated string into the registry note; the
  sandbox compile caught it.
- "The morning wave will see 0 refusals" -> 21 of 21 (order item 1).
- The (iii) premise (fresh pool each morning).
- "Four live mistral-small sites" -> two; the grep caught comments.
- "S1-RPT is not failing" (from a zero) -> it was; the pattern matched the wrong branch.
- The cert command read the pre-CC-97 morning run; now SHA-guarded.
- The LENS045 order: `fetch_s3_data` reads the oldest rows (it reads the newest);
  "24 articles" (8 x 3).
- Two of Claude's own tests failed on their own text before commit (docstring,
  comment) — caught in the sandbox, fixed, re-proven.
- **The CC-110 diff guard crashed on its first run** (bytes compared with str) —
  it had never been run in the sandbox. Re-run against HEAD: `DIFF_GUARD OK`.
- The cert reader's S3-A pattern assumed an `[S3-A]` prefix; S3-A logs under
  `[QUOTA_GUARD]` (the guard's basicConfig wins). The eighth substring slip; the
  position's own words (`window=7d`) found the lines.
- **ESTIMATES THAT HELD:** every sandbox sha matched; CC-97 22/22 on the real
  catalog; the S1 probe prediction (200/stop/5 parts); forensic trigger has no
  conclusion filter; RLS; bites RED as predicted.

## WHAT I DELIBERATELY DID NOT DO
- No Mistral leg wired to S2-F; no matrix (D2 first).
- Did not add the TPD breaker or usage logging to entity extraction (moved, not changed).
- Did not delete the CEREBRAS_API_KEY secret: `get_client()` in S2-D/S2-E/MA still
  builds a Cerebras client before the CC-115 skip; removing the secret breaks them.
- Did not wire the canary lenses into the detector (gas-mask test first).
- Did not wire S2-RPT (D3). Did not touch S3-D storage (D4).

## LEAK LOG
`lens045_cc100.sh.log` held the Supabase project URL (httpx INFO, unmasked
locally) and was uploaded to the chat; deleted locally. Not a credential; RLS
verified the same day (order: settled). Every later script ran a leak scan:
0 hits.
