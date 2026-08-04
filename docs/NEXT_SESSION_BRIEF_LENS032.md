# NEXT SESSION BRIEF — LENS-032
Written 2026-08-04 ~23:55 ICT by Claude (Opus 5) at the LENS-031 close.
SUPERSEDES NEXT_SESSION_BRIEF_LENS031.md entirely.

## TRUST TAGS
[V] verified this session by bytes/logs (~90%)  [I] inferred (~50-60%)  [B] banked, unverified (~30-40%)

---

## THE HEADLINE

- [V] **THE AUG-16 MIGRATION IS COMPLETE AND CERTIFIED. Zero live `llama-3.3-70b`
  strings remain in Project Lens.** The Jul-30 census of 16 resolved as **11 migrated
  + 5 proven inert**. Twelve days of slack remain.
- [V] Both of this session's production commits certified GREEN on the Aug-4 evening
  scheduled runs. Read from logs, not checkmarks.

## PART 0 — FIRST ACTIONS (in this order)

1. [V] `git ls-remote https://github.com/fintelplan/project-lens.git main`.
   Expect this brief's own docs commit or later. **A close doc can never name its own
   SHA — record what ls-remote says and move on (LR-104).** Last CODE commit is
   `e0d8568`.
2. [V] **The work list is NOT in this brief. It is in `docs/LENS_TODO_AUG16.md`** —
   urgent / important / others, with commands, rulings needed, and hazards. **Do not
   copy it into this brief. Dual sources of truth are how S2-D died.**
3. [V] Read `docs/LENS_TODO_AUG16.md` before proposing anything.
4. [V] Optional cert re-read on the newest waves (both already passed once):
   ```bash
   gh run list --repo fintelplan/project-lens --workflow lens-collect.yml --limit 3 \
     --json databaseId,headSha,createdAt,conclusion \
     --jq '.[] | "\(.databaseId) \(.headSha[0:7]) \(.createdAt) \(.conclusion)"'
   ```
   **Check `headSha` FIRST. Absent RED is not GREEN (LR-129).**

---

## WHAT SHIPPED THIS SESSION (9 commits, `bb3fdf1` -> `e0d8568`)

- [V] `c4f0515` probe evidence banked, entity_extract (LR-115)
- [V] `5f2dac5` **CC-27** entity_extract wired to registry, 600 -> fit_max_tokens(1600),
  silent `or GROQ_API_KEY` deleted, lying comment + docstring swept
- [V] `29aa61f` **CC-28a** registry `ai5_watchdog` key_env + fb_key_env
  `GROQ_MANAGER_API_KEY` -> `GROQ_MA_API_KEY`; inert
- [V] `8ef9dce` **CC-28b** `lens_manager.py` marked SUPERSEDED, its 5 dead cliff strings
  swept (5 -> 0). NOT deleted — that is an LR-093 call.
- [V] `2479c00` **CC-28c** AI-5 prompts lifted to `AI5_SYSTEM_PROMPT` +
  `build_ai5_user_msg(ctx)`; byte-identical PROVEN, not merely compiling
- [V] `e609647` **CC-29** AI-5 wired to registry, 300 -> fit_max_tokens(1600), `:264`
  log literal swept, module-level `GROQ_MANAGER_KEY` read deleted
- [V] `e0d8568` `_sb_get` read failures made LOUD — visibility only, control flow
  byte-for-byte unchanged

## CERTS — BOTH PASS

- [V] **CC-27**: Collection #243, run `83856966602`, 15:12-15:35 UTC. **283 wire lines**,
  all `entity_extract calling groq/openai/gpt-oss-120b (prompt N chars, max_tokens 1600)`.
  Mean prompt 2,058 chars, max 3,340. **Zero RED.**
- [V] **CC-29**: Manage+Analyze #252, run `83867608643`, 15:54-16:16 UTC.
  `AI 5 verdict:` (header swept, no model name) then
  `[AI5] groq/openai/gpt-oss-120b prompt 293 chars, max_tokens 1600`, `Verdict: GO`,
  `FULL RUN APPROVED`. **Zero RED, zero `llama-3.3-70b` in the entire log.**
- [V] **CC-24 held a THIRD wave**: ORCH qualities 5.0 / 6.5 / 4.8 / 6.8 — four different
  values, no healing line, no marker-absent warning.
- [V] `e0d8568` verified live in CI: both `[SB] GET ... HTTP 400` lines print in
  production, code 22007, naming the `+00:00` params.

---

## THE STRING LEDGER AT `e0d8568`

- [V] `grep -rn "llama-3\.3-70b" --include=*.py --include=*.yml . | grep -v "/venv/"`
  returns hits, **but ZERO are live production code.** Four are executable and all four
  are proven inert:
  - `lens_orchestrator.py:377-378` FALLBACKS — dead code, no delivery path to the child
  - `lens_framing_rubrics.py:68` — dormant behind `lens-s2f-scoring.yml:75`'s mistral pin
  - `lens_regular_report.py:62` — dead inside dead (`_FORCE_PROVIDER` written, never read)
- [V] The rest are stale comments and docstrings. **Two are actively misleading:**
  `lens_s2_gap.py:15` still names `GROQ_S2_API_KEY` and is literally one of the three
  static sources that caused the LR-116 401 incident; `.github/workflows/lens-compendium.yml:3`
  began lying the moment CC-23 landed.

---

## RULINGS JAMES OWES BEFORE THE NEXT BUILD

- [V] **Gemini project question** — do `GEMINI_S2B_API_KEY` / `GEMINI_S3B_API_KEY` sit on
  a *different Google project* than lens2's `GEMINI_API_KEY`? RPD 20/day is **per
  project, not per key**. Migrating the S2-B/S3-B corpses onto a shared pool would
  starve lens2, the canary's Physical Reality lens. **Gas-mask arm 2.**
- [V] **`DAILY_BUDGET` cross-wire** — `lens_pipeline_runs` is written by Collection, not
  by Manage+Analyze. Should Manage+Analyze get its own counter, or should `DAILY_BUDGET`
  be retired? Fixing the timestamp alone arms a gate on the wrong pipeline's number.
- [V] **`lens_framing_rubrics.py:68`** — delete the unreachable groq branch, or sweep its
  string?
- [B] **`analyze_lens_multi.py`** — when to open it, and whether the S1 rescue is worth an
  analytical change to the canary. **Give it a fresh session.**

---

## HAZARDS — READ BEFORE TOUCHING ANYTHING

- [V] `gh workflow run` fails with `HTTP 403: Must have admin rights to Repository`.
  Manual dispatch goes through the GitHub web UI. `gh run list` / `gh run view --log` work.
- [V] **Cron lines lie about timing.** Collect `0 1`/`0 13` UTC, manage-analyze `28 1`/`28 13`,
  but actual starts run **2.5-3.3 hours late** on the free tier.
- [V] **ALWAYS check `headSha` before reading a cert log.** Two runs fooled me today: empty
  greps that looked like a pass, on pre-CC-27 code.
- [V] **Build log-grep patterns from the LOG, never from the source's prints** (LR-122).
  Broken three times in one session, including in the session that minted it.
- [V] `capture_output=True` means the S1 child's markers can NEVER reach the Actions log,
  however correct the pattern.
- [V] `probe_lens_models.py` is **LF**; everything in `code/` is **CRLF**. Detect per file.
- [V] Patch anchors pure ASCII; use `chr(8220)` or explicit `\xe2\x80\x94` bytes for the em
  dashes these files carry (LR-101). Guard on the NEW content's absence (LR-124).

---

## LIVE vs BANKED — WRITTEN FOR THE NEXT MODEL'S HANDS

### LIVE (verified by bytes or logs this session, ~90%)
- [V] HEAD `e0d8568`, working tree clean, 181 tests pass / 2 known-stale guard fixtures.
- [V] Zero live `llama-3.3-70b`. Both certs green. CC-24 holding three waves.
- [V] `wire('entity_extract')` -> `('groq','openai/gpt-oss-120b','GROQ_API_KEY',1600)`.
- [V] `wire('ai5_watchdog')` -> `('groq','openai/gpt-oss-120b','GROQ_MA_API_KEY',1600)`.
  Registry self-test: 24 roles / 10 wire pairs / 5 limit rows.
- [V] `lens-collect.yml` supplies exactly three secrets: SUPABASE_URL,
  SUPABASE_SERVICE_KEY, GROQ_API_KEY. No GROQ_S2_API_KEY.
- [V] `lens-manage-analyze.yml` supplies `GROQ_MA_API_KEY` at :45 and :67.
  `GROQ_MANAGER_API_KEY` is supplied by NO workflow and read by NO code.
- [V] Entity volume: **283 calls and 116 HTTP 429 in one collection run**; all 429s
  absorbed by the SDK, zero failures. Run 23m 42s vs #242's 12m 57s.
- [V] The `+00:00` bug: PostgREST 400 / 22007, curl-verified both ways. `Z` works.
- [V] `lens_manager.py` is invoked NOWHERE — no workflow, no import.
- [V] Article bodies are HTML: of the 2,000 chars sent, visible text is
  min 53 / median 111 / max 1,957.

### BANKED (from earlier sessions, ~30-40% — re-verify before acting)
- [B] Groq gpt-oss-120b TPD 200,000 and its "92-93.5% headroom" — **never computed against
  a 283-call volume, and TPD readings are time-of-day dependent.** Read
  `x-ratelimit-remaining-tokens-day` live before treating it as a defect (LR-121).
- [B] SambaNova dead (HTTP 402, balance 0, Jul 28).
- [B] Gemini RPD 20/day per project; lens2 at 11/20.
- [B] D-001..D-017 — `LENS_LCLIFF_DECISIONS.md` has not been supplied for three sessions.
  Not citable as settled law without a re-read.
- [B] `gemini-2.5-flash` dies Oct 16; S2-B/S3-B on `gemini-2.0-flash` dead since Jun 1.
- [B] `analyze_lens_multi.py` frozen since `7b968d4` (2026-05-22), so its banked line
  numbers are addresses rather than leads — **but verify anyway.**

### INFERRED (~50-60%)
- [I] The 116 × 429 is partly CC-27's doing: Groq counts requested tokens, and 600 -> 1600
  took each call from ~1,170 to ~2,170 against an 8,000 TPM ceiling. Article volume also
  differed between runs, so this is a strong signal rather than a clean measurement.
- [I] `lens_framing_rubrics.py:68`'s groq branch is unreachable — read `:405-425` before
  deleting anything.

---

## RULES EARNED THIS SESSION (LR-127..133, append to `lens-DOC-002_rules.md`)

- [V] **LR-127** — A fixture's worst case has TWO axes: the prompt SIZE the position sends
  and the OUTPUT DEMAND it provokes. Maximising one can zero out the other. (Amends LR-117,
  earned on entity_extract: ranking by length picked a zero-expert article and certified 6%
  of budget.)
- [V] **LR-128** — Build every instrument from the DATA, never from what you expect the data
  to look like. Three instances in one session: a log-grep built from the source's prints, a
  cert criterion that ignored its own healing line, and a "density" selector that counted
  HTML markup as attribution. (Generalises LR-122 beyond log-greps.)
- [V] **LR-129** — Absent RED is not GREEN. Verify the artifact under test is the one that
  ran (`headSha`) before reading any cert.
- [V] **LR-130** — An empty result from an honest instrument is intelligence. Chasing "why
  isn't this producing?" is the gas-mask reflex; a populated table from a loosened instrument
  is pretend-right bias. (Canary doctrine, arm 3.)
- [V] **LR-131** — Anything the Collection pipeline imports at module scope resolves LAZILY.
  Collection is the canary's air supply, and a module-scope raise there takes down the whole
  wave. The certified module-scope registry pattern is for STANDALONE SCRIPTS only.
  (Canary doctrine, arm 2.)
- [V] **LR-132** — When the real fix needs a ruling, ship the VISIBILITY half alone: make the
  failure loud, keep control flow byte-for-byte unchanged, and say so in the commit.
- [V] **LR-133** — A rule's SCOPE line is load-bearing. The canary doctrine said "read before
  changing anything in System 1", so agents working on Collection or Enrichment judged it
  irrelevant and skipped it for months. Scope a doctrine by what it protects, not by where it
  was discovered.

---

## WHY I CLOSED HERE

- [V] Both certs green, tree clean, no half-finished work, and the next block (Tier 1 in the
  TODO) starts from a clean ledger.
- [V] James is ~20 hours in and the remaining big item — `analyze_lens_multi.py` — is the
  hardest file in the repo and touches the canary directly. **Doing it tired is how S1-001
  happened.**
- [V] Twelve days of slack remain to Aug 16, and GNI is next. The migration is done and
  certified; everything left makes it *safe*, not *finished*.
