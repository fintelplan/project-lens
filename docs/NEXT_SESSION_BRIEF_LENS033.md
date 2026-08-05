# NEXT SESSION BRIEF — LENS-033
Written 2026-08-05 by Claude (Opus 5) at the LENS-032 close.
SUPERSEDES `NEXT_SESSION_BRIEF_LENS032.md` entirely.

## TRUST TAGS
`[V]` verified this session by bytes/logs  `[I]` inferred  `[B]` banked, unverified

---

## THE HEADLINE

- `[V]` The Aug-16 `llama-3.3-70b` migration is **COMPLETE AND CERTIFIED**. Head
  `c5080f8`. Nine commits: CC-30..CC-38. Eleven days of slack remain, and nothing
  left on that cliff can execute.
- `[V]` **THE BIG DISCOVERY: S2-B and S3-B were never dead.** Both run on a Mistral
  fallback every wave and have done for two months. "Dead since Jun 1" describes the
  **Gemini leg**, not the position. TODO 2.1 was built on a false premise and is
  rewritten there.
- `[V]` Mistral-small already handles S2-B's 200-article / 105,122-char prompt in
  production, in ~6 seconds. The registry note's "needs long context, probe on
  flash-lite" is **falsified by production**.

---

## PART 0 — FIRST ACTIONS

1. `git ls-remote`. Expect this brief's own docs commit or later (LR-104: a close doc
   can never name its own SHA). Last **CODE** commit is `c5080f8`.

2. **READ TONIGHT'S MANAGE+ANALYZE LOG FIRST. It is the whole point of CC-38 and the
   prerequisite for everything in Tier A.**

   ```bash
   gh run list --repo fintelplan/project-lens --workflow lens-manage-analyze.yml --limit 3 \
     --json databaseId,headSha,createdAt,conclusion \
     --jq '.[] | "\(.databaseId) \(.headSha[0:7]) \(.createdAt) \(.conclusion)"'
   ```

   Check `headSha` **FIRST** (LR-129), then:

   ```bash
   gh run view <databaseId> --repo fintelplan/project-lens --log > /tmp/ma.log
   grep -n "Mistral usage\|Gemini usage" /tmp/ma.log
   ```

   Two lines, one per position, each giving `prompt_chars` **AND** `prompt_tokens` for
   a real 200-article prompt. Compute chars-per-token from **ONE** line.

3. Read `docs/LENS_TODO_AUG16.md`. The work list is there, not here. **Do not copy it
   into this brief** — dual sources of truth are how S2-D died.

---

## WHAT SHIPPED (verify the SHAs against `git log`, do not trust this list)

| SHA | | What |
| --- | --- | --- |
| `9301945` | CC-30 | S2-F groq default path deleted — it was the DEFAULT, not unreachable; swapping the string would have armed an unprobed 6000-token reasoning path against Groq's 8000 TPM |
| `b1fb954` | CC-31 | three SambaNova fallbacks -> `mistral-small-2603` (**THREE** rows, not two; `s2a_injection` is a live certified position) |
| `c79f594` | CC-32 | probe refuses a baseline candidate after 2026-08-16 |
| `b8e920b` | CC-33 | write-guard test fixture string |
| `7457a92` | CC-34 | doc corrections; Tier 1 marked complete |
| `969ca22` | CC-35 | LR-134, LR-135 (register unbroken LR-090..LR-135 at that commit; the "39" first written here was a HEADING count, **not** a rule count -- see HAZARDS) |
| `d7b79c3` | CC-36 | silent `or GEMINI_API_KEY` removed from S2-B and S3-B — both could reach lens2's key, the canary's Physical Reality lens |
| `fe1d9b5` | CC-37 | provenance: Mistral rows were stamped with the dead Gemini model FIVE ways; now record what actually ran |
| `c5080f8` | CC-38 | provider token usage logged on all four legs (LR-132 visibility half); chars-vs-context units corrected |

---

## CERTS — READ FROM LOGS

- `[V]` Lens CI run `30969263288` GREEN on headSha `b8e920b`.
- `[V]` MA #253 = databaseId `30975849325`, headSha `d7b79c3`, `success` — CC-36
  certified live at the exact head.
- `[V]` S2-F #198 produced findings after CC-30 deleted the groq default — CC-30
  certified live.
- `[V]` Registry self-test: 24 roles / 9 wire pairs / 5 limit rows. Suite **182 passed
  / 2 known-stale** response-guard fixtures.

---

## TIER A — UNBLOCKED THE MOMENT TONIGHT'S NUMBER EXISTS

- `[V]` **A1.** Add a `LIMITS` row for `mistral-small-latest`. It is the model actually
  on the wire for **BOTH** positions and it has **NO row at all**, so `fit_max_tokens`
  and the quota guard cannot resolve a ceiling for the live production path.
- `[V]` **A2.** Re-size `MAX_TOTAL_CHARS` (currently `800000`, sized for Gemini's 1M
  context). **TPM 50,000 binds tighter than CTX**; the registry's own comment tags CTX
  128,000 as "VERIFY, not VERIFIED". Use the **measured** ratio, not a chars-per-token
  rule of thumb — these bodies are HTML.
- `[V]` **A3. THEN** promote Mistral to primary on `s2b_coordination` and `s3b_history`
  and delete the Gemini leg. Recovers ~7.5 min of every ~21 min wave, removes the
  Oct-16 cliff for both positions, and returns the entire Gemini quota to the canary.
  **A3 must not ship before A2.**
- `[V]` **A4.** `RPM_LIMITS`: `gemini-2.5-flash-lite` is **absent** from the table, so
  any Gemini migration silently drops to the default of 10. There is also a **duplicate
  `"gemini-2.0-flash"` key** (both 15) — an editing hazard.

---

## TIER B — UNBLOCKED NOW, NO RULING NEEDED

- `[V]` **B1.** `"context": "1M"` is still written into the DB evidence payload beside
  CC-37's corrected `"model": model_used`. Rows read
  `model: mistral-small-latest, context: 1M`. Provenance commit.
- `[V]` **B2.** `lens_s2b_coordination.py` SYSTEM_PROMPT tells the model "You have 1M
  context — use it to compare ALL reports simultaneously." **This goes ON THE WIRE to
  Mistral, which has ~128k.** Behaviour change, so it needs its own commit and a
  fitness read.
- `[V]` **B3.** S3-B posts SYSTEM_PROMPT **TWICE** on the Mistral leg — once as the
  system message, again nested inside `prompt`. ~3.7k chars wasted per call.
- `[V]` **B4.** `mistral-small-latest` is a **FLOATING ALIAS** against D-015, and it is
  the **live production path for two positions**. TODO 3.5 mis-prioritises this as
  dormant.
- `[V]` **B5.** `lens_quota_guard.py` calls `logging.basicConfig` at **MODULE SCOPE**,
  stamping `[QUOTA_GUARD]` on every line any module logs in that process. **NOT a small
  fix**: `basicConfig` owns the handler AND level for the whole Manage+Analyze run, and
  **certs are read from those logs**. Audit every cert grep pattern in use before
  touching it.
- `[V]` **B6.** 21 tracked `patch_*.py` scripts in the repo root (LR-093).
- `[V]` **B7. Build a real register-integrity gate, in `tests/` so CI enforces it.** It
  must count rule DEFINITIONS across **both** formats (`^## LR-\d{3}` headings and
  `^LR-\d{3}\s` compact entries, excluding prose cross-references — `LR-092` at `:139`
  is one such false positive) and diff the resulting set against the expected unbroken
  sequence. **Both gates in use today pass on a register that has silently lost a
  rule.** It is the only thing that would have caught the LR-124..126 loss.

---

## RULINGS JAMES OWES

> **RULED 2026-08-05 in `928d1f8` (CC-40) — these two are NO LONGER OWED.** LR-136
> (a spec must not state a number its reader can derive; write the COMMAND, not the
> answer) and LR-137 (provenance records the WIRE, not the registry; amends LR-105) are
> **minted and RATIFIED** in `lens-DOC-002_rules.md` at `:619` and `:637`. LR-136 as
> minted records **~13** such errors across nine specs; the "eleven instances" first
> written here was an undercount taken before the session's last commits landed.

- `[V]` **Gemini project-wide cap** — console only, and now much less urgent: if A3
  lands, MA stops calling Gemini entirely.
- `[V]` **DAILY_BUDGET cross-wire** (TODO 2.3).
- `[B]` **`analyze_lens_multi.py`** — hardest file in the repo, touches the canary.
  Give it a fresh session.

---

## HAZARDS — CORRECTED THIS SESSION

- `[V]` The `Commit: <sha>` line near the top of every Actions log is inside
  `Runner Image Provisioner` / `Hosted Compute Agent`. It is **AZURE'S** build commit,
  identical across all workflows. **It is not `headSha`.** Only
  `gh run view <id> --json headSha` gives that.
- `[V]` **CRON LAG SPLITS BY SLOT.** 01:00/01:28 UTC -> 2.8-3.6h late, lands
  10:48-12:02 ICT. 13:00/13:28 UTC -> 1.3-2.6h late, lands 21:17-23:02 ICT. A pooled
  range made this wrong twice. **Do not collapse it.**
- `[V]` **LINE ENDINGS:** `probe_lens_models.py` is CRLF (1206/1206); the pure-LF files
  are `tests/test_lens_write_guard.py` and both docs in `docs/`. `code/` is CRLF.
  **Detect per file, always.**
- `[V]` `lens-DOC-002_rules.md`: the three-EOL-region scar is a **WORKING-TREE
  property, not a repository one.** All three measured this session: the stored blob is
  **uniformly LF** (0 CRLF); *this* checkout carries CRLF / bare-LF / CRLF (248 bare
  LF); and a **fresh clone on this machine checks out uniformly CRLF** (650 CRLF, 0 bare
  LF), because `core.autocrlf=true` is set in the **system** gitconfig
  (`C:/Program Files/Git/etc/gitconfig`) and **no `.gitattributes` is tracked**. The
  region layout therefore depends on which checkout you are standing in — derive it,
  never carry it between sessions. **The operational rule is unchanged: append onto RAW
  BYTES and assert the bare-LF count is unchanged either side.** That invariant holds in
  both checkouts (248 -> 248 here, 0 -> 0 in a fresh clone).
- `[V]` **THE REGISTER'S INTEGRITY GATES ARE BLIND TO THE FAILURE THEY EXIST TO CATCH.**
  `grep -c "^## LR-"` counts **headings** and misses the seven compact one-line rules
  (LR-117..123) stored under `## LENS-030 -- earned rules`, so it is **not** a rule
  count. `grep -o "LR-1[0-9][0-9]" | sort -u` matches **BODY CROSS-REFERENCES**, so a
  rule can lose its definition entirely while the sequence still reads unbroken — which
  is how LR-124..126 vanished undetected. **Until a real gate exists (B7), a register
  append is NOT proven safe by either command.** Derived at LR-137: **41 headings + 7
  compact = 48 defined rules**, LR-090..LR-137, no gaps. Also derived: `LR-057` and
  `LR-058` are cited in body text but have **no definition anywhere in this register**;
  `[I]` they predate it, since it opens at LR-090.
- `[V]` **Anchor uniqueness has TWO failure modes.** Duplicate lines (fixed by a
  different anchor) and **INDENTATION SUBSTRINGS** — in s3b a 24-space
  `analysis = json.loads(raw)` contains the 12-space copy. Anchor with a leading
  newline.
- `[V]` **"Empty env" is untestable on Windows:** `os.environ.clear()` + import gives
  `WinError 10106`. Keep `SystemRoot`/`PATH`/`COMSPEC`/`WINDIR`/`TEMP`/`TMP`/
  `PATHEXT`/`NUMBER_OF_PROCESSORS`, assert zero app vars, run in a subprocess.
- `[V]` `gh secret list` and `gh workflow run` both **403**. Web UI only.
- `[V]` `gh run list` databaseIds are **NOT** the ids in the LENS-031 CERTS section —
  those were log/job ids.
- `[B]` `docs/LENS_LCLIFF_DECISIONS.md` has not been supplied for **FIVE** sessions.
  D-001..D-017 are **not citable as settled law**.

---

## WHY I CLOSED HERE

- `[V]` Everything unblocked was shipped. The next input — real token counts for a real
  prompt — arrives with the 21:17-23:02 wave and **cannot be computed before it**. That
  is not caution; there is nothing to compute.
- `[V]` At the close I recorded **nine** such errors of mine caught at BEV or by Claude
  Code, none reaching production. That number was itself written before the session
  ended, and it was an undercount: LR-136 as minted records **~13 across nine specs**,
  and three more surfaced afterwards — the register count (39 against a real 48), the
  EOL claim (repository against working tree), and my own false alarm that LR-117..123
  had gone missing from the register. Every one was a **number, address, size or
  line-ending stated with the confidence of a measurement**; none was a reasoning error.
  **The rate did not decline across the session** — the undercount is itself an instance.
- `[V]` **OBSERVATION, deliberately not a numbered rule: a close brief written before a
  session's last commits is stale on arrival.** Four doc-correction commits landed in
  one day (`7457a92`, `0606fa1`, `e232c82`, and this one). The cause is structural, not
  carelessness — CC-39 listed two rules as "candidates" in the same breath as asking for
  a ruling on them, and the ruling landed the same afternoon. **A close brief should be
  the LAST commit of a session, or it must not name anything still in flight.** Left as
  an observation on purpose: two rules were minted today already, and a third on the
  same day is rule inflation.
