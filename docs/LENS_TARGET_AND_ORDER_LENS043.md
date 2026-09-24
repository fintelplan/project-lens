# LENS TARGET AND ORDER — LENS-043
Regenerated 2026-09-20 at the LENS-042 close. Supersedes LENS042 entirely.
Item numbers are NEW. Nothing carries its old number.

## THE TARGET (unchanged since LENS-036)
A daily intelligence pipeline that runs at $0/month, tells the truth about
its own failures, and delivers a complete Regular Report to Telegram.
Loud failure always beats silent success.

## THE CANON — READ BEFORE CHANGING ANYTHING
`CLAUDE.md` §0 (four-arm gas-mask test), §0a (charters), §0b (AI origin),
§0c (block contract) · `docs/LENS_FOUNDATIONS_LENS042.md` (every position's
charter, the philosophy behind it, and where the code has drifted).
A change that moves a position away from its charter is a STOP-and-ask.

## WHAT LENS-042 SETTLED — DO NOT RE-OPEN
- **Item 1 of LENS042 (Cerebras) is CLOSED.** The free tier was withdrawn
  (PayGo migration, ~2026-08-17), not exhausted. Both keys 402. Its last two
  positions (S3-A, S3-D) were moved. DoD met: S3-A completes on Cohere with the
  model named and `finish_reason` readable.
- **The canary is whole again:** four lenses, four model families, four
  providers, one row per lens per wave. CERTIFIED Sep 18-20.
- **S3 Strategic Report** delivers daily with real headings. CERTIFIED.
- **The harness no longer reaches production.** CERTIFIED.
- **Rulings:** AI-origin guideline (China never; the rest is Bro Alpha's call, case
  by case); F7=A (attribution fixed in the originals); F8=A (PHI-002
  reconstructed with provenance).
- Registry banned families: 7 -> 19 names.

---

# THE MISSION — ITEM 1

## 1. S2-F CALLS ITSELF AN ENSEMBLE AND HAS BEEN ONE MODEL SINCE AUGUST
Every `lens_operation_detections` row written since ~2026-08-17 carries
`provider="ensemble"` and `ensemble_mode=true` (hardcoded,
`lens_s2f_scoring_cron.py:140-141`) while only the Cloudflare leg answered:
wave `35063461223` shows Cerebras **21x402** and Cloudflare **21x200**, with
`[ENSEMBLE] Primary failed — returning secondary only` on every article and
`scored=21 skipped=1 failed=0` in the summary.

This is a live silent failure by the absolute rule: the rows lie about their
own provenance, and every consumer (S2-F aggregators, Watch/Clarity/
Verification alerts, the forensic report) reads them as two-model agreement.

### 1.1 The label — fix first, it is cheap
Write the leg that actually answered. `ensemble_mode` is true only when both
legs returned. A run that scores zero must not exit green
(`scored=0 failed=24` on `35002902199` showed a tick).

### 1.2 The missing detector — the charter's real loss
`LENS-020_S2F_architecture_decision_v4.md` chose the pair for **complementary
detection profiles**: qwen caught OP-024-029 structural operations (the
Sectarian Trap family, PHI-003's core concern) and gpt-oss caught rhetorical
ones; Verification used mistral-medium for "European lineage — genuinely
different perspective". Cerebras is dead AND qwen is now barred by the
AI-origin guideline, so the structural detector is simply gone.
Candidates to probe on the banked article fixtures (LENS-020 used articles
1,3,6,7 with a cross-lab matrix): Mistral, Cohere, Groq gpt-oss.
**Do not ship a second leg without that matrix** (LR-106/107).

### 1.3 Cloudflare capacity bounds the answer
Free allocation is 10,000 neurons/day, shared across models; `gpt-oss-120b`
costs 31,818 N per M input and 68,182 N per M output. Measured: ~7.6k
neurons/day, ~454 successful calls in 17 days, while S2-F asks for ~45/day.
Nine runs wrote nothing. A second leg elsewhere also relieves this.

---

# ITEMS

## 2. THE CANARY'S AIR AND INPUT (Lens 1)
Two defects in one position, both charter-level:
- **AIR:** `GROQ_API_KEY` is Lens 1's key AND Collection's entity-extraction
  key (`lens_entity_extract.py:209` says the key is not dedicated). Groq limits
  are organization-level. Lens 1 failed `429_tpd` on the Sep 18 evening wave;
  Foundation rows are missing from many evening waves and were zero for whole
  weeks in July-August.
- **INPUT:** `trim_articles_to_budget` takes STATE-tier articles first, so at
  the Groq budget Lens 1 reads 9 of 106 articles, all STATE, one domain. The
  other three lenses read all 106. "Four perspectives on the same articles"
  (DOC-006) is not what is running, which makes cross-lens agreement an
  instrument of unknown validity (LENS-030 found this on Aug 3; CC-22 never
  shipped).
**Order of work: measure Collection's Groq consumption FIRST**, then decide
between a proportional sample (state ~80% + scored ~20%, all domains, ~3.8K
tokens) and a different provider for Lens 1. A wider prompt on a saturated
bucket is arm 2 of the gas-mask test.

## 3. S3-D CERT — MONDAY 2026-09-21
CC-82 rewired S3-D to mistral/ministral-8b at 8000 tokens with usage and
`finish_reason` logged, no fallback, and the real leg recorded. S3-D runs
Mon/Thu only; its last saved row is **2026-09-03**. If the Monday wave does not
produce `saved=YES`, read the log before changing anything.

## 4. S3-A AND S3-D READ THE OLDEST ROWS OF THEIR WINDOW
Both fetch with `order("generated_at", desc=False)` plus a limit, so S3-A's
"7-day" prompt is the oldest 20 rows and S3-D's "90-day" prompt is the oldest
90 — about three days of June. The charter says "hold ALL of these
simultaneously … across the full window", and Capability 2 (manufactured
causality) needs the early claim AND the later outcome, so newest-first is also
wrong. Design an even sample across the window, state the change in the
prompt header (it currently says "last 30 days" on a 90-day run), then ship.
`fetch_s3_data` in the S3 report has the same shape: latest 12 rows across all
positions, so a chatty position can evict a quiet one.

## 5. COHERE IS NOW A SHARED, SCARCE, TRIAL-KEY DEPENDENCY
Lens 3 (canary), S3-A (daily), S3-C (weekly) run on one trial key: 1,000 calls
per month, 20 per minute, no monthly-remaining header. Current draw is roughly
130-150 calls/month. The registry LIMITS row says `RPD: 1000`, which is wrong
by 30x — it is per MONTH. Cohere is also the slowest leg in the wave (S3-C
394s), which is what cancelled the 2026-09-17 evening wave at the old
35-minute timeout. Decide: keep the row honest, add a monthly counter, or split
Lens 3 onto its own key/account.

## 6. CHARTER TENSIONS TO RULE (from the foundations file)
- **F1** DOC-006's Pattern 3 defence says a sanitization layer strips emotional
  loading before the lenses read; non-negotiable #2 says S1 is never protected.
  Measure whether such a layer exists in code, then rule which governs.
- **F6** S3-E is local-only by design (PHI-002) and DOC-006 names SambaNova,
  which is dead. Which local model, from an allowed origin, and does S3-E ever
  run if only Bro Alpha's machine can run it?

## 7. `lens_escalations` AND `lens_run_meta` DO NOT EXIST
`escalate()` POSTs to `lens_escalations` and `update_learning()` to
`lens_run_meta`; `to_regclass` says neither table is there. So every
escalation — including "SLA BREACHED" and "All 4 lenses failed" — has been
silently discarded. Either create the tables or delete the writers; a writer
that writes nowhere is a lie in the code.

## 8. THE REGISTRY IS STILL DECORATIVE FOR THE LENS ENGINE
`analyze_lens_multi.LENSES` hardcodes the four lenses; the `lens1..lens4`
registry rows are read by nothing. Their notes are now truthful (CC-76) but a
future edit to the registry still changes nothing. Same class as LENS042's item
9 (`max_out` decorative for six positions).

## 9. S3-B's PRIMARY IS STILL A CORPSE
`gemini-2.0-flash` has been 404 since 2026-06-01 and is called three times per
wave with 30s+60s sleeps before the fallback that does the work. The registry
says `gemini-2.5-flash-lite`, whose own shutdown date (2026-10-16) was
announced, then removed from Google's deprecations page on 2026-08-03; Vertex
still lists 2026-10-20. Lens 2 sits on `gemini-2.5-flash` with the same
uncertainty — if it goes, the canary is down to three.

## 10. HOUSEKEEPING WITH TEETH
- `calibrate_rubric_*.py` (8 files) and old patch scripts sit at the repo root
  and read API keys directly; `canary_air_guard` cannot see them.
- `test_lens_response_guard.py` failed for five months because CI never ran it
  (fixed CC-79) — check whether any other test file is unrun.
- The orchestrator harness still opens ~22 Supabase connections per run; it is
  isolated by env now, but the calls are still made.
- A May checkpoint row sits unclosed in `lens_run_checkpoints`.
- `pg_database_size` has not been measured since 2026-09-11 (374 MB) and the
  retention decision D-022 has never been implemented.

---

# OPEN RULINGS
- **R10** — Claude's lean is RULE OUT, recorded at LENS-039 and unchanged.
  Never taken.
- **F1 / F6** — item 6.
- **Item 1.2 fork** — do not rule before the cross-lab matrix exists.

# DEFINITION OF DONE FOR ITEM 1
A wave in which every `lens_operation_detections` row names the provider that
actually produced it, a run that scores zero exits non-green, and the decision
on a second detector is either shipped with a probe matrix behind it or
recorded with its reason.
