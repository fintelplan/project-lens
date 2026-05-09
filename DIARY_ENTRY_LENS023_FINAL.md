# Project Lens Diary — LENS-023 (May 7–9, 2026)

## LENS-023 — The Great Machine Repair Marathon
*(May 7, 2026 ~13:30 → May 9, 2026 ~03:30 Thai, ~38 hours)*

**Session window**: May 7–9, 2026
**Commits**: 10 commits (`9812330` → `828ac37`)
**Schema**: lens_drift_findings_sample_size_check relaxed >= 15 → >= 2
**Model**: Claude Sonnet 4.6 adaptive (continued per LR-090)

### Session origin

Opened with 9 uploaded session docs plus Telegram/GitHub screenshots. Full state clear:
S1/S3 docx missing, Forensic Report absent, manage-analyze #71 yellow. Started diagnosis.

### The S1 docx bug — most important fix of the session

The `articles_used` column in `lens_reports` stores full JSON article content — 392K chars
per lens × 4 lenses = 1.5M chars = 836,206 tokens → Mistral 400 bad request. The `|| true`
in the yml swallowed the error silently. S1 docx had been failing for an unknown number of
days with zero visible indication in GitHub Actions.

Diagnosis path: key test → 200 OK → prompt size check → `Prompt length: 1,598,621 chars`
→ `articles_used length: 392,437` → column stores `{"selected": [{full article objects}]}`.

Fix: parse count from JSON blob. Confirmed: PM cron 41.2KB, 460 articles. ✅

New rule: LR-096 — never dump raw DB column into AI prompt without size check first.

### The timeout marathon

manage-analyze #71 cancelled at 30m26s. Root cause: `timeout-minutes: 30` too tight.
James flagged this concern earlier — Claude dismissed it citing "GitHub supports 6 hours"
without checking the yml. LR-097: always check yml before dismissing operator timeout concerns.

Principle locked: never set arbitrary time ceilings on pipelines that complete naturally.
Removed timeouts from manage-analyze, S2-F scoring, collect, forensic-report.
Kept: Compendium 15min, Regular Report 15min, GDELT 10min (safe margins with evidence).

### The Mistral fallback sprint

After fixing timeouts, ran full analysis of all positions with no fallback:
- S3-D: Cerebras 429 Mon/Thu → Mistral fallback added
- S3-B: Gemini RPD exhausted by AM cron → Mistral fallback added
- S2-B: Same Gemini RPD pattern → Mistral fallback added
- S3 report: already had fallback from this session's earlier work
All three now contribute to their respective reports.

### S3-C model resurrection

`command-r-plus` was permanently removed by Cohere Sep 15, 2025 — 404 every run for months.
Wasting ~40s of pipeline time on guaranteed failures. Updated to `command-r-plus-08-2024`,
tested live: 200 OK, "Hello! How can I help you today?" ✅
LR-092 sibling check: quota_guard.py lines 69+90 and README also updated.

### The dedup disaster and cleanup

Watch aggregator runs 2x/day, no "already wrote today" guard. Inserted same voice×lens
pairs every single run. Clarity reads ALL unreviewed Watch rows — each duplicate counted
as separate entry → 49 clarity findings per run. In 3 days: 104 rows, 88 duplicates.

PHI-004 design intent: Watch fires ONCE when pattern first detected. Not re-fire every run.

Fixed: added daily dedup checks to Watch, Clarity, Verification. Deleted 88 duplicates.
Database went from 104 → 16 clean rows.

### Dead sources audit

Of 18 "dead" sources in collection logs, actual status after verification:
- 4 truly dead (404 — removed): SRC-044/047/048/052
- 8 broken RSS / 0 entries (removed): SRC-006/007/050/051/062/063/064/065
- 4 live RSS (kept): SRC-035/036/055/076 (10-30 entries each)
- 2 403 scrapers (kept): SRC-049/075 (block automation, not dead)
Result: 78 → 66 sources, all verified live.

### Forensic Report resurrection

13 consecutive crons with no Forensic Report auto-firing despite manage-analyze green.
Root cause: GitHub silently drops workflow_run event subscription after yml modifications.
All our LENS-022/023 commits modified the yml → subscription dropped.
Code was fine (Apr 30 ran perfectly: 75.4s, 87 citations, sent=True).
Fix: one manual trigger → chain reset → auto-fires on next manage-analyze.
Confirmed: #14 manually triggered 1m49s, S3 positions=['S3-A','S3-B','S3-C','S3-D','S3-E'] ✅

### New builds — S3-F and lens-resume.yml

**lens-resume.yml**: Manual checkpoint resume workflow. The orchestrator already had
`load_checkpoint(run_id)` built since LENS-013. Just needed the yml trigger.

**S3-F Counter-Check**: Adversarial challenger to S3-A and S3-D. Read PHI-002/003/004
in full before building — the philosophy docs shaped every design decision:
- PHI-002: even our system has blind spots
- PHI-003: apparatus-people separation in all outputs including counter-arguments
- PHI-004: phrased as "pattern warrants review" not conclusion, alternative hypotheses always
Data gate: S3-A ≥20 runs AND S3-D ≥4 runs (proxy for ~30 days). SKIPPED_INSUFFICIENT_DATA
until gate met. Provider: Mistral-small (already wired, reliable). Cadence: Mon/Thu.

### T3 steno calibration — deferred

Attempted at 3AM Thai. Cerebras: queue saturated (2 partial successes at conf=0.85/0.92).
Groq: 413 prompt too large. Deferred to LENS-024 at 6-8AM Thai when Cerebras queue fresh.

### Lessons

The most important lesson of LENS-023: **ground truth lives in the file, not in grep or docs.**
PHI-002 was in the S3-E file header. The articles_used size was in the column data.
The timeout was in the yml. The Cohere model was removed 8 months ago but still in code.
Every bug was solved by reading the actual source of truth, not by assuming.

---

**LENS-023 closed**: May 9, 2026 ~03:30 Thai by Sonnet 4.6
**Next**: LENS-024 — verify cron results, T3 calibration if quota fresh, LR-095/096/097 to rules
