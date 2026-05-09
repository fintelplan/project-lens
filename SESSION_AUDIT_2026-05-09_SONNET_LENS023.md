# Session Audit — LENS-023 (Sonnet 4.6 adaptive)
# Date: May 7–9, 2026

**Session model**: Claude Sonnet 4.6 adaptive
**Operator**: James Maverick (Bro Alpha)
**Time**: May 7, 2026 ~13:30 → May 9, 2026 ~03:30 Thai (~38 hours)
**Last commit**: `828ac37`
**Schema change**: lens_drift_findings_sample_size_check relaxed >= 15 → >= 2
**Status**: CLOSED ✅

---

## All commits this session (10 total)

| Commit | Description |
|---|---|
| `9812330` | fix: S1 report articles_used blob (836K tokens → Mistral 400) |
| `193987a` | fix: S3 Strategic report Mistral fallback when Cerebras 429 |
| `7562af7` | fix: S3-E SKIPPED_CI in GitHub Actions (PHI-002 local-only preserved) |
| `9ca75f0` | fix: S3-C model command-r-plus→08-2024 + manage-analyze timeout removed |
| `bfb1b38` | fix: S2-F timeout removed + quota_guard model updated |
| `82522d8` | fix: Mistral fallback for S3-D (Cerebras 429), S3-B (Gemini RPD), S2-B (Gemini RPD) |
| `958d9f2` | fix: remove timeout from collect (15min) and forensic-report (10min) |
| `605ee30` | feat: lens-resume.yml (manual checkpoint resume) + S3-F Counter-Check built |
| `8d13ba5` | fix: dedup guard Watch/Clarity/Verification aggregators (88 duplicates cleaned) |
| `828ac37` | fix: remove 12 dead/broken sources (78→66 sources) |

---

## What we accomplished

### Fix 1 — S1 Canary docx (commit 9812330) ✅ CONFIRMED
**Root cause**: `articles_used` column stores full JSON blob (392K chars/lens × 4 = 1.5M chars → 836K tokens → Mistral 400). `|| true` in yml swallowed error silently for unknown days.
**Diagnosis**: hello test → 200 OK → prompt size check → `Prompt length: 1,598,621 chars` → `articles_used length: 392,437` → root cause.
**Fix**: `len(json.loads(au).get('selected',[]))` replaces raw blob dump.
**Confirmed**: PM cron 41.2KB, 460 articles, quality 6.8/10 ✅

### Fix 2 — S3 Strategic docx (commit 193987a) ✅ CONFIRMED
**Root cause**: S2-F scoring exhausts Cerebras RPM 18:09-18:29 UTC. S3 report starts 18:33 → saturated → 3 attempts all 429.
**Fix**: `call_mistral_fallback()` added after Cerebras exhausted.
**Confirmed**: S3 docx delivered every cron with Historical Parallel section ✅

### Fix 3 — S3-E SKIPPED_CI (commit 7562af7) ✅
**Root cause**: PHI-002 LOCAL-only design. No Ollama in GitHub Actions → ERROR_NO_OLLAMA.
**Key decision**: SambaNova option rejected after reading full file — PHI-002 violation.
**Fix**: Detect `GITHUB_ACTIONS` env var → `SKIPPED_CI` cleanly.

### Fix 4 — sample_size constraint (schema only) ✅
**Root cause**: Constraint `>= 15` vs aggregator minimum 3 → 4 findings silently dropped.
**Fix**: Supabase SQL — relaxed to `>= 2`.

### Fix 5 — S3-C model dead (commit 9ca75f0) ✅
**Root cause**: `command-r-plus` removed by Cohere Sep 15, 2025 → 404 every run.
**Fix**: Updated to `command-r-plus-08-2024`. Verified 200 OK.
**Sibling check**: quota_guard.py + README_lens_quota_guard.md also updated.

### Fix 6 — manage-analyze timeout removed (commit 9ca75f0) ✅
**Root cause**: `timeout-minutes: 30` too tight. #71 cancelled at 30m26s.
**Key lesson**: Never set arbitrary time ceilings. Remove timeout = wait-till-finish.
**Principle**: Only set timeout-minutes when you explicitly want to kill a runaway job.
**LR-097**: Before dismissing any operator timeout concern, read actual yml value first.

### Fix 7 — S2-F + collect + forensic timeouts removed (commits bfb1b38, 958d9f2) ✅
S2-F ran 26m39s (88% of 30min limit). Same principle applied.
collect ran 11-12 min (80% of 15min limit). Forensic Report: unknown runtime, safer without.
Compendium (29s), Regular Report (1m5s), GDELT (6m53s) kept their timeouts — safe margins.

### Fix 8 — Mistral fallbacks S3-D, S3-B, S2-B (commit 82522d8) ✅
**Root cause S3-D**: Cerebras 429, no fallback, Mon/Thu only → fails every busy day.
**Root cause S3-B**: Gemini RPD exhausted by AM cron → PM cron finds 0 remaining.
**Root cause S2-B**: Same Gemini RPD pattern.
**Fix**: `call_mistral_fallback()` added to all three after primary exhausted.
**Confirmed**: S3-B contributing (Historical Parallel section in S3 docx) ✅

### Fix 9 — Watch/Clarity/Verification dedup (commit 8d13ba5) ✅
**Root cause**: No "already wrote today" guard. Both run 2x/day → same voice×lens pairs re-inserted every run → 104 rows accumulated (88 duplicates).
**Cascade**: Watch duplicates → Clarity sees 49+ unreviewed LOW findings → generates clarity row for each → exponential growth.
**Fix A**: Watch — dedup check before insert (ilike phrasing match).
**Fix B**: Clarity — deduplicate watch_voices list by (voice_name, state_actor_lens).
**Fix C**: DB cleanup — deleted 88 duplicate rows, 104→16 clean rows.
**Sibling check**: Verification aggregator got same fix.

### Fix 10 — Dead sources removed (commit 828ac37) ✅
**Process**: Verified URLs → feedparser RSS check → confirmed dead vs broken vs live.
**Removed**: 4×404 (SRC-044/047/048/052) + 8×RSS=0 (SRC-006/007/050/051/062/063/064/065)
**Kept**: SRC-035/036/055/076 (20/10/10/30 RSS entries), SRC-049/075 (403 = blocks scrapers not dead)
**Result**: 78 → 66 sources, all live.

### Built 11 — lens-resume.yml (commit 605ee30) ✅
Manual checkpoint resume workflow. Accepts `run_id` input → passes `LENS_RUN_ID` env to orchestrator which already has `load_checkpoint(run_id)`. Appears in GitHub Actions sidebar.

### Built 12 — S3-F Counter-Check (commit 605ee30) ✅
**Philosophy**: PHI-002 (even our system has blind spots), PHI-003 (apparatus-people separation), PHI-004 (phrased as pattern warrants review).
**Data gate**: S3-A ≥20 runs AND S3-D ≥4 runs (proxy for ~30 days). Returns SKIPPED_INSUFFICIENT_DATA until gate met.
**Provider**: Mistral-small-latest.
**Cadence**: Mon/Thu, same as S3-C.
**Wired**: into lens_s3_orchestrator.py after S3-E.

### Fixed 13 — Forensic Report trigger reset ✅
**Root cause**: GitHub workflow_run event subscription silently dropped after yml modifications. Code was fine (Apr 30 ran perfectly).
**Fix**: Manual trigger → reset the chain → auto-fires on next manage-analyze.
**Confirmed**: #14 manually triggered, 1m49s, 40.7KB delivered, S3 positions=['S3-A','S3-B','S3-C','S3-D','S3-E'] ✅

---

## Failure patterns observed

### "|| true masks everything"
S1 report called with `|| true` → any failure shows green. articles_used bug ran silently for unknown days. No fix applied to `|| true` — removing it would cascade failures. Operator must watch Telegram for docx presence, not just GitHub Actions green.

### "BEV violated mid-session"
Jumped to A/B options for S3-E before reading full file. James caught it. After reading: SambaNova correctly rejected (PHI-002). Rule: BEV is blocking even when fix seems obvious from grep.

### "Pattern Match Bias on 400"
Assumed MISTRAL_API_KEY not in GitHub before checking. James showed screenshot — wrong. Always get `r.text` before diagnosing HTTP errors.

### "Arbitrary timeout is always wrong"
manage-analyze #71 cancelled at 30m26s. LR-097: check yml before dismissing operator concern. Principle: never set time ceilings on pipelines that need to complete naturally.

### "Dedup must be explicit"
Watch/Clarity/Verification ran 2x/day without dedup → 88 duplicate rows in 3 days. PHI-004 cadence design requires once-per-day writing, not once-per-run. Always add dedup check when aggregators write to shared tables.

---

## Rules added this session

| Rule | Description |
|---|---|
| LR-095 | Always log `r.text[:200]` on any HTTP error before diagnosing |
| LR-096 | Never pass raw blob DB columns into AI prompts — check size first |
| LR-097 | Check yml `timeout-minutes` value before dismissing operator concerns |

*(Add to lens-DOC-002_rules.md in LENS-024)*

---

## CLEANUP (LR-093)

**Completed this session:**
- [x] All patch files removed after each fix
- [x] 88 duplicate lens_drift_findings deleted
- [x] 12 dead sources removed from sources.json

**For LENS-024 close:**
- [ ] Update lens-DOC-002_rules.md with LR-095/096/097
- [ ] Verify no test data in lens_entities / lens_entity_mentions
- [ ] Update lens-DOC-001_diary.md
- [ ] Update lens-DOC-004_status.md

---

## Pending → LENS-024

### VERIFY FIRST (check at session open)
- Forensic Report auto-fired on next cron after manual reset? (tonight's AM cron)
- Watch aggregator: 4 findings not 49 after dedup fix?
- S3-F: SKIPPED_INSUFFICIENT_DATA (expected — data gate not met yet)

### IMPORTANT
- T3 steno calibration Article 6 — run at 6-8 AM Thai (Cerebras queue fresh)
- LR-095/096/097 — add to lens-DOC-002_rules.md

### DEFERRED
- S3-F data gate: self-resolving ~20 days (S3-A needs 20 runs)
- S4-B build: July 2026
- Direction A / web app: after S4-B

---

**Session closed**: May 9, 2026 ~03:30 Thai by Sonnet 4.6
**Next session**: LENS-024, Claude Sonnet 4.6 adaptive
