"""
lens_audit_guard.py — Layer 5: Post-Cycle Audit
=================================================
Project Lens | LENS-014 A1

LAYER 5 OF 5-LAYER GUARD SYSTEM:
  Layer 1: PREFLIGHT       (lens_preflight_guard — before cycle)
  Layer 2: QUOTA           (lens_quota_guard — before LLM call)
  Layer 3: RESPONSE        (lens_response_guard — after LLM response)
  Layer 4: WRITE           (lens_write_guard — before Supabase insert)
  Layer 5: POST-CYCLE      (this module — after cycle completes)

Purpose (operator-taught LENS-014 principle):
  "The main duty of every guard system is not to fail main system."

  Audit guard runs AFTER the cycle completes. It verifies that the
  cycle did what it was supposed to do — no silent failures, no
  orphan state, no unexplained degradation.

  Unlike Layers 1-4 which PREVENT damage, Layer 5 DETECTS damage that
  slipped past the others. Its verdict feeds NEXT cycle's preflight
  as operational telemetry.

Scope — 4 checks covering known post-cycle integrity concerns:
  1. recent_report_exists    Cron ran but wrote no report? (F5 critical)
  2. report_has_content      Cycle ran but all reports are empty? (F4 silent)
  3. source_health_delta     Mass source death between cycles?
  4. orphan_checkpoints      Incomplete cycles piling up?

Severity policy:
  - NON-BLOCKING by design — audit runs POST-cycle, nothing to block
  - Findings flagged as WARN or ABORT for severity signaling only
  - Next preflight reads audit findings as context

Threshold calibration status:
  Every threshold value marked with "LENS-015 CALIBRATE" is a best-
  guess shipped tonight. Values need tuning once LENS-014 O1 produces
  real operational data (from 2of1/2of2 cycles starting Apr 18+).

Usage:
  from lens_audit_guard import run_audit

  report = run_audit(cycle_window_hours=24)
  for r in report.failures():
      log.warning(r.to_log_line())
  # Optionally persist report to lens_audit_findings table for trends

Safety invariants satisfied:
  - Stateless per check (each detect() is independent)
  - Fail-safe: audit errors become ERROR results, not crashes
  - Independent: does not import main system logic modules
  - Never raises into main system: all checks wrapped in safe_check()
  - Non-blocking: findings are information, not gates

Authority: LR-074 (guard pattern), LR-071 (schema truth), LENS-014 A1.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from lens_guard_common import (
    GuardScope,
    GuardStatus,
    GuardResult,
    GuardReport,
    safe_check,
)


# ══════════════════════════════════════════════════════════════════════════════
# Thresholds — ALL MARKED FOR LENS-015 CALIBRATION
# ══════════════════════════════════════════════════════════════════════════════

# A cycle is considered "recent" if completed within this many hours.
# LENS-015 CALIBRATE: initial value chosen assuming 2x/day cron (12h apart).
AUDIT_RECENT_WINDOW_HOURS = 24

# Minimum acceptable summary length for a "not-silent" report.
# Below this, we suspect silent LLM filtering.
# LENS-015 CALIBRATE: 200 chars is placeholder. Real summaries are typically
# 1500-5000 chars. 200 is "obvious silent fail" threshold.
AUDIT_MIN_SUMMARY_CHARS = 200

# Maximum acceptable source death rate between consecutive cycles.
# If more than this fraction of sources went from alive -> dead since last
# cycle, something systemic failed (feed server change, rate limit, etc.).
# LENS-015 CALIBRATE: 0.3 is initial guess. Real death rates unknown yet.
AUDIT_MAX_SOURCE_DEATH_RATE = 0.30

# Maximum acceptable orphan checkpoints (checkpoints without resolution).
# An "orphan" = checkpoint row with no resume and no completion within window.
# LENS-015 CALIBRATE: 2 is permissive (allows 1 current-cycle checkpoint).
AUDIT_MAX_ORPHAN_CHECKPOINTS = 2

# How far back to look for orphan checkpoints (hours).
# LENS-015 CALIBRATE: 48h = 2x daily cycle buffer.
AUDIT_ORPHAN_LOOKBACK_HOURS = 48


# ══════════════════════════════════════════════════════════════════════════════
# Supabase client helper (lazy — deferred import, fail-safe)
# ══════════════════════════════════════════════════════════════════════════════
def _get_supabase_client():
    """Return a Supabase client or None if unavailable. Never raises."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 1 — Recent report exists
# ══════════════════════════════════════════════════════════════════════════════
def check_recent_report_exists(
    window_hours: int = AUDIT_RECENT_WINDOW_HOURS,
) -> GuardResult:
    """Verify at least one lens_reports row was written in the last N hours.

    Detects F5 (orchestration failure): cron fired but no output landed.
    This is the most dangerous silent failure because the pipeline appears
    to have run from GitHub Actions perspective but produced nothing.
    """
    sb = _get_supabase_client()
    if sb is None:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="recent_report_exists",
            message="Supabase unreachable — cannot audit",
            details={"window_hours": window_hours},
        )

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
        r = (sb.table("lens_reports")
             .select("id", count="exact")
             .gte("generated_at", cutoff)
             .execute())
        count = r.count if r.count is not None else len(r.data or [])

        if count == 0:
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.ABORT,
                check_name="recent_report_exists",
                message=f"NO reports in last {window_hours}h — silent pipeline failure suspected",
                details={
                    "window_hours": window_hours,
                    "cutoff_utc": cutoff,
                    "report_count": 0,
                    "likely_cause": "orchestrator crashed or cron didn't fire",
                },
            )

        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.OK,
            check_name="recent_report_exists",
            message=f"{count} reports in last {window_hours}h",
            details={"window_hours": window_hours, "report_count": count},
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="recent_report_exists",
            message=f"audit query failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 2 — Reports have content (not all silently-filtered)
# ══════════════════════════════════════════════════════════════════════════════
def check_report_has_content(
    window_hours: int = AUDIT_RECENT_WINDOW_HOURS,
    min_chars: int = AUDIT_MIN_SUMMARY_CHARS,
) -> GuardResult:
    """Verify recent reports have real content, not empty/silent-filled.

    Detects F4 (silent filtering) at cycle scope: if EVERY report in the
    window has a summary shorter than `min_chars`, something systemic
    silently filtered all LLM output (region block, model issue, etc).
    """
    sb = _get_supabase_client()
    if sb is None:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="report_has_content",
            message="Supabase unreachable — cannot audit",
            details={},
        )

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
        r = (sb.table("lens_reports")
             .select("id,summary,generated_at")
             .gte("generated_at", cutoff)
             .execute())
        rows = r.data or []

        if not rows:
            # No rows to check — covered by Check 1 already. Not our problem.
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.OK,
                check_name="report_has_content",
                message="no reports to audit content (Check 1 handles empty case)",
                details={"window_hours": window_hours},
            )

        short_count = sum(
            1 for row in rows
            if not row.get("summary") or len(str(row["summary"]).strip()) < min_chars
        )
        total = len(rows)
        short_rate = short_count / total if total > 0 else 0

        if short_count == total:
            # 100% silent — definitely systemic
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.ABORT,
                check_name="report_has_content",
                message=f"ALL {total} reports have <{min_chars} char summary — systemic silent filtering",
                details={
                    "total_reports": total,
                    "short_reports": short_count,
                    "min_chars": min_chars,
                    "likely_cause": "LLM silent content filtering or model/region issue",
                },
            )
        elif short_rate > 0.5:
            # More than half — suspicious but not necessarily systemic
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.WARN,
                check_name="report_has_content",
                message=f"{short_count}/{total} reports below {min_chars} chars — elevated silent-fill rate",
                details={
                    "total_reports": total,
                    "short_reports": short_count,
                    "short_rate": round(short_rate, 3),
                    "min_chars": min_chars,
                },
            )
        elif short_count > 0:
            # A few shorts — normal variation
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.OK,
                check_name="report_has_content",
                message=f"{total-short_count}/{total} reports have content ({short_count} short)",
                details={
                    "total_reports": total,
                    "short_reports": short_count,
                },
            )
        else:
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.OK,
                check_name="report_has_content",
                message=f"all {total} reports have content (>={min_chars} chars)",
                details={"total_reports": total},
            )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="report_has_content",
            message=f"audit query failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 3 — Source health delta (mass death detection)
# ══════════════════════════════════════════════════════════════════════════════
def check_source_health_delta(
    window_hours: int = AUDIT_RECENT_WINDOW_HOURS,
    max_death_rate: float = AUDIT_MAX_SOURCE_DEATH_RATE,
) -> GuardResult:
    """Verify sources haven't died en masse between cycles.

    Reads lens_source_health rows from the window. Compares:
      - earliest-in-window dead count
      - latest-in-window dead count
    If death increased beyond threshold, systemic collection issue.

    LENS-015 CALIBRATE: this is a naive per-run comparison. Smarter
    calibration would look at per-source-id state transitions.
    """
    sb = _get_supabase_client()
    if sb is None:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="source_health_delta",
            message="Supabase unreachable — cannot audit",
            details={},
        )

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
        r = (sb.table("lens_source_health")
             .select("source_id,is_dead,run_at")
             .gte("run_at", cutoff)
             .order("run_at")
             .execute())
        rows = r.data or []

        if len(rows) < 2:
            # Not enough data to compare. Not an error — just not enough history.
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.OK,
                check_name="source_health_delta",
                message=f"only {len(rows)} source_health rows in window — insufficient for delta",
                details={"rows_seen": len(rows), "window_hours": window_hours},
            )

        # Naive approach: compare first run batch vs last run batch
        # (LENS-015 CALIBRATE: per-source state transition tracking would be better)
        first_run_at = rows[0]["run_at"]
        last_run_at = rows[-1]["run_at"]

        first_batch = [r for r in rows if r["run_at"] == first_run_at]
        last_batch = [r for r in rows if r["run_at"] == last_run_at]

        if not first_batch or not last_batch:
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.OK,
                check_name="source_health_delta",
                message="insufficient batch data for delta",
                details={},
            )

        first_dead = sum(1 for r in first_batch if r.get("is_dead"))
        last_dead = sum(1 for r in last_batch if r.get("is_dead"))

        death_increase = last_dead - first_dead
        # Guard against division by zero on empty batches
        batch_size = max(len(first_batch), len(last_batch), 1)
        death_rate = death_increase / batch_size

        if death_rate > max_death_rate:
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.WARN,
                check_name="source_health_delta",
                message=f"source death rate {round(death_rate*100, 1)}% exceeds threshold {round(max_death_rate*100, 1)}%",
                details={
                    "first_dead": first_dead,
                    "last_dead": last_dead,
                    "death_increase": death_increase,
                    "batch_size": batch_size,
                    "death_rate": round(death_rate, 3),
                    "max_threshold": max_death_rate,
                    "note": "LENS-015 CALIBRATE: threshold is initial guess",
                },
            )

        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.OK,
            check_name="source_health_delta",
            message=f"source death stable ({first_dead} -> {last_dead} dead)",
            details={
                "first_dead": first_dead,
                "last_dead": last_dead,
                "batch_size": batch_size,
            },
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="source_health_delta",
            message=f"audit query failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 4 — Orphan checkpoints
# ══════════════════════════════════════════════════════════════════════════════
def check_orphan_checkpoints(
    lookback_hours: int = AUDIT_ORPHAN_LOOKBACK_HOURS,
    max_orphans: int = AUDIT_MAX_ORPHAN_CHECKPOINTS,
) -> GuardResult:
    """Verify checkpoints aren't piling up unresolved.

    An "orphan" = checkpoint row where:
      - completed_at IS NULL
      - AND it was started more than one cycle ago

    A small number of orphans = normal (current cycle in progress).
    Too many = orchestrator isn't closing cycles properly.
    """
    sb = _get_supabase_client()
    if sb is None:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="orphan_checkpoints",
            message="Supabase unreachable — cannot audit",
            details={},
        )

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        # Fetch recent checkpoints; we'll determine orphan-ness client-side
        # since Supabase PostgREST null filters vary by version.
        r = (sb.table("lens_run_checkpoints")
             .select("run_id,completed_at,created_at")
             .gte("created_at", cutoff)
             .execute())
        rows = r.data or []

        # Orphan = completed_at missing/null
        orphans = [row for row in rows
                   if row.get("completed_at") is None
                   or row.get("completed_at") == ""]

        if len(orphans) > max_orphans:
            return GuardResult(
                scope=GuardScope.AUDIT,
                status=GuardStatus.WARN,
                check_name="orphan_checkpoints",
                message=f"{len(orphans)} orphan checkpoints (threshold: {max_orphans})",
                details={
                    "orphan_count": len(orphans),
                    "threshold": max_orphans,
                    "lookback_hours": lookback_hours,
                    "orphan_run_ids": [o.get("run_id") for o in orphans[:5]],
                    "note": "LENS-015 CALIBRATE: threshold may need adjustment",
                },
            )

        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.OK,
            check_name="orphan_checkpoints",
            message=f"{len(orphans)} orphan(s) within threshold of {max_orphans}",
            details={
                "orphan_count": len(orphans),
                "total_checkpoints": len(rows),
            },
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.AUDIT,
            status=GuardStatus.ERROR,
            check_name="orphan_checkpoints",
            message=f"audit query failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — run all audit checks
# ══════════════════════════════════════════════════════════════════════════════
def run_audit(
    window_hours: int = AUDIT_RECENT_WINDOW_HOURS,
) -> GuardReport:
    """Run all audit checks and return aggregated report.

    Args:
        window_hours: Lookback window for recency-based checks.

    Returns:
        GuardReport. Audit is NON-BLOCKING by design — caller treats
        findings as telemetry, not as main-flight gates.
    """
    report = GuardReport(scope=GuardScope.AUDIT)

    report.add(safe_check(
        GuardScope.AUDIT, "recent_report_exists",
        lambda: check_recent_report_exists(window_hours=window_hours)))

    report.add(safe_check(
        GuardScope.AUDIT, "report_has_content",
        lambda: check_report_has_content(window_hours=window_hours)))

    report.add(safe_check(
        GuardScope.AUDIT, "source_health_delta",
        lambda: check_source_health_delta(window_hours=window_hours)))

    report.add(safe_check(
        GuardScope.AUDIT, "orphan_checkpoints",
        check_orphan_checkpoints))

    return report


# ══════════════════════════════════════════════════════════════════════════════
# CLI — allow running audit standalone as a diagnostic
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    import sys

    report = run_audit()

    print("=" * 72)
    print("PROJECT LENS AUDIT GUARD — Layer 5 of 5")
    print("=" * 72)
    print()
    for r in report.results:
        print(f"  {r.to_log_line()}")
    print()
    print("-" * 72)
    print(report.summary_line())
    print("-" * 72)
    print()
    print("NOTE: audit findings are telemetry, not gates.")
    print("Thresholds marked 'LENS-015 CALIBRATE' may need tuning.")

    if report.failures():
        print()
        print("Findings:")
        for r in report.failures():
            print(f"  [{r.status.value.upper()}] {r.check_name}: {r.message}")

    # Always exit 0 — audit is non-blocking
    sys.exit(0)
