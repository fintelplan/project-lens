"""
lens_write_guard.py — Layer 4: Write Guard
=============================================
Project Lens | LENS-014 A1

LAYER 4 OF 5-LAYER GUARD SYSTEM:
  Layer 1: PREFLIGHT       (lens_preflight_guard — before cycle)
  Layer 2: QUOTA           (lens_quota_guard — before LLM call)
  Layer 3: RESPONSE        (lens_response_guard — after LLM response)
  Layer 4: WRITE           (this module — before Supabase insert)
  Layer 5: POST-CYCLE      (lens_audit_guard — after cycle)

Purpose (operator-taught LENS-014 principle):
  "The main duty of every guard system is not to fail main system."

  Write guard validates rows BEFORE they hit Supabase. Catches garbage
  data (F2 in failure taxonomy) at the gate, not after corruption.

Scope (P0 — critical-path tables):
  - lens_reports          S1 analysis output (one per cycle)
  - lens_run_checkpoints  Orchestrator state (checkpoint + resume)

Deferred to LENS-015 (P1/P2 tables):
  - injection_reports, lens_system3_reports, lens_macro_reports
  - lens_quota_ledger, lens_pipeline_runs, lens_escalations
  - lens_source_health, lens_run_meta, lens_article_refs
  - lens_sources, lens_tiercd_data, lens_raw_articles, lens_predictions

Usage:
  from lens_write_guard import validate_write

  result = validate_write("lens_reports", row_dict)
  if not result.status.is_proceed:
      log.error(result.to_log_line())
      return  # Don't write; main system handles gracefully
  # Write is safe
  sb.table("lens_reports").insert(row_dict).execute()

Safety invariants satisfied:
  - Stateless: no DB query, no side effects
  - Fail-safe: schema mismatch = ABORT (don't write)
  - Bounded: only P0 tables; unknown tables return WARN (permit write)
  - Self-evident: every rejection includes field + reason
  - Never raises: caller gets GuardResult no matter what

Authority: LR-074 (guard pattern), LR-071 (schema truth), LENS-014 A1.
"""
from __future__ import annotations

from typing import Any, Optional

from lens_guard_common import (
    GuardScope,
    GuardStatus,
    GuardResult,
)


# ══════════════════════════════════════════════════════════════════════════════
# Canonical cycle values (keep in sync with lens_cycle.py)
# We import lazily to avoid circular dep risk; import inside functions.
# ══════════════════════════════════════════════════════════════════════════════
_VALID_CYCLES = ("2of1", "2of2", "manual",
                 # Legacy values still permitted for historical writes
                 # (e.g., backfills). Live crons should write canonical only.
                 "morning", "afternoon", "evening", "midnight",
                 "midday", "night")


# ══════════════════════════════════════════════════════════════════════════════
# Schema definitions — one per P0 table
# ══════════════════════════════════════════════════════════════════════════════

SCHEMAS: dict[str, dict[str, Any]] = {
    # ── lens_reports ─────────────────────────────────────────────────────────
    # S1 analysis output. One row per cron cycle. Heavy read by S2/S3/MA.
    "lens_reports": {
        "required": [
            "cycle",         # '2of1' | '2of2' | 'manual' | legacy
            "domain_focus",  # e.g., 'ALL', 'FINANCE', etc.
            "summary",       # main analysis text
            "generated_at",  # ISO timestamp
        ],
        "optional": [
            "id", "food_for_thought", "signals_used", "articles_used",
            "ai_model", "prompt_version", "quality_score", "status",
            "system", "protected", "injection_assumed",
        ],
        "types": {
            "cycle": str,
            "domain_focus": str,
            "summary": str,
            "generated_at": str,
            "food_for_thought": str,
            "ai_model": str,
            "prompt_version": str,
            "quality_score": (int, float),
            "status": str,
            "system": str,
            "protected": bool,
            "injection_assumed": bool,
            "articles_used": int,
        },
        "enums": {
            "cycle": _VALID_CYCLES,
            # status/system enums not locked here — tolerant for now
        },
        "bounds": {
            "quality_score": (0.0, 10.0),  # seen 0-10 range in codebase
        },
        "min_lengths": {
            "summary": 10,  # defend against empty-string silent fills
        },
    },

    # ── lens_run_checkpoints ─────────────────────────────────────────────────
    # Orchestrator state for checkpoint+resume. Corruption = stuck cycles.
    "lens_run_checkpoints": {
        "required": [
            "run_id",
            "cycle",
            "job_count",
            "resume_from",
        ],
        "optional": [
            "lens_1_status", "lens_2_status", "lens_3_status",
            "lens_4_status", "article_ids", "completed_at",
        ],
        "types": {
            "run_id": str,
            "cycle": str,
            "job_count": int,
            "resume_from": int,
            "lens_1_status": str,
            "lens_2_status": str,
            "lens_3_status": str,
            "lens_4_status": str,
            "article_ids": str,   # JSON-encoded list
        },
        "enums": {
            "cycle": _VALID_CYCLES,
        },
        "bounds": {
            "job_count":   (0, 100),   # seen MAX_JOBS=3 in orchestrator; 100 generous
            "resume_from": (1, 5),     # lens 1-4 + completion sentinel 5
        },
        "min_lengths": {
            "run_id": 3,
        },
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# Core validator
# ══════════════════════════════════════════════════════════════════════════════
def validate_write(table: str, row: Any) -> GuardResult:
    """Validate a row against its table schema before Supabase insert.

    Args:
        table: Supabase table name (e.g., 'lens_reports').
        row:   The dict to be inserted.

    Returns:
        GuardResult.
          status=OK    -> safe to write
          status=ABORT -> do NOT write (schema violation)
          status=WARN  -> permit write (unknown table, relax rules)
          status=ERROR -> guard itself confused (treat as ABORT per safety)

    Never raises.
    """
    # ── Shape checks (before touching schema) ────────────────────────────────
    if row is None:
        return GuardResult(
            scope=GuardScope.WRITE,
            status=GuardStatus.ABORT,
            check_name=f"write:{table}",
            message="row is None",
            details={"table": table},
        )
    if not isinstance(row, dict):
        return GuardResult(
            scope=GuardScope.WRITE,
            status=GuardStatus.ABORT,
            check_name=f"write:{table}",
            message=f"row is {type(row).__name__}, expected dict",
            details={"table": table, "actual_type": type(row).__name__},
        )

    # ── Unknown table — WARN, permit (don't block main system on P1/P2) ─────
    if table not in SCHEMAS:
        return GuardResult(
            scope=GuardScope.WRITE,
            status=GuardStatus.WARN,
            check_name=f"write:{table}",
            message=f"no schema registered — write permitted unchecked",
            details={"table": table, "note": "Add schema to SCHEMAS dict in LENS-015"},
        )

    schema = SCHEMAS[table]
    errors: list[str] = []

    # ── Required fields ──────────────────────────────────────────────────────
    for field in schema.get("required", []):
        if field not in row:
            errors.append(f"missing required field '{field}'")
            continue
        val = row[field]
        if val is None:
            errors.append(f"required field '{field}' is None")
        elif isinstance(val, str) and not val.strip():
            errors.append(f"required field '{field}' is empty string")

    # ── Type checks (only on present non-None values) ────────────────────────
    for field, expected_type in schema.get("types", {}).items():
        if field in row and row[field] is not None:
            if not isinstance(row[field], expected_type):
                actual = type(row[field]).__name__
                if isinstance(expected_type, tuple):
                    expected = " or ".join(t.__name__ for t in expected_type)
                else:
                    expected = expected_type.__name__
                errors.append(
                    f"field '{field}': type {actual}, expected {expected}"
                )

    # ── Enum checks ──────────────────────────────────────────────────────────
    for field, allowed in schema.get("enums", {}).items():
        if field in row and row[field] is not None:
            if row[field] not in allowed:
                errors.append(
                    f"field '{field}': value {row[field]!r} not in allowed set"
                )

    # ── Bounded numeric fields ───────────────────────────────────────────────
    for field, (lo, hi) in schema.get("bounds", {}).items():
        if field in row and isinstance(row[field], (int, float)):
            v = row[field]
            if v < lo or v > hi:
                errors.append(
                    f"field '{field}': value {v} out of bounds [{lo}, {hi}]"
                )

    # ── Minimum string lengths (defends against empty-string silent fill) ───
    for field, min_len in schema.get("min_lengths", {}).items():
        if field in row and isinstance(row[field], str):
            actual_len = len(row[field].strip())
            if actual_len < min_len:
                errors.append(
                    f"field '{field}': length {actual_len}, minimum {min_len}"
                )

    # ── Verdict ──────────────────────────────────────────────────────────────
    if errors:
        return GuardResult(
            scope=GuardScope.WRITE,
            status=GuardStatus.ABORT,
            check_name=f"write:{table}",
            message=f"{len(errors)} schema violation(s): {errors[0]}"
                    + (f" (+{len(errors)-1} more)" if len(errors) > 1 else ""),
            details={
                "table": table,
                "errors": errors,
                "row_keys": list(row.keys()),
            },
        )

    return GuardResult(
        scope=GuardScope.WRITE,
        status=GuardStatus.OK,
        check_name=f"write:{table}",
        message=f"schema OK ({len(row)} fields)",
        details={"table": table},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Convenience — batch validate for lists of rows
# ══════════════════════════════════════════════════════════════════════════════
def validate_write_batch(table: str, rows: list) -> list[GuardResult]:
    """Validate multiple rows against a single table's schema.

    Returns a list of GuardResult, one per row. Caller decides whether
    to permit batch write (all OK?) or drop failed rows.
    """
    if not isinstance(rows, list):
        return [GuardResult(
            scope=GuardScope.WRITE,
            status=GuardStatus.ABORT,
            check_name=f"write_batch:{table}",
            message=f"rows is {type(rows).__name__}, expected list",
            details={"table": table},
        )]
    return [validate_write(table, row) for row in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Summary — list tables currently covered
# ══════════════════════════════════════════════════════════════════════════════
def list_covered_tables() -> list[str]:
    """Return tables with schemas registered. Useful for audit/tests."""
    return sorted(SCHEMAS.keys())
