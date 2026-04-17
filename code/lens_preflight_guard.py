"""
lens_preflight_guard.py — Layer 1: Preflight Guard
====================================================
Project Lens | LENS-014 A1

LAYER 1 OF 5-LAYER GUARD SYSTEM:
  Layer 1: PREFLIGHT       (this module — runs before main flight)
  Layer 2: QUOTA           (lens_quota_guard — per LLM call)
  Layer 3: RESPONSE        (lens_response_guard — per LLM response)
  Layer 4: WRITE           (lens_write_guard — before Supabase insert)
  Layer 5: POST-CYCLE      (lens_audit_guard — after cycle completes)

Purpose (operator-taught LENS-014 principle):
  "The main duty of every guard system is not to fail main system."

  Preflight detects invariant violations BEFORE main flight starts.
  No damage done yet, no resources burned. Early detection = cheapest
  intervention point.

Preflight's decision protocol:
  ALL critical checks OK  -> PROCEED  (main flight runs)
  ANY critical check FAIL -> ABORT    (main flight does not start)
  WARN-only failures      -> PROCEED with telemetry

Usage:
  from lens_preflight_guard import run_preflight

  report = run_preflight()
  if not report.should_proceed:
      log.error(report.summary_line())
      for r in report.blocking_failures():
          log.error(r.to_log_line())
      sys.exit(1)  # Do not start main flight
  for r in report.failures():  # includes WARN
      log.warning(r.to_log_line())
  # Main flight proceeds

Safety invariants satisfied:
  - Complete coverage of known preflight-detectable failure modes
  - Fail-safe: guard errors return ABORT, not silent pass
  - Independent: does not import from main system modules that could crash
  - Early detection: runs before ANY expensive operation
  - Self-evident: every check produces readable GuardResult

Authority: LR-074 (guard pattern), operator-taught "safety is the gate"
principle, LENS-014 A1.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Optional

# Common types — zero external deps at module level
from lens_guard_common import (
    GuardScope,
    GuardStatus,
    GuardResult,
    GuardReport,
    safe_check,
)


# ══════════════════════════════════════════════════════════════════════════════
# Critical env vars (main system WILL crash without these)
# ══════════════════════════════════════════════════════════════════════════════
CRITICAL_ENV_VARS = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
    "GROQ_API_KEY",        # main S1 analyzer
]

# Per-position API keys (checked based on which positions will run)
# Note: different keys per position is the LENS-010 quota isolation fix
POSITION_ENV_VARS = {
    "S2-A":    "GROQ_S2_API_KEY",
    "S2-E":    "GROQ_S2E_API_KEY",
    "MA":      "GROQ_MA_API_KEY",
    "lens_3":  "CEREBRAS_API_KEY",
    "lens_2":  "MISTRAL_API_KEY",
    "lens_4":  "SAMBANOVA_API_KEY",
}

# Telemetry env vars — failure is warn, not abort
TELEMETRY_ENV_VARS = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
]


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 1 — Critical env vars present
# ══════════════════════════════════════════════════════════════════════════════
def check_critical_env_vars() -> GuardResult:
    """Verify CRITICAL_ENV_VARS are set and non-empty.

    Failure here = main system cannot start. ABORT.
    """
    missing = []
    for var in CRITICAL_ENV_VARS:
        val = os.environ.get(var, "")
        if not val:
            missing.append(var)

    if missing:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="critical_env_vars",
            message=f"missing critical env vars: {missing}",
            details={"missing": missing, "required": CRITICAL_ENV_VARS},
        )

    return GuardResult(
        scope=GuardScope.PREFLIGHT,
        status=GuardStatus.OK,
        check_name="critical_env_vars",
        message=f"all {len(CRITICAL_ENV_VARS)} critical vars present",
        details={"checked": CRITICAL_ENV_VARS},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 2 — Position-specific API keys
# ══════════════════════════════════════════════════════════════════════════════
def check_position_api_keys(positions: Optional[list[str]] = None) -> GuardResult:
    """Verify API keys for specified positions are set.

    If positions=None, checks all known positions. Failure is ABORT for
    any position listed — main flight cannot call LLM without key.

    Args:
        positions: List of position names to check (e.g., ['S2-A', 'MA']).
                   None = check all.
    """
    to_check = positions if positions is not None else list(POSITION_ENV_VARS.keys())
    missing = {}
    for pos in to_check:
        env_var = POSITION_ENV_VARS.get(pos)
        if env_var is None:
            continue  # unknown position — not our job to validate
        val = os.environ.get(env_var, "")
        if not val:
            missing[pos] = env_var

    if missing:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="position_api_keys",
            message=f"missing API keys for positions: {list(missing.keys())}",
            details={"missing": missing, "positions_checked": to_check},
        )

    return GuardResult(
        scope=GuardScope.PREFLIGHT,
        status=GuardStatus.OK,
        check_name="position_api_keys",
        message=f"all {len(to_check)} position API keys present",
        details={"positions_checked": to_check},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 3 — Telemetry env vars (non-blocking)
# ══════════════════════════════════════════════════════════════════════════════
def check_telemetry_env_vars() -> GuardResult:
    """Verify Telegram telemetry env vars.

    Failure here = alerts won't be sent. Main system still runs.
    Returns WARN, not ABORT.
    """
    missing = []
    for var in TELEMETRY_ENV_VARS:
        val = os.environ.get(var, "")
        if not val:
            missing.append(var)

    if missing:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.WARN,
            check_name="telemetry_env_vars",
            message=f"telemetry degraded: missing {missing}",
            details={"missing": missing, "impact": "telegram_alerts_disabled"},
        )

    return GuardResult(
        scope=GuardScope.PREFLIGHT,
        status=GuardStatus.OK,
        check_name="telemetry_env_vars",
        message="telemetry fully configured",
        details={"checked": TELEMETRY_ENV_VARS},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 4 — tzdata installed (required for lens_cycle.py zoneinfo)
# ══════════════════════════════════════════════════════════════════════════════
def check_tzdata() -> GuardResult:
    """Verify Python zoneinfo can load America/New_York.

    Required for lens_cycle.format_cycle_display(). Without tzdata,
    any lens_cycle import fails with ZoneInfoNotFoundError.
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        # Actually use it to confirm tzdata present, not just the API
        now = datetime.now(tz)
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.OK,
            check_name="tzdata_available",
            message=f"tzdata OK (current DC time: {now.strftime('%H:%M %Z')})",
            details={"timezone_tested": "America/New_York"},
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="tzdata_available",
            message=f"tzdata missing or broken: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__, "fix": "pip install tzdata"},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 5 — Supabase reachable
# ══════════════════════════════════════════════════════════════════════════════
def check_supabase_reachable() -> GuardResult:
    """Verify Supabase is reachable and we can authenticate.

    Tries a trivial read on a known table (lens_reports, limit 1).
    Uses service key — if that fails, auth or network is broken.
    """
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="supabase_reachable",
            message="cannot check: SUPABASE_URL or SUPABASE_SERVICE_KEY missing",
            details={"url_present": bool(url), "key_present": bool(key)},
        )

    try:
        # Import locally — if supabase library not installed, fail gracefully
        from supabase import create_client
        sb = create_client(url, key)
        r = sb.table("lens_reports").select("id").limit(1).execute()
        # r.data will be a list (possibly empty); if we got here, connection works
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.OK,
            check_name="supabase_reachable",
            message="Supabase reachable and authenticated",
            details={"test_table": "lens_reports", "rows_sampled": len(r.data or [])},
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="supabase_reachable",
            message=f"Supabase check failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__, "exception_msg": str(e)[:500]},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 6 — Cycle / schedule alignment (uses lens_cycle)
# ══════════════════════════════════════════════════════════════════════════════
def check_cycle_alignment() -> GuardResult:
    """Verify current time aligns with a canonical cycle OR acknowledge manual.

    This is INFORMATIONAL, not blocking. The main system will write
    whatever get_cycle() returns. Preflight just surfaces what that
    label will be.

    WARN if 'manual' (pipeline stage running off-anchor — e.g. fetch_text
    at 01:00 UTC will land as 'manual', not '2of1'). ABORT never
    triggered — preflight cannot heal this.
    """
    try:
        from lens_cycle import get_cycle, format_cycle_display
        now = datetime.now(timezone.utc)
        cycle = get_cycle(now)
        display = format_cycle_display(cycle, now)

        if cycle in ("2of1", "2of2"):
            return GuardResult(
                scope=GuardScope.PREFLIGHT,
                status=GuardStatus.OK,
                check_name="cycle_alignment",
                message=display,
                details={"cycle": cycle, "utc_time": now.isoformat()},
            )
        else:
            # 'manual' — pipeline is running off-anchor
            return GuardResult(
                scope=GuardScope.PREFLIGHT,
                status=GuardStatus.WARN,
                check_name="cycle_alignment",
                message=f"off-anchor: {display} (will write cycle='manual')",
                details={
                    "cycle": cycle,
                    "utc_time": now.isoformat(),
                    "note": "Pipeline stage firing outside canonical anchor window "
                            "(2of1=01:28 UTC ±15min, 2of2=13:28 UTC ±15min). "
                            "This is expected for staged pipeline workflows like "
                            "lens-collect (01:00), lens-gdelt (01:45), etc.",
                },
            )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ERROR,
            check_name="cycle_alignment",
            message=f"cycle check crashed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 7 — lens_cycle module itself importable + canonical
# ══════════════════════════════════════════════════════════════════════════════
def check_lens_cycle_module() -> GuardResult:
    """Verify lens_cycle module imports cleanly and exposes canonical API.

    A sanity check that O1 integration worked. If anyone deleted or
    broke lens_cycle, this catches it before main flight crashes.
    """
    try:
        from lens_cycle import (
            get_cycle, format_cycle_display, CANONICAL_CYCLES,
            LEGACY_CYCLES, DAY_1_UTC,
        )
        if CANONICAL_CYCLES != ["2of1", "2of2"]:
            return GuardResult(
                scope=GuardScope.PREFLIGHT,
                status=GuardStatus.ABORT,
                check_name="lens_cycle_module",
                message=f"CANONICAL_CYCLES drift: {CANONICAL_CYCLES}",
                details={"expected": ["2of1", "2of2"], "actual": CANONICAL_CYCLES},
            )
        # Smoke: call the function
        result = get_cycle()
        if result not in ("2of1", "2of2", "manual"):
            return GuardResult(
                scope=GuardScope.PREFLIGHT,
                status=GuardStatus.ABORT,
                check_name="lens_cycle_module",
                message=f"get_cycle() returned unexpected value: {result!r}",
                details={"valid_values": ["2of1", "2of2", "manual"]},
            )
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.OK,
            check_name="lens_cycle_module",
            message=f"lens_cycle OK, current cycle={result}",
            details={"canonical_cycles": CANONICAL_CYCLES},
        )
    except Exception as e:
        return GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.ABORT,
            check_name="lens_cycle_module",
            message=f"lens_cycle import/call failed: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__},
        )


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — run all checks and produce a report
# ══════════════════════════════════════════════════════════════════════════════
def run_preflight(positions: Optional[list[str]] = None) -> GuardReport:
    """Run all preflight checks and return an aggregated report.

    Each check is wrapped in safe_check() so no check can crash the
    preflight system. If a check itself errors, it becomes an ERROR
    result (treated as ABORT by should_proceed).

    Args:
        positions: Which positions will run (for API key check).
                   None = check all.

    Returns:
        GuardReport with all check results. Caller inspects:
          - report.should_proceed: bool (True iff safe to proceed)
          - report.blocking_failures(): list of blocking fails
          - report.failures(): all non-OK results (incl. WARN)
          - report.summary_line(): one-line roll-up
    """
    report = GuardReport(scope=GuardScope.PREFLIGHT)

    # Order matters: env vars first (cheap), then tzdata, then
    # Supabase (network), then cycle (depends on tzdata + lens_cycle)
    report.add(safe_check(
        GuardScope.PREFLIGHT, "critical_env_vars", check_critical_env_vars))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "position_api_keys",
        lambda: check_position_api_keys(positions)))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "telemetry_env_vars", check_telemetry_env_vars))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "tzdata_available", check_tzdata))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "lens_cycle_module", check_lens_cycle_module))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "cycle_alignment", check_cycle_alignment))

    report.add(safe_check(
        GuardScope.PREFLIGHT, "supabase_reachable", check_supabase_reachable))

    return report


# ══════════════════════════════════════════════════════════════════════════════
# CLI — allow running preflight standalone as a diagnostic
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Load .env if python-dotenv available (for local diagnostics)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    report = run_preflight()

    print("=" * 72)
    print("PROJECT LENS PREFLIGHT GUARD — Layer 1 of 5")
    print("=" * 72)
    print()
    for r in report.results:
        print(f"  {r.to_log_line()}")
    print()
    print("-" * 72)
    print(report.summary_line())
    print("-" * 72)

    if report.should_proceed:
        print()
        print("DECISION: PROCEED (main flight cleared for departure)")
        sys.exit(0)
    else:
        print()
        print("DECISION: ABORT (main flight must NOT start)")
        print()
        print("Blocking failures:")
        for r in report.blocking_failures():
            print(f"  - {r.check_name}: {r.message}")
            if r.details:
                for k, v in r.details.items():
                    print(f"      {k}: {v}")
        sys.exit(1)
