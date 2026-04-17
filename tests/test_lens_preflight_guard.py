"""
test_lens_preflight_guard.py — Validation suite for lens_preflight_guard
==========================================================================
Project Lens | LENS-014 A1

Tests for Layer 1 preflight guard. Uses unittest.mock to control
os.environ and sidecar imports without affecting the real environment.

Follows convention: bare pytest-style classes, plain asserts,
custom run_all_tests() with hardcoded test_classes list.
"""
from __future__ import annotations
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_guard_common import GuardScope, GuardStatus, GuardResult, GuardReport
from lens_preflight_guard import (
    check_critical_env_vars,
    check_position_api_keys,
    check_telemetry_env_vars,
    check_tzdata,
    check_lens_cycle_module,
    check_cycle_alignment,
    check_supabase_reachable,
    run_preflight,
    CRITICAL_ENV_VARS,
    POSITION_ENV_VARS,
    TELEMETRY_ENV_VARS,
)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
def _full_env():
    """Return a dict with all known env vars set to dummy values."""
    env = {var: "dummy" for var in CRITICAL_ENV_VARS}
    for v in POSITION_ENV_VARS.values():
        env[v] = "dummy"
    for v in TELEMETRY_ENV_VARS:
        env[v] = "dummy"
    return env


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — Critical env vars check
# ══════════════════════════════════════════════════════════════════════════════
class TestCriticalEnvVars:

    def test_01_all_present(self):
        with patch.dict(os.environ, _full_env(), clear=True):
            r = check_critical_env_vars()
            assert r.status == GuardStatus.OK
            assert r.scope == GuardScope.PREFLIGHT

    def test_02_missing_supabase_url(self):
        env = _full_env()
        del env["SUPABASE_URL"]
        with patch.dict(os.environ, env, clear=True):
            r = check_critical_env_vars()
            assert r.status == GuardStatus.ABORT
            assert "SUPABASE_URL" in str(r.details.get("missing", []))

    def test_03_missing_groq_key(self):
        env = _full_env()
        del env["GROQ_API_KEY"]
        with patch.dict(os.environ, env, clear=True):
            r = check_critical_env_vars()
            assert r.status == GuardStatus.ABORT
            assert "GROQ_API_KEY" in str(r.details.get("missing", []))

    def test_04_empty_string_counts_as_missing(self):
        env = _full_env()
        env["SUPABASE_SERVICE_KEY"] = ""
        with patch.dict(os.environ, env, clear=True):
            r = check_critical_env_vars()
            assert r.status == GuardStatus.ABORT
            assert "SUPABASE_SERVICE_KEY" in str(r.details.get("missing", []))

    def test_05_all_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            r = check_critical_env_vars()
            assert r.status == GuardStatus.ABORT
            assert len(r.details.get("missing", [])) == len(CRITICAL_ENV_VARS)


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — Position API keys check
# ══════════════════════════════════════════════════════════════════════════════
class TestPositionApiKeys:

    def test_10_all_positions_have_keys(self):
        with patch.dict(os.environ, _full_env(), clear=True):
            r = check_position_api_keys()
            assert r.status == GuardStatus.OK

    def test_11_scoped_check_passes_when_scoped_keys_present(self):
        # Only check for S2-A (needs GROQ_S2_API_KEY)
        env = {"GROQ_S2_API_KEY": "dummy"}
        with patch.dict(os.environ, env, clear=True):
            r = check_position_api_keys(positions=["S2-A"])
            assert r.status == GuardStatus.OK

    def test_12_scoped_check_fails_for_missing(self):
        # Ask for MA but don't provide its key
        env = {"GROQ_API_KEY": "dummy"}  # has critical but not MA-specific
        with patch.dict(os.environ, env, clear=True):
            r = check_position_api_keys(positions=["MA"])
            assert r.status == GuardStatus.ABORT

    def test_13_unknown_position_silently_skipped(self):
        # Unknown position is not our job to fail on
        with patch.dict(os.environ, _full_env(), clear=True):
            r = check_position_api_keys(positions=["S9-Z-UNKNOWN"])
            assert r.status == GuardStatus.OK


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — Telemetry check (WARN-only)
# ══════════════════════════════════════════════════════════════════════════════
class TestTelemetryEnvVars:

    def test_20_present(self):
        with patch.dict(os.environ, _full_env(), clear=True):
            r = check_telemetry_env_vars()
            assert r.status == GuardStatus.OK

    def test_21_missing_is_warn_not_abort(self):
        env = _full_env()
        del env["TELEGRAM_BOT_TOKEN"]
        with patch.dict(os.environ, env, clear=True):
            r = check_telemetry_env_vars()
            assert r.status == GuardStatus.WARN
            # Critical: telemetry failure does NOT block main flight
            assert r.status.is_proceed is True


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — tzdata check
# ══════════════════════════════════════════════════════════════════════════════
class TestTzData:

    def test_30_available(self):
        # Should be installed in test environment
        r = check_tzdata()
        assert r.status == GuardStatus.OK
        assert "EDT" in r.message or "EST" in r.message


# ══════════════════════════════════════════════════════════════════════════════
# Test class 5 — lens_cycle module check
# ══════════════════════════════════════════════════════════════════════════════
class TestLensCycleModule:

    def test_40_importable_and_canonical(self):
        r = check_lens_cycle_module()
        assert r.status == GuardStatus.OK
        assert r.details.get("canonical_cycles") == ["2of1", "2of2"]


# ══════════════════════════════════════════════════════════════════════════════
# Test class 6 — Cycle alignment (informational)
# ══════════════════════════════════════════════════════════════════════════════
class TestCycleAlignment:

    def test_50_returns_result(self):
        # Doesn't matter if OK or WARN, just that it returns a valid result
        r = check_cycle_alignment()
        assert isinstance(r, GuardResult)
        assert r.scope == GuardScope.PREFLIGHT
        assert r.status in (GuardStatus.OK, GuardStatus.WARN)

    def test_51_never_aborts(self):
        # By design, cycle_alignment never ABORTs — it's informational
        r = check_cycle_alignment()
        assert r.status != GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 7 — run_preflight orchestrator
# ══════════════════════════════════════════════════════════════════════════════
class TestRunPreflight:

    def test_60_returns_report(self):
        # Just verify it returns a GuardReport; actual checks verified elsewhere
        report = run_preflight()
        assert isinstance(report, GuardReport)
        assert report.scope == GuardScope.PREFLIGHT

    def test_61_all_checks_present(self):
        # At minimum, 7 checks should be in the report
        report = run_preflight()
        assert len(report.results) >= 7
        check_names = [r.check_name for r in report.results]
        assert "critical_env_vars" in check_names
        assert "position_api_keys" in check_names
        assert "telemetry_env_vars" in check_names
        assert "tzdata_available" in check_names
        assert "lens_cycle_module" in check_names
        assert "cycle_alignment" in check_names
        assert "supabase_reachable" in check_names

    def test_62_scoped_positions_forwarded(self):
        # When positions passed, only those checked
        report = run_preflight(positions=["S2-A"])
        pos_result = next(r for r in report.results
                          if r.check_name == "position_api_keys")
        checked = pos_result.details.get("positions_checked", [])
        assert checked == ["S2-A"]

    def test_63_no_env_returns_abort_overall(self):
        with patch.dict(os.environ, {}, clear=True):
            report = run_preflight()
            # Missing critical env vars should trigger ABORT somewhere
            assert report.should_proceed is False


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [
        TestCriticalEnvVars,
        TestPositionApiKeys,
        TestTelemetryEnvVars,
        TestTzData,
        TestLensCycleModule,
        TestCycleAlignment,
        TestRunPreflight,
    ]
    for cls in test_classes:
        instance = cls()
        methods = [m for m in dir(instance) if m.startswith("test_")]
        for m in methods:
            try:
                getattr(instance, m)()
                print(f"  [PASS] {cls.__name__}.{m}")
                passed += 1
            except Exception as e:
                print(f"  [FAIL] {cls.__name__}.{m}: {e}")
                failures.append((cls.__name__, m, traceback.format_exc()))
                failed += 1
    print("\n" + "=" * 72)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 72)
    if failures:
        print("\nFAILURE DETAILS:\n")
        for cls_name, m, tb in failures:
            print(f"--- {cls_name}.{m} ---")
            print(tb)
            print()
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
