"""
test_lens_guard_common.py — Validation suite for lens_guard_common
===================================================================
Project Lens | LENS-014 A1

Tests for the shared foundation used by all guards.
Follows test_lens_cycle.py + test_lens_quota_guard.py convention.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_guard_common import (
    GuardScope,
    GuardStatus,
    GuardResult,
    GuardReport,
    safe_check,
)


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — Enum coverage
# ══════════════════════════════════════════════════════════════════════════════
class TestEnums:

    def test_01_scope_has_five_values(self):
        scopes = [s.value for s in GuardScope]
        assert "preflight" in scopes
        assert "quota" in scopes
        assert "response" in scopes
        assert "write" in scopes
        assert "audit" in scopes
        assert len(scopes) == 5

    def test_02_status_has_six_values(self):
        statuses = [s.value for s in GuardStatus]
        assert "ok" in statuses
        assert "heal_success" in statuses
        assert "heal_failed" in statuses
        assert "abort" in statuses
        assert "warn" in statuses
        assert "error" in statuses
        assert len(statuses) == 6

    def test_03_ok_is_proceed(self):
        assert GuardStatus.OK.is_proceed is True
        assert GuardStatus.OK.is_blocking is False

    def test_04_heal_success_is_proceed(self):
        assert GuardStatus.HEAL_SUCCESS.is_proceed is True
        assert GuardStatus.HEAL_SUCCESS.is_blocking is False

    def test_05_warn_is_proceed(self):
        # WARN permits proceed (non-blocking)
        assert GuardStatus.WARN.is_proceed is True
        assert GuardStatus.WARN.is_blocking is False

    def test_06_abort_is_blocking(self):
        assert GuardStatus.ABORT.is_proceed is False
        assert GuardStatus.ABORT.is_blocking is True

    def test_07_heal_failed_is_blocking(self):
        assert GuardStatus.HEAL_FAILED.is_proceed is False
        assert GuardStatus.HEAL_FAILED.is_blocking is True

    def test_08_error_is_blocking(self):
        # Fail-safe: guard errors treated as blocking
        assert GuardStatus.ERROR.is_proceed is False
        assert GuardStatus.ERROR.is_blocking is True


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — GuardResult
# ══════════════════════════════════════════════════════════════════════════════
class TestGuardResult:

    def test_10_basic_construction(self):
        r = GuardResult(
            scope=GuardScope.PREFLIGHT,
            status=GuardStatus.OK,
            check_name="x",
            message="y"
        )
        assert r.scope == GuardScope.PREFLIGHT
        assert r.status == GuardStatus.OK
        assert r.check_name == "x"
        assert r.message == "y"
        assert r.details == {}
        assert r.heal_attempted is False

    def test_11_details_can_be_populated(self):
        r = GuardResult(
            scope=GuardScope.QUOTA, status=GuardStatus.WARN,
            check_name="x", message="y", details={"count": 5},
        )
        assert r.details["count"] == 5

    def test_12_to_log_line_format(self):
        r = GuardResult(
            scope=GuardScope.AUDIT, status=GuardStatus.OK,
            check_name="report_exists", message="63 reports"
        )
        line = r.to_log_line()
        assert "[audit/ok]" in line
        assert "report_exists" in line
        assert "63 reports" in line

    def test_13_to_audit_dict_serializes(self):
        r = GuardResult(
            scope=GuardScope.WRITE, status=GuardStatus.ABORT,
            check_name="x", message="y", details={"k": "v"},
            heal_attempted=True,
        )
        d = r.to_audit_dict()
        assert d["scope"] == "write"
        assert d["status"] == "abort"
        assert d["heal_attempted"] is True
        assert d["details"] == {"k": "v"}


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — GuardReport
# ══════════════════════════════════════════════════════════════════════════════
class TestGuardReport:

    def test_20_empty_report_is_ok(self):
        r = GuardReport(scope=GuardScope.PREFLIGHT)
        assert r.overall_status == GuardStatus.OK
        assert r.should_proceed is True
        assert r.failures() == []
        assert r.blocking_failures() == []

    def test_21_all_ok_proceeds(self):
        r = GuardReport(scope=GuardScope.PREFLIGHT)
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "a", "ok"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "b", "ok"))
        assert r.should_proceed is True

    def test_22_abort_blocks(self):
        r = GuardReport(scope=GuardScope.PREFLIGHT)
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "a", "ok"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.ABORT, "b", "bad"))
        assert r.should_proceed is False
        assert r.overall_status == GuardStatus.ABORT
        assert len(r.blocking_failures()) == 1

    def test_23_error_takes_precedence(self):
        # ERROR is worst-case (fail-safe)
        r = GuardReport(scope=GuardScope.PREFLIGHT)
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.ABORT, "a", "x"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.ERROR, "b", "y"))
        assert r.overall_status == GuardStatus.ERROR

    def test_24_warn_allows_proceed(self):
        r = GuardReport(scope=GuardScope.AUDIT)
        r.add(GuardResult(GuardScope.AUDIT, GuardStatus.WARN, "a", "x"))
        r.add(GuardResult(GuardScope.AUDIT, GuardStatus.OK, "b", "y"))
        assert r.should_proceed is True
        assert r.overall_status == GuardStatus.WARN
        assert len(r.failures()) == 1  # WARN counts as failure (non-OK)
        assert len(r.blocking_failures()) == 0  # but not blocking

    def test_25_summary_line_counts(self):
        r = GuardReport(scope=GuardScope.PREFLIGHT)
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "a", "x"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "b", "y"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.WARN, "c", "z"))
        r.add(GuardResult(GuardScope.PREFLIGHT, GuardStatus.ABORT, "d", "w"))
        line = r.summary_line()
        assert "2/4 ok" in line
        assert "1 warn" in line
        assert "1 blocking" in line


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — safe_check
# ══════════════════════════════════════════════════════════════════════════════
class TestSafeCheck:

    def test_30_normal_result_returned(self):
        def good():
            return GuardResult(GuardScope.PREFLIGHT, GuardStatus.OK, "x", "y")
        r = safe_check(GuardScope.PREFLIGHT, "x", good)
        assert r.status == GuardStatus.OK

    def test_31_exception_becomes_error(self):
        def crash():
            raise ValueError("boom")
        r = safe_check(GuardScope.PREFLIGHT, "crash_test", crash)
        assert r.status == GuardStatus.ERROR
        assert "ValueError" in r.message
        assert "boom" in r.message

    def test_32_wrong_return_type_becomes_error(self):
        def returns_none():
            return None
        r = safe_check(GuardScope.PREFLIGHT, "none_test", returns_none)
        assert r.status == GuardStatus.ERROR
        assert "NoneType" in r.message

    def test_33_error_preserves_check_name(self):
        def crash():
            raise RuntimeError("nope")
        r = safe_check(GuardScope.AUDIT, "my_check", crash)
        assert r.check_name == "my_check"
        assert r.scope == GuardScope.AUDIT

    def test_34_error_includes_exception_type(self):
        def crash():
            raise KeyError("missing")
        r = safe_check(GuardScope.PREFLIGHT, "x", crash)
        assert r.details.get("exception_type") == "KeyError"


# ══════════════════════════════════════════════════════════════════════════════
# Runner — matches convention
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [TestEnums, TestGuardResult, TestGuardReport, TestSafeCheck]
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
