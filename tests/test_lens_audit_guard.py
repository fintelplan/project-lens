"""
test_lens_audit_guard.py — Validation suite for lens_audit_guard
==================================================================
Project Lens | LENS-014 A1

Tests for Layer 5 audit guard. Uses unittest.mock.patch to replace
_get_supabase_client with a controllable fake, exercising each check
across OK / WARN / ABORT / ERROR paths without hitting real Supabase.
"""
from __future__ import annotations
import os
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_guard_common import GuardScope, GuardStatus, GuardReport
from lens_audit_guard import (
    check_recent_report_exists,
    check_report_has_content,
    check_source_health_delta,
    check_orphan_checkpoints,
    run_audit,
    AUDIT_RECENT_WINDOW_HOURS,
    AUDIT_MIN_SUMMARY_CHARS,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fake Supabase client factory
# ══════════════════════════════════════════════════════════════════════════════
def fake_sb(rows=None, count=None, raise_on_query=False):
    """Build a MagicMock Supabase client with configurable return.

    rows: list to return as r.data
    count: int to return as r.count (when select(count='exact'))
    raise_on_query: if True, execute() raises RuntimeError
    """
    sb = MagicMock()
    # Chain: sb.table("X").select("Y").gte(...).lt(...).order(...).limit(...).execute()
    # All of these return a chainable mock; execute() returns the result.
    result = MagicMock()
    result.data = rows if rows is not None else []
    result.count = count

    # Build a chain that returns itself for any method, ending in execute()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.gte.return_value = chain
    chain.lt.return_value = chain
    chain.order.return_value = chain
    chain.limit.return_value = chain
    chain.eq.return_value = chain
    chain.is_.return_value = chain

    if raise_on_query:
        chain.execute.side_effect = RuntimeError("supabase unreachable")
    else:
        chain.execute.return_value = result

    sb.table.return_value = chain
    return sb


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — check_recent_report_exists
# ══════════════════════════════════════════════════════════════════════════════
class TestRecentReportExists:

    def test_01_reports_present(self):
        sb = fake_sb(rows=[{"id": "a"}], count=1)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_recent_report_exists()
            assert r.status == GuardStatus.OK
            assert r.details["report_count"] == 1

    def test_02_zero_reports_is_abort(self):
        sb = fake_sb(rows=[], count=0)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_recent_report_exists()
            assert r.status == GuardStatus.ABORT
            assert "NO reports" in r.message

    def test_03_sb_unavailable_is_error(self):
        with patch("lens_audit_guard._get_supabase_client", return_value=None):
            r = check_recent_report_exists()
            assert r.status == GuardStatus.ERROR

    def test_04_query_failure_is_error(self):
        sb = fake_sb(raise_on_query=True)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_recent_report_exists()
            assert r.status == GuardStatus.ERROR


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — check_report_has_content
# ══════════════════════════════════════════════════════════════════════════════
class TestReportHasContent:

    def test_10_all_content_full(self):
        rows = [
            {"id": "a", "summary": "x" * 500},
            {"id": "b", "summary": "y" * 500},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.OK

    def test_11_all_summaries_short_is_abort(self):
        rows = [
            {"id": "a", "summary": "short"},
            {"id": "b", "summary": "tiny"},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.ABORT
            assert "ALL" in r.message

    def test_12_majority_short_is_warn(self):
        # 2 short, 1 full = 66% short -> WARN
        rows = [
            {"id": "a", "summary": "short"},
            {"id": "b", "summary": "short"},
            {"id": "c", "summary": "x" * 500},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.WARN

    def test_13_few_short_is_ok(self):
        # 1 short, 2 full -> OK with note
        rows = [
            {"id": "a", "summary": "short"},
            {"id": "b", "summary": "x" * 500},
            {"id": "c", "summary": "y" * 500},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.OK

    def test_14_no_rows_ok(self):
        # No rows means Check 1 handles it; Check 2 returns OK
        sb = fake_sb(rows=[])
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.OK

    def test_15_empty_summary_counts_as_short(self):
        rows = [{"id": "a", "summary": ""}]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_report_has_content()
            assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — check_source_health_delta
# ══════════════════════════════════════════════════════════════════════════════
class TestSourceHealthDelta:

    def test_20_insufficient_data(self):
        # Only one row means no delta possible
        rows = [{"source_id": "a", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"}]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_source_health_delta()
            assert r.status == GuardStatus.OK

    def test_21_stable_death_count(self):
        # Same dead count in first and last batch
        rows = [
            {"source_id": "a", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "b", "is_dead": True,  "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "a", "is_dead": False, "run_at": "2026-04-18T12:00:00+00:00"},
            {"source_id": "b", "is_dead": True,  "run_at": "2026-04-18T12:00:00+00:00"},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_source_health_delta()
            assert r.status == GuardStatus.OK

    def test_22_mass_death_is_warn(self):
        # First batch: 0 dead of 4. Last batch: 3 dead of 4 -> 75% death rate, WARN
        rows = [
            {"source_id": "a", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "b", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "c", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "d", "is_dead": False, "run_at": "2026-04-18T00:00:00+00:00"},
            {"source_id": "a", "is_dead": True,  "run_at": "2026-04-18T12:00:00+00:00"},
            {"source_id": "b", "is_dead": True,  "run_at": "2026-04-18T12:00:00+00:00"},
            {"source_id": "c", "is_dead": True,  "run_at": "2026-04-18T12:00:00+00:00"},
            {"source_id": "d", "is_dead": False, "run_at": "2026-04-18T12:00:00+00:00"},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_source_health_delta()
            assert r.status == GuardStatus.WARN

    def test_23_sb_unavailable(self):
        with patch("lens_audit_guard._get_supabase_client", return_value=None):
            r = check_source_health_delta()
            assert r.status == GuardStatus.ERROR


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — check_orphan_checkpoints
# ══════════════════════════════════════════════════════════════════════════════
class TestOrphanCheckpoints:

    def test_30_no_orphans(self):
        rows = [
            {"run_id": "a", "completed_at": "2026-04-18T01:28:00+00:00", "created_at": "2026-04-18T00:00:00+00:00"},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_orphan_checkpoints()
            assert r.status == GuardStatus.OK

    def test_31_few_orphans_ok(self):
        # 1 orphan, threshold 2 -> OK
        rows = [
            {"run_id": "a", "completed_at": "2026-04-18T01:28:00+00:00", "created_at": "2026-04-18T00:00:00+00:00"},
            {"run_id": "b", "completed_at": None,                        "created_at": "2026-04-18T12:00:00+00:00"},
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_orphan_checkpoints()
            assert r.status == GuardStatus.OK

    def test_32_many_orphans_warn(self):
        # 5 orphans, threshold 2 -> WARN
        rows = [
            {"run_id": f"r{i}", "completed_at": None, "created_at": "2026-04-18T00:00:00+00:00"}
            for i in range(5)
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_orphan_checkpoints()
            assert r.status == GuardStatus.WARN
            assert r.details["orphan_count"] == 5

    def test_33_empty_string_completed_at_counts_as_orphan(self):
        rows = [
            {"run_id": f"r{i}", "completed_at": "", "created_at": "2026-04-18T00:00:00+00:00"}
            for i in range(5)
        ]
        sb = fake_sb(rows=rows)
        with patch("lens_audit_guard._get_supabase_client", return_value=sb):
            r = check_orphan_checkpoints()
            assert r.status == GuardStatus.WARN


# ══════════════════════════════════════════════════════════════════════════════
# Test class 5 — run_audit orchestrator
# ══════════════════════════════════════════════════════════════════════════════
class TestRunAudit:

    def test_40_returns_report(self):
        # run_audit uses real Supabase client internally via _get_supabase_client
        # We patch it to return None so every check returns ERROR — still valid report.
        with patch("lens_audit_guard._get_supabase_client", return_value=None):
            report = run_audit()
            assert isinstance(report, GuardReport)
            assert report.scope == GuardScope.AUDIT

    def test_41_all_four_checks_present(self):
        with patch("lens_audit_guard._get_supabase_client", return_value=None):
            report = run_audit()
            check_names = [r.check_name for r in report.results]
            assert "recent_report_exists" in check_names
            assert "report_has_content" in check_names
            assert "source_health_delta" in check_names
            assert "orphan_checkpoints" in check_names

    def test_42_constants_sane(self):
        assert AUDIT_RECENT_WINDOW_HOURS > 0
        assert AUDIT_MIN_SUMMARY_CHARS > 0


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [
        TestRecentReportExists,
        TestReportHasContent,
        TestSourceHealthDelta,
        TestOrphanCheckpoints,
        TestRunAudit,
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
