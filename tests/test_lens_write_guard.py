"""
test_lens_write_guard.py — Validation suite for lens_write_guard
==================================================================
Project Lens | LENS-014 A1

Tests for Layer 4 write guard. Pure schema validation, no mocking
needed (write guard is stateless).
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_guard_common import GuardScope, GuardStatus
from lens_write_guard import (
    validate_write,
    validate_write_batch,
    list_covered_tables,
    SCHEMAS,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures — valid rows for each covered table
# ══════════════════════════════════════════════════════════════════════════════
def valid_lens_reports_row():
    return {
        "cycle": "2of1",
        "domain_focus": "ALL",
        "summary": "A sufficiently long summary of the day's analysis "
                   "that passes minimum length checks.",
        "generated_at": "2026-04-18T01:28:00+00:00",
        "quality_score": 0.85,
        "ai_model": "llama-3.3-70b",
        "status": "pending",
        "system": "S1",
    }


def valid_checkpoint_row():
    return {
        "run_id": "abc123xyz",
        "cycle": "2of1",
        "job_count": 1,
        "resume_from": 2,
        "lens_1_status": "complete",
    }


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — Basic valid-path tests
# ══════════════════════════════════════════════════════════════════════════════
class TestValidRows:

    def test_01_valid_lens_reports(self):
        r = validate_write("lens_reports", valid_lens_reports_row())
        assert r.status == GuardStatus.OK
        assert r.scope == GuardScope.WRITE

    def test_02_valid_checkpoint(self):
        r = validate_write("lens_run_checkpoints", valid_checkpoint_row())
        assert r.status == GuardStatus.OK

    def test_03_valid_with_legacy_cycle(self):
        # Legacy cycle labels should still be permitted (for backfills)
        row = valid_lens_reports_row()
        row["cycle"] = "morning"
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.OK

    def test_04_valid_with_manual_cycle(self):
        row = valid_lens_reports_row()
        row["cycle"] = "manual"
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.OK


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — Shape / None / dict checks
# ══════════════════════════════════════════════════════════════════════════════
class TestShape:

    def test_10_none_row_aborts(self):
        r = validate_write("lens_reports", None)
        assert r.status == GuardStatus.ABORT
        assert "None" in r.message

    def test_11_list_row_aborts(self):
        r = validate_write("lens_reports", [1, 2, 3])
        assert r.status == GuardStatus.ABORT
        assert "list" in r.message.lower()

    def test_12_string_row_aborts(self):
        r = validate_write("lens_reports", "not a dict")
        assert r.status == GuardStatus.ABORT

    def test_13_empty_dict_aborts(self):
        r = validate_write("lens_reports", {})
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — Required field violations
# ══════════════════════════════════════════════════════════════════════════════
class TestRequiredFields:

    def test_20_missing_cycle(self):
        row = valid_lens_reports_row()
        del row["cycle"]
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT
        assert "cycle" in str(r.details.get("errors", []))

    def test_21_missing_summary(self):
        row = valid_lens_reports_row()
        del row["summary"]
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT
        assert "summary" in str(r.details.get("errors", []))

    def test_22_none_value_for_required(self):
        row = valid_lens_reports_row()
        row["summary"] = None
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT

    def test_23_empty_string_for_required(self):
        row = valid_lens_reports_row()
        row["domain_focus"] = "   "  # whitespace only
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT

    def test_24_missing_run_id_checkpoint(self):
        row = valid_checkpoint_row()
        del row["run_id"]
        r = validate_write("lens_run_checkpoints", row)
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — Type violations
# ══════════════════════════════════════════════════════════════════════════════
class TestTypes:

    def test_30_wrong_type_quality_score(self):
        row = valid_lens_reports_row()
        row["quality_score"] = "high"  # should be numeric
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT

    def test_31_int_accepted_for_float(self):
        row = valid_lens_reports_row()
        row["quality_score"] = 1  # int accepted where (int, float) allowed
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.OK

    def test_32_wrong_type_job_count(self):
        row = valid_checkpoint_row()
        row["job_count"] = "1"  # should be int
        r = validate_write("lens_run_checkpoints", row)
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 5 — Enum violations
# ══════════════════════════════════════════════════════════════════════════════
class TestEnums:

    def test_40_invalid_cycle_value(self):
        row = valid_lens_reports_row()
        row["cycle"] = "bogus"
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT
        assert "not in allowed set" in str(r.details.get("errors", []))

    def test_41_invalid_checkpoint_cycle(self):
        row = valid_checkpoint_row()
        row["cycle"] = "bogus"
        r = validate_write("lens_run_checkpoints", row)
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 6 — Bounds violations
# ══════════════════════════════════════════════════════════════════════════════
class TestBounds:

    def test_50_quality_score_negative(self):
        row = valid_lens_reports_row()
        row["quality_score"] = -0.5
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT

    def test_51_resume_from_out_of_range(self):
        row = valid_checkpoint_row()
        row["resume_from"] = 99  # out of [1, 5]
        r = validate_write("lens_run_checkpoints", row)
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 7 — Min-length violations (silent-fill defense)
# ══════════════════════════════════════════════════════════════════════════════
class TestMinLengths:

    def test_60_summary_too_short(self):
        row = valid_lens_reports_row()
        row["summary"] = "short"  # below 10-char minimum
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.ABORT

    def test_61_run_id_too_short(self):
        row = valid_checkpoint_row()
        row["run_id"] = "ab"  # below 3-char minimum
        r = validate_write("lens_run_checkpoints", row)
        assert r.status == GuardStatus.ABORT


# ══════════════════════════════════════════════════════════════════════════════
# Test class 8 — Unknown tables + batch + coverage
# ══════════════════════════════════════════════════════════════════════════════
class TestMiscellaneous:

    def test_70_unknown_table_is_warn(self):
        # Unknown tables get WARN (permissive), not ABORT
        r = validate_write("random_table_xyz", {"x": 1})
        assert r.status == GuardStatus.WARN
        assert r.status.is_proceed is True

    def test_71_extra_fields_ignored(self):
        row = valid_lens_reports_row()
        row["something_extra"] = "irrelevant"
        r = validate_write("lens_reports", row)
        assert r.status == GuardStatus.OK

    def test_72_batch_validate(self):
        rows = [valid_lens_reports_row(), valid_lens_reports_row()]
        results = validate_write_batch("lens_reports", rows)
        assert len(results) == 2
        assert all(r.status == GuardStatus.OK for r in results)

    def test_73_batch_mixed_results(self):
        bad = valid_lens_reports_row()
        bad["cycle"] = "bogus"
        rows = [valid_lens_reports_row(), bad]
        results = validate_write_batch("lens_reports", rows)
        assert len(results) == 2
        assert results[0].status == GuardStatus.OK
        assert results[1].status == GuardStatus.ABORT

    def test_74_batch_non_list(self):
        results = validate_write_batch("lens_reports", "not a list")
        assert len(results) == 1
        assert results[0].status == GuardStatus.ABORT

    def test_75_list_covered_tables(self):
        tables = list_covered_tables()
        assert "lens_reports" in tables
        assert "lens_run_checkpoints" in tables

    def test_76_schemas_registered(self):
        # Sanity — exactly the P0 tables have schemas
        assert "lens_reports" in SCHEMAS
        assert "lens_run_checkpoints" in SCHEMAS


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [
        TestValidRows,
        TestShape,
        TestRequiredFields,
        TestTypes,
        TestEnums,
        TestBounds,
        TestMinLengths,
        TestMiscellaneous,
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
