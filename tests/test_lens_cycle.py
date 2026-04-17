"""
test_lens_cycle.py — Validation suite for lens_cycle
======================================================
Project Lens | LENS-014 O1

Follows test_lens_quota_guard.py + test_lens_response_guard.py convention:
  - Bare pytest-style classes (no unittest.TestCase)
  - Plain assert statements
  - Custom run_all_tests() with hardcoded test_classes list
  - Works standalone: python tests/test_lens_cycle.py
  - Or via pytest: python -m pytest tests/test_lens_cycle.py -v

Authority: LR-006 (read convention before writing), LR-074 (guard pattern).
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timezone, timedelta

# Ensure code/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_cycle import (
    get_cycle,
    format_cycle_display,
    is_legacy_era,
    is_new_era,
    describe_canonical_schedule,
    CANONICAL_CYCLES,
    LEGACY_CYCLES,
    DAY_1_UTC,
    CRON_2OF1_UTC_HOUR,
    CRON_2OF1_UTC_MINUTE,
    CRON_2OF2_UTC_HOUR,
    CRON_2OF2_UTC_MINUTE,
    CYCLE_TOLERANCE_MINUTES,
)


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — get_cycle: anchor times
# ══════════════════════════════════════════════════════════════════════════════
class TestGetCycleAnchors:
    """Test that each anchor UTC time returns the correct cycle."""

    def test_01_2of1_exact_anchor(self):
        t = datetime(2026, 4, 17, 1, 28, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of1"

    def test_02_2of2_exact_anchor(self):
        t = datetime(2026, 4, 17, 13, 28, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of2"

    def test_03_noon_is_manual(self):
        t = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"

    def test_04_midnight_is_manual(self):
        # Midnight UTC (00:00) is far from 01:28 anchor (88 min away)
        t = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — get_cycle: tolerance window
# ══════════════════════════════════════════════════════════════════════════════
class TestGetCycleTolerance:
    """Test ±15 min tolerance around anchors."""

    def test_10_2of1_lower_edge(self):
        # 01:13 = 01:28 - 15 min, within tolerance
        t = datetime(2026, 4, 17, 1, 13, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of1"

    def test_11_2of1_upper_edge(self):
        # 01:43 = 01:28 + 15 min, within tolerance
        t = datetime(2026, 4, 17, 1, 43, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of1"

    def test_12_2of1_just_past_tolerance(self):
        # 01:44 is 16 min past anchor — OUT of tolerance
        t = datetime(2026, 4, 17, 1, 44, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"

    def test_13_2of1_just_before_tolerance(self):
        # 01:12 is 16 min before anchor — OUT of tolerance
        t = datetime(2026, 4, 17, 1, 12, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"

    def test_14_2of2_lower_edge(self):
        t = datetime(2026, 4, 17, 13, 13, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of2"

    def test_15_2of2_upper_edge(self):
        t = datetime(2026, 4, 17, 13, 43, tzinfo=timezone.utc)
        assert get_cycle(t) == "2of2"


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — get_cycle: edge cases + timezone handling
# ══════════════════════════════════════════════════════════════════════════════
class TestGetCycleEdgeCases:

    def test_20_naive_datetime_assumed_utc(self):
        # No tzinfo — should be treated as UTC
        t = datetime(2026, 4, 17, 1, 28)  # naive
        assert get_cycle(t) == "2of1"

    def test_21_non_utc_timezone_converted(self):
        # 21:28 EDT == 01:28 UTC next day
        edt = timezone(timedelta(hours=-4))
        t = datetime(2026, 4, 16, 21, 28, tzinfo=edt)
        assert get_cycle(t) == "2of1"

    def test_22_none_uses_now(self):
        # Calling without argument should not crash
        result = get_cycle()
        assert result in ("2of1", "2of2", "manual")

    def test_23_day_boundary_wrap(self):
        # 00:00 UTC is 88 min from 01:28 anchor — manual
        t = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"

    def test_24_late_night_near_next_cron(self):
        # 23:59 is far from both 01:28 and 13:28 — manual
        t = datetime(2026, 4, 17, 23, 59, tzinfo=timezone.utc)
        assert get_cycle(t) == "manual"


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — format_cycle_display
# ══════════════════════════════════════════════════════════════════════════════
class TestFormatCycleDisplay:

    def test_30_format_contains_cycle_label(self):
        t = datetime(2026, 4, 17, 1, 28, tzinfo=timezone.utc)
        s = format_cycle_display("2of1", t)
        assert "2of1" in s

    def test_31_format_contains_dc_and_mmt(self):
        t = datetime(2026, 4, 17, 1, 28, tzinfo=timezone.utc)
        s = format_cycle_display("2of1", t)
        assert "DC:" in s
        assert "MMT:" in s

    def test_32_format_shows_edt_in_summer(self):
        # April 17 = EDT (daylight saving active)
        t = datetime(2026, 4, 17, 13, 28, tzinfo=timezone.utc)
        s = format_cycle_display("2of2", t)
        assert "EDT" in s

    def test_33_format_shows_est_in_winter(self):
        # January = EST (standard time)
        t = datetime(2026, 1, 15, 13, 28, tzinfo=timezone.utc)
        s = format_cycle_display("2of2", t)
        assert "EST" in s

    def test_34_format_handles_manual(self):
        t = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        s = format_cycle_display("manual", t)
        assert "manual" in s

    def test_35_format_no_trailing_whitespace(self):
        t = datetime(2026, 4, 17, 1, 28, tzinfo=timezone.utc)
        s = format_cycle_display("2of1", t)
        assert s == s.strip()


# ══════════════════════════════════════════════════════════════════════════════
# Test class 5 — Era detection
# ══════════════════════════════════════════════════════════════════════════════
class TestEraDetection:

    def test_40_apr_10_is_legacy(self):
        t = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
        assert is_legacy_era(t) is True
        assert is_new_era(t) is False

    def test_41_apr_17_000000_is_new_era(self):
        # Exactly Day 1 boundary
        t = DAY_1_UTC
        assert is_new_era(t) is True
        assert is_legacy_era(t) is False

    def test_42_apr_17_just_before_midnight_is_legacy(self):
        # 23:59 on Apr 16 — still legacy era
        t = datetime(2026, 4, 16, 23, 59, 59, tzinfo=timezone.utc)
        assert is_legacy_era(t) is True
        assert is_new_era(t) is False

    def test_43_future_date_is_new_era(self):
        t = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        assert is_new_era(t) is True

    def test_44_naive_datetime_handled(self):
        # Naive dt before DAY_1 should still be legacy
        t = datetime(2026, 4, 10, 12, 0)  # naive
        assert is_legacy_era(t) is True


# ══════════════════════════════════════════════════════════════════════════════
# Test class 6 — Constants and schedule description
# ══════════════════════════════════════════════════════════════════════════════
class TestConstants:

    def test_50_canonical_cycles_are_two(self):
        assert CANONICAL_CYCLES == ["2of1", "2of2"]

    def test_51_legacy_cycles_list(self):
        assert "morning" in LEGACY_CYCLES
        assert "afternoon" in LEGACY_CYCLES
        assert "evening" in LEGACY_CYCLES
        assert "midnight" in LEGACY_CYCLES

    def test_52_day_1_is_april_17(self):
        assert DAY_1_UTC.year == 2026
        assert DAY_1_UTC.month == 4
        assert DAY_1_UTC.day == 17
        assert DAY_1_UTC.tzinfo is not None

    def test_53_tolerance_is_fifteen_min(self):
        assert CYCLE_TOLERANCE_MINUTES == 15

    def test_54_anchor_times_match_cron(self):
        # 01:28 UTC and 13:28 UTC — matches workflow schedule
        assert CRON_2OF1_UTC_HOUR == 1
        assert CRON_2OF1_UTC_MINUTE == 28
        assert CRON_2OF2_UTC_HOUR == 13
        assert CRON_2OF2_UTC_MINUTE == 28

    def test_55_describe_schedule_contains_expected_tokens(self):
        s = describe_canonical_schedule()
        assert "2of1" in s
        assert "2of2" in s
        assert "Day 1" in s
        assert "America/New_York" in s
        assert "MMT" in s


# ══════════════════════════════════════════════════════════════════════════════
# Runner — matches convention exactly
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    """Run all tests manually if pytest unavailable."""
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [
        TestGetCycleAnchors,
        TestGetCycleTolerance,
        TestGetCycleEdgeCases,
        TestFormatCycleDisplay,
        TestEraDetection,
        TestConstants,
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
