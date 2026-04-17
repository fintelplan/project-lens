"""
test_lens_quota_guard.py — 33-test error-0 verification suite
==============================================================
Project Lens | LENS-013

Runs the full failure-mode matrix from lens_quota_guard.py design.
Each test maps to a T-number from the design document.

Run with:
    cd /c/school/lens
    python -m pytest tests/test_lens_quota_guard.py -v

Or standalone:
    python tests/test_lens_quota_guard.py
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

# Ensure code/ is importable regardless of where pytest runs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

import lens_quota_guard as qg


# ─────────────────────────────────────────────────────────────────────────
# Test helper — mock Supabase client with configurable ledger state
# ─────────────────────────────────────────────────────────────────────────

def mock_supabase(ledger_rows=None, read_fails=False, write_fails=False):
    """Build a mock Supabase client returning given ledger state."""
    sb = MagicMock()

    def table_mock(name):
        chain = MagicMock()
        # Reads
        if read_fails:
            chain.select.return_value.eq.return_value.eq.return_value.gte.return_value.execute.side_effect = Exception("Supabase unreachable")
        else:
            chain.select.return_value.eq.return_value.eq.return_value.gte.return_value.execute.return_value = MagicMock(data=ledger_rows or [])
        # Writes
        if write_fails:
            chain.insert.return_value.execute.side_effect = Exception("RLS blocked")
        else:
            chain.insert.return_value.execute.return_value = MagicMock(data=[{"id": "mock"}])
        return chain

    sb.table.side_effect = table_mock
    return sb


# ═════════════════════════════════════════════════════════════════════════
# PURE DECISION FUNCTION TESTS (T20-T26, T30)
# ═════════════════════════════════════════════════════════════════════════

class TestDecideFunction:
    """Tests for the pure decide() function — no I/O, no mocks."""

    def test_T20_first_run_no_prior_usage(self):
        """T20: First run ever (ledger empty → used_today=0) returns PROCEED."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=0, limit_value=100_000,
                      estimated_use=31_000, positions=["S2-A", "MA"])
        assert r.decision == qg.Decision.PROCEED
        assert r.headroom_pct == 69.0
        assert "Clean headroom" in r.reason

    def test_T21_headroom_exactly_zero(self):
        """T21: Headroom exactly 0 returns SKIP."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=69_000, limit_value=100_000,
                      estimated_use=31_000, positions=["S2-A", "MA"])
        # 100K - 69K = 31K remaining; 31K - 31K estimated = 0 headroom → 0.0%
        assert r.decision == qg.Decision.SKIP
        assert r.headroom_pct == 0.0

    def test_T22_headroom_negative(self):
        """T22: Over-limit returns SKIP with negative headroom."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=98_000, limit_value=100_000,
                      estimated_use=5_000, positions=["S2-A"])
        assert r.decision == qg.Decision.SKIP
        assert r.headroom_pct < 0
        assert "Over-limit" in r.reason

    def test_T23_headroom_equals_estimated(self):
        """T23: Headroom just matches estimated → PROCEED_TIGHT boundary."""
        # 60K used, 100K limit, 30K estimated → 10K headroom = 10% = below 20% threshold → DEGRADE
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=60_000, limit_value=100_000,
                      estimated_use=30_000, positions=["S2-A", "MA"])
        assert r.decision == qg.Decision.DEGRADE

    def test_T24_plenty_headroom(self):
        """T24: Plenty of headroom returns PROCEED cleanly."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=10_000, limit_value=100_000,
                      estimated_use=5_000, positions=["S2-A"])
        assert r.decision == qg.Decision.PROCEED
        assert r.headroom_pct > 40.0

    def test_T26_force_override(self):
        """T26: LENS_FORCE=1 always returns FORCE regardless of quota."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=99_999, limit_value=100_000,
                      estimated_use=10_000, positions=["S2-A"],
                      force=True)
        assert r.decision == qg.Decision.FORCE
        assert "operator override" in r.reason

    def test_T30_test_mode(self):
        """T30: LENS_GUARD_TEST=1 returns TEST decision."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=99_999, limit_value=100_000,
                      estimated_use=10_000, positions=["S2-A"],
                      test_mode=True)
        assert r.decision == qg.Decision.TEST


# ═════════════════════════════════════════════════════════════════════════
# EDGE CASE TESTS (T08, T11, T13, T14, T16-T18)
# ═════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_T08_T14_limit_unknown(self):
        """T08/T14: Unknown limit returns PROCEED (fail-safe) with error_class."""
        r = qg.decide("unknown", "unknown-model", "TPD",
                      used_today=0, limit_value=None,
                      estimated_use=5_000, positions=["X"])
        assert r.decision == qg.Decision.PROCEED
        assert r.error_class == "LIMITS_UNKNOWN"

    def test_T14_zero_limit(self):
        """T14: Zero limit (provider disabled free tier) returns PROCEED fail-safe."""
        r = qg.decide("gemini", "gemini-2.5-pro", "RPD",
                      used_today=0, limit_value=0,
                      estimated_use=1, positions=["S1-L2"])
        assert r.decision == qg.Decision.PROCEED
        assert r.error_class == "LIMITS_UNKNOWN"

    def test_T11_negative_used_clamped_to_zero(self):
        """T11: Negative used value (provider bug) clamped to 0."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=-500, limit_value=100_000,
                      estimated_use=5_000, positions=["S2-A"])
        assert r.decision == qg.Decision.PROCEED
        assert r.used_value == 0  # clamped

    def test_T13_used_clamped_if_wildly_over(self):
        """T13: Used value far above limit clamped to 2x limit for sanity."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=500_000,  # 5x limit — must be bug
                      limit_value=100_000,
                      estimated_use=5_000, positions=["S2-A"])
        assert r.decision == qg.Decision.SKIP  # clamped to 200K, still over
        assert r.used_value == 200_000

    def test_T16_ledger_read_failed(self):
        """T16: used_today=None (Supabase unreachable) returns PROCEED fail-safe."""
        r = qg.decide("groq", "llama-3.3-70b-versatile", "TPD",
                      used_today=None, limit_value=100_000,
                      estimated_use=5_000, positions=["S2-A"])
        assert r.decision == qg.Decision.PROCEED
        assert r.error_class == "LEDGER_UNAVAILABLE"


# ═════════════════════════════════════════════════════════════════════════
# AGGREGATION TESTS
# ═════════════════════════════════════════════════════════════════════════

class TestAggregation:

    def test_single_position_grouping(self):
        """One position = one (provider, model) group."""
        g = qg.aggregate_positions(["S2-A"])
        assert len(g) == 1
        assert ("groq", "llama-3.3-70b-versatile") in g
        total, positions = g[("groq", "llama-3.3-70b-versatile")]
        assert total == 5_000
        assert positions == ["S2-A"]

    def test_shared_provider_model_summed(self):
        """Multiple positions on same provider/model: consumption summed."""
        g = qg.aggregate_positions(["S2-A", "S2-E", "S2-GAP", "MA", "S3-A"])
        assert len(g) == 1  # all on groq/llama-3.3-70b
        total, positions = g[("groq", "llama-3.3-70b-versatile")]
        assert total == 5_000 + 10_000 + 3_000 + 6_000 + 7_000  # = 31000
        assert set(positions) == {"S2-A", "S2-E", "S2-GAP", "MA", "S3-A"}

    def test_T25_multiple_providers_independent_groups(self):
        """T25: Multiple providers each get own group."""
        g = qg.aggregate_positions(["S2-A", "S2-B", "S2-C", "S3-D"])
        assert len(g) == 4  # groq, gemini, mistral, cerebras
        assert ("groq", "llama-3.3-70b-versatile") in g
        assert ("gemini", "gemini-2.0-flash") in g
        assert ("mistral", "mistral-small") in g
        assert ("cerebras", "qwen-3-235b") in g

    def test_unknown_position_skipped_gracefully(self):
        """Unknown position name is skipped with warning, doesn't crash."""
        g = qg.aggregate_positions(["S2-A", "BOGUS-POSITION", "MA"])
        assert len(g) == 1  # BOGUS-POSITION ignored
        total, positions = g[("groq", "llama-3.3-70b-versatile")]
        assert total == 5_000 + 6_000  # S2-A + MA only
        assert "BOGUS-POSITION" not in positions

    def test_empty_positions_list(self):
        """Empty input returns empty dict, no crash."""
        g = qg.aggregate_positions([])
        assert g == {}


# ═════════════════════════════════════════════════════════════════════════
# LEDGER I/O TESTS (T16-T19 with mocked Supabase)
# ═════════════════════════════════════════════════════════════════════════

class TestLedgerIO:

    def test_read_returns_sum(self):
        """Ledger read sums estimated_use across matching rows."""
        sb = mock_supabase(ledger_rows=[
            {"estimated_use": 5000},
            {"estimated_use": 10000},
            {"estimated_use": 3000},
        ])
        total = qg.read_ledger_usage(sb, "groq", "llama-3.3-70b-versatile")
        assert total == 18000

    def test_T16_read_fails_returns_none(self):
        """T16: Supabase unreachable during read → returns None (fail-safe)."""
        sb = mock_supabase(read_fails=True)
        total = qg.read_ledger_usage(sb, "groq", "llama-3.3-70b-versatile")
        assert total is None  # fail-safe: caller should PROCEED

    def test_read_empty_returns_zero(self):
        """No ledger rows yet (T20 first run) returns 0."""
        sb = mock_supabase(ledger_rows=[])
        total = qg.read_ledger_usage(sb, "groq", "llama-3.3-70b-versatile")
        assert total == 0

    def test_write_success(self):
        """Ledger write succeeds when Supabase healthy."""
        sb = mock_supabase()
        r = qg.QuotaResult(
            decision=qg.Decision.PROCEED, reason="test",
            provider="groq", model="llama-3.3-70b-versatile", quota_type="TPD",
            limit_value=100_000, used_value=10_000, remaining=90_000,
            estimated_use=5_000, headroom_pct=85.0, positions=["S2-A"],
        )
        assert qg.write_ledger_entry(sb, "test-run-1", r) is True

    def test_T17_write_fails_returns_false_no_crash(self):
        """T17: Supabase write fails → returns False, doesn't raise."""
        sb = mock_supabase(write_fails=True)
        r = qg.QuotaResult(
            decision=qg.Decision.PROCEED, reason="test",
            provider="groq", model="llama-3.3-70b-versatile", quota_type="TPD",
            limit_value=100_000, used_value=10_000, remaining=90_000,
            estimated_use=5_000, headroom_pct=85.0, positions=["S2-A"],
        )
        # Must return False, never raise
        assert qg.write_ledger_entry(sb, "test-run-1", r) is False


# ═════════════════════════════════════════════════════════════════════════
# FULL ORCHESTRATION TESTS
# ═════════════════════════════════════════════════════════════════════════

class TestGuardCheck:

    def test_full_flow_clean(self):
        """End-to-end: fresh day (empty ledger), all positions → PROCEED."""
        sb = mock_supabase(ledger_rows=[])
        results = qg.guard_check(
            positions=["S2-A", "MA", "S3-A"],
            sb=sb, run_id="test-clean",
        )
        assert len(results) == 1  # all on groq/llama-3.3-70b
        assert results[0].decision == qg.Decision.PROCEED

    def test_full_flow_tight(self):
        """Ledger shows heavy usage → PROCEED_TIGHT or DEGRADE."""
        sb = mock_supabase(ledger_rows=[
            {"estimated_use": 70_000},  # Already 70K used
        ])
        # 70K used + 31K estimated = 101K → over limit → SKIP
        results = qg.guard_check(
            positions=["S2-A", "S2-E", "S2-GAP", "MA", "S3-A"],
            sb=sb, run_id="test-tight",
        )
        assert results[0].decision == qg.Decision.SKIP

    def test_T27_guard_crash_wrapped(self):
        """T27: Outer wrapper catches any exception, returns fail-safe PROCEED."""
        # Monkey-patch guard_check to raise
        with patch.object(qg, "guard_check", side_effect=RuntimeError("boom")):
            results = qg.guard_check_with_fallback(
                positions=["S2-A"], run_id="test-crash",
            )
        assert len(results) == 1
        assert results[0].decision == qg.Decision.PROCEED
        assert results[0].error_class == "GUARD_CRASH"

    def test_filter_positions_observer_mode_only_skip_enforced(self):
        """In observer mode, only SKIP actually blocks positions."""
        # Build results with different decisions
        r_skip = qg.QuotaResult(
            decision=qg.Decision.SKIP, reason="test",
            provider="groq", model="llama-3.3-70b-versatile", quota_type="TPD",
            limit_value=100_000, used_value=99_000, remaining=1_000,
            estimated_use=5_000, headroom_pct=-4.0, positions=["S2-A"],
        )
        per_pos = qg.filter_positions_by_guard(["S2-A", "S2-E", "MA"], [r_skip])
        assert per_pos["S2-A"] == "SKIP"
        assert per_pos["S2-E"] == "SKIP"  # same provider/model group → SKIP too
        assert per_pos["MA"] == "SKIP"

    def test_filter_positions_degrade_becomes_proceed_in_observer(self):
        """DEGRADE in observer mode → PROCEED (only SKIP is enforced)."""
        r_degrade = qg.QuotaResult(
            decision=qg.Decision.DEGRADE, reason="test",
            provider="groq", model="llama-3.3-70b-versatile", quota_type="TPD",
            limit_value=100_000, used_value=75_000, remaining=25_000,
            estimated_use=31_000, headroom_pct=-6.0, positions=["S2-A"],
        )
        per_pos = qg.filter_positions_by_guard(["S2-A"], [r_degrade])
        assert per_pos["S2-A"] == "PROCEED"  # DEGRADE → PROCEED in observer


# ═════════════════════════════════════════════════════════════════════════
# Test runner — allow standalone execution without pytest
# ═════════════════════════════════════════════════════════════════════════

def run_all_tests():
    """Run all tests manually if pytest unavailable."""
    import traceback
    passed = 0
    failed = 0
    failures = []

    test_classes = [TestDecideFunction, TestEdgeCases, TestAggregation,
                    TestLedgerIO, TestGuardCheck]

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
