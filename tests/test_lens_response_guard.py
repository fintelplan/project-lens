"""
test_lens_response_guard.py — Validation suite for lens_response_guard
========================================================================
Project Lens | LENS-014 I2

Follows the same convention as test_lens_quota_guard.py:
  - Bare pytest-style classes (no unittest.TestCase)
  - Plain `assert` statements (no self.assertEqual)
  - Custom run_all_tests() with hardcoded test_classes list
  - Works standalone: python tests/test_lens_response_guard.py
  - Or via pytest: python -m pytest tests/test_lens_response_guard.py -v

Authority: LR-006 (read convention before writing), LR-074 (guard pattern).
"""
from __future__ import annotations
import os
import sys

# Ensure code/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

from lens_response_guard import (
    validate_parsed_response,
    ValidationResult,
    SCHEMAS,
    format_validation_for_log,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures — valid sample responses for each position
# ══════════════════════════════════════════════════════════════════════════════
def valid_s2a():
    return {
        "analyst": "S2-A",
        "lens_id": "ALL",
        "findings": [
            {"injection_type": "EMOTIONAL_PRIME", "flagged_phrase": "X",
             "confidence": 0.7, "explanation": "E", "actor_beneficiary": "U"}
        ],
        "overall_injection_score": 0.6,
        "injection_goal": "test",
        "contamination_contribution": "MODERATE",
        "analyst_note": "ok",
    }


def valid_ma():
    return {
        "analyst": "MISSION_ANALYST",
        "cycle": "morning",
        "threat_level": "ELEVATED",
        "executive_summary": "A sufficiently long executive summary for testing purposes.",
        "key_findings": [
            {"finding": "F", "confidence": 0.8, "evidence_sources": ["S1"], "significance": "S"}
        ],
        "manufactured_narratives": [],
        "actors_of_concern": [],
        "gcsp_implications": [],
        "quality_score": 0.85,
    }


def valid_s3a():
    return {
        "summary": "A sufficiently long summary of patterns observed over the week.",
        "patterns_found": [{"pattern": "P", "evidence": "E", "confidence": 0.7}],
        "quality_score": 0.8,
        "accelerating_trends": [],
        "decelerating_trends": [],
        "signals_to_watch": [],
    }


def valid_s2e():
    return {
        "analyst": "S2-E",
        "findings": [{"actor": "X", "tier": "HIGH"}],
    }


def valid_s2gap():
    return {
        "analyst": "S2-GAP",
        "findings": [{"gap": "Y"}],
    }


# ══════════════════════════════════════════════════════════════════════════════
# Test class 1 — Basic validation paths
# ══════════════════════════════════════════════════════════════════════════════
class TestBasicValidation:

    def test_01_valid_s2a_passes(self):
        vr = validate_parsed_response(valid_s2a(), "S2-A")
        assert vr.valid is True
        assert vr.errors == []
        assert vr.position == "S2-A"

    def test_02_valid_ma_passes(self):
        vr = validate_parsed_response(valid_ma(), "MA")
        assert vr.valid is True
        assert vr.errors == []

    def test_03_valid_s3a_passes(self):
        vr = validate_parsed_response(valid_s3a(), "S3-A")
        assert vr.valid is True

    def test_04_valid_s2e_passes(self):
        vr = validate_parsed_response(valid_s2e(), "S2-E")
        assert vr.valid is True

    def test_05_valid_s2gap_passes(self):
        vr = validate_parsed_response(valid_s2gap(), "S2-GAP")
        assert vr.valid is True


# ══════════════════════════════════════════════════════════════════════════════
# Test class 2 — Failure modes (what this guard is actually for)
# ══════════════════════════════════════════════════════════════════════════════
class TestFailureModes:

    def test_10_none_response_caught(self):
        vr = validate_parsed_response(None, "S2-A")
        assert vr.valid is False
        assert any("None" in e for e in vr.errors)

    def test_11_non_dict_response_caught(self):
        vr = validate_parsed_response(["a", "list"], "S2-A")
        assert vr.valid is False
        assert any("list" in e.lower() for e in vr.errors)

    def test_12_empty_dict_caught(self):
        vr = validate_parsed_response({}, "S2-A")
        assert vr.valid is False
        # Every required key missing
        assert len(vr.missing_required) >= 4

    def test_13_missing_required_key(self):
        r = valid_s2a()
        del r["findings"]
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False
        assert "findings" in vr.missing_required

    def test_14_empty_required_string(self):
        r = valid_ma()
        r["executive_summary"] = ""
        vr = validate_parsed_response(r, "MA")
        assert vr.valid is False
        assert any("empty" in e.lower() for e in vr.errors)

    def test_15_empty_required_list(self):
        r = valid_s2a()
        r["findings"] = []
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False
        assert any("empty" in e.lower() for e in vr.errors)

    def test_16_none_required_value(self):
        r = valid_s2a()
        r["overall_injection_score"] = None
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False
        assert any("None" in e for e in vr.errors)


# ══════════════════════════════════════════════════════════════════════════════
# Test class 3 — Type + range + enum validation
# ══════════════════════════════════════════════════════════════════════════════
class TestTypeEnforcement:

    def test_20_wrong_type_caught(self):
        r = valid_s2a()
        r["findings"] = "not a list"
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False
        assert any("Type mismatch" in e for e in vr.errors)

    def test_21_int_accepted_as_float(self):
        # overall_injection_score typed as (int, float) — both fine
        r = valid_s2a()
        r["overall_injection_score"] = 1
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is True

    def test_22_out_of_range_score(self):
        r = valid_s2a()
        r["overall_injection_score"] = 1.5
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False
        assert any("Out of bounds" in e for e in vr.errors)

    def test_23_negative_score_caught(self):
        r = valid_ma()
        r["quality_score"] = -0.1
        vr = validate_parsed_response(r, "MA")
        assert vr.valid is False

    def test_24_invalid_threat_level_caught(self):
        r = valid_ma()
        r["threat_level"] = "NUCLEAR"  # not in enum
        vr = validate_parsed_response(r, "MA")
        assert vr.valid is False
        assert any("Invalid value" in e for e in vr.errors)

    def test_25_invalid_contamination_caught(self):
        r = valid_s2a()
        r["contamination_contribution"] = "HEAVY"
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is False

    def test_26_short_summary_caught(self):
        r = valid_ma()
        r["executive_summary"] = "too short"  # under 20 chars
        vr = validate_parsed_response(r, "MA")
        assert vr.valid is False
        assert any("Too short" in e for e in vr.errors)


# ══════════════════════════════════════════════════════════════════════════════
# Test class 4 — Edge cases + unknown positions
# ══════════════════════════════════════════════════════════════════════════════
class TestEdgeCases:

    def test_30_unknown_position_caught(self):
        vr = validate_parsed_response({"x": 1}, "S9-Z")
        assert vr.valid is False
        assert any("Unknown position" in e for e in vr.errors)

    def test_31_extra_unknown_keys_allowed(self):
        # Schema validates required, but extra keys don't break validation
        r = valid_s2a()
        r["something_extra"] = "allowed"
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is True

    def test_32_optional_keys_really_optional(self):
        # injection_goal is optional on S2-A
        r = valid_s2a()
        del r["injection_goal"]
        del r["analyst_note"]
        vr = validate_parsed_response(r, "S2-A")
        assert vr.valid is True

    def test_33_format_for_log_valid(self):
        vr = validate_parsed_response(valid_s2a(), "S2-A")
        msg = format_validation_for_log(vr)
        assert "S2-A" in msg
        assert "OK" in msg

    def test_34_format_for_log_failed(self):
        vr = validate_parsed_response({}, "S2-A")
        msg = format_validation_for_log(vr)
        assert "S2-A" in msg
        assert "FAIL" in msg

    def test_35_format_for_log_truncates(self):
        vr = validate_parsed_response({}, "S2-A")
        msg = format_validation_for_log(vr)
        # Many errors but message is still a single line
        assert "\n" not in msg
        assert "more" in msg.lower() or len(vr.errors) == 1


# ══════════════════════════════════════════════════════════════════════════════
# Runner — matches test_lens_quota_guard.py convention exactly
# ══════════════════════════════════════════════════════════════════════════════
def run_all_tests():
    """Run all tests manually if pytest unavailable."""
    import traceback
    passed = 0
    failed = 0
    failures = []
    test_classes = [TestBasicValidation, TestFailureModes,
                    TestTypeEnforcement, TestEdgeCases]
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
