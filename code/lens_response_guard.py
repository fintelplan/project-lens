"""
lens_response_guard.py — Response Schema Validator
Project Lens | LENS-014 I2

Purpose: detect malformed, incomplete, or silently-filtered LLM responses
BEFORE they propagate downstream. Inspired by GNI-R-234 (silent content
filtering defense).

Design: pure post-parse validator. Callers continue to use their existing
JSON parsing and retry logic. This module layers validation on top — given
a successfully-parsed dict, it returns a structured ValidationResult.

Usage pattern (integrated into existing position code):
    parsed = json.loads(raw)                                # existing
    vr = validate_parsed_response(parsed, position="S2-A")  # NEW
    if not vr.valid:
        log.warning(f"S2-A schema: {vr.errors}")
        # Caller decides: retry, skip, or downgrade

What this does NOT do:
  - Parse JSON (callers already handle that)
  - Retry (callers already have retry loops)
  - Correct invalid responses (caller handles remediation)
  - Send alerts (out of scope for this module)

Rules applied:
  LR-068 — evidence before action (schemas reflect actual produced shapes)
  LR-074 — guard pattern (fail-safe, never crash caller)
  GNI-R-234 — silent-filtering defense
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional


log = logging.getLogger("response_guard")


# ══════════════════════════════════════════════════════════════════════════════
# Schema definitions — one per position
# ══════════════════════════════════════════════════════════════════════════════
# Schema shape:
#   required_keys: list of keys that MUST be present and non-null/non-empty
#   optional_keys: list of keys that may be present (silently ignored if absent)
#   typed_fields:  dict of key -> expected python type (validated if present)
#   bounded_fields: dict of key -> (min, max) tuple for numeric range (validated if present)
#   min_lengths:   dict of string key -> minimum non-whitespace length
# ══════════════════════════════════════════════════════════════════════════════

SCHEMAS: dict[str, dict[str, Any]] = {
    # Mission Analyst — macro synthesis
    "MA": {
        "required_keys": [
            "analyst", "threat_level", "executive_summary", "key_findings",
        ],
        "optional_keys": [
            "cycle", "manufactured_narratives", "adversary_narrative_summary",
            "actors_of_concern", "gcsp_implications", "intelligence_gaps",
            "quality_score", "analyst_note", "cui_bono_synthesis",
        ],
        "typed_fields": {
            "analyst": str,
            "threat_level": str,
            "executive_summary": str,
            "key_findings": list,
            "manufactured_narratives": list,
            "actors_of_concern": list,
            "gcsp_implications": list,
            "quality_score": (int, float),
        },
        "bounded_fields": {
            "quality_score": (0.0, 1.0),
        },
        "min_lengths": {
            "executive_summary": 20,   # Guard against empty-string silent fails
        },
        "enum_fields": {
            "threat_level": ["CRITICAL", "HIGH", "ELEVATED", "MODERATE", "LOW"],
        },
    },

    # S2-A Injection Tracer
    "S2-A": {
        "required_keys": [
            "analyst", "findings", "overall_injection_score",
            "contamination_contribution",
        ],
        "optional_keys": [
            "lens_id", "injection_goal", "analyst_note",
        ],
        "typed_fields": {
            "analyst": str,
            "findings": list,
            "overall_injection_score": (int, float),
            "contamination_contribution": str,
        },
        "bounded_fields": {
            "overall_injection_score": (0.0, 1.0),
        },
        "enum_fields": {
            "contamination_contribution": ["SURFACE", "MODERATE", "DEEP"],
        },
    },

    # S2-E Legitimacy Filter
    "S2-E": {
        "required_keys": [
            "analyst", "findings",
        ],
        "optional_keys": [
            "lens_id", "actors_flagged", "tier_distribution",
            "analyst_note", "correction_to_ma",
        ],
        "typed_fields": {
            "analyst": str,
            "findings": list,
        },
    },

    # S2-GAP Gap Analyst
    "S2-GAP": {
        "required_keys": [
            "analyst", "findings",
        ],
        "optional_keys": [
            "gaps_detected", "analyst_note", "correction_to_ma",
        ],
        "typed_fields": {
            "analyst": str,
            "findings": list,
        },
    },

    # S3-A Pattern Intelligence
    "S3-A": {
        "required_keys": [
            "summary", "patterns_found",
        ],
        "optional_keys": [
            "sequence_found", "distraction_event", "structural_event",
            "accelerating_trends", "decelerating_trends", "first_domino",
            "hidden_builder", "signals_to_watch", "corrections_to_s2",
            "quality_score", "ach_check", "sectarian_trap_signal",
        ],
        "typed_fields": {
            "summary": str,
            "patterns_found": list,
            "accelerating_trends": list,
            "decelerating_trends": list,
            "signals_to_watch": list,
            "quality_score": (int, float),
        },
        "bounded_fields": {
            "quality_score": (0.0, 1.0),
        },
        "min_lengths": {
            "summary": 20,
        },
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# Result type
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ValidationResult:
    """Return contract for validate_parsed_response.

    valid: True iff every validation check passed.
    errors: list of human-readable error strings. Empty when valid=True.
    position: the position identifier used for schema lookup.
    missing_required: subset of errors — keys missing from response.
    """
    valid: bool
    position: str
    errors: list[str] = field(default_factory=list)
    missing_required: list[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
# Core validator
# ══════════════════════════════════════════════════════════════════════════════
def validate_parsed_response(parsed: Any, position: str) -> ValidationResult:
    """Validate a parsed LLM response against the schema for the given position.

    Args:
        parsed: the dict (or other) returned by json.loads() of LLM output.
        position: one of the keys in SCHEMAS ('MA', 'S2-A', 'S2-E', 'S2-GAP', 'S3-A').

    Returns:
        ValidationResult with valid=True when all checks pass, or valid=False
        with errors describing each failure.

    Never raises. Unknown positions return valid=False with a clear error.
    """
    # ── Position unknown ──────────────────────────────────────────────────────
    if position not in SCHEMAS:
        return ValidationResult(
            valid=False,
            position=position,
            errors=[f"Unknown position '{position}' — no schema registered"],
        )

    schema = SCHEMAS[position]
    errors: list[str] = []
    missing: list[str] = []

    # ── Top-level shape check ─────────────────────────────────────────────────
    if parsed is None:
        return ValidationResult(
            valid=False, position=position,
            errors=["Response is None — likely silent filtering or empty output"],
        )

    if not isinstance(parsed, dict):
        return ValidationResult(
            valid=False, position=position,
            errors=[f"Response is {type(parsed).__name__}, expected dict"],
        )

    # ── Required keys present and not-empty ───────────────────────────────────
    for key in schema.get("required_keys", []):
        if key not in parsed:
            missing.append(key)
            errors.append(f"Missing required key: '{key}'")
            continue

        val = parsed[key]
        # Empty-detection: guard against "" for strings and [] for lists
        if val is None:
            errors.append(f"Required key '{key}' is None")
        elif isinstance(val, str) and not val.strip():
            errors.append(f"Required key '{key}' is empty string")
        elif isinstance(val, (list, dict)) and len(val) == 0:
            errors.append(f"Required key '{key}' is empty {type(val).__name__}")

    # ── Type checks (only if key is present) ──────────────────────────────────
    for key, expected_type in schema.get("typed_fields", {}).items():
        if key in parsed and parsed[key] is not None:
            if not isinstance(parsed[key], expected_type):
                actual = type(parsed[key]).__name__
                if isinstance(expected_type, tuple):
                    expected = " or ".join(t.__name__ for t in expected_type)
                else:
                    expected = expected_type.__name__
                errors.append(
                    f"Type mismatch on '{key}': got {actual}, expected {expected}"
                )

    # ── Bounded numeric fields ────────────────────────────────────────────────
    for key, (lo, hi) in schema.get("bounded_fields", {}).items():
        if key in parsed and isinstance(parsed[key], (int, float)):
            v = parsed[key]
            if v < lo or v > hi:
                errors.append(
                    f"Out of bounds '{key}': {v} not in [{lo}, {hi}]"
                )

    # ── Minimum string lengths (detect near-empty fills) ──────────────────────
    for key, min_len in schema.get("min_lengths", {}).items():
        if key in parsed and isinstance(parsed[key], str):
            stripped_len = len(parsed[key].strip())
            if stripped_len < min_len:
                errors.append(
                    f"Too short '{key}': {stripped_len} chars, minimum {min_len}"
                )

    # ── Enum fields (allowed value set) ───────────────────────────────────────
    for key, allowed in schema.get("enum_fields", {}).items():
        if key in parsed and parsed[key] is not None:
            if parsed[key] not in allowed:
                errors.append(
                    f"Invalid value for '{key}': {parsed[key]!r} not in {allowed}"
                )

    return ValidationResult(
        valid=(len(errors) == 0),
        position=position,
        errors=errors,
        missing_required=missing,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Convenience — summarize for logging
# ══════════════════════════════════════════════════════════════════════════════
def format_validation_for_log(vr: ValidationResult) -> str:
    """Produce a one-line summary suitable for log.warning() output."""
    if vr.valid:
        return f"[{vr.position}] schema OK"
    n = len(vr.errors)
    first = vr.errors[0] if vr.errors else "unknown"
    suffix = f" (+{n-1} more)" if n > 1 else ""
    return f"[{vr.position}] schema FAIL: {first}{suffix}"
