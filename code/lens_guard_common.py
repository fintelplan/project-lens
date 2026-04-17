"""
lens_guard_common.py — Shared Foundation For All Guards
========================================================
Project Lens | LENS-014 A1 (Architecture fix 1)

Purpose:
  Common types, enums, and utilities shared across Project Lens's
  guard system. Provides the vocabulary and contracts for:
    - lens_quota_guard.py    (per-LLM-call safety)
    - lens_response_guard.py (per-response-parse safety)
    - lens_preflight_guard.py (per-cycle safety)

Design principle (operator-taught LENS-014):
  "The main duty of every guard system is not to fail main system."

  Guards are organized by SCOPE (per-call, per-response, per-cycle),
  not by category. All guards share the detect-classify-heal-verify-
  gate-audit pattern with scope-appropriate implementations.

Safety invariants every guard MUST satisfy:
  1. Never false-positive on 'safe'   (guard says OK but main fails)
  2. Fail-safe default                (guard error => assume UNSAFE)
  3. Audit trail                      (every decision logged)
  4. Bounded healing scope            (re-verify after heal)
  5. Escalation protocol              (un-healable => operator alert)
  6. Never raise into main system     (catch own errors, return status)

Authority: LR-074 (guard pattern), operator-taught safety-is-the-gate
principle (session LENS-014, April 18 2026 MMT).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ══════════════════════════════════════════════════════════════════════════════
# Scope — WHERE the guard operates
# ══════════════════════════════════════════════════════════════════════════════
class GuardScope(str, Enum):
    """Scope identifies when/where the guard runs in the cycle lifecycle.

    PREFLIGHT: once per cron run, BEFORE any LLM calls begin.
        Checks environment, config, schema, dependencies.
        Blocking: main flight does not start if preflight FAIL.

    QUOTA: just-in-time, BEFORE each LLM call.
        Checks API headroom, model availability.
        Blocking: individual call skipped if quota FAIL.

    RESPONSE: just-in-time, AFTER each LLM call parses.
        Validates response schema, detects silent filtering.
        Non-blocking currently (logs WARN).
    """
    PREFLIGHT = "preflight"
    QUOTA = "quota"
    RESPONSE = "response"
    WRITE = "write"
    AUDIT = "audit"


# ══════════════════════════════════════════════════════════════════════════════
# Status — WHAT the guard decided
# ══════════════════════════════════════════════════════════════════════════════
class GuardStatus(str, Enum):
    """Status communicates the decision outcome to the caller.

    OK:            check passed, proceed normally.
    HEAL_SUCCESS:  check failed initially, heal applied, re-verify passed.
    HEAL_FAILED:   check failed, heal attempted but re-verify failed.
    ABORT:         check failed, no heal available, main flight must not proceed.
    WARN:          check failed but non-blocking (legacy response-guard behavior).
    ERROR:         guard itself crashed (fail-safe: caller treats as ABORT).
    """
    OK = "ok"
    HEAL_SUCCESS = "heal_success"
    HEAL_FAILED = "heal_failed"
    ABORT = "abort"
    WARN = "warn"
    ERROR = "error"

    @property
    def is_proceed(self) -> bool:
        """True iff main flight can proceed under this status."""
        return self in (GuardStatus.OK, GuardStatus.HEAL_SUCCESS, GuardStatus.WARN)

    @property
    def is_blocking(self) -> bool:
        """True iff main flight must be blocked."""
        return self in (GuardStatus.HEAL_FAILED, GuardStatus.ABORT, GuardStatus.ERROR)


# ══════════════════════════════════════════════════════════════════════════════
# Result — unified return type for all guards
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class GuardResult:
    """Unified result contract for any guard check.

    Fields:
        scope:      Which guard layer produced this result
        status:     Decision outcome (see GuardStatus)
        check_name: Human-readable identifier for logs/audit
        message:    One-line summary suitable for log output
        details:    Structured data for audit trail / debugging
        heal_attempted: True if heal was tried (regardless of outcome)
    """
    scope: GuardScope
    status: GuardStatus
    check_name: str
    message: str
    details: dict = field(default_factory=dict)
    heal_attempted: bool = False

    def to_log_line(self) -> str:
        """Produce a one-line log summary.

        Example: '[PREFLIGHT/ok] cron_schedule: all 6 workflows aligned'
        """
        return f"[{self.scope.value}/{self.status.value}] {self.check_name}: {self.message}"

    def to_audit_dict(self) -> dict:
        """Produce a dict suitable for writing to Supabase audit table."""
        return {
            "scope": self.scope.value,
            "status": self.status.value,
            "check_name": self.check_name,
            "message": self.message,
            "details": self.details,
            "heal_attempted": self.heal_attempted,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Collection — results from multiple checks in a single guard run
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class GuardReport:
    """Aggregated results from running multiple checks in one guard pass.

    Used especially by preflight which runs many checks per cycle.
    Provides roll-up summary: any ABORT? any WARN? overall verdict?
    """
    scope: GuardScope
    results: list[GuardResult] = field(default_factory=list)

    def add(self, result: GuardResult) -> None:
        """Append a result to this report."""
        self.results.append(result)

    @property
    def overall_status(self) -> GuardStatus:
        """Worst-case status across all results. Fail-safe: ABORT wins."""
        if not self.results:
            return GuardStatus.OK
        if any(r.status == GuardStatus.ERROR for r in self.results):
            return GuardStatus.ERROR
        if any(r.status == GuardStatus.ABORT for r in self.results):
            return GuardStatus.ABORT
        if any(r.status == GuardStatus.HEAL_FAILED for r in self.results):
            return GuardStatus.HEAL_FAILED
        if any(r.status == GuardStatus.WARN for r in self.results):
            return GuardStatus.WARN
        if any(r.status == GuardStatus.HEAL_SUCCESS for r in self.results):
            return GuardStatus.HEAL_SUCCESS
        return GuardStatus.OK

    @property
    def should_proceed(self) -> bool:
        """True iff main flight can proceed (no ABORT/ERROR/HEAL_FAILED)."""
        return self.overall_status.is_proceed

    def failures(self) -> list[GuardResult]:
        """All results with non-OK status (WARN included)."""
        return [r for r in self.results if r.status != GuardStatus.OK]

    def blocking_failures(self) -> list[GuardResult]:
        """Only results that block main flight."""
        return [r for r in self.results if r.status.is_blocking]

    def summary_line(self) -> str:
        """One-line roll-up suitable for log or Telegram."""
        total = len(self.results)
        ok = sum(1 for r in self.results if r.status == GuardStatus.OK)
        warn = sum(1 for r in self.results if r.status == GuardStatus.WARN)
        blocked = len(self.blocking_failures())
        return (
            f"[{self.scope.value}] {ok}/{total} ok, {warn} warn, {blocked} blocking "
            f"=> overall={self.overall_status.value}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Safe-call wrapper — apply fail-safe discipline to any check function
# ══════════════════════════════════════════════════════════════════════════════
def safe_check(
    scope: GuardScope,
    check_name: str,
    check_fn,
    *,
    error_message: str = "check raised unexpected exception",
) -> GuardResult:
    """Run a check function, returning a GuardResult even if the check crashes.

    This is the fail-safe wrapper that enforces safety invariant #6:
    'Never raise into main system'. Any exception inside check_fn becomes
    a GuardResult with status=ERROR.

    Args:
        scope: Which guard layer is running this check
        check_name: Human-readable name for this check
        check_fn: A callable that returns a GuardResult (or raises)
        error_message: Message used if check_fn crashes

    Returns:
        A GuardResult. Never raises.
    """
    try:
        result = check_fn()
        if not isinstance(result, GuardResult):
            # Defensive: if check_fn didn't return a GuardResult, wrap it
            return GuardResult(
                scope=scope,
                status=GuardStatus.ERROR,
                check_name=check_name,
                message=f"check returned {type(result).__name__}, expected GuardResult",
                details={"returned_value": repr(result)[:200]},
            )
        return result
    except Exception as e:
        return GuardResult(
            scope=scope,
            status=GuardStatus.ERROR,
            check_name=check_name,
            message=f"{error_message}: {type(e).__name__}: {e}",
            details={"exception_type": type(e).__name__, "exception_msg": str(e)[:500]},
        )
