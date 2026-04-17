"""
lens_cycle.py — Canonical Cycle Label + Timezone Display
Project Lens | LENS-014 O1

Purpose:
  Single source of truth for cycle labels across all Project Lens code.
  Replaces 4 divergent implementations (analyze_lens.py, analyze_lens_multi.py,
  lens_orchestrator.py inline _cycle(), fetch_text.py inline logic) that were
  producing 5 different label vocabularies (morning/afternoon/evening/midnight/
  midday/night/manual) from the same UTC hours.

Design principles (operator-directed):
  1. Timezone-neutral canonical labels: '2of1' and '2of2' (scalable to Nof1..N)
  2. Primary timezone anchor = Washington DC (GCSP mission perspective)
  3. Operator display also in Myanmar Time (MMT, UTC+6:30, no DST)
  4. DC timezone auto-adjusts for EDT/EST via zoneinfo America/New_York
  5. Day 1 = April 17, 2026 UTC — clean-slate boundary between legacy era
     (pre-Day-1, mixed labels) and new era (post-Day-1, canonical)
  6. Legacy data preserved, not rewritten (LENS-015 analytics can use either era)

Cron schedule this module is calibrated to:
  2of1 = UTC 01:28 (DC: 21:28 EDT previous day | MMT: 07:58 same day)
  2of2 = UTC 13:28 (DC: 09:28 EDT same day    | MMT: 19:58 same day)

Tolerance: ±15 min each side of scheduled UTC time counts as that cycle.
Anything else = 'manual' (interactive test run, catch-up, etc.).

Authority: LR-068 (evidence before action), LR-074 (guard pattern —
fail-safe defaults to 'manual' rather than misclassifying).
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python < 3.9 fallback (shouldn't happen on our venv — Python 3.11)
    from backports.zoneinfo import ZoneInfo  # type: ignore


# ══════════════════════════════════════════════════════════════════════════════
# Canonical constants
# ══════════════════════════════════════════════════════════════════════════════

CANONICAL_CYCLES = ["2of1", "2of2"]
"""New-era production cycle labels. Use these in all new code."""

LEGACY_CYCLES = ["morning", "afternoon", "evening", "midnight"]
"""Pre-Day-1 labels. Historical only — do not emit in new code."""

DAY_1_UTC = datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)
"""Day 1 of the 2x/day canonical-label era.

Rows generated >= this datetime SHOULD use CANONICAL_CYCLES.
Rows generated < this datetime are LEGACY_CYCLES (or drift artifacts).
"""

# Cron schedule anchors (UTC)
CRON_2OF1_UTC_HOUR = 1
CRON_2OF1_UTC_MINUTE = 28
CRON_2OF2_UTC_HOUR = 13
CRON_2OF2_UTC_MINUTE = 28

# Tolerance window around each cron time (minutes)
CYCLE_TOLERANCE_MINUTES = 15

# Timezones
DC_TZ = ZoneInfo("America/New_York")  # auto-handles EDT/EST
MMT_TZ = timezone(timedelta(hours=6, minutes=30))  # Myanmar — no DST, fixed offset


# ══════════════════════════════════════════════════════════════════════════════
# Core: determine canonical cycle from current time
# ══════════════════════════════════════════════════════════════════════════════

def get_cycle(now: Optional[datetime] = None) -> str:
    """Determine the canonical cycle label for the current (or given) UTC time.

    Returns one of: '2of1', '2of2', 'manual'.

    A UTC time qualifies for a scheduled cycle if it falls within
    ±CYCLE_TOLERANCE_MINUTES of that cycle's anchor time.

    Args:
        now: Optional UTC datetime. If None, uses datetime.now(timezone.utc).
             Accepting an argument makes this function testable.

    Returns:
        Canonical cycle label.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Normalize to UTC if naive or in a different timezone
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    # Compute minutes-since-midnight for current time and each anchor
    current_minutes = now.hour * 60 + now.minute
    anchor_2of1 = CRON_2OF1_UTC_HOUR * 60 + CRON_2OF1_UTC_MINUTE
    anchor_2of2 = CRON_2OF2_UTC_HOUR * 60 + CRON_2OF2_UTC_MINUTE

    # Delta calculation — handle wrap-around at 24h boundary
    def _within_tolerance(current: int, anchor: int) -> bool:
        """Check if current is within tolerance of anchor, wrap-aware."""
        diff = abs(current - anchor)
        # Handle day-boundary wrap (e.g., 00:10 and 23:50 are 20 min apart, not 23h 40m)
        diff = min(diff, 1440 - diff)
        return diff <= CYCLE_TOLERANCE_MINUTES

    if _within_tolerance(current_minutes, anchor_2of1):
        return "2of1"
    if _within_tolerance(current_minutes, anchor_2of2):
        return "2of2"
    return "manual"


# ══════════════════════════════════════════════════════════════════════════════
# Display: format cycle with DC + MMT time
# ══════════════════════════════════════════════════════════════════════════════

def format_cycle_display(cycle: str, now: Optional[datetime] = None) -> str:
    """Format cycle label with both DC and MMT times for human-readable logs.

    Example output:
        '2of1 (DC: 09:28 PM EDT Thu | MMT: 08:00 AM Fri)'
        '2of2 (DC: 09:28 AM EDT Fri | MMT: 08:00 PM Fri)'
        'manual (DC: 03:14 PM EDT Fri | MMT: 01:44 AM Sat)'

    Args:
        cycle: One of '2of1', '2of2', 'manual', or any other string
               (displayed verbatim).
        now: Optional UTC datetime for timestamp formatting. Default = now().

    Returns:
        Human-readable string safe for logs, Telegram, reports.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    dc = now.astimezone(DC_TZ)
    mmt = now.astimezone(MMT_TZ)

    dc_str = dc.strftime("%I:%M %p %Z %a").lstrip("0")
    mmt_str = mmt.strftime("%I:%M %p %a").lstrip("0")

    return f"{cycle} (DC: {dc_str} | MMT: {mmt_str})"


# ══════════════════════════════════════════════════════════════════════════════
# Era detection helpers
# ══════════════════════════════════════════════════════════════════════════════

def is_legacy_era(dt: datetime) -> bool:
    """True if dt is before Day 1 UTC (legacy era, mixed/drift labels).

    Accepts timezone-aware or naive datetime (naive assumed UTC).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt < DAY_1_UTC


def is_new_era(dt: datetime) -> bool:
    """True if dt is at or after Day 1 UTC (canonical-label era)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= DAY_1_UTC


# ══════════════════════════════════════════════════════════════════════════════
# Convenience: describe the module state
# ══════════════════════════════════════════════════════════════════════════════

def describe_canonical_schedule() -> str:
    """Return a human-readable description of the canonical cycle schedule.

    Useful for startup logs, Telegram status messages, documentation.
    """
    lines = [
        "Project Lens canonical cycle schedule (LENS-014 O1):",
        f"  2of1: UTC {CRON_2OF1_UTC_HOUR:02d}:{CRON_2OF1_UTC_MINUTE:02d} "
        f"(±{CYCLE_TOLERANCE_MINUTES} min tolerance)",
        f"  2of2: UTC {CRON_2OF2_UTC_HOUR:02d}:{CRON_2OF2_UTC_MINUTE:02d} "
        f"(±{CYCLE_TOLERANCE_MINUTES} min tolerance)",
        f"  Day 1: {DAY_1_UTC.isoformat()}",
        f"  Timezones: DC={DC_TZ.key}, MMT=UTC+06:30 (fixed, no DST)",
    ]
    return "\n".join(lines)
