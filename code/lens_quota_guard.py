"""
lens_quota_guard.py — Pre-flight Quota Guard (OBSERVER MODE)
=============================================================
Project Lens | LENS-013

Purpose:
  Prevent the Run #29 class of failure: positions burning retry storms on an
  already-exhausted quota, leaving downstream positions (MA, S3-A) with no
  headroom. Guard runs BEFORE expensive positions fire, decides PROCEED /
  DEGRADE / SKIP / FORCE based on ledger-recorded consumption vs known limits.

Philosophy (DOC-006):
  - Fail-safe, not fail-secure: on guard error, cron PROCEEDS. Blocking
    legitimate work is worse than degraded telemetry.
  - Fire-and-forget ledger: if Supabase write fails, cron still proceeds.
  - Conservative estimates: when in doubt, assume worse consumption.
  - Observer first: this MVP logs decisions but doesn't yet enforce DEGRADE.
    Only SKIP blocks positions. Full enforcement lands in LENS-014 after
    2-3 cron cycles of observed data (LR-068).

Rules referenced:
  LR-068 Evidence before upgrade
  LR-074 Pre-Flight Quota Guard Pattern (GNI-R-112 import)
  LR-075 Sacred Cron Inviolability (GNI-R-121 import)

Imported from GNI Autonomous ai_engine/quota_guard.py pattern, adapted for
Project Lens multi-position architecture.

Author: Team Geeks (Bro Alpha + Claude Opus 4.7)
Date:   2026-04-17
"""
from __future__ import annotations

import os
import sys
import json
import logging
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional, NamedTuple

from dotenv import load_dotenv
load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUOTA_GUARD] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("quota_guard")


# ─────────────────────────────────────────────────────────────────────────
# 1. Constants — provider limits and per-position consumption estimates
# ─────────────────────────────────────────────────────────────────────────

# Known daily limits per (provider, model). Conservative free-tier values.
# Update when provider tiers change. Sourced from provider documentation
# as of 2026-04-17.
PROVIDER_LIMITS: dict[tuple[str, str], dict[str, int]] = {
    ("groq",      "llama-3.3-70b-versatile"): {"TPD": 100_000},
    ("groq",      "qwen3-32b"):                {"TPD": 100_000},
    ("gemini",    "gemini-2.0-flash"):         {"RPD": 1_500},
    ("cerebras",  "qwen-3-235b"):              {"TPD": 1_000_000},
    ("mistral",   "mistral-small"):            {"RPD": 2_000},
    ("sambanova", "llama-3.3-70b"):            {"TPD": 500_000},
    ("cohere",    "command-r-plus"):           {"RPD": 1_000},
}

# Per-position consumption estimates per cron run, based on Run #29 telemetry.
# Format: position -> (provider, model, estimated_units_per_cron)
# Units are TPD-tokens for Groq/Cerebras/SambaNova and RPD-requests for Gemini/
# Mistral/Cohere, matching the quota_type of the provider's limit.
POSITION_CONSUMPTION: dict[str, tuple[str, str, int]] = {
    "S1-L1":  ("groq",      "qwen3-32b",                 4_000),
    "S1-L2":  ("gemini",    "gemini-2.0-flash",              1),
    "S1-L3":  ("cerebras",  "qwen-3-235b",               5_000),
    "S1-L4":  ("cerebras",  "qwen-3-235b",               5_000),
    "S2-A":   ("groq",      "llama-3.3-70b-versatile",   5_000),
    "S2-B":   ("gemini",    "gemini-2.0-flash",              1),
    "S2-C":   ("mistral",   "mistral-small",                 1),
    "S2-D":   ("groq",      "qwen3-32b",                 2_000),
    "S2-E":   ("groq",      "llama-3.3-70b-versatile",  10_000),
    "S2-GAP": ("groq",      "llama-3.3-70b-versatile",   3_000),
    "MA":     ("groq",      "llama-3.3-70b-versatile",   6_000),
    "S3-A":   ("groq",      "llama-3.3-70b-versatile",   7_000),
    "S3-B":   ("gemini",    "gemini-2.0-flash",              1),
    "S3-C":   ("cohere",    "command-r-plus",                1),
    "S3-D":   ("cerebras",  "qwen-3-235b",               6_000),
    "S3-E":   ("sambanova", "llama-3.3-70b",             3_000),
}

# Decision thresholds (headroom percent)
THRESHOLD_TIGHT    = 40.0   # below this: PROCEED_TIGHT
THRESHOLD_DEGRADE  = 20.0   # below this: DEGRADE
THRESHOLD_SKIP     =  0.0   # below or equal: SKIP


# ─────────────────────────────────────────────────────────────────────────
# 2. Types — decision and snapshot
# ─────────────────────────────────────────────────────────────────────────

class Decision:
    PROCEED        = "PROCEED"
    PROCEED_TIGHT  = "PROCEED_TIGHT"
    DEGRADE        = "DEGRADE"
    SKIP           = "SKIP"
    FORCE          = "FORCE"
    TEST           = "TEST"


@dataclass
class QuotaResult:
    """Outcome of a quota check for one (provider, model, quota_type) tuple."""
    decision:       str
    reason:         str
    provider:       str
    model:          str
    quota_type:     str                       # 'TPD' or 'RPD'
    limit_value:    Optional[int]             # None if unknown
    used_value:     Optional[int]             # None if ledger read failed
    remaining:      Optional[int]             # None if can't compute
    estimated_use:  int
    headroom_pct:   Optional[float]           # None if can't compute
    error_class:    Optional[str]   = None    # 'LEDGER_FAIL', 'LIMITS_UNKNOWN', etc
    positions:      list           = field(default_factory=list)

    def to_row(self, run_id: str) -> dict:
        """Convert to lens_quota_ledger row dict."""
        return {
            "run_id":         run_id,
            "provider":       self.provider,
            "model":          self.model,
            "quota_type":     self.quota_type,
            "limit_value":    self.limit_value,
            "used_value":     self.used_value,
            "remaining":      self.remaining,
            "estimated_use":  self.estimated_use,
            "headroom_pct":   self.headroom_pct,
            "decision":       self.decision,
            "reason":         self.reason,
            "error_class":    self.error_class,
            "positions":      self.positions,
        }


# ─────────────────────────────────────────────────────────────────────────
# 3. Core logic — pure decision function (no I/O, easy to test)
# ─────────────────────────────────────────────────────────────────────────

def decide(
    provider:      str,
    model:         str,
    quota_type:    str,
    used_today:    Optional[int],
    limit_value:   Optional[int],
    estimated_use: int,
    positions:     list,
    force:         bool = False,
    test_mode:     bool = False,
) -> QuotaResult:
    """
    Pure decision function: given inputs, return decision without side effects.
    Safe to call repeatedly. Handles T20-T26 (edge cases) + T30 (test mode).

    Args:
        provider:      'groq', 'gemini', etc.
        model:         e.g. 'llama-3.3-70b-versatile'
        quota_type:    'TPD' or 'RPD'
        used_today:    sum from ledger since 00:00 UTC; None if read failed
        limit_value:   from PROVIDER_LIMITS; None if unknown
        estimated_use: this cron's expected consumption
        positions:     positions being checked (for logging)
        force:         LENS_FORCE=1 override
        test_mode:     LENS_GUARD_TEST=1

    Returns:
        QuotaResult with decision + all inputs recorded.
    """
    # T30: test mode override — never decide based on real data in tests
    if test_mode:
        return QuotaResult(
            decision=Decision.TEST,
            reason="LENS_GUARD_TEST=1 — returning PROCEED for test safety",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=None, estimated_use=estimated_use,
            headroom_pct=None, positions=list(positions),
        )

    # T26: FORCE override — admin emergency bypass
    if force:
        return QuotaResult(
            decision=Decision.FORCE,
            reason="LENS_FORCE=1 — operator override, proceeding regardless",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=(limit_value - used_today) if (limit_value is not None and used_today is not None) else None,
            estimated_use=estimated_use, headroom_pct=None,
            positions=list(positions),
        )

    # Fail-safe branch: if we don't know the limit, PROCEED conservatively
    # (T08 schema drift, T14 provider reports 0 limit)
    if limit_value is None or limit_value <= 0:
        return QuotaResult(
            decision=Decision.PROCEED,
            reason=f"Unknown or zero limit for {provider}/{model} — proceeding conservatively (fail-safe)",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=None, estimated_use=estimated_use,
            headroom_pct=None, error_class="LIMITS_UNKNOWN",
            positions=list(positions),
        )

    # Fail-safe branch: if ledger read failed, PROCEED with warning
    # (T16 Supabase unreachable, T17 RLS blocks read)
    if used_today is None:
        return QuotaResult(
            decision=Decision.PROCEED,
            reason=f"Ledger read failed for {provider}/{model} — proceeding conservatively",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=None,
            remaining=None, estimated_use=estimated_use,
            headroom_pct=None, error_class="LEDGER_UNAVAILABLE",
            positions=list(positions),
        )

    # T11: clamp negative used values to 0 (provider bug)
    used_today = max(0, used_today)
    # T13: clamp used to limit (can happen if we double-logged)
    used_today = min(used_today, limit_value * 2)  # allow some over-limit reporting

    remaining = limit_value - used_today
    headroom = remaining - estimated_use
    headroom_pct = (headroom / limit_value) * 100.0

    # T22: headroom negative — already over limit
    if headroom_pct < THRESHOLD_SKIP:
        return QuotaResult(
            decision=Decision.SKIP,
            reason=f"Over-limit: {used_today}/{limit_value} used ({headroom_pct:.1f}% headroom) — skip positions",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=remaining, estimated_use=estimated_use,
            headroom_pct=headroom_pct, positions=list(positions),
        )

    # T21: headroom exactly 0 (edge case)
    if headroom_pct == THRESHOLD_SKIP:
        return QuotaResult(
            decision=Decision.SKIP,
            reason=f"Zero headroom: {used_today}/{limit_value} with estimated {estimated_use} needed",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=remaining, estimated_use=estimated_use,
            headroom_pct=headroom_pct, positions=list(positions),
        )

    # Below DEGRADE threshold: skip heaviest positions
    if headroom_pct < THRESHOLD_DEGRADE:
        return QuotaResult(
            decision=Decision.DEGRADE,
            reason=f"Low headroom: {headroom_pct:.1f}% — consider skipping heaviest positions",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=remaining, estimated_use=estimated_use,
            headroom_pct=headroom_pct, positions=list(positions),
        )

    # Below TIGHT threshold: proceed but advise operator
    if headroom_pct < THRESHOLD_TIGHT:
        return QuotaResult(
            decision=Decision.PROCEED_TIGHT,
            reason=f"Tight headroom: {headroom_pct:.1f}% — proceed with caution",
            provider=provider, model=model, quota_type=quota_type,
            limit_value=limit_value, used_value=used_today,
            remaining=remaining, estimated_use=estimated_use,
            headroom_pct=headroom_pct, positions=list(positions),
        )

    # T24: plenty of headroom
    return QuotaResult(
        decision=Decision.PROCEED,
        reason=f"Clean headroom: {headroom_pct:.1f}% ({remaining}/{limit_value} remaining)",
        provider=provider, model=model, quota_type=quota_type,
        limit_value=limit_value, used_value=used_today,
        remaining=remaining, estimated_use=estimated_use,
        headroom_pct=headroom_pct, positions=list(positions),
    )


# ─────────────────────────────────────────────────────────────────────────
# 4. Aggregation — per-provider positions + consumption
# ─────────────────────────────────────────────────────────────────────────

def aggregate_positions(
    positions: list[str]
) -> dict[tuple[str, str], tuple[int, list[str]]]:
    """
    Group positions by (provider, model) and sum their estimated consumption.

    Args:
        positions: list like ['S2-A', 'S2-E', 'MA']

    Returns:
        dict mapping (provider, model) -> (total_estimated_use, positions_list)

    Handles T25: multiple providers, independent aggregation per (provider, model).
    """
    grouped: dict[tuple[str, str], tuple[int, list[str]]] = {}
    for pos in positions:
        if pos not in POSITION_CONSUMPTION:
            log.warning(f"Unknown position '{pos}' — skipping consumption estimate")
            continue
        provider, model, estimated = POSITION_CONSUMPTION[pos]
        key = (provider, model)
        if key in grouped:
            prev_total, prev_list = grouped[key]
            grouped[key] = (prev_total + estimated, prev_list + [pos])
        else:
            grouped[key] = (estimated, [pos])
    return grouped


# ─────────────────────────────────────────────────────────────────────────
# 5. Ledger I/O — fail-safe reads and writes
# ─────────────────────────────────────────────────────────────────────────

def read_ledger_usage(
    sb,
    provider: str,
    model: str,
) -> Optional[int]:
    """
    Sum consumption logged to lens_quota_ledger since 00:00 UTC today.
    Returns None on any failure (fail-safe: caller decides PROCEED).

    Handles T16-T18 (Supabase unreachable / RLS / schema stale).
    """
    try:
        midnight_utc = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
        r = (
            sb.table("lens_quota_ledger")
              .select("estimated_use")
              .eq("provider", provider)
              .eq("model", model)
              .gte("cron_time_utc", midnight_utc)
              .execute()
        )
        rows = r.data or []
        total = sum(row.get("estimated_use", 0) or 0 for row in rows)
        return total
    except Exception as e:
        log.warning(f"Ledger read failed for {provider}/{model}: {e}")
        return None


def write_ledger_entry(sb, run_id: str, result: QuotaResult) -> bool:
    """
    Write one QuotaResult to lens_quota_ledger.
    Returns True on success, False on any failure.
    Never raises — fire-and-forget per Principle 2.

    Handles T16-T19.
    """
    try:
        row = result.to_row(run_id)
        sb.table("lens_quota_ledger").insert(row).execute()
        return True
    except Exception as e:
        # Log to stderr AS WELL as failing gracefully — GitHub Actions captures stderr
        msg = f"LEDGER_WRITE_FAIL for {result.provider}/{result.model}: {e}"
        log.error(msg)
        print(msg, file=sys.stderr)
        return False


# ─────────────────────────────────────────────────────────────────────────
# 6. Main entry point — orchestrates all above with outer try/except
# ─────────────────────────────────────────────────────────────────────────

def guard_check(
    positions: list[str],
    run_id: Optional[str] = None,
    sb = None,
    record_ledger: bool = True,
) -> list[QuotaResult]:
    """
    Core guard check. Returns list of QuotaResult, one per (provider, model)
    group. Does NOT raise on errors — always returns a result list.

    This is the function lens_orchestrator.py will call pre-flight.

    Args:
        positions: list of position names to check, e.g. ['S2-A','S2-E','MA']
        run_id:    string tag for ledger; auto-generated if None
        sb:        optional Supabase client; fetched fresh if None
        record_ledger: whether to write ledger entries (default True)

    Returns:
        list[QuotaResult] with one entry per unique (provider, model) used
        by the given positions.
    """
    # Auto-generate run_id if not provided
    if run_id is None:
        run_id = f"guard_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # Env-var overrides (T26, T30)
    force     = os.environ.get("LENS_FORCE", "0") == "1"
    test_mode = os.environ.get("LENS_GUARD_TEST", "0") == "1"

    # Supabase client — fetch if not provided, don't crash if it fails
    if sb is None and not test_mode:
        try:
            from supabase import create_client
            sb = create_client(
                os.environ["SUPABASE_URL"],
                os.environ["SUPABASE_SERVICE_KEY"],
            )
        except Exception as e:
            log.warning(f"Supabase client init failed: {e} — proceeding without ledger")
            sb = None

    # Group positions by (provider, model)
    grouped = aggregate_positions(positions)

    results: list[QuotaResult] = []
    for (provider, model), (estimated, pos_list) in grouped.items():
        # Lookup known limits (T14: unknown = fail-safe proceed)
        limits_dict = PROVIDER_LIMITS.get((provider, model), {})
        if not limits_dict:
            # Unknown provider/model — log and proceed
            result = decide(
                provider=provider, model=model, quota_type="UNKNOWN",
                used_today=None, limit_value=None,
                estimated_use=estimated, positions=pos_list,
                force=force, test_mode=test_mode,
            )
            result.error_class = "LIMITS_UNKNOWN"
            results.append(result)
            continue

        # For each quota_type the provider tracks (usually one: TPD or RPD)
        for quota_type, limit_value in limits_dict.items():
            # Read ledger usage (T16-T18 handled inside)
            used_today = None
            if sb is not None and not test_mode:
                used_today = read_ledger_usage(sb, provider, model)

            # Decide
            result = decide(
                provider=provider, model=model, quota_type=quota_type,
                used_today=used_today, limit_value=limit_value,
                estimated_use=estimated, positions=pos_list,
                force=force, test_mode=test_mode,
            )
            results.append(result)

    # Write ledger entries — fire and forget (T16-T19)
    if record_ledger and sb is not None and not test_mode:
        for result in results:
            write_ledger_entry(sb, run_id, result)

    # Log summary
    log.info(f"Guard check complete: {len(results)} quota groups evaluated for run_id={run_id}")
    for r in results:
        log.info(f"  [{r.decision}] {r.provider}/{r.model} ({r.quota_type}) "
                 f"positions={r.positions} reason={r.reason}")

    return results


def guard_check_with_fallback(
    positions: list[str],
    run_id: Optional[str] = None,
    sb = None,
    record_ledger: bool = True,
) -> list[QuotaResult]:
    """
    Outer wrapper that catches ANY exception from guard_check() and returns
    a fail-safe PROCEED result. This is the function external callers should
    use to guarantee never-crash behavior.

    Handles T27: guard crashes with unexpected exception.
    Handles T28: guard takes too long (future: add timeout wrapper).
    """
    try:
        return guard_check(positions=positions, run_id=run_id, sb=sb,
                           record_ledger=record_ledger)
    except Exception as e:
        # Total guard failure. Log everywhere. Return safe default.
        tb = traceback.format_exc()
        log.error(f"GUARD_CRASH: {e}\n{tb}")
        print(f"GUARD_CRASH: {e}\n{tb}", file=sys.stderr)
        return [QuotaResult(
            decision=Decision.PROCEED,
            reason=f"Guard crashed ({type(e).__name__}: {e}) — fail-safe PROCEED",
            provider="unknown", model="unknown", quota_type="UNKNOWN",
            limit_value=None, used_value=None, remaining=None,
            estimated_use=0, headroom_pct=None,
            error_class="GUARD_CRASH",
            positions=list(positions),
        )]


# ─────────────────────────────────────────────────────────────────────────
# 7. Helper for pipeline code — decide what positions to run
# ─────────────────────────────────────────────────────────────────────────

def filter_positions_by_guard(
    positions: list[str],
    results: list[QuotaResult],
) -> dict[str, str]:
    """
    Given positions and guard results, return a decision per position.

    Returns:
        dict like {'S2-A': 'PROCEED', 'MA': 'SKIP', 'S3-A': 'SKIP'}

    In OBSERVER MODE: only SKIP is enforced. DEGRADE/PROCEED_TIGHT become PROCEED.
    """
    # Build lookup: (provider, model) -> decision
    decision_by_key: dict[tuple[str, str], str] = {}
    for r in results:
        decision_by_key[(r.provider, r.model)] = r.decision

    per_position: dict[str, str] = {}
    for pos in positions:
        if pos not in POSITION_CONSUMPTION:
            per_position[pos] = "UNKNOWN"
            continue
        provider, model, _ = POSITION_CONSUMPTION[pos]
        raw = decision_by_key.get((provider, model), Decision.PROCEED)

        # OBSERVER MODE: only SKIP is enforced
        if raw == Decision.SKIP:
            per_position[pos] = "SKIP"
        elif raw == Decision.FORCE:
            per_position[pos] = "PROCEED"  # force always proceeds
        elif raw == Decision.TEST:
            per_position[pos] = "PROCEED"  # test mode always proceeds
        else:
            # PROCEED, PROCEED_TIGHT, DEGRADE all become PROCEED in observer mode
            per_position[pos] = "PROCEED"
    return per_position


# ─────────────────────────────────────────────────────────────────────────
# 8. CLI entry point — standalone testing
# ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pre-flight quota guard for Project Lens")
    parser.add_argument("--positions", type=str, default="S2-A,S2-E,S2-GAP,MA,S3-A",
                        help="Comma-separated position names")
    parser.add_argument("--no-write", action="store_true",
                        help="Don't write to ledger (dry run)")
    parser.add_argument("--test-mode", action="store_true",
                        help="Set LENS_GUARD_TEST=1 for this run")
    args = parser.parse_args()

    if args.test_mode:
        os.environ["LENS_GUARD_TEST"] = "1"

    positions = [p.strip() for p in args.positions.split(",") if p.strip()]
    log.info(f"Running guard check for positions: {positions}")

    results = guard_check_with_fallback(
        positions=positions,
        record_ledger=(not args.no_write),
    )

    print("\n" + "=" * 72)
    print("QUOTA GUARD RESULTS")
    print("=" * 72)
    for r in results:
        print(f"\n[{r.decision}] {r.provider}/{r.model} ({r.quota_type})")
        print(f"  positions:     {r.positions}")
        print(f"  limit:         {r.limit_value}")
        print(f"  used_today:    {r.used_value}")
        print(f"  remaining:     {r.remaining}")
        print(f"  estimated_use: {r.estimated_use}")
        print(f"  headroom_pct:  {r.headroom_pct}")
        print(f"  reason:        {r.reason}")
        if r.error_class:
            print(f"  error_class:   {r.error_class}")

    per_position = filter_positions_by_guard(positions, results)
    print("\n" + "-" * 72)
    print("Per-position decisions (OBSERVER MODE — only SKIP is enforced):")
    print("-" * 72)
    for pos, decision in per_position.items():
        print(f"  {pos:8} -> {decision}")
