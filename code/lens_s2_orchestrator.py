"""
lens_s2_orchestrator.py — System 2 Orchestrator
Project Lens | LENS-009
Runs: S2-A → S2-B → S2-C → S2-D → S2-E → Mission Analyst
Called by: lens-manage-analyze.yml after lens_orchestrator.py
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

# Add code directory to path so imports work from yml
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lens_s2a_injection    import run_s2a
from lens_s2b_coordination import run_s2b
from lens_s2c_emotion      import run_s2c
from lens_s2d_adversary    import run_s2d
from lens_s2e_legitimacy   import run_s2e
from lens_mission_analyst  import run_mission_analyst

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-ORC] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2_orchestrator")

# ── Constants ─────────────────────────────────────────────────────────────────
STAGGER_BETWEEN_POSITIONS = 12   # seconds between S2 positions — avoid provider hammering


def get_latest_cycle() -> Optional[str]:
    """Get the most recent cycle label from lens_reports."""
    try:
        from supabase import create_client
        sb = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_KEY"]
        )
        result = sb.table("lens_reports") \
            .select("cycle") \
            .order("generated_at", desc=True) \
            .limit(1) \
            .execute()
        if result.data:
            cycle = result.data[0].get("cycle")
            log.info(f"Latest cycle from lens_reports: {cycle}")
            return cycle
    except Exception as e:
        log.warning(f"Could not fetch latest cycle: {e}")
    return None


def run_s2_orchestrator() -> dict:
    """Run all System 2 positions in sequence with shared run_id."""
    start   = time.time()
    run_id  = f"s2_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    cycle   = get_latest_cycle()

    log.info("=" * 60)
    log.info(f"SYSTEM 2 ORCHESTRATOR START")
    log.info(f"run_id : {run_id}")
    log.info(f"cycle  : {cycle}")
    log.info("=" * 60)

    results = {}
    positions = [
        ("S2-A", run_s2a),
        ("S2-B", run_s2b),
        ("S2-C", run_s2c),
        ("S2-D", run_s2d),
        ("S2-E", run_s2e),
    ]

    # ── Run S2-A through S2-E ─────────────────────────────────────────────────
    for name, fn in positions:
        log.info(f"--- Running {name} ---")
        try:
            result = fn(cycle=cycle, run_id=run_id)
            status = result.get("status", "UNKNOWN")
            results[name] = status
            log.info(f"{name} finished: {status}")
        except Exception as e:
            log.error(f"{name} crashed: {e}")
            results[name] = f"CRASH: {e}"

        # Stagger between positions
        log.info(f"Stagger {STAGGER_BETWEEN_POSITIONS}s before next position...")
        time.sleep(STAGGER_BETWEEN_POSITIONS)

    # ── Run Mission Analyst last ───────────────────────────────────────────────
    log.info("--- Running Mission Analyst ---")
    try:
        ma_result = run_mission_analyst(cycle=cycle, run_id=run_id)
        results["MISSION_ANALYST"] = ma_result.get("status", "UNKNOWN")
        log.info(f"Mission Analyst finished: {results['MISSION_ANALYST']}")
    except Exception as e:
        log.error(f"Mission Analyst crashed: {e}")
        results["MISSION_ANALYST"] = f"CRASH: {e}"

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed   = round(time.time() - start, 1)
    passed    = sum(1 for v in results.values() if v == "COMPLETE")
    total     = len(results)
    all_ok    = passed == total

    summary = {
        "status":          "COMPLETE" if all_ok else "PARTIAL",
        "run_id":          run_id,
        "cycle":           cycle,
        "positions_passed": passed,
        "positions_total":  total,
        "results":         results,
        "elapsed_seconds": elapsed,
    }

    log.info("=" * 60)
    log.info(f"SYSTEM 2 ORCHESTRATOR COMPLETE")
    log.info(f"Passed : {passed}/{total}")
    log.info(f"Elapsed: {elapsed}s")
    log.info(f"Results: {results}")
    log.info("=" * 60)

    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    run_s2_orchestrator()
