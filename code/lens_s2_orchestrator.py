"""
lens_s2_orchestrator.py — System 2 Orchestrator
Project Lens | LENS-009
Runs: S2-A → S2-B → S2-C → S2-D → S2-E → Mission Analyst
Called by: lens-manage-analyze.yml after lens_orchestrator.py

TPMGuard: prevents 429 cascades by tracking tokens per 60s per key.
GNI pattern adapted for Project Lens S2 multi-provider architecture.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lens_s2a_injection    import run_s2a
from lens_s2b_coordination import run_s2b
from lens_s2c_emotion      import run_s2c
from lens_s2d_adversary    import run_s2d
from lens_s2e_legitimacy   import run_s2e
from lens_mission_analyst  import run_mission_analyst

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-ORC] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2_orchestrator")


# ── TPMGuard ──────────────────────────────────────────────────────────────────
class TPMGuard:
    """
    Rolling 60-second token window tracker per API key.
    Adapted from GNI MAD pipeline guard pattern.

    Purpose: prevent 429 cascades when multiple S2 positions share same key.
    Usage: call wait_until_clear() before starting each Groq-S2 position.
    """

    # Conservative TPM limits — Groq free tier
    TPM_LIMITS = {
        "GROQ_S2":      6_000,   # S2-A, S2-B, S2-D all share this key
        "GROQ_S2E":     6_000,   # S2-E dedicated key
        "GROQ_MANAGER": 6_000,   # Mission Analyst
        "MISTRAL":     30_000,   # S2-C — generous free tier
    }

    # Estimated tokens per position (rough, safe overestimate)
    POSITION_COST = {
        "S2-A": 16_000,   # 8 reports × ~2K tokens
        "S2-B":  5_000,   # 1 cross-report call
        "S2-C": 12_000,   # 8 reports × ~1.5K — Mistral key, tracked separately
        "S2-D":  3_000,   # 1 adversary batch
        "S2-E": 16_000,   # 8 reports × ~2K — GROQ_S2E key
        "MA":    6_000,   # 1 synthesis call — GROQ_MANAGER key
    }

    # Which API key each position uses
    POSITION_KEY = {
        "S2-A": "GROQ_S2",
        "S2-B": "GROQ_S2",
        "S2-C": "MISTRAL",
        "S2-D": "GROQ_S2",
        "S2-E": "GROQ_S2E",
        "MA":   "GROQ_MANAGER",
    }

    def __init__(self):
        self.usage = {}  # key -> list of (timestamp, tokens)

    def log_usage(self, key: str, tokens: int):
        """Record that a position completed using ~tokens on this key."""
        now = time.time()
        if key not in self.usage:
            self.usage[key] = []
        self.usage[key].append((now, tokens))

    def tokens_in_last_60s(self, key: str) -> int:
        """Count tokens used in last 60 seconds for this key."""
        now = time.time()
        cutoff = now - 60.0
        if key not in self.usage:
            return 0
        self.usage[key] = [(t, tok) for t, tok in self.usage[key] if t > cutoff]
        return sum(tok for _, tok in self.usage[key])

    def wait_until_clear(self, key: str, headroom_needed: int = 3000, label: str = ""):
        """
        Wait until the key's TPM window has enough headroom.
        Never crashes — just waits and retries every 10s.
        """
        limit = self.TPM_LIMITS.get(key, 6000)
        waited_total = 0
        while True:
            used = self.tokens_in_last_60s(key)
            available = limit - used
            if available >= headroom_needed:
                if waited_total > 0:
                    log.info(f"[TPMGuard] {key}{' '+label if label else ''}: window clear "
                             f"({available}/{limit} available) after {waited_total}s wait")
                return
            wait_secs = 10
            log.info(f"[TPMGuard] {key}{' '+label if label else ''}: window has "
                     f"{used}/{limit} tokens — waiting {wait_secs}s "
                     f"(need {headroom_needed} headroom)...")
            time.sleep(wait_secs)
            waited_total += wait_secs

    def pre_position_check(self, position: str):
        """Run before each position: wait if needed, then proceed."""
        key = self.POSITION_KEY.get(position, "GROQ_S2")
        needed = min(self.POSITION_COST.get(position, 5000), 5000)  # cap at 5K check
        self.wait_until_clear(key, headroom_needed=needed, label=f"({position})")


# ── Cycle detection ───────────────────────────────────────────────────────────
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


# ── Main orchestrator ─────────────────────────────────────────────────────────
def run_s2_orchestrator() -> dict:
    """Run all System 2 positions in sequence with TPM protection."""
    start  = time.time()
    run_id = f"s2_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    cycle  = get_latest_cycle()
    guard  = TPMGuard()

    log.info("=" * 60)
    log.info("SYSTEM 2 ORCHESTRATOR START")
    log.info(f"run_id : {run_id}")
    log.info(f"cycle  : {cycle}")
    log.info("=" * 60)

    results = {}
    positions = [
        ("S2-A", run_s2a,  "GROQ_S2",      12),
        ("S2-B", run_s2b,  "GROQ_S2",      12),
        ("S2-C", run_s2c,  "MISTRAL",       6),
        ("S2-D", run_s2d,  "GROQ_S2",      12),
        ("S2-E", run_s2e,  "GROQ_S2E",      6),
    ]

    for name, fn, key, post_stagger in positions:
        log.info(f"--- Running {name} ---")

        # TPM guard: wait until key window has headroom
        guard.pre_position_check(name)

        try:
            result = fn(cycle=cycle, run_id=run_id)
            status = result.get("status", "UNKNOWN")
            results[name] = status
            log.info(f"{name} finished: {status}")
            # Log estimated token usage for this position
            guard.log_usage(key, TPMGuard.POSITION_COST.get(name, 5000))
        except Exception as e:
            log.error(f"{name} crashed: {e}")
            results[name] = f"CRASH: {e}"

        log.info(f"Stagger {post_stagger}s before next position...")
        time.sleep(post_stagger)

    # Mission Analyst — uses GROQ_MANAGER (separate key)
    log.info("--- Running Mission Analyst ---")
    guard.pre_position_check("MA")
    try:
        ma_result = run_mission_analyst(cycle=cycle, run_id=run_id)
        results["MISSION_ANALYST"] = ma_result.get("status", "UNKNOWN")
        log.info(f"Mission Analyst finished: {results['MISSION_ANALYST']}")
    except Exception as e:
        log.error(f"Mission Analyst crashed: {e}")
        results["MISSION_ANALYST"] = f"CRASH: {e}"

    elapsed = round(time.time() - start, 1)
    passed  = sum(1 for v in results.values() if v == "COMPLETE")
    total   = len(results)

    summary = {
        "status":           "COMPLETE" if passed == total else "PARTIAL",
        "run_id":           run_id,
        "cycle":            cycle,
        "positions_passed": passed,
        "positions_total":  total,
        "results":          results,
        "elapsed_seconds":  elapsed,
    }

    log.info("=" * 60)
    log.info("SYSTEM 2 ORCHESTRATOR COMPLETE")
    log.info(f"Passed : {passed}/{total}")
    log.info(f"Elapsed: {elapsed}s")
    log.info(f"Results: {results}")
    log.info("=" * 60)

    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    run_s2_orchestrator()
