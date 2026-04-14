"""
lens_s2_orchestrator.py
Project Lens — System 2 Orchestrator

Runs all System 2 positions in sequence after System 1 completes.
Called by GitHub Actions lens-manage-analyze.yml.

Current positions:
  S2-A  Injection Tracer       ✅ BUILT — llama-3.3-70b via Groq
  S2-B  Coordination Analyzer  ⏳ LENS-010 next
  S2-C  Emotion Decoder        ⏳ LENS-010 (needs MISTRAL_API_KEY secret)
  S2-D  Adversary Narrative    ⏳ LENS-010
  S2-E  Legitimacy Filter      ⏳ LENS-010

Architecture rules (LR-058 through LR-064):
  - Reads System 1 outputs only. Never modifies System 1 scripts.
  - One-way flow: lens_reports (in) → lens_injection_reports (out)
  - System 1 stays unprotected. System 2 studies what System 1 absorbed.

Session: LENS-010
"""

import sys
import traceback
from datetime import datetime, timezone


def run_s2a():
    """S2-A: Injection Tracer — traces how System 1 was manipulated."""
    print("\n[S2-ORC] Starting S2-A: Injection Tracer...")
    try:
        from lens_system2 import main as s2a_main
        s2a_main()
        print("[S2-ORC] S2-A complete ✅")
        return True
    except Exception as e:
        print(f"[S2-ORC] S2-A failed: {e}")
        traceback.print_exc()
        return False


def run_s2b():
    """S2-B: Coordination Analyzer — NOT YET BUILT (LENS-010)."""
    print("\n[S2-ORC] S2-B: Coordination Analyzer — not yet built, skipping.")
    return True


def run_s2c():
    """S2-C: Emotion Decoder — NOT YET BUILT (needs MISTRAL_API_KEY)."""
    print("\n[S2-ORC] S2-C: Emotion Decoder — not yet built, skipping.")
    return True


def run_s2d():
    """S2-D: Adversary Narrative — NOT YET BUILT (LENS-010)."""
    print("\n[S2-ORC] S2-D: Adversary Narrative — not yet built, skipping.")
    return True


def run_s2e():
    """S2-E: Legitimacy Filter — NOT YET BUILT (LENS-010)."""
    print("\n[S2-ORC] S2-E: Legitimacy Filter — not yet built, skipping.")
    return True


def main():
    print("\n" + "=" * 60)
    print("Project Lens — System 2 Orchestrator")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("  Positions: S2-A ✅  S2-B/C/D/E ⏳")
    print("=" * 60)

    results = {}

    # S2-A — Injection Tracer (live)
    results['S2-A'] = run_s2a()

    # S2-B through S2-E — stubs, skip cleanly
    results['S2-B'] = run_s2b()
    results['S2-C'] = run_s2c()
    results['S2-D'] = run_s2d()
    results['S2-E'] = run_s2e()

    # Summary
    print("\n" + "=" * 60)
    print("System 2 Orchestrator — Run Summary")
    print("=" * 60)
    for pos, ok in results.items():
        status = "✅" if ok else "❌"
        print(f"  {status} {pos}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n[S2-ORC] {len(failed)} position(s) failed: {failed}")
        # Don't exit 1 — partial success is acceptable.
        # S2-A failure is the only critical one.
        if 'S2-A' in failed:
            print("[S2-ORC] S2-A failed — injection trace unavailable this cycle.")
            sys.exit(1)
    else:
        print("\n[S2-ORC] All positions completed.")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
