"""
lens_s3_orchestrator.py
Project Lens — System 3 Orchestrator

Runs all System 3 positions after System 2 completes.

Positions:
  S3-A  lens_s3a_patterns.py    run_s3a()  llama-3.3-70b   Groq       DAILY
  S3-B  lens_s3b_truehistory.py run_s3b()  gemini-2.0-flash Google     DAILY
  S3-D  lens_s3d_longterm.py    run_s3d()  qwen-3-235b     Cerebras   2x/WEEK

Deferred (need accounts):
  S3-C  Bias Drift Monitor — command-r-plus (Cohere) — needs Cohere account
  S3-E  Self-Check LOCAL   — llama-3.1-70b (Ollama)  — needs 16GB RAM local

Session: LENS-010
"""

import sys, traceback
from datetime import datetime, timezone

RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")


def _run(position, fn, **kwargs):
    print(f"\n[S3-ORC] ── {position} ──────────────────────────────")
    try:
        result = fn(**kwargs)
        if isinstance(result, dict):
            status = result.get("status", "UNKNOWN")
            ok = status not in ("SAVE_FAILED", "ERROR", "ANALYSIS_FAILED")
            symbol = "✅" if ok else "⚠️ "
            print(f"[S3-ORC] {position} {symbol}  status={status}")
            return ok, result
        return bool(result), {}
    except Exception as e:
        print(f"[S3-ORC] {position} ❌  exception: {e}")
        traceback.print_exc()
        return False, {}


def main():
    print("\n" + "=" * 60)
    print("Project Lens — System 3 Orchestrator")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  run_id: {RUN_ID}")
    print("  Positions: S3-A · S3-B · S3-D  |  S3-C/E deferred")
    print("=" * 60)

    results = {}

    from lens_s3a_patterns   import run_s3a
    from lens_s3b_truehistory import run_s3b
    from lens_s3d_longterm   import run_s3d

    ok_a, _ = _run("S3-A Pattern Intelligence",  run_s3a, run_id=RUN_ID)
    results["S3-A"] = ok_a

    ok_b, _ = _run("S3-B True History",          run_s3b, run_id=RUN_ID)
    results["S3-B"] = ok_b

    print("\n[S3-ORC] S3-C: Bias Drift Monitor — deferred (needs Cohere account)")
    results["S3-C"] = True  # skip cleanly

    ok_d, _ = _run("S3-D Long-term Researcher",  run_s3d, run_id=RUN_ID)
    results["S3-D"] = ok_d

    print("\n[S3-ORC] S3-E: Self-Check LOCAL — deferred (needs Ollama local setup)")
    results["S3-E"] = True  # skip cleanly

    print("\n" + "=" * 60)
    print("System 3 Orchestrator — Run Summary")
    print("=" * 60)
    for pos, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {pos}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n[S3-ORC] {len(failed)} failed: {failed}")
    else:
        print("\n[S3-ORC] All positions complete.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
