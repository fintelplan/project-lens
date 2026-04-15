"""
lens_s3_orchestrator.py
Project Lens — System 3 Orchestrator

S3-A  lens_s3a_patterns.py     daily      llama-3.3-70b   Groq/GROQ_S3_API_KEY
S3-B  lens_s3b_truehistory.py  daily      gemini-2.0-flash Google/GEMINI_API_KEY
S3-C  NOT BUILT                weekly     command-r-plus  Cohere — needs account
S3-D  lens_s3d_longterm.py     Mon+Thu    qwen-3-235b     Cerebras/CEREBRAS_API_KEY
S3-E  lens_s3e_selfcheck.py    daily      llama-3.3-70b   SambaNova/SAMBANOVA_API_KEY

S3-E replaces original Ollama LOCAL design.
SambaNova = RDU hardware (3rd type: Groq=LPU, Cerebras=WSE, SambaNova=RDU)
Defends Pattern 5: Recursive Self-Injection — the only pattern with no prior defense.

Session: LENS-010 (S3-E added)
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
            print(f"[S3-ORC] {position} {'✅' if ok else '⚠️ '}  status={status}")
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
    print("  S3-A · S3-B · S3-D · S3-E  |  S3-C deferred")
    print("=" * 60)

    results = {}

    from lens_s3a_patterns    import run_s3a
    from lens_s3b_truehistory import run_s3b
    from lens_s3d_longterm    import run_s3d
    from lens_s3e_selfcheck   import run_s3e

    ok_a, _ = _run("S3-A Pattern Intelligence",  run_s3a, run_id=RUN_ID)
    results["S3-A"] = ok_a

    ok_b, _ = _run("S3-B True History",          run_s3b, run_id=RUN_ID)
    results["S3-B"] = ok_b

    print("\n[S3-ORC] S3-C: Bias Drift Monitor — deferred (needs Cohere account)")
    results["S3-C"] = True

    ok_d, _ = _run("S3-D Long-term Researcher",  run_s3d, run_id=RUN_ID)
    results["S3-D"] = ok_d

    ok_e, _ = _run("S3-E Self-Check (SambaNova)", run_s3e, run_id=RUN_ID)
    results["S3-E"] = ok_e

    print("\n" + "=" * 60)
    print("System 3 — Run Summary")
    print("=" * 60)
    for pos, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {pos}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n[S3-ORC] {len(failed)} failed: {failed}")
        # S3-E failure is a WARNING — Pattern 5 defense unavailable this cycle
        if "S3-E" in failed:
            print("[S3-ORC] WARNING: S3-E failed — Pattern 5 defense unavailable this cycle.")
    else:
        print("\n[S3-ORC] All positions complete.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
