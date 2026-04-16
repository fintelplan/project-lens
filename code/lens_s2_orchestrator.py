"""
lens_s2_orchestrator.py
Project Lens — System 2 Orchestrator

Calls all System 2 positions in sequence after System 1 completes.
Called by GitHub Actions lens-manage-analyze.yml.

All positions built in LENS-009. Orchestrator updated in LENS-010.

Positions:
  S2-A  lens_s2a_injection.py    run_s2a()           llama-3.3-70b  GROQ_S2_API_KEY
  S2-B  lens_s2b_coordination.py run_s2b()           gemini-1.5-flash GEMINI_API_KEY
  S2-C  lens_s2c_emotion.py      run_s2c()           mistral-small  MISTRAL_API_KEY
  S2-D  lens_s2d_adversary.py    run_s2d()           qwen3-32b      GROQ_S2_API_KEY
  S2-E  lens_s2e_legitimacy.py   run_s2e()           llama-3.3-70b  GROQ_S2E_API_KEY
  MA    lens_mission_analyst.py  run_mission_analyst() llama-3.3-70b GROQ_MANAGER_API_KEY

Architecture: LR-058 to LR-064.
  One-way flow. System 1 scripts FROZEN.
  S2 reads lens_reports → writes lens_injection_reports.
  MA reads lens_reports + lens_injection_reports → writes lens_macro_reports.

Session: LENS-010 (orchestrator fix)
"""

import sys
import traceback
from datetime import datetime, timezone

# ── Shared run identity ───────────────────────────────────────────────────────
RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")



def check_groq_tpd(api_key_env: str, threshold: int, label: str) -> bool:
    """1-token test call to Groq. Returns True if quota >= threshold."""
    import requests, os
    key = os.environ.get(api_key_env, '')
    if not key:
        print(f'[PRE-FLIGHT] {label}: {api_key_env} not set — skipping check')
        return True
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'model': 'llama-3.3-70b-versatile', 'messages': [{'role': 'user', 'content': 'hi'}], 'max_tokens': 1},
            timeout=10
        )
        remaining = int(r.headers.get('x-ratelimit-remaining-tokens', 999999))
        print(f'[PRE-FLIGHT] {label}: {remaining:,} tokens remaining (threshold={threshold:,})')
        if remaining < threshold:
            print(f'[PRE-FLIGHT] {label}: quota too low — clean skip (exit 0)')
            return False
        return True
    except Exception as e:
        print(f'[PRE-FLIGHT] {label}: check failed ({e}) — proceeding anyway')
        return True

def check_groq_tpd(api_key_env: str, threshold: int, label: str) -> bool:
    """1-token test call to Groq. Returns True if quota >= threshold."""
    import requests, os
    key = os.environ.get(api_key_env, '')
    if not key:
        print(f'[PRE-FLIGHT] {label}: {api_key_env} not set — skipping check')
        return True
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'model': 'llama-3.3-70b-versatile', 'messages': [{'role': 'user', 'content': 'hi'}], 'max_tokens': 1},
            timeout=10
        )
        remaining = int(r.headers.get('x-ratelimit-remaining-tokens', 999999))
        print(f'[PRE-FLIGHT] {label}: {remaining:,} tokens remaining (threshold={threshold:,})')
        if remaining < threshold:
            print(f'[PRE-FLIGHT] {label}: quota too low — clean skip (exit 0)')
            return False
        return True
    except Exception as e:
        print(f'[PRE-FLIGHT] {label}: check failed ({e}) — proceeding anyway')
        return True
def _run(position, fn, **kwargs):
    """Run a single position. Returns (ok, summary_dict)."""
    print(f"\n[S2-ORC] ── {position} ──────────────────────────────")
    try:
        result = fn(**kwargs)
        # All position functions return a dict with at least 'status'
        if isinstance(result, dict):
            status = result.get("status", "UNKNOWN")
            ok = status not in ("SAVE_FAILED", "ERROR", "NO_REPORTS")
            if ok:
                print(f"[S2-ORC] {position} ✅  status={status}")
            else:
                print(f"[S2-ORC] {position} ⚠️   status={status}")
            return ok, result
        # Legacy: bool return
        return bool(result), {}
    except Exception as e:
        print(f"[S2-ORC] {position} ❌  exception: {e}")
        traceback.print_exc()
        return False, {"status": "EXCEPTION", "error": str(e)}


def main():
    print("\n" + "=" * 60)
    print("Project Lens — System 2 Orchestrator")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  run_id: {RUN_ID}")
    print("  Positions: S2-A · S2-B · S2-C · S2-D · S2-E · MA")
    print("=" * 60)

    results = {}

    # ── Pre-flight: Groq S2 quota check ──────────────────────────────────────
    if not check_groq_tpd('GROQ_S2_API_KEY', 8000, 'S2'):
        sys.exit(0)

    # ── S2-A: Injection Tracer ────────────────────────────────────────────────
    from lens_s2a_injection import run_s2a
    ok_a, _ = _run("S2-A", run_s2a, run_id=RUN_ID)
    results["S2-A"] = ok_a

    # ── S2-B: Coordination Analyzer ──────────────────────────────────────────
    from lens_s2b_coordination import run_s2b
    ok_b, _ = _run("S2-B", run_s2b, run_id=RUN_ID)
    results["S2-B"] = ok_b

    # ── S2-C: Emotion Decoder ─────────────────────────────────────────────────
    from lens_s2c_emotion import run_s2c
    ok_c, _ = _run("S2-C", run_s2c, run_id=RUN_ID)
    results["S2-C"] = ok_c

    # ── S2-D: Adversary Narrative ─────────────────────────────────────────────
    from lens_s2d_adversary import run_s2d
    ok_d, _ = _run("S2-D", run_s2d, run_id=RUN_ID)
    from lens_s2_gap import run_s2_gap
    ok_gap, _ = _run("S2-GAP Gap Analysis", run_s2_gap, run_id=RUN_ID)
    results["S2-D"] = ok_d
    results["S2-GAP"] = ok_gap

    # ── S2-E: Legitimacy Filter ───────────────────────────────────────────────
    from lens_s2e_legitimacy import run_s2e
    ok_e, _ = _run("S2-E", run_s2e, run_id=RUN_ID)
    results["S2-E"] = ok_e

    # ── Mission Analyst: S1 + S2 synthesis ───────────────────────────────────
    from lens_mission_analyst import run_mission_analyst
    ok_ma, _ = _run("Mission Analyst", run_mission_analyst, run_id=RUN_ID)
    results["Mission Analyst"] = ok_ma

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("System 2 Orchestrator — Run Summary")
    print("=" * 60)
    for pos, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {pos}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n[S2-ORC] {len(failed)} position(s) did not complete: {failed}")
        # S2-A is the critical position — without injection trace, MA has no corrections
        if "S2-A" in failed:
            print("[S2-ORC] WARNING: S2-A failed — Mission Analyst ran without injection corrections.")
            sys.exit(1)
    else:
        print("\n[S2-ORC] All positions complete.")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
