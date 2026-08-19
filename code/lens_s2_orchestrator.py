"""
lens_s2_orchestrator.py
Project Lens — System 2 Orchestrator

Calls all System 2 positions in sequence after System 1 completes.
Called by GitHub Actions lens-manage-analyze.yml.

All positions built in LENS-009. Orchestrator updated in LENS-010.

Positions:
  S2-A  lens_s2a_injection.py    run_s2a()           llama-3.3-70b  GROQ_S2A_API_KEY (dedicated)
  S2-B  lens_s2b_coordination.py run_s2b()           gemini-1.5-flash GEMINI_S2B_API_KEY
  S2-C  lens_s2c_emotion.py      run_s2c()           mistral-small  MISTRAL_API_KEY
  S2-D  lens_s2d_adversary.py    run_s2d()           qwen3-32b      GROQ_API_KEY
  S2-E  lens_s2e_legitimacy.py   run_s2e()           llama-3.3-70b  GROQ_S2E_API_KEY
  MA    lens_mission_analyst.py  run_mission_analyst() llama-3.3-70b GROQ_MA_API_KEY

Architecture: LR-058 to LR-064.
  One-way flow. System 1 scripts FROZEN.
  S2 reads lens_reports → writes injection_reports.
  MA reads lens_reports + injection_reports → writes lens_macro_reports.

Session: LENS-010 (orchestrator fix)
"""

import sys
import traceback
import logging
log = logging.getLogger("S2-ORC")
from datetime import datetime, timezone
from lens_models import limits_for, GROQ_GPT_OSS_120B

# ── Shared run identity ───────────────────────────────────────────────────────
RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")

# -- Outcome classification (CC-58, LENS-038 order item 1.1) ------------------
# ALLOWLIST, not denylist. A status this file has never heard of is a FAILURE,
# never a success. The three-name denylist this replaces printed a green tick
# for ANALYSIS_FAILED, QUOTA_SKIP, NO_S1_REPORTS, NO_S1_DATA, NO_ARTICLES,
# NO_RAW_ARTICLES, INSUFFICIENT_ARTICLES, SKIP, S1_PARTIAL_ARRIVAL and
# S1_ZERO_ARRIVAL. Confirmed live on run 32209212733: S2-B returned
# ANALYSIS_FAILED and the wave still ended "All positions complete."
#
# These three tuples are the whole list of non-failure statuses FOR THE
# POSITIONS THIS ORCHESTRATOR CALLS -- S2-A/B/C/D/GAP/E, Mission Analyst
# and S4-E. Adding a new status to any of them REQUIRES adding it here;
# that obligation is the point of an allowlist.
#
# SYSTEM 3 HAS A DIFFERENT VOCABULARY and its own _run: SKIPPED,
# SKIPPED_CADENCE and SKIPPED_CI are live there (run 32261985640) and
# appear nowhere in System 2. Do NOT copy these tuples into
# lens_s3_orchestrator.py without adding them first -- order item 1.3.
#
# DEGRADED is not SKIP and not FAIL. lens_mission_analyst.py:849 returns the
# arrival statuses only AFTER save_macro_report() succeeded -- the report
# exists, it was built on thin S1 input. It must not alarm and it must not
# be allowed to print "All positions complete."
STATUS_OK = ("COMPLETE", "OK")
STATUS_SKIP = ("QUOTA_SKIP", "SKIP")
STATUS_DEGRADED = ("S1_PARTIAL_ARRIVAL", "S1_ZERO_ARRIVAL")
GLYPH = {"OK": "✅", "SKIP": "⏭", "DEGRADED": "⚠️", "FAIL": "❌"}



def check_groq_tpm(api_key_env: str, threshold: int, label: str) -> bool:
    """1-token test call to Groq. Reads x-ratelimit-remaining-tokens, which is a
    PER-MINUTE counter -- CC-16 renamed this from check_groq_tpd because the old
    name invited a per-DAY threshold that the per-minute window could never pass."""
    import requests, os
    from lens_models import GROQ_GPT_OSS_120B as PREFLIGHT_MODEL
    key = os.environ.get(api_key_env, '')
    if not key:
        print(f'[PRE-FLIGHT] {label}: {api_key_env} not set — skipping check')
        return True
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'model': PREFLIGHT_MODEL, 'messages': [{'role': 'user', 'content': 'hi'}], 'max_tokens': 1},
            timeout=10
        )
        if r.status_code != 200 or 'x-ratelimit-remaining-tokens' not in r.headers:
            print('[PRE-FLIGHT] %s: ALARM http=%s no quota header -- TPM check is BLIND'
                  % (label, r.status_code))
            return True
        remaining = int(r.headers.get('x-ratelimit-remaining-tokens', 999999))
        print(f'[PRE-FLIGHT] {label}: {remaining:,} tokens remaining (threshold={threshold:,})')
        if remaining < threshold:
            print(f'[PRE-FLIGHT] {label}: quota too low — clean skip (exit 0)')
            return False
        return True
    except Exception as e:
        print(f'[PRE-FLIGHT] {label}: check failed ({e}) — proceeding anyway')
        return True

def check_groq_tpm(api_key_env: str, threshold: int, label: str) -> bool:
    """1-token test call to Groq. Reads x-ratelimit-remaining-tokens, which is a
    PER-MINUTE counter -- CC-16 renamed this from check_groq_tpd because the old
    name invited a per-DAY threshold that the per-minute window could never pass."""
    import requests, os
    from lens_models import GROQ_GPT_OSS_120B as PREFLIGHT_MODEL
    key = os.environ.get(api_key_env, '')
    if not key:
        print(f'[PRE-FLIGHT] {label}: {api_key_env} not set — skipping check')
        return True
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'model': PREFLIGHT_MODEL, 'messages': [{'role': 'user', 'content': 'hi'}], 'max_tokens': 1},
            timeout=10
        )
        if r.status_code != 200 or 'x-ratelimit-remaining-tokens' not in r.headers:
            print('[PRE-FLIGHT] %s: ALARM http=%s no quota header -- TPM check is BLIND'
                  % (label, r.status_code))
            return True
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
    """Run a single position. Returns (outcome, summary_dict).

    outcome is one of "OK" / "SKIP" / "DEGRADED" / "FAIL" -- NOT a bool.
    Four outcomes exist in this repo and there were only ever two words for
    them, so a deliberate skip had to borrow the word for success (CC-58).
    """
    print(f"\n[S2-ORC] ── {position} ──────────────────────────────")
    try:
        result = fn(**kwargs)
        # All position functions return a dict with at least 'status'
        if isinstance(result, dict):
            status = result.get("status", "UNKNOWN")
            outcome = ("OK" if status in STATUS_OK
                       else "SKIP" if status in STATUS_SKIP
                       else "DEGRADED" if status in STATUS_DEGRADED
                       else "FAIL")
            if outcome == "OK":
                print(f"[S2-ORC] {position} ✅  status={status}")
            else:
                print(f"[S2-ORC] {position} ⚠️   status={status}")
            return outcome, result
        # Legacy: bool return
        return ("OK" if result else "FAIL"), {}
    except Exception as e:
        print(f"[S2-ORC] {position} ❌  exception: {e}")
        traceback.print_exc()
        return "FAIL", {"status": "EXCEPTION", "error": str(e)}


def main():
    print("\n" + "=" * 60)
    print("Project Lens — System 2 Orchestrator")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  run_id: {RUN_ID}")
    print("  Positions: S2-A · S2-B · S2-C · S2-D · S2-E · MA")
    print("=" * 60)

    results = {}

    # -- Pre-flight: Groq quota check per key (LENS-022 LR-094)
    # S2-A: dedicated GROQ_S2A_API_KEY. S2-GAP: GROQ_S2DGCOM_API_KEY (CC-15).
    # S2-E moved to Cerebras (D-016) and is not covered by this Groq check.
    # Threshold derives from the registry (LR-108): 25% of the pair's TPM.
    _PF_TPM = (limits_for('groq', GROQ_GPT_OSS_120B) or {}).get('TPM')
    _PF_MIN = max(1000, int(_PF_TPM * 0.25)) if _PF_TPM else 2000
    s2a_ok = check_groq_tpm('GROQ_S2A_API_KEY', _PF_MIN, 'S2-A dedicated')
    s2gap_ok = check_groq_tpm('GROQ_S2DGCOM_API_KEY', _PF_MIN, 'S2-GAP')
    if not s2a_ok and not s2gap_ok:
        print('[PRE-FLIGHT] Both keys exhausted -- clean skip')
        sys.exit(0)

    # ── S2-A: Injection Tracer ────────────────────────────────────────────────
    from lens_s2a_injection import run_s2a
    st_a, _ = _run("S2-A", run_s2a, run_id=RUN_ID)
    results["S2-A"] = st_a

    # ── S2-B: Coordination Analyzer ──────────────────────────────────────────
    from lens_s2b_coordination import run_s2b
    st_b, _ = _run("S2-B", run_s2b, run_id=RUN_ID)
    results["S2-B"] = st_b

    # ── S2-C: Emotion Decoder ─────────────────────────────────────────────────
    from lens_s2c_emotion import run_s2c
    st_c, _ = _run("S2-C", run_s2c, run_id=RUN_ID)
    results["S2-C"] = st_c

    # ── S2-D: Adversary Narrative ─────────────────────────────────────────────
    from lens_s2d_adversary import run_s2d
    st_d, _ = _run("S2-D", run_s2d, run_id=RUN_ID)
    from lens_s2_gap import run_s2_gap
    st_gap, _ = _run("S2-GAP Gap Analysis", run_s2_gap, run_id=RUN_ID)
    results["S2-D"] = st_d
    results["S2-GAP"] = st_gap

    # ── S2-E: Legitimacy Filter ───────────────────────────────────────────────
    from lens_s2e_legitimacy import run_s2e
    st_e, _ = _run("S2-E", run_s2e, run_id=RUN_ID)
    results["S2-E"] = st_e

    # ── Mission Analyst: S1 + S2 synthesis ───────────────────────────────────
    from lens_mission_analyst import run_mission_analyst
    st_ma, _ = _run("Mission Analyst", run_mission_analyst, run_id=RUN_ID)
    results["Mission Analyst"] = st_ma
    # S4-E: Upgrade Trigger Monitor — runs last, zero quota impact
    try:
        from lens_s4_upgrade_monitor import run_upgrade_monitor
        upgrade_result = run_upgrade_monitor()
        results["S4-E"] = "OK" if upgrade_result.get("status") == "OK" else "FAIL"
        log.info(f"S4-E thresholds: {upgrade_result.get('thresholds', {})}")
    except Exception as e:
        log.warning(f"S4-E upgrade monitor failed (non-fatal): {e}")
        results["S4-E"] = "FAIL"

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("System 2 Orchestrator — Run Summary")
    print("=" * 60)
    for pos, outcome in results.items():
        print(f"  {GLYPH.get(outcome, '?')} {pos}")

    failed = [k for k, v in results.items() if v == "FAIL"]
    skipped = [k for k, v in results.items() if v == "SKIP"]
    degraded = [k for k, v in results.items() if v == "DEGRADED"]

    # -- DELIVERY IS UNCONDITIONAL (CC-58, order item 1.2) -------------------
    # These two calls used to sit in the else-branch of "if failed:", so a
    # single failed position suppressed BOTH reports and the run still exited
    # 0. The wave that went wrong is the wave the operator most needs the
    # report from; a message that never arrives is the silent failure this
    # target exists to remove. lens_s3_orchestrator.py:153 already delivers
    # first and reports afterwards -- this makes S2 agree with its sibling.
    # The two calls stay in SEPARATE try blocks on purpose: S3 nests them and
    # a Telegram failure there silently takes the step report with it.
    try:
        from lens_telegram import send_s2_intelligence
        send_s2_intelligence()
    except Exception as _te:
        print(f"[S2-ORC] Telegram step failed (non-fatal): {_te}")
    try:
        from lens_s2_step_report import run_s2_report
        run_s2_report(run_id=RUN_ID)
    except Exception as _s2r:
        log.warning(f"S2 step report failed (non-fatal): {_s2r}")

    if skipped:
        print(f"\n[S2-ORC] {len(skipped)} position(s) deliberately skipped: {skipped}")
    if degraded:
        print(f"\n[S2-ORC] {len(degraded)} position(s) produced output on DEGRADED input: {degraded}")
    if failed:
        print(f"\n[S2-ORC] {len(failed)} position(s) did not complete: {failed}")
        # S2-A is the critical position -- without injection trace, MA has no corrections
        if "S2-A" in failed:
            print("[S2-ORC] WARNING: S2-A failed -- Mission Analyst ran without injection corrections.")
            print("=" * 60 + "\n")
            sys.exit(1)
    elif not degraded:
        print("\n[S2-ORC] All positions complete.")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
