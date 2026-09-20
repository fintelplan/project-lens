"""CC-85 -- the S2-F row must name the leg that actually answered.

Runs offline: detect_operations_in_article is stubbed, no provider is called.
    python tests/test_s2f_provenance.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_framing_rubrics as R
import lens_s2f_scoring_cron as CRON

PASS = 0
FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


def stub(status_by_provider):
    def _f(**kw):
        prov = os.environ.get("S2F_PROVIDER", "")
        st = status_by_provider[prov]
        return R.DetectionResult(
            status=st,
            state_actor_lens=kw["state_actor_lens"],
            stage_filter=kw["stage_filter"],
            operations_detected=[{"id": "OP-001"}] if st == "OK" else [],
            confidence=0.5 if st == "OK" else 0.0,
            provider=prov,
            model=prov + "-model",
        )
    return _f


def run(statuses):
    real = R.detect_operations_in_article
    R.detect_operations_in_article = stub(statuses)
    try:
        return R.detect_operations_ensemble(
            article_title="t", article_body="b" * 900, article_source="s",
            voice_name="v", voice_type="author", state_actor_lens="xi_office",
            stage_filter="early_warning", inter_model_sleep=0.0,
        )
    finally:
        R.detect_operations_in_article = real


print("== DetectionResult carries provenance ==")
d = R.DetectionResult(status="OK", state_actor_lens="x", stage_filter="early_warning")
check("default provider", d.provider, "")
check("default ensemble_mode", d.ensemble_mode, False)

print("== both legs OK -> a real ensemble ==")
r = run({"cerebras": "OK", "cloudflare": "OK"})
check("provider", r.provider, "cerebras+cloudflare")
check("ensemble_mode", r.ensemble_mode, True)

print("== primary 402, secondary OK -> one leg, and it says so ==")
r = run({"cerebras": "LLM_FAILED", "cloudflare": "OK"})
check("provider", r.provider, "cloudflare")
check("ensemble_mode", r.ensemble_mode, False)

print("== secondary fails -> primary only ==")
r = run({"cerebras": "OK", "cloudflare": "LLM_FAILED"})
check("provider", r.provider, "cerebras")
check("ensemble_mode", r.ensemble_mode, False)

print("== both fail ==")
r = run({"cerebras": "LLM_FAILED", "cloudflare": "LLM_FAILED"})
check("status", r.status, "LLM_FAILED")
check("ensemble_mode", r.ensemble_mode, False)

print("== a run that wrote nothing is not green ==")
check("scored=0 failed=24", CRON.scoring_exit_code(0, 24), 1)
check("scored=24 failed=0", CRON.scoring_exit_code(24, 0), 0)
check("scored=0 failed=0 (no work)", CRON.scoring_exit_code(0, 0), 0)
check("scored=1 failed=23 (known gap)", CRON.scoring_exit_code(1, 23), 0)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
