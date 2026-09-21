"""CC-85 + CC-95 -- the S2-F row names the leg that answered, and only live
legs are called.

CC-85: DetectionResult carries provider/model/ensemble_mode, and a result is
an ensemble only when two or more legs returned OK. CC-95: Cerebras answered
402 on every call from ~2026-08-17 and is removed; each provider paces with
its own guard.

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
CALLS = []
TWO_LEGS = [("cerebras", "CEREBRAS_MODEL", "gpt-oss-120b"),
            ("cloudflare", "CLOUDFLARE_MODEL", "@cf/openai/gpt-oss-120b")]


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
        CALLS.append(prov)
        st = status_by_provider[prov]          # a provider not listed here crashes the test
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


def run(statuses, legs=TWO_LEGS):
    CALLS.clear()
    real = R.detect_operations_in_article
    R.detect_operations_in_article = stub(statuses)
    try:
        return R.detect_operations_ensemble(
            article_title="t", article_body="b" * 900, article_source="s",
            voice_name="v", voice_type="author", state_actor_lens="xi_office",
            stage_filter="early_warning", inter_model_sleep=0.0, legs=legs,
        )
    finally:
        R.detect_operations_in_article = real


print("== DetectionResult carries provenance ==")
d = R.DetectionResult(status="OK", state_actor_lens="x", stage_filter="early_warning")
check("default provider", d.provider, "")
check("default ensemble_mode", d.ensemble_mode, False)

print("== two legs, both OK -> a real ensemble ==")
r = run({"cerebras": "OK", "cloudflare": "OK"})
check("provider", r.provider, "cerebras+cloudflare")
check("ensemble_mode", r.ensemble_mode, True)

print("== first leg fails, second OK -> one leg, and it says so ==")
r = run({"cerebras": "LLM_FAILED", "cloudflare": "OK"})
check("provider", r.provider, "cloudflare")
check("ensemble_mode", r.ensemble_mode, False)

print("== second leg fails -> first only ==")
r = run({"cerebras": "OK", "cloudflare": "LLM_FAILED"})
check("provider", r.provider, "cerebras")
check("ensemble_mode", r.ensemble_mode, False)

print("== both fail -> the first failure ==")
r = run({"cerebras": "LLM_FAILED", "cloudflare": "LLM_FAILED"})
check("status", r.status, "LLM_FAILED")
check("provider", r.provider, "cerebras")
check("ensemble_mode", r.ensemble_mode, False)

print("== CC-95: production calls only the live leg ==")
check("default legs", [l[0] for l in R.ENSEMBLE_LEGS], ["cloudflare"])
keys = ("S2F_PROVIDER", "CLOUDFLARE_MODEL", "CEREBRAS_MODEL")
before = {k: os.environ.get(k) for k in keys}
r = run({"cloudflare": "OK"}, legs=None)
check("only cloudflare was called", list(CALLS), ["cloudflare"])
check("provider", r.provider, "cloudflare")
check("one leg is not an ensemble", r.ensemble_mode, False)
check("environment restored, CLOUDFLARE_MODEL included",
      {k: os.environ.get(k) for k in keys}, before)

print("== CC-95: one pacing guard per provider ==")
R._tpm_guards.clear()
g1 = R._guard_for("cloudflare")
g2 = R._guard_for("mistral")
check("same provider, same guard", R._guard_for("cloudflare") is g1, True)
check("different providers, different guards", g1 is g2, False)
g1.log_usage(6000)
check("cloudflare's tokens do not throttle mistral", g2.tokens_in_last_60s(), 0)
check("cloudflare's own window counts them", g1.tokens_in_last_60s(), 6000)
R._tpm_guards.clear()

print("== a run that wrote nothing is not green ==")
check("scored=0 failed=24", CRON.scoring_exit_code(0, 24), 1)
check("scored=24 failed=0", CRON.scoring_exit_code(24, 0), 0)
check("scored=0 failed=0 (no work)", CRON.scoring_exit_code(0, 0), 0)
check("scored=1 failed=23 (known gap)", CRON.scoring_exit_code(1, 23), 0)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
