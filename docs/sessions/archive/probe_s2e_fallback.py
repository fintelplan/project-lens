"""LENS-037 -- S2-E fallback-leg probe: mechanics AND calibration.

Fixture from probe_lens_models.fixture_s2e_legitimacy (production's own
builder, BUG-002 reproduced faithfully). Correction verdict from S2-E's own
build_correction_to_ma. Nothing here mirrors production logic.
"""
import os, sys, json, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "code"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from dotenv import load_dotenv; load_dotenv()
except Exception:
    pass
import requests
from lens_models import wire, fallback, fit_max_tokens, assert_model_known
import lens_s2e_legitimacy as s2e
from probe_lens_models import fixture_s2e_legitimacy

PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2e_legitimacy")
leg = fallback("s2e_legitimacy")
assert leg is not None and len(leg) == 3, "fallback() arity/None guard (LR-142)"
FB_PROVIDER, FB_MODEL, FB_KEY_ENV = leg
print("primary : %s / %s  max_out=%s" % (PROVIDER, MODEL, MAX_OUT))
print("leg     : %s / %s  key_env=%s" % (FB_PROVIDER, FB_MODEL, FB_KEY_ENV))
assert_model_known(FB_PROVIDER, FB_MODEL)
print("assert_model_known: OK")

key = os.environ.get(FB_KEY_ENV)
if not key:
    raise SystemExit("%s not set" % FB_KEY_ENV)

fx = fixture_s2e_legitimacy()
prompt_chars = len(fx.system) + len(fx.user)
max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, FB_PROVIDER, FB_MODEL)
print("fixture : %s" % json.dumps(fx.detail))
print("prompt_chars=%d  max_tokens=%d  temperature=%s"
      % (prompt_chars, max_tokens, fx.temperature))
print("BASELINE BAND (Cerebras era, Jul 29 - Aug 17): "
      "actors/row 6.5-13.0 mean ~9.0 | low/row 2.4-5.4 mean ~3.5 | mandatory ~95%")
print("-" * 72)

for trial in (1, 2, 3):
    t0 = time.time()
    r = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={"Authorization": "Bearer " + key,
                 "Content-Type": "application/json"},
        json={"model": FB_MODEL,
              "messages": [{"role": "system", "content": fx.system},
                           {"role": "user", "content": fx.user}],
              "max_tokens": max_tokens,
              "temperature": fx.temperature},
        timeout=180)
    dt = round(time.time() - t0, 1)
    if r.status_code != 200:
        print("trial %d: HTTP %s %s" % (trial, r.status_code, r.text[:200]))
        continue
    b = r.json()
    ch = b["choices"][0]
    u = b.get("usage") or {}
    raw = ch["message"]["content"].strip()
    fence = chr(96) * 3
    if raw.startswith(fence):
        raw = raw.split(fence)[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        parsed = json.loads(raw.strip())
    except Exception as e:
        print("trial %d: JSON PARSE FAILED (%s) len=%d finish=%s"
              % (trial, e, len(raw), ch.get("finish_reason")))
        continue
    aa = parsed.get("actors_assessed") or []
    lo = parsed.get("low_legitimacy_actors_pushing_narrative") or []
    tiers = {}
    for a in aa:
        t = a.get("legitimacy_tier", "?")
        tiers[t] = tiers.get(t, 0) + 1
    corr = s2e.build_correction_to_ma(parsed)
    vr = s2e.validate_parsed_response(parsed, "S2-E")
    print("trial %d: HTTP 200 %ss finish=%s prompt=%s completion=%s total=%s "
          "budget_used=%.0f%%"
          % (trial, dt, ch.get("finish_reason"), u.get("prompt_tokens"),
             u.get("completion_tokens"), u.get("total_tokens"),
             100.0 * (u.get("completion_tokens") or 0) / max_tokens))
    print("         CALIBRATION actors=%d low=%d tiers=%s schema_valid=%s"
          % (len(aa), len(lo), tiers, vr.valid))
    print("         CORRECTION  mandatory=%s depth=%s adj=%s"
          % (corr.get("mandatory"), corr.get("contamination_depth"),
             corr.get("confidence_adjustment")))
