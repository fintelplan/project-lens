"""LENS-037 -- S2-D fallback-leg probe: mechanics AND calibration.

Fixture from probe_lens_models.fixture_s2d_adversary (production's own
round-robin fetch, _split_batches and build_articles_prompt). S2-D has NO
response guard on its primary path, so this probe adds none -- a leg that
validates while the primary does not is an asymmetry, not an improvement.

CALIBRATION NOTE: the saved row pools key_claims across ALL batches
(run_s2d: all_claims.extend). This probe measures ONE batch. Compare against
the per-batch band, not the per-row band.
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
from probe_lens_models import fixture_s2d_adversary

PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2d_adversary")
leg = fallback("s2d_adversary")
assert leg is not None and len(leg) == 3, "fallback() arity/None guard (LR-142)"
FB_PROVIDER, FB_MODEL, FB_KEY_ENV = leg
print("primary : %s / %s  max_out=%s" % (PROVIDER, MODEL, MAX_OUT))
print("leg     : %s / %s  key_env=%s" % (FB_PROVIDER, FB_MODEL, FB_KEY_ENV))
assert_model_known(FB_PROVIDER, FB_MODEL)
print("assert_model_known: OK")

key = os.environ.get(FB_KEY_ENV)
if not key:
    raise SystemExit("%s not set" % FB_KEY_ENV)

fx = fixture_s2d_adversary()
prompt_chars = len(fx.system) + len(fx.user)
max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, FB_PROVIDER, FB_MODEL)
print("fixture : %s" % json.dumps(fx.detail))
print("prompt_chars=%d  max_tokens=%d  temperature=%s"
      % (prompt_chars, max_tokens, fx.temperature))
print("BASELINE, Cerebras era, PER ROW (pooled over ~3 batches): "
      "claims/row 26.6 (max 37) | consistency 0.78-0.92 mean 0.853")
print("=> PER-BATCH expectation is roughly claims/row divided by the batch count.")
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
    if "<think>" in raw and "</think>" in raw:
        raw = raw[raw.index("</think>") + 8:].strip()
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
    claims = parsed.get("key_claims") or []
    actors = parsed.get("named_actors") or {}
    tone = str(parsed.get("emotional_tone", ""))
    cons = parsed.get("narrative_consistency_score")
    missing = [k for k in ("analyst", "primary_narrative", "key_claims",
                           "named_actors", "emotional_tone",
                           "counter_narrative_target", "call_to_action",
                           "narrative_consistency_score", "narrative_fractures")
               if k not in parsed]
    print("trial %d: HTTP 200 %ss finish=%s prompt=%s completion=%s total=%s "
          "budget_used=%.0f%%"
          % (trial, dt, ch.get("finish_reason"), u.get("prompt_tokens"),
             u.get("completion_tokens"), u.get("total_tokens"),
             100.0 * (u.get("completion_tokens") or 0) / max_tokens))
    print("         CALIBRATION claims=%d consistency=%s heroes=%d villains=%d victims=%d"
          % (len(claims), cons, len(actors.get("heroes") or []),
             len(actors.get("villains") or []), len(actors.get("victims") or [])))
    print("         TONE (%d chars, label vs prose): %s" % (len(tone), tone[:90]))
    print("         missing_fields=%s" % (missing or "none"))
