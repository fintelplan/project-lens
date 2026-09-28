"""CC-131 -- S2-B and S3-B call a live Gemini model, and the registry says the same.

LENS-048: both positions hard-coded gemini-2.0-flash (shut down 2026-06-01) while the registry
said gemini-2.5-flash-lite (LR-105): every wave called a dead model, got 404, fell back to Mistral,
and PROVIDER HEALTH printed a DOWN line that needed no response. Probe (production code, writes
suppressed): gemini-3.5-flash-lite answered 4 of 4, no fallback.
    python tests/test_cc131_gemini_primary.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
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


def src(name):
    return open(os.path.join(CODE, name), "rb").read().decode("utf-8").replace("\r\n", "\n")   # LR-277


def call_site_model(name):
    # The Gemini SDK is not installed in CI, so the module is read, not imported.
    m = re.search(r'^MODEL\s*=\s*"([^"]+)"', src(name), re.M)
    return m.group(1) if m else None


import lens_models as LM
LIVE = "gemini-3.5-flash-lite"
for role, f in (("s2b_coordination", "lens_s2b_coordination.py"), ("s3b_history", "lens_s3b_truehistory.py")):
    print(f"== {role} ==")
    check(f"{f}: call site calls the live model", call_site_model(f), LIVE)
    check(f"registry {role} names the same model", LM.ROLES[role]["model"], call_site_model(f))
    check(f"registry {role} provider", LM.ROLES[role]["provider"], "gemini")
    check(f"registry {role} fallback stays Mistral", LM.ROLES[role]["fb_model"], "ministral-8b-2512")
    check(f"{f}: the fallback model line is unchanged", 'MISTRAL_FALLBACK_MODEL = "ministral-8b-2512"' in src(f), True)

print("== the registry knows the pair, and does not predict a death Google withdrew ==")
check("('gemini', live) is a known wire pair", ("gemini", LIVE) in LM._KNOWN_WIRE, True)
lm = src("lens_models.py")
check("no 'dies 2026-10-16' on gemini-2.5-flash", "dies 2026-10-16" in lm, False)
check("no live MODEL = gemini-2.0-flash anywhere in S2-B / S3-B",
      any(call_site_model(f) == "gemini-2.0-flash" for f in ("lens_s2b_coordination.py", "lens_s3b_truehistory.py")), False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
