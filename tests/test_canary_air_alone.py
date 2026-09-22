"""CC-113 -- the canary breathes alone (gas-mask arm 2, as a gate).

2026-09-22 evening: entity extraction shared GROQ_API_KEY with Lens 1 and drew 160
'tokens per day' refusals; Lens 1 failed 429_tpd. No other role may use a canary
lens's key, and Collection must hand entity extraction the key it now reads.
    python tests/test_canary_air_alone.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "code"))

import lens_models as M

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


canary = set(M.CANARY_LENS_ROLES)
# KNOWN DEBT (LENS-045): these roles share a canary lens's key today. The gate is a
# ratchet: a NEW sharer turns it red; removing one is progress. Lens 1 carries none.
# Lens 3: Cohere trial, ~1,000 calls/month shared with S3-A and S3-C (order item 12).
# Lens 4: Mistral limits are per model; arithmetic says no contention (LENS-045 1.3),
# but a Mistral death takes the canary lens and these roles together (D2/D7).
KNOWN = {
    "lens3": {"s3a_patterns", "s3c_drift"},
    "lens4": {"mission_analyst", "regular_report", "s1_report", "s2_report", "s2a_injection",
              "s2b_coordination", "s2c_emotion", "s2d_adversary", "s2e_legitimacy",
              "s3a_patterns", "s3b_history", "s3d_longterm", "s3f_countercheck"},
}
print("== no NEW role breathes a canary lens's key (ratchet) ==")
for lens in sorted(canary):
    spec = M.ROLES[lens]
    k = spec["key_env"]
    sharers = sorted(r for r, s in M.ROLES.items() if r not in canary
                     and k in (s.get("key_env"), s.get("fb_key_env")))
    new = sorted(set(sharers) - KNOWN.get(lens, set()))
    check(f"{lens} ({k}) NEW sharers", new, [])
check("lens1 (the canary's Groq org) carries no debt at all", "lens1" in KNOWN, False)

print("== entity extraction has its own Groq org, and Collection passes it ==")
e = M.ROLES["entity_extract"]
check("entity_extract key_env", e["key_env"], "GROQ_S3_API_KEY")
check("entity_extract fb_key_env", e["fb_key_env"], "GROQ_S3_API_KEY")
wf = open(os.path.join(HERE, "..", ".github", "workflows", "lens-collect.yml"), "rb").read()
check("lens-collect.yml passes GROQ_S3_API_KEY", b"GROQ_S3_API_KEY: ${{ secrets.GROQ_S3_API_KEY }}" in wf, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
