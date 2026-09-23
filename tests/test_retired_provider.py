"""CC-115 -- a retired provider is not called; its positions go straight to their fallback.

S2-D, S2-E and MA import the Cerebras SDK at module scope (not installed in CI), so
their skip is checked by bytes: it sits just before the primary loop and returns the
same fallback call the file already makes after the loop.
    python tests/test_retired_provider.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)

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


print("== the registry names the dead provider, with its evidence ==")
check("cerebras is retired", "cerebras" in M.RETIRED_PROVIDERS, True)
check("with a reason", "402" in M.RETIRED_PROVIDERS.get("cerebras", ""), True)
for role in ("s2d_adversary", "s2e_legitimacy", "mission_analyst"):
    r = M.ROLES[role]
    check(f"{role}: primary retired, fallback alive",
          (r["provider"] in M.RETIRED_PROVIDERS, r.get("fb_provider") not in M.RETIRED_PROVIDERS and bool(r.get("fb_provider"))),
          (True, True))

print("== each position skips the primary before its loop ==")
for f, call in (("lens_s2d_adversary.py", b"_call_fallback_leg(user_message, prompt_chars)"),
                ("lens_s2e_legitimacy.py", b"_call_fallback_leg(user_message, prompt_chars, lens_name)"),
                ("lens_mission_analyst.py", b"_call_fallback_leg(user_message, max_tokens)")):
    b = open(os.path.join(CODE, f), "rb").read()
    i = b.find(b"RETIRED_PROVIDERS:   # CC-115")
    j = b.find(b"_last_err = None   # CC-107")
    check(f"{f}: the check comes before the primary loop", 0 < i < j, True)
    check(f"{f}: it returns the file's own fallback call", (b"return " + call) in b[i:j], True)
    check(f"{f}: and that call is the one after the loop too", b.count(b"return " + call), 2)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
