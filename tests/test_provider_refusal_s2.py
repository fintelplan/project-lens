"""CC-107 -- item 11 coverage, batch 1: MA, S2-C, S2-D, S2-E and S2-GAP record refusals.

Checked by bytes: each site calls record_refusal once where the leg is given up,
lazily and wrapped, and nothing is added at module scope. Behaviour is certified
by the next wave's PROVIDER_REFUSAL lines, not here.
    python tests/test_provider_refusal_s2.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")

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


SITES = {
    "lens_s2d_adversary.py":   [b"record_refusal(PROVIDER, MODEL, exc=_last_err)",
                                b"record_refusal(FB_PROVIDER, FB_MODEL, status=resp.status_code, text=resp.text)"],
    "lens_s2e_legitimacy.py":  [b"record_refusal(PROVIDER, MODEL, exc=_last_err)",
                                b"record_refusal(FB_PROVIDER, FB_MODEL, status=resp.status_code, text=resp.text)"],
    "lens_mission_analyst.py": [b"record_refusal(PROVIDER, MODEL, exc=_last_err)",
                                b"record_refusal(FB_PROVIDER, FB_MODEL, status=resp.status_code, text=resp.text)"],
    "lens_s2c_emotion.py":     [b'record_refusal("mistral", MODEL, status=_last_http[0] if _last_http else None'],
    "lens_s2_gap.py":          [b"record_refusal(PROVIDER, MODEL, exc=e)"],
}

for f, calls in SITES.items():
    print(f"== {f} ==")
    b = open(os.path.join(CODE, f), "rb").read()
    for c in calls:
        check(f"calls {c[:48].decode()}...", b.count(c), 1)
    check("every import is lazy (none at module scope)",
          re.search(rb"(?m)^(from|import) lens_provider_refusal", b) is None, True)
    check("every record is wrapped", b.count(b"from lens_provider_refusal import record_refusal"),
          b.count(b"# CC-107 (item 11)"))

s2c = open(os.path.join(CODE, "lens_s2c_emotion.py"), "rb").read()
i = s2c.find(b"_last_http = (resp.status_code, resp.text)")
j = s2c.find(b'raise Exception(f"Mistral {resp.status_code}', i)
check("S2-C keeps the real status BEFORE re-raising it as text", i > 0 and j > i, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
