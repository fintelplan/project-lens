"""CC-112 -- D4 ruled A: S3-D stores its whole answer, not 6 of its 14 fields.

ICD 203 asks analytic products to incorporate alternatives and to explain change
over time; S3-D produced both (ach_check, the drift fields) and they were thrown
away at the insert. The dict entry is checked by bytes; gate 9 guards the rest.
    python tests/test_s3d_keeps_all.py
"""
import os
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


b = open(os.path.join(CODE, "lens_s3d_longterm.py"), "rb").read()
i = b.find(b"    record = {")
j = b.find(b"}", b.find(b'"elapsed_seconds"', i)) if b.find(b'"elapsed_seconds"', i) > 0 else b.find(b"\n    }", i)
rec = b[i:j]
check("the record dict is found", i > 0 and j > i, True)
check("it stores the whole parsed answer", b'"analysis_full":     analysis,' in rec, True)
check("the six older columns are still written", all(k in rec for k in (
    b'"patterns_found"', b'"structural_trends"', b'"summary"', b'"signals_to_watch"',
    b'"corrections_to_s2"', b'"time_horizon"')), True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
