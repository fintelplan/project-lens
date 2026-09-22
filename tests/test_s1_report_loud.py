"""CC-100 -- the S1 canary report cannot fail silently.

2026-09-22: S1-RPT called mistral-small-latest, got 429 three times, logged
"Mistral failed to generate S1 report" -- and the wave stayed green, because
the workflow ran it with `|| true` and the script always exited 0.
    python tests/test_s1_report_loud.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "code"))

import lens_s1_report as R

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


print("== the model is not the refused class ==")
check("MODEL", R.MODEL, "ministral-8b-2512")
check("no mistral-small in MODEL", "mistral-small" in R.MODEL, False)
check("headroom: MAX_TOKENS >= 6000 (probe used 3,096)", R.MAX_TOKENS >= 6000, True)
check("finish_reason is logged", b"finish_reason={fr}" in open(R.__file__, "rb").read(), True)

print("== only a delivered report is a green step ==")
check("COMPLETE", R.exit_code({"status": "COMPLETE"}), 0)
for st in ("AI_FAILED", "SEND_FAILED", "DOCX_FAILED", "NO_DATA", "ERROR"):
    check(st, R.exit_code({"status": st}), 1)
check("no status at all", R.exit_code({}), 1)

print("== nothing swallows it ==")
wf = open(os.path.join(HERE, "..", ".github", "workflows", "lens-manage-analyze.yml"), "rb").read()
check("workflow has no `lens_s1_report.py || true`", b"lens_s1_report.py || true" in wf, False)
check("workflow still runs the report", b"python code/lens_s1_report.py" in wf, True)
src = open(R.__file__, "rb").read()
check('the docx no longer claims "Mistral-small"', b'Mistral-small")' in src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
