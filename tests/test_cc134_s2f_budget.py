"""CC-134 -- S2-F's two daily runs fit Cloudflare's window together.

Bytes, Sep 25-29 2026: the Cloudflare neuron budget behaved as a rolling ~24 h window of roughly
34-39 calls, not a 00:00 UTC reset (Sep 29 morning: first call refused, 0 of 24). Two runs asking 24
each starve the second. The cap per run is half the smallest window seen.
    python tests/test_cc134_s2f_budget.py
"""
import os
import re
import sys

ROOT = os.environ.get("LENS_ROOT_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PASS = 0
FAIL = 0
WINDOW_CALLS_MEASURED = 34   # smallest rolling-window total observed, Sep 25-29 (LENS-048)
RUNS_PER_DAY = 2             # crons '30 13' and '30 1' in the same workflow


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


wf = open(os.path.join(ROOT, ".github", "workflows", "lens-s2f-scoring.yml"), "rb").read().decode("utf-8").replace("\r\n", "\n")
m = re.search(r'^\s*S2F_MAX_CALLS:\s*"(\d+)"', wf, re.M)
cap = int(m.group(1)) if m else None
crons = re.findall(r"^\s*-\s*cron:", wf, re.M)
check("S2F_MAX_CALLS is set in the workflow", cap is not None, True)
check("runs per day match the crons", len(crons), RUNS_PER_DAY)
check("both runs fit the measured window", cap is not None and cap * RUNS_PER_DAY <= WINDOW_CALLS_MEASURED, True)
check("the cap is not starved to nothing", cap is not None and cap >= 12, True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
