"""CC-134 / CC-134b -- S2-F's runs fit Cloudflare's rolling 24 h window.

CC-134 (LENS-048) sized the cap for two runs in a ~34-call window: 17 a run. The bytes read at the
LENS-049 open (ten S2-F runs, Sep 27 - Oct 2 2026) say otherwise: enforcement is a rolling 24 h
window that refuses once ~11-12k neurons were spent before the run, and start times drift
(07:03Z-07:29Z, 18:28Z-20:09Z), so a run often has the two runs before it inside its window. On 17,
every third run was refused at its first call (Sep 29 morning, Sep 30 evening, Oct 2 morning, the
last one on a new UTC day -- a 00:00 UTC reset would have scored it). CC-134b: a run and the two
before it, each at the cap, must fit Cloudflare's documented 10,000 neurons at ~325 a call.
    python tests/test_cc134_s2f_budget.py
"""
import os
import re
import sys

ROOT = os.environ.get("LENS_ROOT_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PASS = 0
FAIL = 0
RUNS_PER_DAY = 2          # crons '30 13' and '30 1' in the same workflow
RUNS_IN_WINDOW = 3        # a run and the two before it (start times drift ~1.5 h)
NEURONS_PER_CALL = 325    # Cloudflare dashboard, LENS-048: ~300-330 per relevant call
DAILY_NEURONS = 10000     # documented free allocation; refusals were seen at ~11-12k
MIN_CAP = 8               # never below the pre-CC-129 article count


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
check("the window model follows the crons (a run plus the runs of the day before)", RUNS_IN_WINDOW, len(crons) + 1)
check("three runs at the cap fit the documented allocation",
      cap is not None and cap * RUNS_IN_WINDOW * NEURONS_PER_CALL <= DAILY_NEURONS, True)
check("the cap is not starved to nothing", cap is not None and cap >= MIN_CAP, True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
