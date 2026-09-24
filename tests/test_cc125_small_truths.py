"""
test_cc125_small_truths.py -- gate for CC-125. Line-ending blind on purpose: a source scan that
matches "\\n" gives the wrong answer on a CRLF working copy (LENS-047, test_alert_on_change).
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
FAILS = []


def read(path):
    return open(path, "rb").read().decode("utf-8").replace("\r\n", "\n")


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r want=%r" % (got, want)))
    if not ok:
        FAILS.append(name)


RETIRED = re.compile(r"llama-3\.3-70b|gemini-1\.5-flash|gemini-2\.0-flash|mistral-small|qwen3-32b|gpt-oss-120b")
for f, n in (("lens_s2_orchestrator.py", 20), ("lens_s3_orchestrator.py", 15),
             ("lens_s2c_emotion.py", 6), ("lens_s3f_countercheck.py", 6)):
    head = read(os.path.join(CODE, f)).split("\n")[:n]
    check("no retired model named in the header of " + f, [l.strip() for l in head if RETIRED.search(l)], [])

wf = read(os.path.join(ROOT, ".github", "workflows", "lens-manage-analyze.yml"))
check("S2 orchestrator runs unbuffered", "python -u code/lens_s2_orchestrator.py" in wf, True)

ma = read(os.path.join(CODE, "lens_mission_analyst.py"))
check("no silent MODERATE default", 'analysis.get("threat_level", "MODERATE")' in ma, False)
check("a missing level is stored as UNKNOWN", 'analysis.get("threat_level") or "UNKNOWN"' in ma, True)

import lens_telegram as T          # noqa: E402
try:
    T.alert_decision("UNKNOWN", ["HIGH", "HIGH"])
    ok = True
except Exception as e:
    ok = "%s: %s" % (type(e).__name__, e)
check("the alert rule survives an UNKNOWN level", ok, True)

cron = read(os.path.join(CODE, "lens_s2f_scoring_cron.py"))
check("S2-F says its coverage (D6)", "S2F_COVERAGE" in cron and "_attempted = scored + failed + quota_skipped" in cron, True)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
