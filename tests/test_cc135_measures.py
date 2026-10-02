"""CC-135 -- order item 3 Phase A (selection): no reader ranks, caps or maxes
injection_reports.confidence_score, a column that holds six measures (S2-A/B model-stated
confidence, S2-C manipulation score, S2-D narrative consistency, S2-E share of low-legitimacy
actors, S2-GAP gap quality). ISO/IEC 11179: one data element, one concept, one value domain.
Bytes, Oct 2 2026 (24 h, 67 rows): the Compendium's order-then-limit-30 kept 0 of S2-E's 8 rows.
    python tests/test_cc135_measures.py
"""
import os
import re
import sys

ROOT = os.environ.get("LENS_ROOT_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CODE = os.path.join(ROOT, "code")
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
    return open(os.path.join(CODE, name), "rb").read().decode("utf-8").replace("\r\n", "\n")


FILES = sorted(f for f in os.listdir(CODE) if f.endswith(".py"))
ORDER = re.compile(r"""\.order\(\s*["']confidence_score""")
MAXC = re.compile(r"max\([^\n]*confidence_score")
INSERT = re.compile(r"""table\(\s*["']injection_reports["']\s*\)\s*\.insert""")

check("no reader orders by confidence_score", [f for f in FILES if ORDER.search(src(f))], [])
check("no max over confidence_score", [f for f in FILES if MAXC.search(src(f))], [])

import lens_s2_measures as M

writers = [f for f in FILES if INSERT.search(src(f))]
analysts = set()
for f in writers:
    analysts |= set(re.findall(r'"analyst":\s*"([A-Z0-9-]+)"', src(f)))
check("the six writers are found", len(writers) >= 6, True)
check("every analyst written to injection_reports has a named measure", sorted(analysts - set(M.MEASURES)), [])
check("an unknown analyst is shown as unnamed, never as confidence", M.measure_label("S2-Z"), M.UNNAMED)

rows = [
    {"analyst": "S2-A", "injection_type": "X"},
    {"analyst": "S2-B", "injection_type": "Y"},
    {"analyst": "S2-A", "injection_type": "X"},
    {"analyst": "S2-A", "injection_type": "NONE"},
    {"analyst": "S2-B", "injection_type": "NO_COORDINATION"},
]
t = M.top_finding(rows)
check("top finding is the most frequent type, by count",
      (t.get("injection_type"), t.get("n"), t.get("n_findings"), t.get("analysts")), ("X", 2, 3, ["S2-A"]))
check("a tie breaks the same way every time",
      M.top_finding([{"analyst": "S2-B", "injection_type": "B"}, {"analyst": "S2-A", "injection_type": "A"}]).get("injection_type"), "A")
check("no findings gives zero", M.top_finding([{"analyst": "S2-A", "injection_type": "NONE"}]).get("n_findings"), 0)

tg = src("lens_telegram.py")
check("the Brief's S2 line is the most frequent finding", "top_s2 = top_finding(s2)" in tg, True)
check("the Brief prints no conf= on its S2 line", "conf={top_s2" in tg, False)
check("S3-E samples S2 evenly in time", 's2, _s2_total = sample_window(sb, "injection_reports",' in src("lens_s3e_selfcheck.py"), True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
