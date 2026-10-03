"""CC-136 -- order item 3 Phase A (labels): every number read from injection_reports.confidence_score
is shown under its own analyst's measure (lens_s2_measures), never as "confidence", "conf=" or
"Score:". The column holds six measures (CC-135); S3-C's prompt did not even say which analyst a
number came from. A ratchet (LR-254): no line that prints the column may carry the old labels.
    python tests/test_cc136_labels.py
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
OLD_LABEL = re.compile(r"Confidence:|conf=|confidence=|Score:")
bad = []
for f in FILES:
    for i, line in enumerate(src(f).split("\n"), 1):
        if "confidence_score" in line and OLD_LABEL.search(line):
            bad.append(f"{f}:{i}")
check("no line that prints the column carries an old label", bad, [])

# Sites that print through a variable (the number was read on an earlier line). Each pair is the
# whole site, not a fragment: the S2-F lines in the same files (compendium, forensic report) print
# lens_operation_detections.confidence, a different column, and keep conf= -- Phase A covers
# injection_reports only (item 3 design, section 5). A file-wide "conf=" check failed on them.
VIA_VARIABLE = {
    "lens_mission_analyst.py": ("type={inj_type} confidence={conf}", "type={inj_type} {measure_key(analyst)}={conf}"),
    "lens_forensic_report.py": ("[{itype} conf={conf:.2f}]", "[{itype} {measure_key(analyst)}={conf:.2f}]"),
    "lens_compendium.py": ("[{analyst}] conf={conf:.2f}", "[{analyst}] {measure_label(analyst)} {conf:.2f}"),
}
for f, (old, new) in VIA_VARIABLE.items():
    s = src(f)
    check(f"{f}: the old label is gone and the measure is named", (old in s, new in s), (False, True))
tg = src("lens_telegram.py")
check("S2 message names S2-A's and S2-B's measure",
      ("({conf:.0%} {measure_label('S2-A')})" in tg, "({conf:.0%} {measure_label('S2-B')})" in tg), (True, True))

USES = {"lens_s2_step_report.py": 2, "lens_mission_analyst.py": 1, "lens_s2_gap.py": 1, "lens_compendium.py": 1,
        "lens_forensic_report.py": 1, "lens_telegram.py": 2, "lens_s3a_patterns.py": 1, "lens_s3c_biasdrift.py": 1,
        "lens_s3d_longterm.py": 1, "lens_s3e_selfcheck.py": 1}
got = {f: len(re.findall(r"measure_(?:label|key)\(", src(f))) for f in USES}
check("each of the 12 sites names its measure", {f: got[f] >= n for f, n in USES.items()}, {f: True for f in USES})
check("S3-C now says which analyst a number came from", "Analyst: {r.get('analyst')}" in src("lens_s3c_biasdrift.py"), True)

import lens_s2_measures as M
check("no measure is called plain confidence", [a for a in M.MEASURES if M.measure_key(a) == "confidence"], [])
by_type = {"NONE": [1, 2, 3], "FRAMING": [1]}
check("the Compendium's dominant method is a finding before NONE",
      max(by_type, key=lambda k: (M.is_finding(k), len(by_type[k]))), "FRAMING")
check("the Compendium uses that rule", "key=lambda k: (is_finding(k), len(by_type[k]))" in src("lens_compendium.py"), True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
