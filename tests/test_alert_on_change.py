"""CC-116 -- the CRITICAL alert fires on a change; the Brief says how long the level held.
    python tests/test_alert_on_change.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)

import lens_telegram as T

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


d = T.alert_decision
print("== when the alert fires ==")
check("CRITICAL after six CRITICALs: quiet", d("CRITICAL", ["CRITICAL"] * 6)[0], False)
check("CRITICAL after a full day of HIGH: escalation", d("CRITICAL", ["HIGH", "HIGH", "CRITICAL"])[0], True)
check("CRITICAL after one HIGH flip: NOT news (hysteresis)", d("CRITICAL", ["HIGH", "CRITICAL"])[0], False)
LIVE = ["CRITICAL", "CRITICAL", "CRITICAL", "HIGH", "CRITICAL", "HIGH", "CRITICAL",
        "HIGH", "HIGH", "HIGH", "CRITICAL", "HIGH", "HIGH"]   # 2026-09-23, this run excluded
check("the live history: a CRITICAL now is quiet", d("CRITICAL", LIVE)[0], False)
# replay the live sequence oldest-first: how many alerts would the week have sent?
seq = list(reversed(["CRITICAL"] + LIVE))
sent = sum(1 for i, lvl in enumerate(seq) if d(lvl, list(reversed(seq[:i])))[0])
print(f"  (the live week: {sent} alerts instead of {sum(1 for s in seq if s in ('HIGH', 'CRITICAL'))})")
check("the live week sends at most 3 alerts", sent <= 3, True)
check("HIGH after ELEVATED: escalation", d("HIGH", ["ELEVATED"])[0], True)
check("HIGH after CRITICAL: not an escalation, but HIGH was seen?", d("HIGH", ["CRITICAL", "HIGH"])[0], False)
check("first CRITICAL in 7 days, after a drop", d("CRITICAL", ["HIGH", "HIGH"])[0], True)
check("no history: alert", d("CRITICAL", [])[0], True)
check("history unreadable: alert (loud side)", d("CRITICAL", None)[0], True)
check("ELEVATED never alerts", d("ELEVATED", [])[0], False)
check("the quiet reason says so", "unchanged" in d("CRITICAL", ["CRITICAL"])[1], True)

print("== previous_threats excludes this run, and fails loud ==")
class Q:
    def __init__(self, rows): self.rows = rows
    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, n): return self
    def execute(self): return type("R", (), {"data": self.rows})
rows = [{"threat_level": "CRITICAL", "run_id": "now"}, {"threat_level": "HIGH", "run_id": "earlier"}]
T._get_sb = lambda: type("S", (), {"table": lambda self, n: Q(rows)})()
check("this run is excluded", T.previous_threats("now"), ["HIGH"])
def boom(): raise RuntimeError("db down")
T._get_sb = boom
check("a read failure returns None", T.previous_threats("now"), None)

print("== the Brief says how long the level held ==")
data = {"ma": {"threat_level": "CRITICAL", "executive_summary": "x", "quality_score": 0.8, "run_id": "r"},
        "s2": [], "s3": {}, "s1": [], "s1_missing": [], "s2f": [], "top_entity": {},
        "trend": [{"threat_level": "CRITICAL"}] * 3 + [{"threat_level": "HIGH"}]}
brief = T.format_daily_brief(data)
check("3 waves in a row", "THREAT: CRITICAL</b> (3 waves in a row)" in brief, True)
data["trend"] = [{"threat_level": "CRITICAL"}] * 7
check("7+ when the window is full", "(7+ waves in a row)" in T.format_daily_brief(data), True)

print("== Mission Analyst decides, and no longer alerts on every CRITICAL ==")
ma = open(os.path.join(CODE, "lens_mission_analyst.py"), "rb").read()
check("the old rule is gone", b'if summary.get("threat_level") in ("CRITICAL", "HIGH"):' in ma, False)
check("the decision gates the send", b"if _send:\n            send_critical_alert(" in ma, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
