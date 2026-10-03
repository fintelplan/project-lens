"""CC-138 -- order item 2 (completion test L2.3): the Daily Brief says when a scoring run was cut.
Bytes: Sep 29 morning (0 of 24) and Sep 30 evening (0 of 17) were refused by Cloudflare's daily
quota while the Brief printed only "Scoring (24 h): ..." from the rows that did exist -- a starved day
read as a quiet one. The refusal is already stored in lens_provider_events (source = the script's
file name, class daily_quota); the Brief now reads it. ISA-18.2: the line appears only on a day it
needs a response.
    python tests/test_cc138_brief_cut.py
"""
import os
import re
import sys
from datetime import datetime, timezone

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


def read(*p):
    return open(os.path.join(ROOT, *p), "rb").read().decode("utf-8").replace("\r\n", "\n")


import lens_s2f_delivery_rules as D

now = datetime(2026, 10, 2, 8, 0, tzinfo=timezone.utc)
base = {"scored_24h": 20, "articles_24h": 16, "applicable_24h": 8, "ops_24h": 28, "open": []}
cut = dict(base, cuts_24h=["2026-10-02T07:22:53.1+00:00"])
lines = D.s2f_brief_lines(cut, now)
check("a cut run is said, with the time it ended", any("daily quota" in l and "07:22" in l for l in lines), True)
check("a day without a cut says nothing extra", any("cut" in l.lower() for l in D.s2f_brief_lines(base, now)), False)
check("a failed read of the cuts says so",
      any("Scoring cuts: status unavailable" in l for l in D.s2f_brief_lines(dict(base, cuts_error="HTTP 500"), now)), True)
check("the scoring line is still there on a cut day", any(l.startswith("Scoring (24 h):") for l in lines), True)

tg = read("code", "lens_telegram.py")
m = re.search(r'\.eq\("source", "([^"]+)"\)\.eq\("class", "daily_quota"\)', tg)
check("the Brief reads S2-F's daily_quota refusals", bool(m), True)
wf = read(".github", "workflows", "lens-s2f-scoring.yml")
runs = re.findall(r"run:\s*python\s+(\S+lens_s2f_scoring_cron\.py)", wf)
# record_refusal stores source = os.path.basename(sys.argv[0]): the file name the workflow runs.
check("the source it reads is the file name the workflow runs (what record_refusal stores)",
      (m.group(1) if m else None) in {os.path.basename(r) for r in runs} and len(runs) == 1, True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
