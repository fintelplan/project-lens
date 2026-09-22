"""CC-108 -- S3-C reads its whole 30-day window, evenly in time, past the cap.

Before CC-108 S3-C asked for `order asc` + `limit 40`: the OLDEST 40 S1 reports
(~5 days) and the OLDEST 40 S2 findings (~1 day) of a 30-day drift window.
The fake returns at most 1000 rows per request, as PostgREST does.
    python tests/test_s3c_window.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_window_sample as W
import lens_s3c_biasdrift as S3C

PASS = 0
FAIL = 0
CAP = 1000
DAY = timedelta(days=1)
END = datetime(2026, 9, 22, tzinfo=timezone.utc)


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


class Resp:
    def __init__(self, data):
        self.data = data


class Query:
    def __init__(self, outer, name):
        self.outer, self.name = outer, name
        self.lo = self.hi = self.n = None
        self.ids = None

    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def order(self, *a, **k): return self

    def limit(self, n):
        self.n = n
        return self

    def range(self, lo, hi):
        self.lo, self.hi = lo, hi
        return self

    def in_(self, col, vals):
        self.ids = set(vals)
        return self

    def execute(self):
        self.outer.requests += 1
        rows = self.outer.tables[self.name]
        if self.ids is not None:
            rows = [r for r in rows if r["id"] in self.ids]
        if self.lo is not None:
            rows = rows[self.lo: min(self.hi + 1, self.lo + CAP)]
        else:
            rows = rows[: min(self.n if self.n is not None else CAP, CAP)]
        return Resp(rows)


class FakeSB:
    def __init__(self, tables):
        self.tables = tables
        self.requests = 0

    def table(self, name):
        return Query(self, name)


def uniform(prefix, field, n, days):
    start = END - timedelta(days=days)
    step = timedelta(days=days) / n
    return [{"id": f"{prefix}-{i}", "domain_focus": "d", "summary": "s", "cycle": "c",
             "analyst": "a", "injection_type": "t", "evidence": "e", "confidence_score": 0.5,
             field: (start + step * i).isoformat()} for i in range(n)]


def when(r, f):
    return W._parse_ts(r[f])


print("== S3-C, Lens-shaped: 239 S1 reports and 1,169 S2 findings in 30 days ==")
s1 = uniform("s1", "generated_at", 239, 30)
s2 = uniform("s2", "created_at", 1169, 30)
sb = FakeSB({"lens_reports": s1, "injection_reports": s2})
g1, g2 = S3C.fetch_reports(sb)
check("40 S1 sampled", len(g1), 40)
check("40 S2 sampled", len(g2), 40)
check("S1 REACHES THE NEWEST DAY", when(s1[-1], "generated_at") - when(g1[-1], "generated_at") < DAY, True)
check("S2 REACHES THE NEWEST DAY PAST THE CAP", when(s2[-1], "created_at") - when(g2[-1], "created_at") < DAY, True)
check("S1 starts in the oldest day", when(g1[0], "generated_at") - when(s1[0], "generated_at") < DAY, True)
check("S1 newest third gets its share", 11 <= W.newest_third(g1, "generated_at") <= 16, True)
check("S2 newest third gets its share", 11 <= W.newest_third(g2, "created_at") <= 16, True)
check("ids are selected (source_reports is no longer empty)", all(r.get("id") for r in g1), True)
check("pages: S1 1 id page + 1 fetch, S2 2 id pages + 1 fetch", sb.requests, 5)

print("== the old query shape is gone, and Cohere refusals are recorded ==")
src = open(S3C.__file__, "rb").read()
check('no .order("generated_at", desc=False)', b'.order("generated_at", desc=False)' in src, False)
check('no .order("created_at", desc=False)', b'.order("created_at", desc=False)' in src, False)
check("records Cohere's refusal", b'record_refusal("cohere", MODEL, exc=_last_err)' in src, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
