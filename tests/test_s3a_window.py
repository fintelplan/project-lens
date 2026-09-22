"""CC-99 -- S3-A reads its whole 7-day window, evenly in time, past the cap.

Before CC-99 S3-A asked for `order asc` + `limit`: the OLDEST 20 S1 reports and
the OLDEST 15 S2 findings of the week. The fake returns at most 1000 rows per
request, as PostgREST does (LENS-044: a test double must enforce production's
limits). Also proves the sampler moved unchanged: S3-D's names ARE the module's.
    python tests/test_s3a_window.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_window_sample as W
import lens_s3d_longterm as S3D
import lens_s3a_patterns as S3A

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
    def __init__(self, outer):
        self.outer = outer
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
        rows = self.outer.rows
        if self.ids is not None:
            rows = [r for r in rows if r["id"] in self.ids]
        if self.lo is not None:
            rows = rows[self.lo: min(self.hi + 1, self.lo + CAP)]
        else:
            rows = rows[: min(self.n if self.n is not None else CAP, CAP)]
        return Resp(rows)


class FakeSB:
    def __init__(self, rows):
        self.rows = rows
        self.requests = 0

    def table(self, name):
        return Query(self)


def uniform(field, n, days):
    start = END - timedelta(days=days)
    step = timedelta(days=days) / n
    return [{"id": f"r-{i}", "domain_focus": "d", "summary": "s", "cycle": "c",
             "analyst": "a", "injection_type": "t", "evidence": "e",
             "confidence_score": 0.5, field: (start + step * i).isoformat()} for i in range(n)]


def when(r, field):
    return W._parse_ts(r[field])


print("== the sampler moved, unchanged: S3-D's names ARE the module's ==")
for n in ("PAGE", "MAX_PAGES", "even_sample", "_parse_ts", "even_sample_by_time",
          "newest_third", "window_span", "sample_window"):
    check(f"S3D.{n} is lens_window_sample.{n}", getattr(S3D, n) is getattr(W, n), True)
d_src = open(S3D.__file__, "rb").read()
check("S3-D no longer defines sample_window (one source)", b"def sample_window(" in d_src, False)
check("the module does not call basicConfig", b"basicConfig(" in open(W.__file__, "rb").read(), False)

print("== S1: ~8 reports a day for 7 days ==")
s1 = uniform("generated_at", 56, 7)
sb = FakeSB(s1)
got = S3A.fetch_s1_reports(sb, 7)
check("20 sampled", len(got), S3A.MAX_S1_REPORTS)
check("REACHES THE NEWEST DAY", when(s1[-1], "generated_at") - when(got[-1], "generated_at") < DAY, True)
check("starts in the oldest day", when(got[0], "generated_at") - when(s1[0], "generated_at") < DAY, True)
check("the newest third of the week gets its share", 5 <= W.newest_third(got, "generated_at") <= 8, True)
check("oldest-first", got == sorted(got, key=lambda r: r["generated_at"]), True)
check("1 id page + 1 row fetch", sb.requests, 2)

print("== S2: 1,500 findings in 7 days, past the 1000-row cap ==")
s2 = uniform("created_at", 1500, 7)
sb = FakeSB(s2)
got2 = S3A.fetch_s2_reports(sb, 7)
check("15 sampled", len(got2), S3A.MAX_S2_REPORTS)
check("REACHES THE NEWEST DAY PAST THE CAP", when(s2[-1], "created_at") - when(got2[-1], "created_at") < DAY, True)
check("2 id pages + 1 row fetch", sb.requests, 3)

print("== the old query shape is gone ==")
a_src = open(S3A.__file__, "rb").read()
check('no .order("generated_at", desc=False)', b'.order("generated_at", desc=False)' in a_src, False)
check('no .order("created_at", desc=False)', b'.order("created_at", desc=False)' in a_src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
