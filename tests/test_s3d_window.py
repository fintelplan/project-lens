"""CC-93 + CC-94 -- S3-D reads its whole window, says which window it read,
and is not stopped by the 1000-row cap.

On 2026-09-21 S3-D, the 30-day researcher, read 2026-08-22 to 2026-09-02
(order asc + limit 30 = the OLDEST thirty rows) and never saw the last
nineteen days. CC-93 sampled the window evenly but still in one request, and
injection_reports held more than 1000 rows in 30 days, so the S2 sample ended
at 2026-09-19. The fake below returns at most 1000 rows per request, exactly
as PostgREST does here, so a single-request fetch cannot pass.
    python tests/test_s3d_window.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_s3d_longterm as S3D
from lens_models import wire

PASS = 0
FAIL = 0
CAP = 1000


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


def s1_rows(n, days):
    base = datetime(2026, 8, 22, tzinfo=timezone.utc)
    step = days * 24.0 / n
    return [{"id": f"s1-{i}", "domain_focus": "d", "summary": "s", "cycle": "c",
             "generated_at": (base + timedelta(hours=step * i)).isoformat()} for i in range(n)]


def s2_rows(n, days):
    base = datetime(2026, 6, 23, tzinfo=timezone.utc)
    step = days * 24.0 / n
    return [{"id": f"s2-{i}", "analyst": "a", "injection_type": "t", "evidence": "e",
             "confidence_score": 0.5,
             "created_at": (base + timedelta(hours=step * i)).isoformat()} for i in range(n)]


print("== even_sample ==")
idx = S3D.even_sample(list(range(100)), 30)
check("30 picked", len(idx), 30)
check("first is the oldest", idx[0], 0)
check("last is the newest", idx[-1], 99)
check("strictly increasing", all(a < b for a, b in zip(idx, idx[1:])), True)
check("fewer rows than k -> all", S3D.even_sample(list(range(10)), 30), list(range(10)))
check("k=1 -> the newest", S3D.even_sample(list(range(5)), 1), [4])
check("empty", S3D.even_sample([], 30), [])

print("== S1, 100 rows over 30 days ==")
rows = s1_rows(100, 30)
got = S3D.fetch_s1_reports(FakeSB(rows), 30)
check("30 rows", len(got), 30)
check("starts at the oldest day", got[0]["generated_at"][:10], rows[0]["generated_at"][:10])
check("REACHES THE NEWEST DAY", got[-1]["generated_at"][:10], rows[-1]["generated_at"][:10])

print("== S2, 2,500 rows over 90 days -- past the 1000-row cap ==")
rows = s2_rows(2500, 90)
sb = FakeSB(rows)
got2 = S3D.fetch_s2_reports(sb, 90)
check("60 rows for a 90-day window", len(got2), 60)
check("oldest-first", got2 == sorted(got2, key=lambda r: r["created_at"]), True)
check("S2 REACHES THE NEWEST DAY PAST THE CAP",
      got2[-1]["created_at"][:10], rows[-1]["created_at"][:10])
check("requests: 3 pages of ids + 1 fetch of the chosen rows", sb.requests, 4)

print("== one source for model and budget (LR-105) ==")
p, m, k, mo = wire("s3d_longterm")
check("MODEL is the registry's", S3D.MODEL, m)
check("MAX_TOKENS is the registry's", S3D.MAX_TOKENS, mo)
check("registry budget", mo, 16000)
check("timeout covers 16000 at the slowest probed throughput", S3D.REQUEST_TIMEOUT_S >= 229, True)

print("== no label says 30 when the window is not 30 ==")
src = open(S3D.__file__, "rb").read()
check('no hardcoded "30_DAY"', b'"30_DAY"' in src, False)
check("no header built from LOOKBACK_DAYS", b"last {LOOKBACK_DAYS} days" in src, False)
check("no timeout=180", b"timeout=180" in src, False)
check("no single-request window fetch left", b"WINDOW_FETCH_CAP" in src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
