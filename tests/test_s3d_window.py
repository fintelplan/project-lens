"""CC-93 + CC-94 + CC-96 -- S3-D reads its whole window, evenly in TIME,
past the 1000-row cap, and says which window it read.

2026-09-21: S3-D read the OLDEST 30 rows (CC-93); its even sample stopped at
PostgREST's 1000-row page (CC-94); and at 90 days an even-by-ROW sample gave
June-August ~87% of the draw because those months hold duplicated rows from
before CC-75 (CC-96). The fake returns at most 1000 rows per request, as the
database does.
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
DAY = timedelta(days=1)


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


END = datetime(2026, 9, 21, tzinfo=timezone.utc)


def uniform(prefix, field, n, days):
    start = END - timedelta(days=days)
    step = timedelta(days=days) / n
    return [{"id": f"{prefix}-{i}", "domain_focus": "d", "summary": "s", "cycle": "c",
             "analyst": "a", "injection_type": "t", "evidence": "e", "confidence_score": 0.5,
             field: (start + step * i).isoformat()} for i in range(n)]


def lens_shaped(field):
    """Lens's real 90-day shape: ~26 rows/day for 60 days, then ~8/day for 30."""
    start = END - timedelta(days=90)
    rows = []
    for i in range(1560):
        rows.append(start + timedelta(days=60) / 1560 * i)
    for i in range(240):
        rows.append(start + timedelta(days=60) + timedelta(days=30) / 240 * i)
    return [{"id": f"x-{i}", "domain_focus": "d", "summary": "s", "cycle": "c",
             "analyst": "a", "injection_type": "t", "evidence": "e", "confidence_score": 0.5,
             field: ts.isoformat()} for i, ts in enumerate(rows)]


def when(r, field):
    return S3D._parse_ts(r[field])


def in_last_30(rows, field):
    cut = END - timedelta(days=30)
    return sum(1 for r in rows if when(r, field) >= cut)


print("== even_sample (by row count) still behaves ==")
idx = S3D.even_sample(list(range(100)), 30)
check("30 picked", len(idx), 30)
check("first / last", (idx[0], idx[-1]), (0, 99))
check("fewer rows than k -> all", S3D.even_sample(list(range(10)), 30), list(range(10)))
check("empty", S3D.even_sample([], 30), [])

print("== even_sample_by_time on uniform data ==")
rows = uniform("u", "generated_at", 100, 30)
got = S3D.even_sample_by_time(rows, 30, "generated_at")
check("30 picked", len(got), 30)
check("starts within a day of the oldest",
      when(got[0], "generated_at") - when(rows[0], "generated_at") < DAY, True)
check("ends within a day of the newest",
      when(rows[-1], "generated_at") - when(got[-1], "generated_at") < DAY, True)
check("oldest-first", got == sorted(got, key=lambda r: r["generated_at"]), True)

print("== THE DEFECT: Lens-shaped density, 90 rows over 90 days ==")
dense = lens_shaped("generated_at")
by_row = S3D.even_sample(dense, 90)
by_time = S3D.even_sample_by_time(dense, 90, "generated_at")
print(f"  newest 30 days: by row count {in_last_30(by_row, 'generated_at')}, "
      f"by time {in_last_30(by_time, 'generated_at')} (of 90)")
check("by row count starves the newest 30 days (documents the defect)",
      in_last_30(by_row, "generated_at") < 20, True)
check("BY TIME GIVES THE NEWEST 30 DAYS THEIR THIRD",
      27 <= in_last_30(by_time, "generated_at") <= 33, True)
check("newest_third agrees", 27 <= S3D.newest_third(by_time, "generated_at") <= 33, True)

print("== REAL DATA HAS GAPS: seven empty days in the newest thirty ==")
GAP_LO = END - timedelta(days=20)
GAP_HI = END - timedelta(days=13)
gapped = [r for r in lens_shaped("generated_at")
          if not (GAP_LO <= when(r, "generated_at") < GAP_HI)]
gs = S3D.even_sample_by_time(gapped, 90, "generated_at")
print(f"  gapped: {len(gs)} sampled, {in_last_30(gs, 'generated_at')} in the newest 30 days")
check("the sample stays at k", len(gs), 90)
check("no row chosen twice", len({r['id'] for r in gs}), 90)
check("newest 30 days still get their third", 27 <= in_last_30(gs, "generated_at") <= 33, True)
check("NOTHING INVENTED: no sampled row is dated inside the gap",
      any(GAP_LO <= when(r, "generated_at") < GAP_HI for r in gs), False)
check("oldest-first", gs == sorted(gs, key=lambda r: r["generated_at"]), True)

print("== an unparseable timestamp falls back, loudly, to row count ==")
bad = uniform("b", "generated_at", 100, 30)
bad[40]["generated_at"] = "not-a-time"
check("still 30", len(S3D.even_sample_by_time(bad, 30, "generated_at")), 30)

print("== fetch_s1_reports at 90 days on Lens-shaped data ==")
sb = FakeSB(lens_shaped("generated_at"))
got1 = S3D.fetch_s1_reports(sb, 90)
check("newest 30 days get their third", 27 <= in_last_30(got1, "generated_at") <= 33, True)
check("pages past the cap: 2 id pages + 1 row fetch", sb.requests, 3)

print("== fetch_s2_reports, 2,500 uniform rows over 90 days ==")
s2 = uniform("s2", "created_at", 2500, 90)
sb = FakeSB(s2)
got2 = S3D.fetch_s2_reports(sb, 90)
check("60 rows", len(got2), 60)
check("reaches within two days of the newest",
      when(s2[-1], "created_at") - when(got2[-1], "created_at") < 2 * DAY, True)
check("3 id pages + 1 row fetch", sb.requests, 4)

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
check("no single-request window fetch", b"WINDOW_FETCH_CAP" in src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
