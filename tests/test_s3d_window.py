"""CC-93 -- S3-D reads its whole window, and says which window it read.

On 2026-09-21 S3-D, the 30-day researcher, read 2026-08-22 to 2026-09-02
(order asc + limit 30 = the OLDEST thirty rows) and never saw the last
nineteen days. Its own ACH check then called the '30-day acceleration' a
possible reporting artifact. On Thursdays the 90-day run was also labelled
30 days in the prompt, the log and the stored row.
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
    """Honours limit(), as PostgREST does -- so the OLD query really does
    return only the oldest rows here, and the test can tell the difference."""
    def __init__(self, rows):
        self.rows = rows
        self.n = None

    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def order(self, *a, **k): return self

    def limit(self, n):
        self.n = n
        return self

    def execute(self):
        return Resp(self.rows[: self.n] if self.n is not None else self.rows)


class FakeSB:
    def __init__(self, rows): self.rows = rows
    def table(self, name): return Query(self.rows)


base = datetime(2026, 8, 22, tzinfo=timezone.utc)
s1 = [{"id": i, "domain_focus": "d", "summary": "s", "cycle": "c",
       "generated_at": (base + timedelta(hours=7.2 * i)).isoformat()} for i in range(100)]
s2 = [{"analyst": "a", "injection_type": "t", "evidence": "e", "confidence_score": 0.5,
       "created_at": (base + timedelta(hours=7.2 * i)).isoformat()} for i in range(100)]
OLDEST = s1[0]["generated_at"][:10]
NEWEST = s1[-1]["generated_at"][:10]

print("== even_sample ==")
idx = S3D.even_sample(list(range(100)), 30)
check("30 picked", len(idx), 30)
check("first is the oldest", idx[0], 0)
check("last is the newest", idx[-1], 99)
check("strictly increasing", all(a < b for a, b in zip(idx, idx[1:])), True)
check("fewer rows than k -> all", S3D.even_sample(list(range(10)), 30), list(range(10)))
check("k=1 -> the newest", S3D.even_sample(list(range(5)), 1), [4])
check("empty", S3D.even_sample([], 30), [])

print("== fetch_s1_reports reads the whole 30-day window ==")
got = S3D.fetch_s1_reports(FakeSB(s1), 30)
check("30 rows", len(got), 30)
check("starts at the oldest day", got[0]["generated_at"][:10], OLDEST)
check("REACHES THE NEWEST DAY", got[-1]["generated_at"][:10], NEWEST)

print("== fetch_s2_reports reads the whole window ==")
got2 = S3D.fetch_s2_reports(FakeSB(s2), 30)
check("20 rows", len(got2), 20)
check("S2 REACHES THE NEWEST DAY", got2[-1]["created_at"][:10], NEWEST)

print("== one source for model and budget (LR-105) ==")
p, m, k, mo = wire("s3d_longterm")
check("MODEL is the registry's", S3D.MODEL, m)
check("MAX_TOKENS is the registry's", S3D.MAX_TOKENS, mo)
check("registry budget", mo, 16000)
check("timeout covers 16000 at 70 tok/s", S3D.REQUEST_TIMEOUT_S >= 229, True)

print("== no label says 30 when the window is not 30 ==")
src = open(S3D.__file__, "rb").read()
check('no hardcoded "30_DAY"', b'"30_DAY"' in src, False)
check("no header built from LOOKBACK_DAYS", b"last {LOOKBACK_DAYS} days" in src, False)
check("no timeout=180", b"timeout=180" in src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
