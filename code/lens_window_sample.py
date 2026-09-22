"""lens_window_sample.py -- sample a time window evenly in TIME, past PostgREST's page cap.

CC-99 (LENS-045): moved here UNCHANGED from lens_s3d_longterm.py (CC-93, CC-94,
CC-96, LENS-044) so S3-A can use it without importing another position's module,
which runs logging.basicConfig and the model registry at import. S3-D imports
every name back, so its behaviour and its gate (tests/test_s3d_window.py) are
untouched. Must not call logging.basicConfig: the caller's log format wins.
"""
import logging
from datetime import datetime

log = logging.getLogger("lens_window_sample")


PAGE = 1000        # CC-94: PostgREST returns at most 1000 rows per request here
MAX_PAGES = 20     # 20,000 rows; a 90-day injection_reports window is ~3,000 today


def even_sample(rows: list, k: int) -> list:
    """CC-93 (LENS-044): k rows spread evenly across the window, oldest first.

    The query used to be `order asc` + `limit k` -- the OLDEST k rows. On
    2026-09-21 that meant S3-D, the 30-day researcher, read 2026-08-22 to
    2026-09-02 and never saw the last nineteen days, and its own ACH check
    then called the '30-day acceleration' a possible reporting artifact.
    Newest-k would only move the blind spot to the other end. The charter
    asks what accumulates ACROSS the window, so the sample spans it.
    """
    n = len(rows)
    if k <= 0 or n == 0:
        return []
    if n <= k:
        return list(rows)
    if k == 1:
        return [rows[-1]]
    idx = sorted({round(i * (n - 1) / (k - 1)) for i in range(k)})
    return [rows[i] for i in idx]


def _parse_ts(v):
    s = str(v or "").strip().replace(" ", "T", 1)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def even_sample_by_time(rows: list, k: int, ts_field: str) -> list:
    """CC-96 (LENS-044): up to k rows spread evenly in TIME, oldest first.

    CC-93's even_sample spread the sample by ROW COUNT. On 2026-09-21 the
    90-day S1 window held 1,803 rows, 1,564 of them before 2026-08-22: the
    child process ignored --single-lens until CC-75 and wrote every lens four
    times a wave. By row count, ~78 of 90 samples came from June-August and
    the newest 30 days got ~12. A pipeline bug's density was being read as
    the world's.

    The window is cut into k equal stretches of time; each contributes the
    row nearest its midpoint. A stretch with NO rows lends its place to the
    nearest real row not already chosen, so the sample stays at k. Nothing is
    invented: no sampled row is dated inside a gap, and the model still sees
    the gap. (Without this, seven empty days in the last thirty cut the
    Monday sample from 30 to 23 -- found by the live dry run, 2026-09-21.)
    Rows must arrive oldest-first.
    """
    n = len(rows)
    if k <= 0 or n == 0:
        return []
    if n <= k:
        return list(rows)
    ts = [_parse_ts(r.get(ts_field)) for r in rows]
    if any(x is None for x in ts):
        log.warning(f"even_sample_by_time: unparseable {ts_field} -- falling back to row-count sampling")
        return even_sample(rows, k)
    span = (ts[-1] - ts[0]).total_seconds()
    if span <= 0:
        return even_sample(rows, k)
    width = span / k
    best = {}
    for i, x in enumerate(ts):
        off = (x - ts[0]).total_seconds()
        b = min(k - 1, int(off / width))
        d = abs(off - (b + 0.5) * width)
        if b not in best or d < best[b][0]:
            best[b] = (d, i)
    chosen = {best[b][1] for b in best}
    if len(chosen) < k:
        import bisect
        offs = [(x - ts[0]).total_seconds() for x in ts]
        for b in range(k):
            if b in best or len(chosen) >= k:
                continue
            mid = (b + 0.5) * width
            j = bisect.bisect_left(offs, mid)
            lo, hi = j - 1, j
            while lo >= 0 or hi < n:
                cands = []
                if lo >= 0:
                    cands.append((abs(offs[lo] - mid), lo))
                if hi < n:
                    cands.append((abs(offs[hi] - mid), hi))
                _, c = min(cands)
                if c not in chosen:
                    chosen.add(c)
                    break
                if c == lo:
                    lo -= 1
                else:
                    hi += 1
    return [rows[i] for i in sorted(chosen)]


def newest_third(rows: list, ts_field: str) -> int:
    """How many sampled rows fall in the newest third of the span they cover."""
    ts = [x for x in (_parse_ts(r.get(ts_field)) for r in rows) if x is not None]
    if len(ts) < 2:
        return len(ts)
    cut = ts[0] + (ts[-1] - ts[0]) * 2 / 3
    return sum(1 for x in ts if x >= cut)


def window_span(rows: list, field: str) -> str:
    dates = [str(r.get(field) or "")[:10] for r in rows if r.get(field)]
    return f", spanning {dates[0]} to {dates[-1]}" if dates else ""


def sample_window(sb, table: str, cols: str, ts_field: str, cutoff: str, k: int):
    """CC-94 (LENS-044): sample the WHOLE window, however many rows it holds.

    Pass 1 pages through (id, timestamp) only -- light -- so the sample sees
    every row. Pass 2 fetches the full columns for the k chosen ids only.
    CC-93's single fetch stopped at the 1000-row cap: on 2026-09-21
    injection_reports held more than 1000 rows in 30 days and the S2 sample
    ended at 2026-09-19; a 90-day window would have ended a third of the way.
    Returns (rows oldest-first, total rows seen in the window).
    """
    keys, page = [], 0
    while True:
        r = sb.table(table).select(f"id,{ts_field}") \
            .gte(ts_field, cutoff).order(ts_field, desc=False) \
            .range(page * PAGE, page * PAGE + PAGE - 1).execute()
        batch = r.data or []
        keys.extend(batch)
        page += 1
        if len(batch) < PAGE:
            break
        if page >= MAX_PAGES:
            log.warning(f"{table}: window exceeds {MAX_PAGES * PAGE} rows -- "
                        f"the sample covers only the oldest {len(keys)}")
            break
    chosen = even_sample_by_time(keys, k, ts_field)   # CC-96
    if not chosen:
        return [], len(keys)
    ids = [c["id"] for c in chosen]
    r = sb.table(table).select(cols).in_("id", ids).execute()
    rows = sorted(r.data or [], key=lambda x: str(x.get(ts_field) or ""))
    if len(rows) != len(ids):
        log.warning(f"{table}: asked for {len(ids)} sampled rows, got {len(rows)}")
    return rows, len(keys)
