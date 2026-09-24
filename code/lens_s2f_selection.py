"""
lens_s2f_selection.py -- which articles S2-F Stage 1 scores (CC-122, LENS-047)

Pure: no network, no database, no clock. lens_s2f_scoring_cron.get_recent_articles feeds it
the window's rows, the recent detection count per voice, and the ids already scored.

Why: LENS-020 decision v4 gives the Watch tier "every article, every cron run". On Apr 29 the
run was capped at the newest 8 rows by collected_at ("fits 30-min cron window") and the
400-character check was applied after the cap. The collector writes a batch from parallel
threads within about a second, so "newest 8" meant whichever source's thread finished last:
975 of 1,093 S2-F detections in 45 days were RT (4 voices in all) while 200-285 long articles
per window went unread, and on Sep 24 the last eight were TASS teasers and nothing was scored.
PHI-004's Watch alert speaks of "[N] voices across [outlets]"; PHI-003 asks for symmetric
scrutiny. S2-D had the same monopoly (TASS) and got a per-source round-robin at LENS-017 B-2.

Rule: long enough first; not yet scored; one article per source per round, newest first
within a source; the source whose last scoring is OLDEST goes first (never scored in
RECENT_DAYS = oldest of all; ties by name), so the rotation continues across runs.
Ordering by count instead was rejected on replay: RT's 975 past detections would have kept
it out for up to RECENT_DAYS -- a new monopoly's mirror image. max_articles still bounds the
run's provider calls; lookback can be wide because scored articles are excluded.
"""

MIN_BODY_CHARS = 400
RECENT_DAYS = 10          # the Watch window (WATCH_WINDOW_DAYS default)


def _src(row):
    return row.get("source_name") or "unknown"


def last_scored_by(detections):
    """detections: rows with voice_name, scored_at -> {voice: newest scored_at}."""
    last = {}
    for d in detections or []:
        v, t = d.get("voice_name"), str(d.get("scored_at") or "")
        if v and t > last.get(v, ""):
            last[v] = t
    return last


def select_articles(rows, last_scored, max_articles, exclude_ids=(), min_chars=MIN_BODY_CHARS):
    """rows: lens_raw_articles rows (id, content, source_name, collected_at, ...), any order.
    last_scored: {voice or source name: newest scored_at in the last RECENT_DAYS}.
    Returns (picked, report)."""
    exclude = set(exclude_ids or ())
    long_rows = [r for r in rows if len(r.get("content") or "") >= min_chars]
    fresh = [r for r in long_rows if r.get("id") not in exclude]
    by_src = {}
    for r in sorted(fresh, key=lambda r: str(r.get("collected_at") or ""), reverse=True):
        by_src.setdefault(_src(r), []).append(r)
    order = sorted(by_src, key=lambda s: (str(last_scored.get(s) or ""), s))
    picked, rnd = [], 0
    while len(picked) < max_articles and any(rnd < len(v) for v in by_src.values()):
        for s in order:
            if rnd < len(by_src[s]):
                picked.append(by_src[s][rnd])
                if len(picked) >= max_articles:
                    break
        rnd += 1
    report = {"window": len(rows), "long": len(long_rows), "short": len(rows) - len(long_rows),
              "already_scored": len(long_rows) - len(fresh), "sources": len(by_src),
              "picked": len(picked), "picked_sources": [_src(r) for r in picked]}
    return picked, report


def report_line(report, lookback_hours):
    """One log line a reader can check against the rule."""
    return ("Selection (last %dh): %d articles, %d long enough, %d short, %d already scored, "
            "%d sources -> %d picked: %s" % (
                lookback_hours, report["window"], report["long"], report["short"],
                report["already_scored"], report["sources"], report["picked"],
                ", ".join(report["picked_sources"]) or "none"))
