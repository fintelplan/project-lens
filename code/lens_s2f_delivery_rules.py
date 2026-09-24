"""
lens_s2f_delivery_rules.py -- when Direction B speaks (CC-120, LENS-047)

Pure decision logic: no network, no database, no clock. lens_s2f_direction_b.py feeds it
the HIGH rows of lens_drift_findings, the days on which the S2-F aggregators wrote rows,
and the last ledger entry (lens_s2f_deliveries) per finding; it returns what to send.

Why (completion test L3.4, PHI-004 "noisy alerts train readers in distrust"): one finding
is sent once until it changes. Before CC-120 Direction B re-sent the newest five unreviewed
HIGH rows on every run. The Verification aggregator writes one row per finding per day, so
one finding arrived about ten times a day (1,254 messages May 18 - Sep 24, 300 in the last
30 days), while three findings that stopped being confirmed (RT x xi_office after Jul 3,
TeleSUR x trump_office after Aug 5, RT x khamenei_office after Aug 22) were never announced
as ended.

Design (ISA-18.2 / IEC 62682 alarm management: no repeating or chattering alarms; on-delay
and off-delay against chattering; a return to normal is a state change too):
  NEW      a finding appears, or reappears after CLEARED. Sent at once (on-delay 0: the
           aggregator's own 15-article / 45-day gate already filters).
  CHANGED  the reported set of persistent operations changes. An operation joins only when
           HOLD_DAYS consecutive daily rows show it, and leaves only when HOLD_DAYS
           consecutive daily rows lack it. Without this the set chatters at the threshold
           (OP-015, OP-027 and OP-035 moved in and out through September).
  CLEARED  no row for the finding on CLEAR_ALIVE_DAYS days on which the aggregators DID
           write rows. Days with no S2-F rows at all (Aug 23-30, the database outage) do
           not count: a dark pipeline is not a finding that ended.
  Anything else sends nothing.

Numbers from the replay on the real history (lens047_l34_replay): HOLD_DAYS=3 cut the last
30 days from 300 messages to 5. CLEAR_ALIVE_DAYS=7: findings that flickered at the
15-article gate were gone 3-4 aggregator days (TeleSUR Jun 15-18, RT x khamenei Jul 20-22);
findings that really stopped were gone 8 days or more.
"""
import re

HOLD_DAYS = 3
CLEAR_ALIVE_DAYS = 7
OPEN_KINDS = ("NEW", "CHANGED")

_HEAD = re.compile(r"VERIFICATION FINDING \(([^)]*)\): ([^\n]*)")


def voice_of(phrasing):
    """Voice name from a Verification row's phrasing, or None. The table has no voice
    column; the aggregator writes 'VERIFICATION FINDING (<lens>): <voice>' as line one."""
    m = _HEAD.match(phrasing or "")
    return m.group(2).strip() if m else None


def persistent_ops(framing_mean, recur):
    """Operations whose article count is >= recur. framing_mean holds op -> article count
    (the aggregator's op_article_counts)."""
    out = set()
    for op, n in (framing_mean or {}).items():
        if isinstance(n, bool) or not isinstance(n, (int, float)):
            continue
        if n >= recur:
            out.add(op)
    return frozenset(out)


def daily_series(rows, recur):
    """rows: HIGH rows with created_at, state_actor_lens, framing_mean, finding_phrasing.
    Returns (series, unparsed): series maps (voice, lens) -> [(day, ops, row), ...] oldest
    first, keeping the newest row of each UTC day; unparsed lists rows with no voice."""
    by_pair, unparsed = {}, []
    for r in rows:
        voice = voice_of(r.get("finding_phrasing"))
        if not voice:
            unparsed.append(r)
            continue
        day = str(r["created_at"])[:10]
        slot = by_pair.setdefault((voice, r["state_actor_lens"]), {})
        old = slot.get(day)
        if old is None or str(r["created_at"]) >= str(old["created_at"]):
            slot[day] = r
    series = {}
    for pair, days in by_pair.items():
        series[pair] = [(d, persistent_ops(days[d].get("framing_mean"), recur), days[d])
                        for d in sorted(days)]
    return series, unparsed


def decide(series, last, alive_days, today, hold=None, clear_after=None):
    """What to send for one finding.
    series      [(day, ops, row), ...] oldest first (may be empty)
    last        None, or the newest ledger entry: {"kind", "reported_ops", "sent_at"}
    alive_days  set of 'YYYY-MM-DD' on which the aggregators wrote any row
    today       'YYYY-MM-DD'
    Returns None, or {"kind", "reported_ops", "added", "dropped", "row",
                      "last_day", "alive_since"}."""
    hold = HOLD_DAYS if hold is None else hold
    clear_after = CLEAR_ALIVE_DAYS if clear_after is None else clear_after
    latest_day = series[-1][0] if series else None
    if latest_day is None:
        alive_since = None
    else:
        alive_since = sum(1 for d in alive_days if latest_day < d <= today)
    gone = latest_day is None or alive_since >= clear_after
    row = series[-1][2] if series else None
    is_open = last is not None and last.get("kind") in OPEN_KINDS

    if is_open:
        reported = set(last.get("reported_ops") or [])
        if gone:
            return {"kind": "CLEARED", "reported_ops": sorted(reported), "added": [],
                    "dropped": [], "row": row, "last_day": latest_day,
                    "alive_since": alive_since}
        hist = [ops for _, ops, _ in series[-hold:]]
        if len(hist) < hold:
            return None
        added = {op for op in set().union(*hist) if all(op in h for h in hist)} - reported
        dropped = {op for op in reported if all(op not in h for h in hist)}
        if not added and not dropped:
            return None
        return {"kind": "CHANGED", "reported_ops": sorted((reported | added) - dropped),
                "added": sorted(added), "dropped": sorted(dropped), "row": row,
                "last_day": latest_day, "alive_since": alive_since}

    if gone:
        return None                     # never announce a finding that is already over
    if last is not None and latest_day <= str(last.get("sent_at") or "")[:10]:
        return None                     # only the rows that were already cleared
    return {"kind": "NEW", "reported_ops": sorted(series[-1][1]), "added": [], "dropped": [],
            "row": row, "last_day": latest_day, "alive_since": alive_since}
