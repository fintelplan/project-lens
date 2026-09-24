"""
test_s2f_delivery_rules.py -- gate for CC-120 (completion test L3.4: one finding is sent
once until it changes). Pure: no network, no database. Exits 1 on any failure.
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.environ.get("LENS_CODE_DIR")
                or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))
import lens_s2f_delivery_rules as R          # noqa: E402

D0 = date(2026, 9, 1)
FAILS = []


def day(n):
    return (D0 + timedelta(days=n)).isoformat()


def row(n, ops, voice="RT (Russia Today) English", lens="trump_office", hour="06"):
    return {"id": "r%d" % n, "created_at": day(n) + "T%s:30:00+00:00" % hour,
            "state_actor_lens": lens, "sample_size": 40,
            "framing_mean": {op: 5 for op in ops},
            "finding_phrasing": "VERIFICATION FINDING (%s): %s\n\nbody" % (lens, voice)}


def series(spec):
    """spec: {day_index: [ops]} -> series for the one pair."""
    s, unparsed = R.daily_series([row(n, ops) for n, ops in spec.items()], 3)
    assert not unparsed
    return list(s.values())[0] if s else []


def alive(a, b):
    return {day(n) for n in range(a, b + 1)}


def opened(ops, n=0, kind="NEW"):
    return {"kind": kind, "reported_ops": sorted(ops), "sent_at": day(n) + "T07:00:00+00:00"}


def check(name, got, want_kind, **want):
    kind = got["kind"] if got else None
    ok = kind == want_kind and all(got.get(k) == v for k, v in want.items())
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r" % (got,)))
    if not ok:
        FAILS.append(name)


# 1. empty ledger, current finding -> NEW once, with the newest row's operations
s = series({0: ["A", "B"], 1: ["A", "B", "C"]})
check("new_on_empty_ledger", R.decide(s, None, alive(0, 1), day(1)), "NEW",
      reported_ops=["A", "B", "C"])

# 2. empty ledger, finding already over (7 live aggregator days without it) -> nothing
s = series({0: ["A"]})
check("stale_never_announced", R.decide(s, None, alive(0, 7), day(7)), None)

# 3. open, same operations -> nothing (the bug CC-120 fixes)
s = series({0: ["A", "B"], 1: ["A", "B"], 2: ["A", "B"]})
check("unchanged_is_quiet", R.decide(s, opened(["A", "B"]), alive(0, 2), day(2)), None)

# 4. an operation chattering at the threshold -> nothing
s = series({0: ["A", "C"], 1: ["A"], 2: ["A", "C"]})
check("chatter_is_quiet", R.decide(s, opened(["A"]), alive(0, 2), day(2)), None)

# 5. an operation held on HOLD_DAYS consecutive rows -> CHANGED, added
s = series({0: ["A", "C"], 1: ["A", "C"], 2: ["A", "C"]})
check("join_after_hold", R.decide(s, opened(["A"]), alive(0, 2), day(2)), "CHANGED",
      added=["C"], dropped=[], reported_ops=["A", "C"])

# 6. a reported operation absent on HOLD_DAYS consecutive rows -> CHANGED, dropped
s = series({0: ["A"], 1: ["A"], 2: ["A"]})
check("drop_after_hold", R.decide(s, opened(["A", "B"]), alive(0, 2), day(2)), "CHANGED",
      dropped=["B"], reported_ops=["A"])
s = series({0: ["A", "B"], 1: ["A"], 2: ["A"]})
check("no_drop_before_hold", R.decide(s, opened(["A", "B"]), alive(0, 2), day(2)), None)

# 7. open finding missing: 6 live days -> nothing; 7 live days -> CLEARED
s = series({0: ["A"]})
check("not_cleared_at_6", R.decide(s, opened(["A"]), alive(0, 6), day(6)), None)
check("cleared_at_7", R.decide(s, opened(["A"]), alive(0, 7), day(7)), "CLEARED",
      last_day=day(0), alive_since=7, reported_ops=["A"])

# 8. open finding missing while the pipeline is dark -> nothing
check("dark_pipeline_not_cleared",
      R.decide(s, opened(["A"]), alive(0, 0) | alive(11, 12), day(12)), None)

# 9. after CLEARED: the old rows stay quiet, a new row is NEW again
cleared = {"kind": "CLEARED", "reported_ops": ["A"], "sent_at": day(7) + "T07:00:00+00:00"}
check("cleared_rows_stay_quiet", R.decide(series({0: ["A"]}), cleared, alive(0, 7), day(7)), None)
check("reappears_as_new", R.decide(series({0: ["A"], 9: ["A", "B"]}), cleared, alive(0, 9), day(9)),
      "NEW", reported_ops=["A", "B"])

# 10. two runs on the same day decide the same thing; after sending, the second is quiet
s = series({0: ["A", "C"], 1: ["A", "C"], 2: ["A", "C"]})
first = R.decide(s, opened(["A"]), alive(0, 2), day(2))
after = {"kind": first["kind"], "reported_ops": first["reported_ops"],
         "sent_at": day(2) + "T07:00:00+00:00"}
check("second_run_same_day_quiet", R.decide(s, after, alive(0, 2), day(2)), None)

# 11. helpers: threshold inclusive, bools and text ignored, newest row of a day kept, no voice
ops = R.persistent_ops({"A": 3, "B": 2, "C": True, "D": "9", "E": 3.0}, 3)
check("persistent_ops", {"kind": "X", "v": sorted(ops)}, "X", v=["A", "E"])
s, unparsed = R.daily_series([row(0, ["A"], hour="01"), row(0, ["A", "B"], hour="09"),
                              {"created_at": day(0), "state_actor_lens": "x",
                               "finding_phrasing": "no header", "framing_mean": {}}], 3)
pair = ("RT (Russia Today) English", "trump_office")
check("newest_row_of_day", {"kind": "X", "v": sorted(s[pair][0][1]), "u": len(unparsed)}, "X",
      v=["A", "B"], u=1)

# 12. CC-121: the Daily Brief's S2-F lines (completion test L2.3)
from datetime import datetime, timezone                 # noqa: E402
NOW = datetime(2026, 9, 24, 15, 36, tzinfo=timezone.utc)
led = [{"voice_name": "RT", "state_actor_lens": "trump_office", "kind": "NEW",
        "reported_ops": ["A", "B"], "sent_at": "2026-09-24T17:00:00+00:00"},
       {"voice_name": "RT", "state_actor_lens": "xi_office", "kind": "NEW",
        "reported_ops": ["A"], "sent_at": "2026-07-01T17:00:00+00:00"},
       {"voice_name": "RT", "state_actor_lens": "xi_office", "kind": "CLEARED",
        "reported_ops": ["A"], "sent_at": "2026-07-10T17:00:00+00:00"}]
opn = R.open_findings(led)
check("open_findings", {"kind": "X", "v": [(f["lens"], f["ops"]) for f in opn]}, "X",
      v=[("trump_office", 2)])
stale = R.s2f_brief_lines({"scored_24h": 0, "newest_scored_at": "2026-09-23T06:27:12.03+00:00",
                           "open": opn}, NOW)
check("stopped_scorer_says_how_long",
      {"kind": "X", "v": ("nothing scored in the last 24 h" in stale[0] and "(33 h ago)" in stale[0]
                          and "accumulating" not in " ".join(stale))}, "X", v=True)
check("open_finding_named", {"kind": "X", "v": stale[1]}, "X",
      v="Verification: 1 open, unchanged since last told \u2014 RT \u00d7 trump_office: "
        "2 ops, told 2026-09-24 (NEW)")
live = R.s2f_brief_lines({"scored_24h": 44, "applicable_24h": 3, "ops_24h": 13, "open": []}, NOW)
check("scored_counts_and_none_open", {"kind": "X", "v": live}, "X",
      v=["Scoring (24 h): 44 articles scored, 3 with operations (13 operations)",
         "Verification: no open findings"])
bad = R.s2f_brief_lines({"scoring_error": "boom <x>", "ledger_error": "gone"}, NOW)
check("errors_are_said_not_hidden", {"kind": "X", "v": bad}, "X",
      v=["Scoring: status unavailable (boom &lt;x&gt;)", "Verification: status unavailable (gone)"])
check("old_list_shape_tolerated", {"kind": "X", "v": len(R.s2f_brief_lines([], NOW))}, "X", v=2)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
