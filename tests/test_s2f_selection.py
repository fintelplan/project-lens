"""
test_s2f_selection.py -- gate for CC-122 (S2-F Stage 1: filter first, then one article per
source per round, the source scored longest ago first). Pure. Exits 1 on any failure.
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import os
import sys

sys.path.insert(0, os.environ.get("LENS_CODE_DIR")
                or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))
import lens_s2f_selection as S          # noqa: E402

FAILS = []
N = [0]


def art(src, n_chars=900, minute=0):
    N[0] += 1
    return {"id": "a%d" % N[0], "source_name": src, "content": "x" * n_chars,
            "collected_at": "2026-09-24T05:36:%02d" % minute}


def check(name, ok, got=None):
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r" % (got,)))
    if not ok:
        FAILS.append(name)


# 1. the Sep 22-23 shape: RT is the last batch writer -- it must not take every slot
rows = [art("RT", minute=59) for _ in range(8)] + [art(s, minute=10) for s in "ABCDEFG"]
p, rep = S.select_articles(rows, {}, 8)
srcs = [r["source_name"] for r in p]
check("no_monopoly", srcs.count("RT") == 1 and len(set(srcs)) == 8, srcs)

# 2. the Sep 24 shape: the newest eight are teasers -- the long ones behind them are scored
rows = [art("TASS", n_chars=121, minute=59) for _ in range(8)] + [art(s) for s in "ABC"]
p, rep = S.select_articles(rows, {}, 8)
check("filter_before_cap", len(p) == 3 and rep["short"] == 8, (len(p), rep))

# 3. never scored first, then the oldest last scoring; ties by name
rows = [art("RT"), art("SCMP"), art("Dawn")]
p, _ = S.select_articles(rows, {"RT": "2026-09-24T06:30", "SCMP": "2026-09-20T06:30"}, 2)
check("oldest_turn_first", [r["source_name"] for r in p] == ["Dawn", "SCMP"],
      [r["source_name"] for r in p])

# 3b. a heavy past does not lock a source out: order is by WHEN, not HOW MANY
det = [{"voice_name": "RT", "scored_at": "2026-09-14T06:00"}] * 900 + \
      [{"voice_name": "SCMP", "scored_at": "2026-09-23T06:00"}] * 20
p, _ = S.select_articles([art("RT"), art("SCMP")], S.last_scored_by(det), 1)
check("no_catch_up_starvation", [r["source_name"] for r in p] == ["RT"], [r["source_name"] for r in p])

# 4. fewer sources than slots: a second round, newest first within a source
a1, a2, a3, b1 = art("A", minute=1), art("A", minute=2), art("A", minute=3), art("B")
p, _ = S.select_articles([a1, a2, a3, b1], {}, 3)
check("second_round", [r["id"] for r in p] == [a3["id"], b1["id"], a2["id"]], [r["id"] for r in p])

# 5. already-scored articles are not picked again
x, y = art("A"), art("B")
p, rep = S.select_articles([x, y], {}, 8, exclude_ids={x["id"]})
check("already_scored_excluded", [r["id"] for r in p] == [y["id"]] and rep["already_scored"] == 1, rep)

# 6. nothing long enough -> nothing picked, and the report says why
p, rep = S.select_articles([art("TASS", 100), art("BBC", 90)], {}, 8)
line = S.report_line(rep, 6)
check("empty_is_explained", p == [] and "2 articles, 0 long enough, 2 short" in line, line)

# 7. the cap holds
rows = [art(s) for s in "ABCDEFGHIJKL"]
p, _ = S.select_articles(rows, {}, 8)
check("cap_holds", len(p) == 8, len(p))

# 8. exactly the threshold counts as long enough
p, _ = S.select_articles([art("A", n_chars=400), art("B", n_chars=399)], {}, 8)
check("threshold_inclusive", [r["source_name"] for r in p] == ["A"], [r["source_name"] for r in p])

# 9. rotation across runs: 12 sources, 4 slots, 3 runs -> every source once
last, got = {}, []
for run in range(3):
    p, _ = S.select_articles([art(s) for s in "ABCDEFGHIJKL"], last, 4)
    for r in p:
        last[r["source_name"]] = "2026-09-2%dT06:00" % (run + 1)
    got += [r["source_name"] for r in p]
check("rotation_covers_all", sorted(got) == list("ABCDEFGHIJKL"), got)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
