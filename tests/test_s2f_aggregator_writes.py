"""CC-89 -- the Watch and Verification write paths, exercised offline.

CC-87 gave Clarity a write counter; CC-88 gave it a real gate after a dry run
had been used as a paper one. The same two defects were still live in the
other two aggregators: a rejected write was logged and swallowed, and "--dry"
was read as a lens name.

This gate goes RED if either regresses. Note the fake client below: CC-88's
fake had no ilike() and answered every non-insert query with an entity row,
which would have made Watch's and Verification's dedup check fire and skip
every write while the test read it as success.
    python tests/test_s2f_aggregator_writes.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_s2f_watch_aggregator as W
import lens_s2f_verification_aggregator as V
import lens_s2f_clarity_aggregator as CL
import lens_s2f_helpers as H

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
    def __init__(self, outer, table):
        self.outer = outer
        self.table_name = table
        self.row = None
        self.is_dedup = False

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def gte(self, *a, **k):
        return self

    def ilike(self, *a, **k):
        self.is_dedup = True        # only the dedup check uses ilike
        return self

    def limit(self, *a, **k):
        return self

    def insert(self, row):
        self.row = row
        return self

    def execute(self):
        if self.is_dedup:
            return Resp([])                          # nothing written today
        if self.row is None:
            return Resp([{"id": "entity-0001"}])     # the entity lookup
        if self.outer.reject:
            raise Exception('violates check constraint on lens_drift_findings')
        self.outer.written.append(self.row)
        return Resp([{"id": "row-0001"}])


class FakeClient:
    def __init__(self, reject=False):
        self.written = []
        self.reject = reject

    def table(self, name):
        return Query(self, name)


def watch_finding(n):
    return {
        "voice_name": "Test Voice",
        "state_actor_lens": "xi_office",
        "article_count": n,
        "operation_counts": {},
        "alternative_hypotheses": [],
        "finding_confidence": "LOW",
        "evidence_article_ids": [],
        "finding_phrasing": "Watch alert (xi_office): Test Voice - test",
        "rubric_version": "v2-operations",
    }


def verification_finding(n):
    return {
        "voice_name": "Test Voice",
        "state_actor_lens": "xi_office",
        "article_count": n,
        "op_article_counts": {},
        "avg_confidence": 0.5,
        "alternative_hypotheses": [],
        "finding_confidence": "HIGH",
        "evidence_article_ids": [],
        "finding_phrasing": "VERIFICATION FINDING (xi_office): Test Voice",
        "rubric_version": "v2-operations",
    }


print("== Watch counts what it actually wrote ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient()
written, failed = W._write_findings(c, [watch_finding(3), watch_finding(7)])
check("watch written", written, 2)
check("watch failed", failed, 0)
check("rows that reached the wire", len(c.written), 2)

print("== a rejected Watch write is counted, not swallowed ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient(reject=True)
written, failed = W._write_findings(c, [watch_finding(3)])
check("watch written", written, 0)
check("watch failed", failed, 1)
check("rows that survived", len(c.written), 0)

print("== Verification counts what it actually wrote ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient()
written, failed = V._write_findings(c, [verification_finding(15)])
check("verification written", written, 1)
check("verification failed", failed, 0)

print("== a rejected Verification write is counted, not swallowed ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient(reject=True)
written, failed = V._write_findings(c, [verification_finding(15)])
check("verification written", written, 0)
check("verification failed", failed, 1)

print("== --dry is not a lens name ==")
check("bare --dry", H.lens_filter_from_argv(["prog", "--dry"]), None)
check("lens then flag", H.lens_filter_from_argv(["prog", "xi_office", "--dry"]), "xi_office")
check("flag then lens", H.lens_filter_from_argv(["prog", "--dry", "xi_office"]), "xi_office")
check("no arguments", H.lens_filter_from_argv(["prog"]), None)

print("== all three aggregators read the one implementation ==")
for mod in (W, V, CL):
    name = os.path.basename(mod.__file__)
    with open(mod.__file__, "rb") as fh:
        b = fh.read()
    check(f"{name} calls the helper", b.count(b"lens_filter_from_argv") > 0, True)
    check(f"{name} has no inline argv[1] lens", b.count(b"sys.argv[1] if len(sys.argv)"), 0)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
