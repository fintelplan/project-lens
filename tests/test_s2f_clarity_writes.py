"""CC-88 -- the Clarity write path, exercised offline against a fake client.

CC-87 claimed a dry run as its gate. The dry run had parsed "--dry" as a lens
name and queried nothing, so it proved nothing. This is the real gate.
    python tests/test_s2f_clarity_writes.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

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

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def insert(self, row):
        self.row = row
        return self

    def execute(self):
        if self.row is None:
            return Resp([{"id": "entity-0001"}])
        if self.outer.reject_below is not None and self.row["sample_size"] < self.outer.reject_below:
            raise Exception(
                'violates check constraint "lens_drift_findings_sample_size_check"'
            )
        self.outer.written.append(self.row)
        return Resp([{"id": "row-0001"}])


class FakeClient:
    def __init__(self, reject_below=None):
        self.written = []
        self.reject_below = reject_below

    def table(self, name):
        return Query(self, name)


def finding(article_count, conf="MEDIUM"):
    return {
        "voice_name": "Test Voice",
        "state_actor_lens": "xi_office",
        "article_count": article_count,
        "operation_counts": {},
        "coherence_score": 0.5,
        "alternative_hypotheses": [],
        "finding_confidence": conf,
        "evidence_article_ids": [],
        "finding_phrasing": "test",
        "rubric_version": "v2-operations",
    }


print("== the true sample size reaches the row, including zero ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient()
written, failed = CL._write_findings(c, [finding(0), finding(5)])
check("written", written, 2)
check("failed", failed, 0)
check("sample sizes on the wire", [r["sample_size"] for r in c.written], [0, 5])

print("== a total dissolution is no longer padded up to 1 ==")
check("zero stays zero", c.written[0]["sample_size"], 0)

print("== a rejected write is counted, not swallowed ==")
H._STATE_OFFICE_CACHE.clear()
c = FakeClient(reject_below=2)          # the constraint as it was before CC-87
written, failed = CL._write_findings(c, [finding(0), finding(5)])
check("written", written, 1)
check("failed", failed, 1)
check("rows that survived", len(c.written), 1)

print("== --dry is not a lens name (CC-88) ==")
argv = ["prog", "--dry"]
check("no lens filter", next((a for a in argv[1:] if not a.startswith("--")), None), None)
argv = ["prog", "xi_office", "--dry"]
check("lens filter found", next((a for a in argv[1:] if not a.startswith("--")), None), "xi_office")

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
