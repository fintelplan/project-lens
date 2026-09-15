#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
patch_cc58b.py -- correction to CC-58 BEFORE the commit.

Run 32261985640 printed three status strings my sweep never saw, because I
grepped code/lens_s2*.py + lens_mission_analyst.py + lens_s4*.py and never
lens_s3*.py:

    [S3-ORC] S3-C ... status=SKIPPED_CADENCE
    [S3-ORC] S3-D ... status=SKIPPED
    [S3-ORC] S3-E ... status=SKIPPED_CI

Nothing breaks -- lens_s3_orchestrator.py has its own _run and its own
denylist, and every status S2's positions actually emitted on that wave
(COMPLETE, ANALYSIS_FAILED, OK) is classified correctly. What is wrong is
the COMMENT: it calls the three tuples "the WHOLE list of statuses that are
not failures", which is true only for the positions this orchestrator calls.
A future session extending the allowlist to S3 (order item 1.3) would trust
that sentence and silently turn three deliberate skips into failures.

One purpose: make the comment say what it can prove.
"""
import sys
import py_compile
from pathlib import Path

TARGET = Path("code/lens_s2_orchestrator.py")

raw = TARGET.read_bytes()
if raw.count(b"\r\n") != raw.count(b"\n"):
    sys.exit("ABORT: target is not pure CRLF")
src = raw.decode("utf-8")

OLD = ("# These three tuples are the WHOLE list of statuses that are not failures.\r\n"
       "# Adding a new status to any position REQUIRES adding it here; that\r\n"
       "# obligation is the point of an allowlist.\r\n")

NEW = ("# These three tuples are the whole list of non-failure statuses FOR THE\r\n"
       "# POSITIONS THIS ORCHESTRATOR CALLS -- S2-A/B/C/D/GAP/E, Mission Analyst\r\n"
       "# and S4-E. Adding a new status to any of them REQUIRES adding it here;\r\n"
       "# that obligation is the point of an allowlist.\r\n"
       "#\r\n"
       "# SYSTEM 3 HAS A DIFFERENT VOCABULARY and its own _run: SKIPPED,\r\n"
       "# SKIPPED_CADENCE and SKIPPED_CI are live there (run 32261985640) and\r\n"
       "# appear nowhere in System 2. Do NOT copy these tuples into\r\n"
       "# lens_s3_orchestrator.py without adding them first -- order item 1.3.\r\n")

n = src.count(OLD)
print("anchor count = %d" % n)
if n != 1:
    sys.exit("ABORT: anchor did not match exactly once. Nothing written.")

out = src.replace(OLD, NEW, 1)

for sym, exp in [("WHOLE list", 0), ("SKIPPED_CADENCE", 1), ("SKIPPED_CI", 1),
                 ("STATUS_OK", 2), ("STATUS_SKIP", 2), ("STATUS_DEGRADED", 2),
                 ("order item 1.3", 1)]:
    got = out.count(sym)
    print("  %-18s expect %d  got %d  %s" % (sym, exp, got, "OK" if got == exp else "MISMATCH"))
    if got != exp:
        sys.exit("ABORT: symbol mismatch. Nothing written.")

blob = out.encode("utf-8")
if blob.count(b"\r\n") != blob.count(b"\n"):
    sys.exit("ABORT: result is not pure CRLF. Nothing written.")

TARGET.write_bytes(blob)
after = TARGET.read_bytes()
print("bytes %d -> %d   crlf %d   bare lf %d" %
      (len(raw), len(after), after.count(b"\r\n"),
       after.count(b"\n") - after.count(b"\r\n")))
py_compile.compile(str(TARGET), doraise=True)
print("py_compile OK")
