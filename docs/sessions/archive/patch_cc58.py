#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
patch_cc58.py -- LENS-038, order item 1 (1.1 AND 1.2, shipped together).

1.1  lens_s2_orchestrator._run's failure test is a DENYLIST. Make it an
     ALLOWLIST. FOUR labels, THREE consequences, default-deny:
        OK        COMPLETE, OK
        SKIP      QUOTA_SKIP, SKIP            -- deliberate, not a failure
        DEGRADED  S1_PARTIAL_ARRIVAL,         -- MA DID save a report
                  S1_ZERO_ARRIVAL                (lens_mission_analyst.py:849)
        FAIL      everything else, incl. UNKNOWN and EXCEPTION
     Only FAIL enters `failed`. DEGRADED gets its own line and blocks the
     "All positions complete." string, so a zero-S1 wave can never claim
     completion -- but it does not alarm, because S1_PARTIAL_ARRIVAL is one
     long lens report away from firing every wave (order item 7, 96.0%).

1.2  Delivery lives in the else-branch of "if failed:", so ANY failure
     silently suppressed both the Telegram intelligence report and the S2
     step report, at exit 0. Split the axes: deliver unconditionally, then
     report. lens_s3_orchestrator.py:153 already does it this way. The two
     calls keep S2's SEPARATE try blocks -- do not copy S3's nested version,
     where a Telegram failure takes the step report with it.

Also renames the seven `ok_x` locals to `st_x`: they now hold a string, and
a variable called `ok_` holding "FAIL" is the one-name-two-meanings disease
that killed S2-D. S4-E does NOT go through _run -- it is judged inline at
:179/:183 -- so it is the easiest consumer to miss; E8/E9 cover it.

The S2-A sys.exit(1) is PRESERVED and only MOVED after delivery. Its trigger
set is provably unchanged: for every status S2-A can return, the old denylist
and this allowlist put the same set into `failed`. That matters because
lens-manage-analyze.yml runs under bash -e with no continue-on-error, so a
non-zero exit here already cancels the whole System 3 step.

Discipline: LR-078 ship-to-file, binary mode, one purpose. LR-147 delta
counts asserted BEFORE write_bytes. LR-149 written fresh, not stream-edited.
LR-151 no chr() arithmetic; raw strings carry the literal backslash-n that
lives inside the target's f-strings.

Idempotent by construction: every anchor must match EXACTLY ONCE.

Run from the repo root:  python patch_cc58.py
"""
import sys
import py_compile
from pathlib import Path

TARGET = Path("code/lens_s2_orchestrator.py")


def C(s):
    """LF -> CRLF. The target is pure CRLF and must stay that way.
    Inside a raw string a two-character \\n sequence is NOT a newline, so
    this converts only real line breaks -- f-string escapes survive."""
    return s.replace("\n", "\r\n")


raw = TARGET.read_bytes()
if raw.count(b"\r\n") != raw.count(b"\n"):
    sys.exit("ABORT: target is not pure CRLF -- re-read before patching")
CRLF_BEFORE = raw.count(b"\r\n")
src = raw.decode("utf-8")

EDITS = []

# E1 -- the allowlist constants, at module scope beside RUN_ID.
EDITS.append((
    C(r'''RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
'''),
    C(r'''RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")

# -- Outcome classification (CC-58, LENS-038 order item 1.1) ------------------
# ALLOWLIST, not denylist. A status this file has never heard of is a FAILURE,
# never a success. The three-name denylist this replaces printed a green tick
# for ANALYSIS_FAILED, QUOTA_SKIP, NO_S1_REPORTS, NO_S1_DATA, NO_ARTICLES,
# NO_RAW_ARTICLES, INSUFFICIENT_ARTICLES, SKIP, S1_PARTIAL_ARRIVAL and
# S1_ZERO_ARRIVAL. Confirmed live on run 32209212733: S2-B returned
# ANALYSIS_FAILED and the wave still ended "All positions complete."
#
# These three tuples are the WHOLE list of statuses that are not failures.
# Adding a new status to any position REQUIRES adding it here; that
# obligation is the point of an allowlist.
#
# DEGRADED is not SKIP and not FAIL. lens_mission_analyst.py:849 returns the
# arrival statuses only AFTER save_macro_report() succeeded -- the report
# exists, it was built on thin S1 input. It must not alarm and it must not
# be allowed to print "All positions complete."
STATUS_OK = ("COMPLETE", "OK")
STATUS_SKIP = ("QUOTA_SKIP", "SKIP")
STATUS_DEGRADED = ("S1_PARTIAL_ARRIVAL", "S1_ZERO_ARRIVAL")
GLYPH = {"OK": "✅", "SKIP": "⏭", "DEGRADED": "⚠️", "FAIL": "❌"}
''')))

# E2 -- _run's docstring: the return is no longer a bool.
EDITS.append((
    C(r'''    """Run a single position. Returns (ok, summary_dict)."""
'''),
    C(r'''    """Run a single position. Returns (outcome, summary_dict).

    outcome is one of "OK" / "SKIP" / "DEGRADED" / "FAIL" -- NOT a bool.
    Four outcomes exist in this repo and there were only ever two words for
    them, so a deliberate skip had to borrow the word for success (CC-58).
    """
''')))

# E3 -- the denylist itself.
EDITS.append((
    C(r'''            ok = status not in ("SAVE_FAILED", "ERROR", "NO_REPORTS")
'''),
    C(r'''            outcome = ("OK" if status in STATUS_OK
                       else "SKIP" if status in STATUS_SKIP
                       else "DEGRADED" if status in STATUS_DEGRADED
                       else "FAIL")
''')))

# E4/E5 -- the remaining uses of the old boolean inside the dict branch.
EDITS.append((C('            if ok:\n'), C('            if outcome == "OK":\n')))
EDITS.append((C('            return ok, result\n'),
              C('            return outcome, result\n')))

# E6 -- the legacy bool-returning path.
EDITS.append((
    C(r'''        # Legacy: bool return
        return bool(result), {}
'''),
    C(r'''        # Legacy: bool return
        return ("OK" if result else "FAIL"), {}
''')))

# E7 -- the exception path.
EDITS.append((
    C('        return False, {"status": "EXCEPTION", "error": str(e)}\n'),
    C('        return "FAIL", {"status": "EXCEPTION", "error": str(e)}\n')))

# E8/E9 -- S4-E assigns results[] inline, bypassing _run entirely.
EDITS.append((C('        results["S4-E"] = upgrade_result.get("status") == "OK"\n'),
              C('        results["S4-E"] = "OK" if upgrade_result.get("status") == "OK" else "FAIL"\n')))
EDITS.append((C('        results["S4-E"] = False\n'),
              C('        results["S4-E"] = "FAIL"\n')))

# E10..E23 -- ok_x -> st_x. The locals now hold a string, not a bool.
for old_name, new_name, call in [
    ("ok_a", "st_a", '_run("S2-A", run_s2a, run_id=RUN_ID)'),
    ("ok_b", "st_b", '_run("S2-B", run_s2b, run_id=RUN_ID)'),
    ("ok_c", "st_c", '_run("S2-C", run_s2c, run_id=RUN_ID)'),
    ("ok_d", "st_d", '_run("S2-D", run_s2d, run_id=RUN_ID)'),
    ("ok_gap", "st_gap", '_run("S2-GAP Gap Analysis", run_s2_gap, run_id=RUN_ID)'),
    ("ok_e", "st_e", '_run("S2-E", run_s2e, run_id=RUN_ID)'),
    ("ok_ma", "st_ma", '_run("Mission Analyst", run_mission_analyst, run_id=RUN_ID)'),
]:
    EDITS.append((C("    %s, _ = %s\n" % (old_name, call)),
                  C("    %s, _ = %s\n" % (new_name, call))))
for key, old_name, new_name in [
    ("S2-A", "ok_a", "st_a"), ("S2-B", "ok_b", "st_b"), ("S2-C", "ok_c", "st_c"),
    ("S2-D", "ok_d", "st_d"), ("S2-GAP", "ok_gap", "st_gap"),
    ("S2-E", "ok_e", "st_e"), ("Mission Analyst", "ok_ma", "st_ma"),
]:
    EDITS.append((C('    results["%s"] = %s\n' % (key, old_name)),
                  C('    results["%s"] = %s\n' % (key, new_name))))

# E24 -- the summary and the delivery split (order item 1.2).
EDITS.append((
    C(r'''    for pos, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {pos}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n[S2-ORC] {len(failed)} position(s) did not complete: {failed}")
        # S2-A is the critical position — without injection trace, MA has no corrections
        if "S2-A" in failed:
            print("[S2-ORC] WARNING: S2-A failed — Mission Analyst ran without injection corrections.")
            sys.exit(1)
    else:
        # Telegram intelligence report
        try:
            from lens_telegram import send_s2_intelligence
            send_s2_intelligence()
        except Exception as _te:
            print(f"[S2-ORC] Telegram step failed (non-fatal): {_te}")
        try:
            from lens_s2_step_report import run_s2_report
            run_s2_report(run_id=RUN_ID)
        except Exception as _s2r:
            log.warning(f"S2 step report failed (non-fatal): {_s2r}")
        print("\n[S2-ORC] All positions complete.")
'''),
    C(r'''    for pos, outcome in results.items():
        print(f"  {GLYPH.get(outcome, '?')} {pos}")

    failed = [k for k, v in results.items() if v == "FAIL"]
    skipped = [k for k, v in results.items() if v == "SKIP"]
    degraded = [k for k, v in results.items() if v == "DEGRADED"]

    # -- DELIVERY IS UNCONDITIONAL (CC-58, order item 1.2) -------------------
    # These two calls used to sit in the else-branch of "if failed:", so a
    # single failed position suppressed BOTH reports and the run still exited
    # 0. The wave that went wrong is the wave the operator most needs the
    # report from; a message that never arrives is the silent failure this
    # target exists to remove. lens_s3_orchestrator.py:153 already delivers
    # first and reports afterwards -- this makes S2 agree with its sibling.
    # The two calls stay in SEPARATE try blocks on purpose: S3 nests them and
    # a Telegram failure there silently takes the step report with it.
    try:
        from lens_telegram import send_s2_intelligence
        send_s2_intelligence()
    except Exception as _te:
        print(f"[S2-ORC] Telegram step failed (non-fatal): {_te}")
    try:
        from lens_s2_step_report import run_s2_report
        run_s2_report(run_id=RUN_ID)
    except Exception as _s2r:
        log.warning(f"S2 step report failed (non-fatal): {_s2r}")

    if skipped:
        print(f"\n[S2-ORC] {len(skipped)} position(s) deliberately skipped: {skipped}")
    if degraded:
        print(f"\n[S2-ORC] {len(degraded)} position(s) produced output on DEGRADED input: {degraded}")
    if failed:
        print(f"\n[S2-ORC] {len(failed)} position(s) did not complete: {failed}")
        # S2-A is the critical position -- without injection trace, MA has no corrections
        if "S2-A" in failed:
            print("[S2-ORC] WARNING: S2-A failed -- Mission Analyst ran without injection corrections.")
            print("=" * 60 + "\n")
            sys.exit(1)
    elif not degraded:
        print("\n[S2-ORC] All positions complete.")
''')))

# ------------------------------------------------------- anchor uniqueness
print("ANCHORS (%d edits)" % len(EDITS))
bad = 0
for i, (old, new) in enumerate(EDITS, 1):
    n = src.count(old)
    head = old.strip().splitlines()[0][:60]
    if n != 1:
        bad += 1
        print("  E%-2d count=%d  <<< %s" % (i, n, head))
    else:
        print("  E%-2d count=%d  %s" % (i, n, head))
if bad:
    sys.exit("ABORT: %d anchor(s) did not match exactly once. Nothing written." % bad)

# ------------------------------------- LR-147 delta arithmetic, before write
SYMBOLS = ["outcome", "STATUS_OK", "STATUS_SKIP", "STATUS_DEGRADED", "GLYPH",
           "skipped", "degraded", "failed", "sys.exit(1)",
           "send_s2_intelligence", "run_s2_report", '"FAIL"', "if ok:",
           'not in ("SAVE_FAILED"', "ok_", "st_"]

out = src
for old, new in EDITS:
    out = out.replace(old, new, 1)

print("\nSYMBOL TABLE (pre / delta / pred / final)")
rows_bad = 0
for sym in SYMBOLS:
    pre = src.count(sym)
    delta = sum(new.count(sym) - old.count(sym) for old, new in EDITS)
    pred = pre + delta
    final = out.count(sym)
    flag = "OK" if pred == final else "MISMATCH"
    zero = "  (zero in old -- green proves nothing, LR-147)" if pre == 0 else ""
    print("  %-24s %3d %+4d %4d %4d  %s%s" % (sym, pre, delta, pred, final, flag, zero))
    if pred != final:
        rows_bad += 1
if rows_bad:
    sys.exit("ABORT: %d symbol row(s) mismatched. Nothing written." % rows_bad)

blob = out.encode("utf-8")
if blob.count(b"\r\n") != blob.count(b"\n"):
    sys.exit("ABORT: result is not pure CRLF. Nothing written.")

TARGET.write_bytes(blob)

after = TARGET.read_bytes()
print("\nWRITTEN")
print("  bytes %d -> %d" % (len(raw), len(after)))
print("  crlf  %d -> %d   bare lf %d" %
      (CRLF_BEFORE, after.count(b"\r\n"), after.count(b"\n") - after.count(b"\r\n")))
py_compile.compile(str(TARGET), doraise=True)
print("  py_compile OK")
