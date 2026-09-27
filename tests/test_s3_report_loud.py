"""CC-128 -- System 3 says when it did not deliver.

LENS-048 read (bytes): the S3 orchestrator printed a failed strategic report and
exited 0; it ignored send_s3_intelligence's False (Sep 25 morning: a Telegram 400
lost the S3 message, the step stayed green, nothing said so); and its Groq
pre-flight skipped all of System 3 with sys.exit(0). CC-117 gave S2 its voice;
this is S3's copy of the same rule.
    python tests/test_s3_report_loud.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)

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


def blind(b):
    return b.replace(b"\r\n", b"\n")   # LR-277: line-ending blind


import lens_s3_step_report as R

check("announce_failure exists", hasattr(R, "announce_failure"), True)
if not hasattr(R, "announce_failure"):
    print(f"\nRESULT: {PASS} passed, {FAIL} failed (stopped)")
    sys.exit(1)

sent = []
R.send_telegram_text = lambda t: sent.append(t) or True

print("== a report that did not arrive is announced ==")
check("COMPLETE -> no announcement", R.announce_failure({"status": "COMPLETE"}), False)
check("COMPLETE sent nothing", sent, [])
for st in ("AI_FAILED", "NO_DATA", "DOCX_FAILED", "SEND_FAILED", "ERROR", "EXCEPTION"):
    sent.clear()
    check(st + " -> announced", R.announce_failure({"status": st}), True)
    check(st + " message names it", bool(sent) and "FAILED" in sent[0] and st in sent[0], True)
sent.clear()
check("no status at all -> announced", R.announce_failure({}), True)

print("== the orchestrator, end to end with every position stubbed ==")
ran = []
for mod, fn in (("lens_s3a_patterns", "run_s3a"), ("lens_s3b_truehistory", "run_s3b"),
                ("lens_s3c_biasdrift", "run_s3c"), ("lens_s3d_longterm", "run_s3d"),
                ("lens_s3e_selfcheck", "run_s3e"), ("lens_s3f_countercheck", "run_s3f")):
    m = types.ModuleType(mod)
    setattr(m, fn, (lambda name: (lambda **k: ran.append(name) or {"status": "OK"}))(fn))
    sys.modules[mod] = m

state = {}


def _value(key):
    v = state[key]
    if isinstance(v, Exception):
        raise v
    return v


tele = types.ModuleType("lens_telegram")
tele.send_s3_intelligence = lambda run_id=None: _value("msg")
sys.modules["lens_telegram"] = tele
R.run_s3_report = lambda: _value("rpt")

import lens_s3_orchestrator as O
O.check_groq_tpm = lambda *a, **k: state["pf"]


def wave(pf=True, msg=True, rpt=None):
    state.update(pf=pf, msg=msg, rpt={"status": "COMPLETE"} if rpt is None else rpt)
    sent.clear()
    ran.clear()
    code = 0
    try:
        O.main()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    return code, list(sent), len(ran)


c, s, n = wave()
check("all delivered -> green", c, 0)
check("all delivered -> no notice", s, [])
check("all six positions ran", n, 6)

c, s, n = wave(msg=False)
check("message refused -> red", c, 1)
check("message refused -> one notice", len(s), 1)
check("the notice names the S3 message", bool(s) and "S3 Message FAILED" in s[0], True)

c, s, n = wave(msg=RuntimeError("Telegram error 400"))
check("message raised -> red", c, 1)
check("message raised -> one notice", len(s), 1)

c, s, n = wave(rpt={"status": "AI_FAILED"})
check("report failed -> red", c, 1)
check("report failed -> the S3 report notice",
      [x for x in s if "S3 Strategic Report FAILED today: AI_FAILED" in x] != [], True)

c, s, n = wave(rpt=RuntimeError("boom"))
check("report raised -> red", c, 1)
check("report raised -> EXCEPTION said", any("EXCEPTION" in x for x in s), True)

c, s, n = wave(msg=False, rpt={"status": "SEND_FAILED"})
check("both failed -> red", c, 1)
check("both failed -> two notices", len(s), 2)

c, s, n = wave(pf=False)
check("pre-flight skip -> red", c, 1)
check("pre-flight skip -> said", any("System 3 did not run" in x for x in s), True)
check("pre-flight skip -> no position ran", n, 0)

print("== bytes: the old silences are gone; red skips nothing ==")
orc = blind(open(os.path.join(CODE, "lens_s3_orchestrator.py"), "rb").read())
check("no Telegram failure called non-fatal", b"Telegram step report failed (non-fatal)" in orc, False)
check("no clean-skip exit 0 left", b"sys.exit(0)" in orc, False)
wf = blind(open(os.path.join(ROOT, ".github", "workflows", "lens-manage-analyze.yml"), "rb").read())
i = wf.find(b"python code/lens_s3_orchestrator.py")
j = wf.find(b"python code/lens_provider_health.py")
check("premise: the health step follows S3 under !cancelled()",
      i != -1 and j > i and b"if: ${{ !cancelled() }}" in wf[i:j], True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
