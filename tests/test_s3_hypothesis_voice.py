"""CC-118 -- the S3 message holds hypotheses, not forecasts.

2026-09-23: the reader was told "a global conflict between major powers becomes
inevitable ... Confidence 85% ... Prediction recorded -- System 4 will verify this".
lens_predictions held 112 rows and 0 had ever been checked: the verifier was never built.
    python tests/test_s3_hypothesis_voice.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(HERE, "..", "code", "lens_telegram.py")
SRC = open(SRC_PATH, "rb").read()

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


print("== the reader is no longer promised a check that has never happened ==")
check("no 'System 4 will verify this'", b"System 4 will verify this" in SRC, False)
check("no confidence percent to the reader", b"Confidence {(p.get('confidence'" in SRC, False)
check("no 'this becomes inevitable:' header", b"this becomes inevitable:</b>" in SRC, False)
check("the recheck line is live", b"_checked_line(sb)" in SRC, True)
check("S3-D header no longer claims 30 days", b"structurally in 30 days" in SRC, False)
check("S3-B summary is unwrapped", b"_plain_summary(s3b.get" in SRC, True)

print("== the helpers, run on their own ==")
text = SRC.decode("utf-8")
m = re.search(r"# --- CC-118 helpers \(LENS-046\) ---\r?\n(.*?)# --- end CC-118 ---", text, re.S)
check("helper block present", bool(m), True)
if not m:
    print()
    print(f"RESULT: {PASS} passed, {FAIL} failed (stopped: the rest needs the helpers)")
    sys.exit(1)

warned = []


class _Log:
    def warning(self, msg):
        warned.append(msg)


ns = {"log": _Log()}
exec(m.group(1), ns)

ps = ns["_plain_summary"]
check("JSON unwrapped", ps('{"plain_english": "The US-Iran conflict is a replay.", "x": 1}'),
      "The US-Iran conflict is a replay.")
check("truncated JSON still unwrapped",
      ps('{"plain_english":"The US-Iran conflict is a **structural replay of the Cold'),
      "The US-Iran conflict is a **structural replay of the Cold")
check("plain prose untouched", ps("  The pattern is old.  "), "The pattern is old.")
check("empty stays empty", ps(None), "")

cn = ns["_certainty_note"]
got = cn("If patterns continue, a global conflict between major powers becomes inevitable.")
check("certain wording is marked", "open hypothesis" in got, True)
check("the record itself is not edited", got.startswith("If patterns continue, a global conflict"), True)
check("certain wording is logged", len(warned) == 1 and "CERTAINTY_LANGUAGE" in warned[0], True)
warned.clear()
check("hedged wording passes unmarked", cn("A new alignment may emerge."), "A new alignment may emerge.")
check("nothing logged for it", warned, [])
check("'Inevitably' caught (case)", "open hypothesis" in cn("Inevitably, prices rise."), True)
check("'will happen' caught", "open hypothesis" in cn("This will happen by December."), True)


class _Q:
    def __init__(self, total, done):
        self.total, self.done, self.checked = total, done, False
        self.not_ = self

    def table(self, name):
        self.checked = False
        return self

    def select(self, *a, **k):
        return self

    def is_(self, *a):
        self.checked = True
        return self

    def limit(self, n):
        return self

    def execute(self):
        class R:
            pass
        r = R()
        r.count = self.done if self.checked else self.total
        return r


cl = ns["_checked_line"]
check("0 of 112 said plainly", cl(_Q(112, 0)), "0 of 112 recorded hypotheses have been checked so far")


class _Broken:
    def table(self, name):
        raise RuntimeError("db down")


warned.clear()
check("a DB failure is said, not hidden", cl(_Broken()), "check count unavailable")
check("and logged", len(warned) == 1, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
