"""
test_cc127.py -- gate for CC-127: Telegram HTML never carries an unknown tag or a bare '&';
S3-A stores first_domino without <sign> tags; the MA fallback asks once more on invalid JSON
and a failed analysis tells the operator. No network. Line-ending blind.
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import os
import sys
import types

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
FAILS = []


def read(name):
    return open(os.path.join(CODE, name), "rb").read().decode("utf-8").replace("\r\n", "\n")


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r want=%r" % (got, want)))
    if not ok:
        FAILS.append(name)


import lens_telegram as T          # noqa: E402
P = T._presentable
check("unknown tag escaped", P("confirmed if: <sign>trade shift</sign>"), "confirmed if: &lt;sign>trade shift&lt;/sign>")
check("bare < and & escaped", P("a < b & c"), "a &lt; b &amp; c")
check("our tags and entities kept", P("<b>x</b> <i>y</i> <code>z</code> &amp; &lt;"), "<b>x</b> <i>y</i> <code>z</code> &amp; &lt;")
check("<br> is not <b>", P("<br>"), "&lt;br>")

s3a = read("lens_s3a_patterns.py")
a = s3a.index("# Save to lens_system3_reports")
check("S3-A strips <sign> before saving", 'analysis["first_domino"] = __import__("re").sub(r"</?sign>"' in s3a[:a], True)

import lens_mission_analyst as MA  # noqa: E402
os.environ.setdefault(MA.FB_KEY_ENV, "test-key")
answers, calls = [], []


class _R:
    status_code = 200
    text = ""

    def __init__(self, content):
        self.content = content

    def json(self):
        return {"choices": [{"message": {"content": self.content}}], "usage": {}}


def _post(*a, **k):
    calls.append(1)
    return _R(answers[len(calls) - 1])


MA.requests.post = _post
answers[:] = ['{"threat_level": "HIGH" "x": 1}', '{"threat_level": "HIGH", "key_findings": []}']
out = MA._call_fallback_leg("u", 100)
check("invalid JSON is asked once more", (isinstance(out, dict), len(calls)), (True, 2))
calls.clear()
answers[:] = ['{"a" "b"}', '{"a" "b"}', '{"a": 1}']
check("only once more, then None", (MA._call_fallback_leg("u", 100), len(calls)), (None, 2))
check("a failed analysis tells the operator", "MISSION ANALYST FAILED" in read("lens_mission_analyst.py"), True)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
