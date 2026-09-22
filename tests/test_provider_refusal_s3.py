"""CC-109 -- S3-F (Mistral, no fallback) and the Regular Report record their refusals.

S3-F runs its own call_mistral with requests.post replaced; the Regular Report
imports python-docx, not installed in CI, so it is checked by bytes.
    python tests/test_provider_refusal_s3.py
"""
import logging
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)
os.environ.setdefault("MISTRAL_API_KEY", "test-mistral-key-000000")

PASS = 0
FAIL = 0
LINES = []


class Cap(logging.Handler):
    def emit(self, rec):
        LINES.append(rec.getMessage())


logging.getLogger("provider_refusal").addHandler(Cap())


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


print("== S3-F: two refused attempts leave ONE Mistral line ==")
import lens_s3f_countercheck as F
class R429:
    status_code = 429
    text = '{"message":"Rate limit exceeded","code":"1300"}'
F.requests.post = lambda *a, **k: R429()
F.time.sleep = lambda s: None
LINES.clear()
check("S3-F gives up", F.call_mistral("prompt"), None)
check("one line for two attempts", len(LINES), 1)
check("names mistral and the model", f"provider=mistral model={F.MODEL}" in (LINES[0] if LINES else ""), True)
check("class rate_limit, status 429", "class=rate_limit status=429" in (LINES[0] if LINES else ""), True)

def boom(*a, **k):
    raise ConnectionError("network down")
F.requests.post = boom
LINES.clear()
check("a network error also gives up", F.call_mistral("prompt"), None)
check("and is recorded once", len(LINES), 1)

print("== Regular Report: both the primary and the fallback provider are recorded ==")
b = open(os.path.join(CODE, "lens_regular_report.py"), "rb").read()
check("primary", b.count(b"record_refusal(provider, model, exc=e)"), 1)
check("fallback", b.count(b"record_refusal(provider, model, exc=e2)"), 1)
check("no module-scope import", re.search(rb"(?m)^(from|import) lens_provider_refusal", b) is None, True)

from lens_provider_refusal import _PENDING
_PENDING.clear()
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
