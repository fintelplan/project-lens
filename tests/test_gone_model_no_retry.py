"""CC-106 -- S3-B stops retrying a model that is gone.

gemini-2.0-flash answers 404 "no longer available" on every wave; S3-B retried it
three times with 30s and 60s sleeps before falling back (~90s a wave). S2-B already
breaks on a 404 (CC-70). The decision uses CC-102's classifier on the provider's
own words; google-genai is not installed in CI, so the loop is checked by bytes.
    python tests/test_gone_model_no_retry.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)

from lens_provider_refusal import _status_of, classify

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


GEM_404 = ("404 NOT_FOUND. {'error': {'code': 404, 'message': 'This model models/gemini-2.0-flash "
           "is no longer available. Please update your code.', 'status': 'NOT_FOUND'}}")


class ClientErrorLike(Exception):      # google-genai's ClientError carries .code
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


print("== the decision, on Gemini's own words ==")
e = ClientErrorLike(404, GEM_404)
check("404 no longer available -> model_gone", classify(_status_of(e, str(e)), str(e)), "model_gone")
e = ClientErrorLike(429, "429 RESOURCE_EXHAUSTED. quota")
check("429 is retried, not gone", classify(_status_of(e, str(e)), str(e)) == "model_gone", False)
e = ClientErrorLike(503, "503 UNAVAILABLE")
check("503 is retried, not gone", classify(_status_of(e, str(e)), str(e)) == "model_gone", False)

print("== the loop: the check sits after the failure log and before the sleep ==")
b = open(os.path.join(CODE, "lens_s3b_truehistory.py"), "rb").read()
i = b.find(b'log.warning(f"Attempt {attempt} failed: {e}")')
j = b.find(b"if attempt < 3: time.sleep(30 * attempt)", i)
seg = b[i:j]
check("both lines found, in order", i > 0 and j > i, True)
check("the gone check is between them", b'== "model_gone"' in seg, True)
check("and it breaks", b"break" in seg, True)
check("lazy and wrapped", b"from lens_provider_refusal import" in seg and b"except Exception:" in seg, True)

print("== S2-B counts the attempts it really made ==")
s = open(os.path.join(CODE, "lens_s2b_coordination.py"), "rb").read()
check('no fixed "failed after {MAX_RETRIES} attempts"', b'failed after {MAX_RETRIES} attempts")' in s, False)
check("counts {attempt}", b"failed after {attempt} of {MAX_RETRIES} attempts" in s, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
