"""CC-92 -- the provider's own failure reason must reach the log, redacted.

detect_operations_in_article captured the provider error into
DetectionResult.error and nothing ever printed it. On 2026-09-20 seventy-two
Cloudflare refusals produced no reason at all, and the session spent an hour
guessing at a mechanism the log already held.

Redaction is built from real environment values rather than from a pattern,
because a pattern is a guess about what a secret looks like.
    python tests/test_s2f_error_redaction.py
"""
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))

import lens_framing_rubrics as R

PASS = 0
FAIL = 0

SECRET = "acct_9f3b2c7d1e5a4806bb21"          # 22 chars, stands in for a real one
SHORT = "abc123"                              # under the 12-char floor


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())

    def text(self):
        return "\n".join(self.lines)


class Completions:
    def create(self, **kw):
        raise Exception(
            "Error code: 429 - "
            f"https://api.cloudflare.com/client/v4/accounts/{SECRET}/ai/v1 "
            "{'success': False, 'errors': [{'code': 4006, "
            "'message': 'you have used up your daily free allocation'}]}"
        )


class Chat:
    completions = Completions()


class FakeClient:
    chat = Chat()


print("== _redact removes a real environment value ==")
os.environ["CLOUDFLARE_ACCOUNT_ID"] = SECRET
os.environ["HARMLESS_SETTING"] = "a_long_but_not_secret_value"
check("secret gone", SECRET in R._redact(f"x {SECRET} y"), False)
check("placeholder in", "***" in R._redact(f"x {SECRET} y"), True)
check("non-secret name untouched",
      "a_long_but_not_secret_value" in R._redact("a_long_but_not_secret_value"), True)

print("== short values are left alone (they would mangle the text) ==")
os.environ["TINY_API_KEY"] = SHORT
check("short kept", SHORT in R._redact(f"see {SHORT} here"), True)

print("== the failure reason reaches the log, without the credential ==")
cap = Capture()
R.log.addHandler(cap)
real = R._get_llm_client
R._get_llm_client = lambda: (FakeClient(), "@cf/openai/gpt-oss-120b", "cloudflare")
try:
    res = R.detect_operations_in_article(
        article_title="t", article_body="b" * 900, article_source="s",
        voice_name="v", voice_type="author", state_actor_lens="xi_office",
        stage_filter="early_warning",
    )
finally:
    R._get_llm_client = real
    R.log.removeHandler(cap)

logged = cap.text()
check("status", res.status, "LLM_FAILED")
check("something was logged", len(logged) > 0, True)
check("the provider code is in the log", "4006" in logged, True)
check("the provider is named", "cloudflare" in logged, True)
check("THE SECRET IS NOT IN THE LOG", SECRET in logged, False)
check("redacted marker present", "***" in logged, True)
check("the secret is not in the result either", SECRET in (res.error or ""), False)
check("the result still carries the code", "4006" in (res.error or ""), True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
