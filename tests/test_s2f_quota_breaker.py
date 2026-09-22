"""CC-97 -- S2-F stops calling a provider that has refused for the day.

Built on the REAL openai SDK over an httpx MockTransport, so the SDK's own
retry behaviour is inside the test. A hand-written fake would not retry and
could not see the three POSTs per refusal the 2026-09-21 evening wave paid
(LENS-044: a test double must enforce production's limits). The refusal body
is Cloudflare's own, copied byte for byte from run 35640858699.

Offline: no network, no Supabase, no real key.
    python tests/test_s2f_quota_breaker.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))
os.environ["CLOUDFLARE_API_TOKEN"] = "test-token-not-a-secret"
os.environ["CLOUDFLARE_ACCOUNT_ID"] = "test-account"
os.environ["CLOUDFLARE_MODEL"] = "@cf/openai/gpt-oss-120b"

import httpx
import openai

import lens_framing_rubrics as R
import lens_s2f_scoring_cron as CRON

R._RETRY_BACKOFF_SEC = (0, 0)
PASS = 0
FAIL = 0

REFUSAL = {"errors": [{"message": "AiError: AiError: you have used up your daily free allocation of 10,000 neurons, please upgrade to Cloudflare's Workers Paid plan if you would like to continue usage. (69332557-23bf-4248-93f2-24869e46788c)", "code": 4006}], "success": False, "result": {}, "messages": []}
RATE_LIMIT = {"errors": [{"message": "rate limited", "code": 3040}], "success": False, "result": {}, "messages": []}
BAD_REQUEST = {"errors": [{"message": "bad input near 'code': 4006", "code": 5006}], "success": False, "result": {}, "messages": []}
OK_JSON = json.dumps({"operations_detected": [], "operations_not_present": [],
                      "confidence": 0.5, "not_applicable": True,
                      "food_for_thought": "sent at 18:54:24.1640060Z, code 4006 in prose"})


def completion():
    return {"id": "t", "object": "chat.completion", "created": 0, "model": "m",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": OK_JSON}}]}


class Server:
    def __init__(self, script):
        self.script = list(script)
        self.requests = 0

    def handle(self, request):
        self.requests += 1
        status, body = self.script.pop(0) if self.script else (500, {"errors": []})
        return httpx.Response(status, json=body)


_RealOpenAI = openai.OpenAI
SERVER = None


def _factory(*a, **kw):
    kw["http_client"] = httpx.Client(transport=httpx.MockTransport(SERVER.handle))
    return _RealOpenAI(*a, **kw)


openai.OpenAI = _factory


def fresh(script):
    global SERVER
    SERVER = Server(script)
    R._quota_tripped.clear()
    R._tpm_guards.clear()


def detect():
    os.environ["S2F_PROVIDER"] = "cloudflare"
    return R.detect_operations_in_article(
        article_title="t", article_body="word " * 200, article_source="s",
        voice_name="v", voice_type="author", state_actor_lens="xi_office",
        stage_filter="early_warning")


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


print("== the day's refusal is sent once, then the breaker holds ==")
fresh([(429, REFUSAL)] * 9)
r1 = detect()
check("first call fails", r1.status, "LLM_FAILED")
check("one POST, not three", SERVER.requests, 1)
check("provider's own words kept", "10,000 neurons" in (r1.error or ""), True)
check("breaker tripped", "cloudflare" in R._quota_tripped, True)
r2 = detect()
r3 = detect()
check("second call skipped", r2.status, "SKIP_QUOTA")
check("third call skipped", r3.status, "SKIP_QUOTA")
check("no POST after the trip", SERVER.requests, 1)
check("skip names the provider", r2.provider, "cloudflare")

print("== a run whose only call was refused is still RED (CC-85) ==")
check("scored=0 failed=1", CRON.scoring_exit_code(0, 1), 1)

print("== a rate limit is not the day's refusal: retried, not tripped ==")
fresh([(429, RATE_LIMIT), (200, completion())])
r = detect()
check("status", r.status, "OK")
check("two POSTs", SERVER.requests, 2)
check("not tripped", R._quota_tripped, {})

print("== '4006' in text is not the code: a substring cannot trip it ==")
fresh([(200, completion())])
r = detect()
check("200 carrying '4006' in prose", r.status, "OK")
check("not tripped by a 200", R._quota_tripped, {})
fresh([(400, BAD_REQUEST)])
r = detect()
check("400 whose message quotes 4006", r.status, "LLM_FAILED")
check("400 is not retried", SERVER.requests, 1)
check("not tripped by a 400", R._quota_tripped, {})

print("== server errors keep the SDK's retry count ==")
fresh([(503, {"errors": []})] * 3)
r = detect()
check("status", r.status, "LLM_FAILED")
check("three POSTs", SERVER.requests, 3)
check("not tripped", R._quota_tripped, {})

print("== the breaker is per provider ==")
fresh([(429, REFUSAL)])
detect()
check("cloudflare tripped", "cloudflare" in R._quota_tripped, True)
check("mistral untouched", "mistral" in R._quota_tripped, False)

R._quota_tripped.clear()
R._tpm_guards.clear()
openai.OpenAI = _RealOpenAI
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
