"""CC-102 -- every provider refusal becomes one line of one shape (item 11, phase 1).

Classification is tested on the providers' OWN bodies, copied from the logs read on
2026-09-22 (Cloudflare 4006, Groq TPD, Gemini 404) and 2026-09-20 (Cerebras 402).
The S2-F path goes through the real openai SDK; S1-RPT through its own call_mistral.
    python tests/test_provider_refusal.py
"""
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)
os.environ["CLOUDFLARE_API_TOKEN"] = "test-token-not-a-secret"
os.environ["CLOUDFLARE_ACCOUNT_ID"] = "test-account"
os.environ["MISTRAL_API_KEY"] = "test-mistral-key-000000"

import httpx
import openai

import lens_provider_refusal as P

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


CF_4006 = "Error code: 429 - {'errors': [{'message': \"AiError: AiError: you have used up your daily free allocation of 10,000 neurons, please upgrade to Cloudflare's Workers Paid plan if you would like to continue usage. (69332557-23bf-4248-93f2-24869e46788c)\", 'code': 4006}], 'success': False, 'result': {}, 'messages': []}"
GROQ_TPD = "Error code: 429 - {'error': {'message': 'Rate limit reached for model `openai/gpt-oss-120b` in organization org_x service tier on_demand on tokens per day (TPD): Limit 200000, Used 199747', 'type': 'tokens', 'code': 'rate_limit_exceeded'}}"
GROQ_TPM = "Error code: 429 - {'error': {'message': 'Rate limit reached for model `openai/gpt-oss-120b` on tokens per minute (TPM): Limit 8000, Used 7900', 'code': 'rate_limit_exceeded'}}"
GEM_404 = "404 NOT_FOUND. {'error': {'code': 404, 'message': 'This model models/gemini-2.0-flash is no longer available. Please update your code.', 'status': 'NOT_FOUND'}}"
CER_402 = "Error code: 402 - {'message': 'Payment required', 'type': 'invalid_request_error', 'code': 'payment_required'}"

print("== classification on the providers' own words ==")
check("Cloudflare 4006", P.classify(P._status_of(None, CF_4006), CF_4006), "daily_quota")
check("Groq TPD", P.classify(P._status_of(None, GROQ_TPD), GROQ_TPD), "daily_quota")
check("Groq TPM is a rate limit", P.classify(P._status_of(None, GROQ_TPM), GROQ_TPM), "rate_limit")
check("Gemini 404 status parsed", P._status_of(None, GEM_404), 404)
check("Gemini 404", P.classify(404, GEM_404), "model_gone")
check("Cerebras 402", P.classify(P._status_of(None, CER_402), CER_402), "payment")
check("403 tier_not_allowed", P.classify(403, "tier_not_allowed"), "auth")
check("503", P.classify(503, "unavailable"), "server")
check("a 429 whose text holds 1640060 is not the daily code", P.classify(429, "ts 18:54:24.1640060Z"), "rate_limit")

print("== one line, redacted, never raises ==")
os.environ["FAKE_PROVIDER_API_KEY"] = "super-secret-value-12345"
LINES.clear()
P.record_refusal("groq", "m", status=429, text="bad key super-secret-value-12345 tokens per day")
check("one line", len(LINES), 1)
check("its shape", LINES[0].startswith("PROVIDER_REFUSAL provider=groq model=m class=daily_quota status=429 reason="), True)
check("the secret is gone", "super-secret-value-12345" in LINES[0], False)
class Evil:
    def __str__(self):
        raise RuntimeError("boom")
check("a hostile exception does not raise", P.record_refusal("x", "y", exc=Evil()), None)

print("== S2-F: the real SDK path writes the line ==")
import lens_framing_rubrics as R
R._RETRY_BACKOFF_SEC = (0, 0)
body = {"errors": [{"message": "AiError: AiError: you have used up your daily free allocation of 10,000 neurons", "code": 4006}], "success": False, "result": {}, "messages": []}
_Real = openai.OpenAI
def _factory(*a, **kw):
    kw["http_client"] = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(429, json=body)))
    return _Real(*a, **kw)
openai.OpenAI = _factory
R._quota_tripped.clear(); R._tpm_guards.clear()
os.environ["S2F_PROVIDER"] = "cloudflare"
LINES.clear()
R.detect_operations_in_article(article_title="t", article_body="word " * 200, article_source="s",
                               voice_name="v", voice_type="author", state_actor_lens="xi_office",
                               stage_filter="early_warning")
openai.OpenAI = _Real
R._quota_tripped.clear(); R._tpm_guards.clear()
check("one S2-F refusal line", len(LINES), 1)
check("it names cloudflare, daily_quota", "provider=cloudflare" in LINES[0] and "class=daily_quota" in LINES[0], True)

print("== S1-RPT: its own call_mistral writes the line once, not once per attempt ==")
import lens_s1_report as S1
class FakeResp:
    status_code = 429
    text = '{"object":"error","message":"Service tier capacity exceeded","code":"1300"}'
S1.requests.post = lambda *a, **k: FakeResp()
S1.time.sleep = lambda s: None
LINES.clear()
check("call_mistral gives up", S1.call_mistral("x"), None)
check("one line for three attempts", len(LINES), 1)
check("it names mistral, rate_limit", "provider=mistral" in LINES[0] and "class=rate_limit" in LINES[0], True)

print("== S2-B and S3-B call it (their Gemini SDK is not installed in CI) ==")
for f in ("lens_s2b_coordination.py", "lens_s3b_truehistory.py"):
    b = open(os.path.join(CODE, f), "rb").read()
    check(f"{f}: record_refusal(\"google\", MODEL, exc=_last_err)",
          b'record_refusal("google", MODEL, exc=_last_err)' in b, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
