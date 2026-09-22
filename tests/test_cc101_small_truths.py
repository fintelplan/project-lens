"""CC-101 -- five small truths from the LENS-045 OTHERS list.

(a) S2-B/S3-B comments said the fallback posts mistral-small-latest; it has not since CC-70.
(b) their log line said "falling back to Mistral-small" while the wire said ministral-8b.
(c) a PARSE_FAILED S2-F result did not say which provider produced the unparseable text.
(d) already_scored() turned any error into "not scored", silently.
(e) the Mistral client's default was an alias in the refused class (D-015).
    python tests/test_cc101_small_truths.py
"""
import json
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)
os.environ["CLOUDFLARE_API_TOKEN"] = "test-token-not-a-secret"
os.environ["CLOUDFLARE_ACCOUNT_ID"] = "test-account"

import httpx
import openai

import lens_framing_rubrics as R
import lens_s2f_scoring_cron as CRON

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


def read(name):
    return open(os.path.join(CODE, name), "rb").read()


print("== (a) + (b) S2-B and S3-B say what runs ==")
for f in ("lens_s2b_coordination.py", "lens_s3b_truehistory.py"):
    b = read(f)
    check(f"{f}: no 'falling back to Mistral-small'", b"falling back to Mistral-small" in b, False)
    check(f"{f}: no stale 'the wire says' comment", b"the wire says" in b, False)
    check(f"{f}: fallback is ministral-8b-2512", b'MISTRAL_FALLBACK_MODEL = "ministral-8b-2512"' in b, True)

print("== (c) PARSE_FAILED names the provider ==")
_Real = openai.OpenAI
def _factory(*a, **kw):
    body = {"id": "t", "object": "chat.completion", "created": 0, "model": "m",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "this is not json"}}]}
    kw["http_client"] = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json=body)))
    return _Real(*a, **kw)
openai.OpenAI = _factory
R._quota_tripped.clear(); R._tpm_guards.clear()
os.environ["S2F_PROVIDER"] = "cloudflare"
r = R.detect_operations_in_article(article_title="t", article_body="word " * 200, article_source="s",
                                   voice_name="v", voice_type="author", state_actor_lens="xi_office",
                                   stage_filter="early_warning")
openai.OpenAI = _Real
R._tpm_guards.clear()
check("status", r.status, "PARSE_FAILED")
check("provider named", r.provider, "cloudflare")
check("model named", bool(r.model), True)

print("== (d) already_scored is loud when it fails ==")
class Boom:
    def table(self, name):
        raise RuntimeError("simulated outage")
seen = []
class H(logging.Handler):
    def emit(self, rec):
        seen.append(rec.getMessage())
CRON.log.addHandler(H())
check("still returns False (behaviour kept)", CRON.already_scored(Boom(), "id", "xi_office", "early_warning"), False)
check("and says so", any("already_scored check failed" in m for m in seen), True)

print("== (e) the Mistral default is not the refused alias ==")
rb_src = read("lens_framing_rubrics.py")
check("the call site no longer defaults to the alias", b'"MISTRAL_MODEL", "mistral-medium-latest"' in rb_src, False)
check("it defaults to the dated id", b'"MISTRAL_MODEL", "ministral-8b-2512"' in rb_src, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
