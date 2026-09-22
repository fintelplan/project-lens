"""CC-104 -- Groq (S2-A, entity extraction) and Cohere (S3-A) leave a refusal line.

S2-A and S3-A run their own failure paths here with the network replaced.
entity_extract is checked by bytes: the groq SDK is not installed in CI, and the
module is on the canary's air supply -- the record must be lazy and wrapped.
    python tests/test_provider_refusal_sites.py
"""
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)

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


print("== S2-A: a Groq 429 that never clears leaves one Groq line ==")
import lens_s2a_injection as A
A.time.sleep = lambda s: None
class Boom(Exception):
    status_code = 429
class Groqish:
    class chat:
        class completions:
            @staticmethod
            def create(**kw):
                raise Boom("Error code: 429 - rate limit on tokens per minute (TPM)")
LINES.clear()
out = A.call_injection_tracer(Groqish, {"id": "r1", "domain_focus": "L", "summary": "text " * 50}, [])
check("S2-A gives up", out, None)
check("one line", len(LINES), 1)
check("names the registry provider", f"provider={A.PROVIDER}" in LINES[0], True)
check("class rate_limit", "class=rate_limit" in LINES[0], True)
LINES.clear()
A.call_injection_tracer(Groqish, {"id": "r1", "domain_focus": "L", "summary": "text " * 50}, [], model="ministral-8b-2512")
check("the Mistral leg is named mistral", "provider=mistral model=ministral-8b-2512" in (LINES[0] if LINES else ""), True)

print("== S3-A: a Cohere 402 leaves one Cohere line per leg given up ==")
import lens_s3a_patterns as S
from lens_models import get_role
role = get_role("s3a_patterns")
os.environ[role["key_env"]] = "k-not-real-0000000000"
if role.get("fb_key_env"):
    os.environ[role["fb_key_env"]] = "k-not-real-1111111111"
S.time.sleep = lambda s: None
S._post_leg = lambda prov, model, key, mt, prompt: (402, "", None, {}, False, "payment required")
LINES.clear()
res = S._generate("prompt", 100, 500)
check("S3-A gives up", res, (None, None, None))
check(f"a line for {role['provider']}", any(f"provider={role['provider']}" in l and "class=payment" in l for l in LINES), True)
check("one line per leg", len(LINES), 2 if role.get("fb_provider") else 1)

print("== entity_extract: recorded, lazily, and still returns [] ==")
b = open(os.path.join(CODE, "lens_entity_extract.py"), "rb").read()
i = b.find(b'log.warning(f"Groq call failed: {e}")')
seg = b[i:i + 700]
check("the record follows the Groq failure", b"record_refusal(provider, model, exc=e)" in seg, True)
check("imported inside the except (lazy)", b"from lens_provider_refusal import record_refusal" in seg, True)
check("wrapped", b"except Exception:" in seg, True)
check("still returns []", b"return []" in seg, True)
check("nothing new at module scope", b"\nfrom lens_provider_refusal" in b or b"\nimport lens_provider_refusal" in b, False)

P._PENDING.clear()
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
