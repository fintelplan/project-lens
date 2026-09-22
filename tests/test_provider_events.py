"""CC-103 -- provider refusals are buffered and stored in ONE request at exit,
only inside Actions, and storing never raises (gas-mask arm 2).
No network: requests.post is replaced for the whole test.
    python tests/test_provider_events.py
"""
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "code"))

import requests
import lens_provider_refusal as P

PASS = 0
FAIL = 0
LINES = []
CALLS = []


class Cap(logging.Handler):
    def emit(self, rec):
        LINES.append(rec.getMessage())


logging.getLogger("provider_refusal").addHandler(Cap())
logging.getLogger("provider_refusal").setLevel(logging.INFO)


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


class Resp:
    def __init__(self, code, text=""):
        self.status_code = code
        self.text = text


def fake_post(status):
    def _p(url, json=None, timeout=None, headers=None):
        CALLS.append({"url": url, "rows": json, "timeout": timeout, "headers": headers})
        if status == "raise":
            raise requests.ConnectionError("network down")
        return Resp(status, "boom")
    return _p


real_post = requests.post
saved = {k: os.environ.get(k) for k in ("GITHUB_ACTIONS", "GITHUB_RUN_ID", "SUPABASE_URL",
                                        "SUPABASE_SERVICE_KEY", "SUPABASE_KEY")}


def env(**kw):
    for k in saved:
        os.environ.pop(k, None)
    for k, v in kw.items():
        os.environ[k] = v


def fresh():
    P._PENDING.clear()
    CALLS.clear()
    LINES.clear()


print("== severity follows ITIL: exception = the provider may be gone ==")
for c, s in (("payment", "exception"), ("model_gone", "exception"), ("auth", "exception"),
             ("daily_quota", "warning"), ("rate_limit", "warning"), ("server", "warning"),
             ("other", "warning")):
    check(c, P.severity(c), s)

print("== buffered, not written per call ==")
fresh()
env(GITHUB_ACTIONS="true", GITHUB_RUN_ID="4242",
    SUPABASE_URL="https://example.test", SUPABASE_SERVICE_KEY="service-key-not-real-000")
requests.post = fake_post(201)
P.record_refusal("google", "gemini-2.0-flash", status=404, text="no longer available")
P.record_refusal("cloudflare", "@cf/x", status=429, text="daily free allocation")
check("no request while recording", len(CALLS), 0)
check("two rows pending", len(P._PENDING), 2)
row = P._PENDING[0]
check("row class", row["class"], "model_gone")
check("row severity", row["severity"], "exception")
check("row run_id", row["run_id"], "4242")
check("row source is this script", row["source"], "test_provider_events.py")
check("the CC-102 line shape is unchanged", "class=model_gone status=404 reason=" in LINES[0], True)
check("flush registered at exit", P._ATEXIT["registered"], True)

print("== one request stores them all ==")
check("flush returns 2", P.flush(), 2)
check("exactly one request", len(CALLS), 1)
check("to the events table", CALLS[0]["url"], "https://example.test/rest/v1/lens_provider_events")
check("both rows in it", len(CALLS[0]["rows"]), 2)
check("short timeout", CALLS[0]["timeout"], 10)
check("nothing left pending", len(P._PENDING), 0)
check("a second flush does nothing", (P.flush(), len(CALLS)), (0, 1))

print("== outside Actions nothing is written, and it says so ==")
fresh()
env(SUPABASE_URL="https://example.test", SUPABASE_SERVICE_KEY="service-key-not-real-000")
P.record_refusal("groq", "m", status=429, text="tokens per day")
check("local flush stores 0", P.flush(), 0)
check("no request", len(CALLS), 0)
check("and says why", any("not running in Actions" in l for l in LINES), True)

print("== a failed store is loud and never raises ==")
for label, st in (("HTTP 500", 500), ("network down", "raise")):
    fresh()
    env(GITHUB_ACTIONS="true", SUPABASE_URL="https://example.test",
        SUPABASE_SERVICE_KEY="service-key-not-real-000")
    requests.post = fake_post(st)
    P.record_refusal("groq", "m", status=429, text="x")
    check(f"{label}: flush returns 0", P.flush(), 0)
    check(f"{label}: says NOT stored", any("PROVIDER_EVENTS NOT stored" in l for l in LINES), True)
    check(f"{label}: the service key is not in the log",
          any("service-key-not-real-000" in l for l in LINES), False)

print("== no Supabase env: loud, no request ==")
fresh()
env(GITHUB_ACTIONS="true")
P.record_refusal("groq", "m", status=429, text="x")
check("stores 0", P.flush(), 0)
check("no request", len(CALLS), 0)

requests.post = real_post
P._PENDING.clear()
env(**{k: v for k, v in saved.items() if v is not None})
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
