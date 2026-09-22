"""CC-105 -- PROVIDER HEALTH: the rule, the message, and that it never fails the wave.
No network: fetch and send are replaced.
    python tests/test_provider_health.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "code"))

import lens_provider_health as H

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


def row(p, m, c, run, n=1, t="2026-09-22T06:44:00+00:00"):
    return {"provider": p, "model": m, "class": c, "n": n, "run_id": run, "created_at": t}


print("== the death rule ==")
rows = [row("google", "gemini-2.0-flash", "model_gone", "r1"),
        row("google", "gemini-2.0-flash", "model_gone", "r2"),
        row("cerebras", "gpt-oss-120b", "payment", "r2"),
        row("groq", "openai/gpt-oss-120b", "daily_quota", "c1", n=58)]
lines, quiet = H.summarize(rows)
check("two runs of model_gone -> DOWN first", lines[0].startswith("DOWN  google/gemini-2.0-flash  model_gone x2 in 2 runs"), True)
check("one run of payment -> EXCEPTION, watching", lines[1].startswith("EXCEPTION (1 run, watching)  cerebras/gpt-oss-120b  payment x1 in 1 run "), True)
check("daily_quota -> WARN, counted by n", lines[2].startswith("WARN  groq/openai/gpt-oss-120b  daily_quota x58 in 1 run "), True)
check("quiet providers are named", quiet, ["cloudflare", "cohere", "mistral"])

print("== the message ==")
text = H.compose(lines, quiet)
check("says it is not the canary", "not the canary" in text.splitlines()[0], True)
check("carries the known limit", "Successes are not stored." in text, True)
check("empty window says so", "No provider refusal stored." in H.compose([], list(H.WATCHED)), True)

print("== it never fails the wave ==")
sent = []
H.send = lambda text: sent.append(text) or True
def boom(*a, **k):
    raise RuntimeError("HTTP 503")
H.fetch = boom
os.environ["SUPABASE_URL"] = "https://example.test"
os.environ["SUPABASE_SERVICE_KEY"] = "service-key-not-real-000"
check("a failed read exits 0", H.main(), 0)
check("and tells the operator", "provider health unavailable" in sent[-1], True)
H.fetch = lambda url, key, since: rows
check("a normal read exits 0", H.main(), 0)
check("and sends the DOWN line", "DOWN  google/gemini-2.0-flash" in sent[-1], True)
os.environ.pop("SUPABASE_URL")
check("no Supabase env exits 0", H.main(), 0)

print("== it calls no provider (ruling a) ==")
src = open(H.__file__, "rb").read()
for host in (b"api.groq.com", b"api.mistral.ai", b"api.cohere", b"generativelanguage", b"cloudflare.com/client"):
    check(f"no {host.decode()}", host in src, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
