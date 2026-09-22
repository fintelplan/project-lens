"""CC-111 -- the Compendium's zero-failure guard keeps shipping, and says so.

synthesize_intro() returns a canned sentence when Groq fails (CC-23 kept that on
purpose). The refusal now reaches provider health, and the log names the canned
intro for what it is. Runs the real function with the Groq client replaced.
    python tests/test_compendium_refusal.py
"""
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "code"))

import lens_compendium as C
from lens_provider_refusal import _PENDING

PASS = 0
FAIL = 0
LINES = []


class Cap(logging.Handler):
    def emit(self, rec):
        LINES.append(rec.getMessage())


logging.getLogger().addHandler(Cap())   # root only: provider_refusal propagates here


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


class TPD(Exception):
    status_code = 429


class Client:
    class chat:
        class completions:
            @staticmethod
            def create(**kw):
                raise TPD("Error code: 429 - Rate limit reached on tokens per day (TPD): Limit 200000")


C.get_groq = lambda: Client
LINES.clear()
out = C.synthesize_intro("data")
check("still ships: the canned intro comes back", out.startswith("Project Lens Intelligence Compendium"), True)
refusals = [l for l in LINES if l.startswith("PROVIDER_REFUSAL")]
check("one refusal line", len(refusals), 1)
check("names the registry provider, class daily_quota",
      f"provider={C._PROVIDER} model={C._MODEL} class=daily_quota" in (refusals[0] if refusals else ""), True)
check("the canned intro is named in the log", any("CANNED sentence" in l for l in LINES), True)

class Ok:
    class chat:
        class completions:
            @staticmethod
            def create(**kw):
                class R:
                    class m:
                        content = " A real intro. "
                    choices = [type("c", (), {"message": m})]
                return R
C.get_groq = lambda: Ok
LINES.clear()
check("a success is untouched", C.synthesize_intro("data"), "A real intro.")
check("and records nothing", [l for l in LINES if l.startswith("PROVIDER_REFUSAL")], [])

_PENDING.clear()
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
