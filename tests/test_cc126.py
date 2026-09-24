"""
test_cc126.py -- gate for CC-126: S3-A writes its first_domino column, entity extraction stops
calling Groq once refused for the day (D7), the S2 report drops invented classification
markings. Line-ending blind. No network: groq and the refusal recorder are stubbed.
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import os
import re
import sys
import types

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
FAILS = []


def read(name):
    return open(os.path.join(CODE, name), "rb").read().decode("utf-8").replace("\r\n", "\n")


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r want=%r" % (got, want)))
    if not ok:
        FAILS.append(name)


# 1. S3-A: first_domino is a top-level field of the record it inserts
s3a = read("lens_s3a_patterns.py")
a = s3a.index("record = {")
rec = s3a[a:s3a.index("\n    }\n", a)]
check("S3-A writes the first_domino column",
      bool(re.search(r'\n        "first_domino":\s+analysis\.get\("first_domino"', rec)), True)

# 2. D7: the daily-token breaker
refusals = []
fake_refusal = types.ModuleType("lens_provider_refusal")
fake_refusal.record_refusal = lambda *a, **k: refusals.append(1)
sys.modules["lens_provider_refusal"] = fake_refusal
calls, MESSAGE = [], ["Error code: 429 - Rate limit reached for model on tokens per day (TPD): Limit 200000"]


class _Completions:
    def create(self, **kw):
        calls.append(1)
        raise Exception(MESSAGE[0])


class _Groq:
    def __init__(self, api_key=None, **kw):
        self.chat = types.SimpleNamespace(completions=_Completions())


fake_groq = types.ModuleType("groq")
fake_groq.Groq = _Groq
sys.modules["groq"] = fake_groq
import lens_entity_extract as E          # noqa: E402
from lens_models import wire             # noqa: E402
os.environ.setdefault(wire("entity_extract")[2], "test-key")
body = "An analyst said the talks would fail. " * 20
got = [E._extract_experts_via_llm("t", body, "src") for _ in range(3)]
check("a daily-token refusal trips the breaker", E._TPD_TRIPPED["on"], True)
check("no Groq call once tripped", len(calls), 1)
check("the skipped articles are counted", E._TPD_TRIPPED["skipped"], 2)
check("extraction still returns lists", got, [[], [], []])
E._TPD_TRIPPED.update(on=False, skipped=0)
MESSAGE[0] = "Error code: 429 - Rate limit reached on tokens per minute (TPM)"
E._extract_experts_via_llm("t", body, "src")
check("a per-minute 429 does not trip it", E._TPD_TRIPPED["on"], False)

# 3. S2 report: markings
import lens_s2_step_report as S          # noqa: E402
for line, want in (("CLASSIFIED // DISSEMINATE TO GCSP EDUCATORS ONLY", True),
                   ("## **CLASSIFIED**", True),
                   ("PART A \u2014 THE INJECTION ARCHITECTURE", False),
                   ("The report is not classified and stays with the operator.", False)):
    check("marking? %s" % line[:40], S._is_marking(line), want)
check("render_docx asks _is_marking", "if _is_marking(line):" in read("lens_s2_step_report.py"), True)
try:
    import docx  # noqa: F401
    path = S.render_docx("CLASSIFIED // DISSEMINATE TO GCSP EDUCATORS ONLY\nPART A \u2014 X\nbody text",
                         "2026-09-25", {"threat_level": "HIGH"})
    from docx import Document
    texts = [p.text for p in Document(path).paragraphs]
    check("the rendered report carries no marking", any("CLASSIFIED" in t for t in texts), False)
    check("the rendered report keeps its PART heading", any(t.startswith("PART A") for t in texts), True)
except ImportError:
    print("SKIP render check: python-docx not installed")

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
