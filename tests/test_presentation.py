"""
test_presentation.py -- gate for CC-124 (L2.4 labels, L2.5 presentation): word-boundary clips,
one presentation pass in the shared sender, the trend read oldest to newest, no "ALL" heading,
no hard message slices left, and scheduled runs keep their cycle. Exits 1 on any failure.
LENS_CODE_DIR may point at another copy of code/ (used to prove the gate bites).
"""
import inspect
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.environ.get("LENS_CODE_DIR")
                or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "code"))
import lens_telegram as T          # noqa: E402
import lens_cycle as C             # noqa: E402

FAILS = []


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else "  got=%r want=%r" % (got, want)))
    if not ok:
        FAILS.append(name)


ELL = "\u2026"
check("clip_at_word", T._clip("potentially leading to a new alignment", 20), "potentially leading" + ELL)
check("clip_short_untouched", T._clip("short", 20), "short")
check("clip_none", T._clip(None, 20), "")
check("clip_never_mid_word", T._clip("Bab el-Mandeb strait", 6).endswith("el" + ELL), False)

P = T._presentable
check("md_heading", P("## SUMMARY"), "SUMMARY")
check("md_bold", P("a **finance trap** b"), "a finance trap b")
check("md_italic", P("the *Roman Empire* then"), "the Roman Empire then")
check("json_wrapper", P('{"text":"The landscape repeats"}'), "The landscape repeats")
check("json_wrapper_cut", P('{"text":"The landscape repeats the'), "The landscape repeats the")
check("html_and_arithmetic_kept", P("<b>keep</b> 2*3*4 and * bullet"), "<b>keep</b> 2*3*4 and * bullet")

sent = []


class _R:
    status_code = 200
    text = "ok"


def _post(url, json=None, data=None, timeout=None):
    sent.append((json or data or {}).get("text"))
    return _R()


T.requests.post = _post
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "t")
os.environ.setdefault("TELEGRAM_CHAT_ID", "c")
T.send_message("## Head\n**bold**")
check("sender_applies_the_pass", sent[-1] if sent else None, "Head\nbold")

src = inspect.getsource(T)
left = [l.strip() for l in src.split("\n")
        if re.search(r"\[:\d{2,4}\]", l) and not re.search(r"log\.|r\.text|str\(e\)", l)]
check("no_hard_message_slices_left", left, [])
check("no_ALL_heading", 'focus != "ALL"' in inspect.getsource(T.send_s1_intelligence), True)

data = {"ma": {"threat_level": "HIGH", "executive_summary": "x", "quality_score": 0.9, "run_id": "r"},
        "s2": [], "s3": {}, "s1": [], "s1_missing": [], "s2f": {}, "top_entity": {},
        "trend": [{"threat_level": "HIGH"}, {"threat_level": "CRITICAL"}, {"threat_level": "LOW"}]}
brief = T.format_daily_brief(data)
check("trend_oldest_first", "LOW \u2192 CRITICAL \u2192 HIGH  (oldest \u2192 newest)" in brief, True)

for ev, hh, mm, want in (("schedule", 17, 54, "2of2"), ("schedule", 6, 30, "2of1"),
                         ("workflow_dispatch", 17, 54, "manual"), (None, 17, 54, "manual"),
                         (None, 1, 30, "2of1")):
    if ev:
        os.environ["GITHUB_EVENT_NAME"] = ev
    else:
        os.environ.pop("GITHUB_EVENT_NAME", None)
    check("cycle_%s_%02d%02d" % (ev, hh, mm), C.get_cycle(datetime(2026, 9, 24, hh, mm, tzinfo=timezone.utc)), want)
os.environ.pop("GITHUB_EVENT_NAME", None)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
