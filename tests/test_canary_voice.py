"""CC-114 -- the canary's voice counts the lenses that spoke THIS wave, and names the rest.

Replays the 2026-09-22 evening shape: Lens 1 failed 429_tpd, three lenses wrote rows,
and the morning's four rows sat just below them.
    python tests/test_canary_voice.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
sys.path.insert(0, CODE)
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "t")
os.environ.setdefault("TELEGRAM_CHAT_ID", "c")

import lens_canary_wave as W

PASS = 0
FAIL = 0
DASH = "\u2014"


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


def row(lens, t, q=6.0):
    return {"summary": f"[{lens} {DASH} x] text of {lens}", "generated_at": t, "quality_score": q,
            "domain_focus": "ALL", "cycle": "manual"}


EVENING = [row("Sovereignty Check", "2026-09-22T17:45:13+00:00", 7.1),
           row("Causal Chain", "2026-09-22T17:43:57+00:00", 5.8),
           row("Physical Reality", "2026-09-22T17:42:10+00:00", 6.55),
           row("Sovereignty Check", "2026-09-22T06:36:47+00:00", 7.1),
           row("Causal Chain", "2026-09-22T06:35:24+00:00", 5.55),
           row("Physical Reality", "2026-09-22T06:33:54+00:00", 6.55),
           row("Foundation", "2026-09-22T06:33:37+00:00", 2.3)]

print("== the 2026-09-22 evening shape ==")
got, missing = W.select_wave(EVENING)
check("three lenses spoke", [W.lens_of(r) for r in got], ["Physical Reality", "Causal Chain", "Sovereignty Check"])
check("Foundation is named missing", missing, ["Foundation"])
check("no morning row is borrowed", all(r["generated_at"].startswith("2026-09-22T17") for r in got), True)
check("the note says so", W.missing_note(missing), " -- MISSING: Foundation")

print("== a full wave ==")
got, missing = W.select_wave(EVENING[3:])
check("four lenses, Lens 1..4 order", [W.lens_of(r) for r in got], list(W.LENSES))
check("none missing", missing, [])

print("== nothing at all, and unreadable summaries ==")
check("empty -> all four missing", W.select_wave([]), ([], list(W.LENSES)))
odd = [{"summary": "no prefix", "generated_at": "2026-09-22T17:45:00+00:00"}]
check("unreadable prefix is said, not guessed", W.select_wave(odd)[1], ["(lens names unreadable)"])

print("== the canary block, run for real with the database and Telegram replaced ==")
import lens_telegram as T


class Q:
    def __init__(self, rows): self.rows = rows
    def select(self, *a, **k): return self
    def order(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def limit(self, n): self.n = n; return self
    def execute(self): return type("R", (), {"data": self.rows[: self.n]})


class SB:
    def table(self, name):
        return Q(EVENING if name == "lens_reports" else [])


sent = []
T._get_sb = lambda: SB()
T.send_message = lambda text, parse_mode="HTML": sent.append(text) or True
T.send_s1_intelligence()
msg = sent[-1] if sent else ""
check("the block says 3/4 and names Foundation", "3/4 lenses spoke this wave. MISSING: Foundation" in msg, True)
check("the morning Sovereignty row is not in it", msg.count("text of Sovereignty Check"), 1)

class Empty(SB):
    def table(self, name): return Q([])
T._get_sb = lambda: Empty()
T.send_s1_intelligence()
check("no lens at all is SENT, not skipped", "0/4 lenses spoke this wave" in (sent[-1] if sent else ""), True)

print("== the old reads are gone ==")
for f in ("lens_telegram.py", "lens_s1_report.py"):
    b = open(os.path.join(CODE, f), "rb").read()
    check(f"{f}: no lens_reports limit(4)", b".limit(4)" in b, False)
s1 = open(os.path.join(CODE, "lens_s1_report.py"), "rb").read()
check('S1 prompt no longer says a fixed "(4 lenses)"', b"LENS REPORTS (4 lenses):" in s1, False)
check('S1 intro no longer says a fixed "4 lenses |"', b'f"4 lenses | ' in s1, False)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
