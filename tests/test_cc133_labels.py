"""CC-133 -- reader labels count what they say (L2.4, L2.5).

LENS-048 screenshots: the Regular Report caption showed "**Objective:** ..." (the prompt's own
boilerplate, markdown raw -- the caption is sent without a parse mode); the Daily Brief's
"7-DAY TREND" printed the last three reports; the S1 message said "N articles analyzed" for the
12-hour collection pool; S3-A's <sign> strip left "confirmed if:  X".
    python tests/test_cc133_labels.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
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


def src(name):
    return open(os.path.join(CODE, name), "rb").read().decode("utf-8").replace("\r\n", "\n")   # LR-277


print("== Regular Report caption ==")
import lens_regular_report as RR
report = ("# **INTELLIGENCE REPORT**\n"
          "## **PART 1 \u2014 INJECTION DETECTION**\n"
          "**Objective:** Exhaustive analysis of how today's information environment was shaped, with a focus on adversary narratives.\n"
          "- **Timing sync** across three outlets pushed one framing of the Hormuz talks within an hour of each other.\n"
          "## **PART 2 \u2014 RECOVERY**\n"
          "**Objective:** Synthesis that no single position would surface.\n"
          "Cross-position convergence shows the same **sovereignty** frame in S2-A, S2-B and S3-A this week.\n")
cap = RR.build_caption(report, {"valid_citations": 7})
check("no ** in the caption", "**" in cap, False)
check("the prompt's Objective line is not a finding", "Objective" in cap, False)
check("part 1 is the first real finding", "Timing sync across three outlets" in cap, True)
check("part 2 is the first real finding", "Cross-position convergence shows the same sovereignty frame" in cap, True)
check("no leading list marker", "- Timing" in cap, False)

print("== Daily Brief trend label ==")
tg = src("lens_telegram.py")
check("no '7-DAY TREND' over three reports", "7-DAY TREND" in tg, False)
check("the label says what it shows", "LAST 3 REPORTS" in tg, True)
check("it still shows three", "trend[:3]" in tg, True)

print("== S1 message count label ==")
s1 = src("lens_s1_report.py")
check("no 'articles analyzed' for the collection pool", "articles analyzed" in s1, False)
check("the count says it is the 12 h collection", s1.count("articles collected (12 h)") >= 2, True)

print("== S3-A first_domino spacing ==")
s3a = src("lens_s3a_patterns.py")
i = s3a.find('analysis["first_domino"] = __import__("re").sub(r"</?sign>"')
j = s3a.find('__import__("re").sub(r"[ \\t]{2,}", " ", analysis["first_domino"])')
check("the space collapse follows the tag strip", i != -1 and j > i, True)
sample = re.sub(r"</?sign>", "", "confirmed if: <sign> More countries; disconfirmed if: <sign>x</sign>").strip()
check("the two regexes leave single spaces", re.sub(r"[ \t]{2,}", " ", sample), "confirmed if: More countries; disconfirmed if: x")

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
