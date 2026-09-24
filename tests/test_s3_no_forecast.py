"""CC-119 -- the S3 prompts ask for causes already in motion, held as hypotheses; never forecasts.

LENS-010 wrote S3-A's prompt with "what event becomes inevitable in 30-90 days?" and, in the same
prompt, "Never predict. Identify what is already in motion." After the August model change the model
obeyed the first: 0 of 88 stored first dominoes worded as certain May-Jul, 10 of 24 Aug-Sep.
    python tests/test_s3_no_forecast.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
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
    return open(os.path.join(CODE, name), "rb").read()


A, D, R = src("lens_s3a_patterns.py"), src("lens_s3d_longterm.py"), src("lens_s3_step_report.py")

print("== the forecast questions are gone ==")
for label, blob, old in (
    ("S3-A Q4", A, b"what event becomes inevitable"),
    ("S3-A first_domino field", A, b"what becomes inevitable if patterns continue"),
    ("S3-D convergence", D, b"collision is becoming inevitable"),
    ("S3-D analog", D, b"analog predicts what happens next"),
    ("step report Part B", R, b"increasingly inevitable"),
    ("step report Part F title", R, b"VERDICT AND PREDICTIONS"),
    ("step report feeds no confidence percent", R, b"Confidence: {p.get('confidence',0):.0%}"),
    ("step report caption", R, b"and predictions</i>"),
):
    check(label + " no longer asks for a forecast", old in blob, False)

print("== what replaces them ==")
check("S3-A asks for the cause already in motion", b"What cause, already in motion, starts the chain" in A, True)
check("S3-A asks for a confirming and a disconfirming sign", b"disconfirm it" in A, True)
check("S3-A keeps its charter line", b"Never predict. Identify what is already in motion." in A, True)
check("S3-D holds the pressure point as a hypothesis", b"Name the pressure point as a hypothesis" in D, True)
check("step report asks for confirming and disconfirming signs", b"which would disconfirm it" in R, True)
check("step report says none has been checked", b"none has been checked yet" in R, True)

print("== no reader breaks: the JSON keys stay ==")
check('S3-A still emits "first_domino"', b'"first_domino":' in A, True)
check('S3-D still emits "predicted_next"', b'"predicted_next":' in D, True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
