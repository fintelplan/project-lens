"""CC-141 -- order item 11 (L2.4): the Daily Brief's SYSTEM 3 lines (patterns, summary, First
Domino) come from the newest S3-A row, and the Brief can go out before S3-A writes -- so a reader
saw the previous wave's row with no date. The Patterns line now says when the row was written.
The function is read out of lens_telegram.py with ast, so the test needs none of its imports.
    python tests/test_cc141_s3_when.py
"""
import ast
import os
import sys

ROOT = os.environ.get("LENS_ROOT_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
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


src = open(os.path.join(ROOT, "code", "lens_telegram.py"), "rb").read().decode("utf-8")
fn = [n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == "_s3_when"]
check("_s3_when is defined", len(fn), 1)
ns = {}
if fn:
    exec(compile(ast.Module(body=fn, type_ignores=[]), "lens_telegram.py", "exec"), ns)
f = ns.get("_s3_when", lambda s: None)
check("a row's time is said, to the minute, in UTC",
      f({"generated_at": "2026-10-02T07:39:12.5+00:00"}), " -- S3-A row written 2026-10-02 07:39 UTC")
check("no time is said as unknown, not left blank", f({}), " -- S3-A row time unknown")
check("a missing row is safe", f(None), " -- S3-A row time unknown")
check("the Patterns line carries it", 'f"Patterns: {pcnt} detected" + _s3_when(s3),' in src, True)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
