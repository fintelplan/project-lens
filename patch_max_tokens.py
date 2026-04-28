"""patch_max_tokens.py — fix MAX_TOKENS truncation causing JSON parse failures.
Per LENS-019.5 calibration round 4 finding: long articles produce malformed
JSON when MAX_TOKENS=3000 cuts the response mid-string."""
import sys, pathlib

p = pathlib.Path("code/lens_framing_rubrics.py")
src = p.read_text(encoding="utf-8")

if "MAX_TOKENS = 6000" in src:
    print("Already patched.")
    sys.exit(0)

if "MAX_TOKENS = 3000" not in src:
    print("FAIL: anchor 'MAX_TOKENS = 3000' not found.")
    sys.exit(1)

src = src.replace("MAX_TOKENS = 3000", "MAX_TOKENS = 6000", 1)
p.write_text(src, encoding="utf-8")

import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: MAX_TOKENS 3000 -> 6000, compiles cleanly.")
print("Now: python calibrate_rubric_article1_v3.py")
