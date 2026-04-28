"""patch_article7_openrouter.py — toggle Article 7 calibration to OpenRouter provider.

Cross-lab bias testing on Article 7 (Reuters via ET BrandEquity, China information
war on Taiwan, anti-Beijing investigative framing). Mirror to Article 6 in the
original symmetry test. Run on gpt-oss-120b to complete the 4-article matrix.

Usage:
  python patch_article7_openrouter.py openrouter  # switch to OpenRouter
  python patch_article7_openrouter.py cerebras    # restore Cerebras (default)
  python patch_article7_openrouter.py status      # show current

Idempotent. Safe to run multiple times.
"""
import sys
import pathlib

p = pathlib.Path("calibrate_rubric_article7_reuters.py")
if not p.exists():
    print("FAIL: calibrate_rubric_article7_reuters.py not in cwd")
    sys.exit(1)

if len(sys.argv) < 2 or sys.argv[1] not in ("openrouter", "cerebras", "status"):
    print(__doc__)
    sys.exit(1)

mode = sys.argv[1]
src = p.read_text(encoding="utf-8")

CEREBRAS_LINE = 'os.environ["S2F_PROVIDER"] = "cerebras"'
OPENROUTER_LINE = 'os.environ["S2F_PROVIDER"] = "openrouter"'

if mode == "status":
    if CEREBRAS_LINE in src:
        print("Current: cerebras (qwen-3-235b)")
    elif OPENROUTER_LINE in src:
        print("Current: openrouter")
    else:
        print("UNKNOWN: no S2F_PROVIDER override found in calibrate_rubric_article7_reuters.py")
    sys.exit(0)

if mode == "openrouter":
    if OPENROUTER_LINE in src:
        print("Already set to openrouter.")
        sys.exit(0)
    if CEREBRAS_LINE not in src:
        print("FAIL: cannot find cerebras anchor in calibrate_rubric_article7_reuters.py")
        sys.exit(1)
    src = src.replace(CEREBRAS_LINE, OPENROUTER_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: Article 7 will run on OpenRouter.")
elif mode == "cerebras":
    if CEREBRAS_LINE in src:
        print("Already set to cerebras.")
        sys.exit(0)
    if OPENROUTER_LINE not in src:
        print("FAIL: cannot find openrouter anchor in calibrate_rubric_article7_reuters.py")
        sys.exit(1)
    src = src.replace(OPENROUTER_LINE, CEREBRAS_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: Article 7 restored to cerebras (qwen-3-235b).")

import py_compile
py_compile.compile(str(p), doraise=True)
