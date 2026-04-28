"""patch_article6_openrouter.py — toggle Article 6 calibration to OpenRouter provider.

Post-LENS-019.5 bias-test: run Article 6 (Chosunbiz, pro-Beijing stenographic)
on Llama 4 Maverick free variant via OpenRouter for comparison against
Cerebras+qwen-3-235b result that we already have.

Usage:
  python patch_article6_openrouter.py openrouter  # switch to OpenRouter
  python patch_article6_openrouter.py cerebras    # restore Cerebras (default)
  python patch_article6_openrouter.py status      # show current

Idempotent. Safe to run multiple times.
"""
import sys
import pathlib

p = pathlib.Path("calibrate_rubric_article6_chosunbiz.py")
if not p.exists():
    print("FAIL: calibrate_rubric_article6_chosunbiz.py not in cwd")
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
        print("Current: openrouter (llama-4-maverick:free)")
    else:
        print("UNKNOWN: manual inspection needed")
    sys.exit(0)

if mode == "openrouter":
    if OPENROUTER_LINE in src:
        print("Already set to openrouter.")
        sys.exit(0)
    if CEREBRAS_LINE not in src:
        print("FAIL: cannot find cerebras anchor")
        sys.exit(1)
    src = src.replace(CEREBRAS_LINE, OPENROUTER_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: Article 6 will run on OpenRouter (Llama 4 Maverick free).")
    print("Now: python calibrate_rubric_article6_chosunbiz.py")
    print("After: python patch_article6_openrouter.py cerebras  # restore")
elif mode == "cerebras":
    if CEREBRAS_LINE in src:
        print("Already set to cerebras.")
        sys.exit(0)
    if OPENROUTER_LINE not in src:
        print("FAIL: cannot find openrouter anchor")
        sys.exit(1)
    src = src.replace(OPENROUTER_LINE, CEREBRAS_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: Article 6 restored to cerebras (qwen-3-235b).")

import py_compile
py_compile.compile(str(p), doraise=True)
