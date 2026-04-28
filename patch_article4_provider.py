"""patch_article4_provider.py — toggle Article 4 calibration provider.

LENS-019.5 Test 1 (post-LENS-019.5 model evaluation):
Same article on both providers for head-to-head comparison.

Usage:
  python patch_article4_provider.py groq      # switch to Groq+llama-3.3
  python patch_article4_provider.py cerebras  # switch back to Cerebras+qwen-3-235b
  python patch_article4_provider.py status    # show current setting

Idempotent. Safe to run multiple times.
"""
import sys
import pathlib

p = pathlib.Path("calibrate_rubric_article4_scmp.py")
if not p.exists():
    print("FAIL: calibrate_rubric_article4_scmp.py not in cwd")
    sys.exit(1)

if len(sys.argv) < 2 or sys.argv[1] not in ("groq", "cerebras", "status"):
    print(__doc__)
    sys.exit(1)

mode = sys.argv[1]
src = p.read_text(encoding="utf-8")

CEREBRAS_LINE = 'os.environ["S2F_PROVIDER"] = "cerebras"'
GROQ_LINE = 'os.environ["S2F_PROVIDER"] = "groq"'

# Status check
if mode == "status":
    if CEREBRAS_LINE in src:
        print("Current: cerebras (qwen-3-235b)")
    elif GROQ_LINE in src:
        print("Current: groq (llama-3.3-70b-versatile)")
    else:
        print("UNKNOWN: neither line found, manual inspection needed")
    sys.exit(0)

# Switch logic
if mode == "groq":
    if GROQ_LINE in src:
        print("Already set to groq.")
        sys.exit(0)
    if CEREBRAS_LINE not in src:
        print("FAIL: cannot find cerebras anchor; manual inspection needed")
        sys.exit(1)
    src = src.replace(CEREBRAS_LINE, GROQ_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: switched to groq (llama-3.3-70b-versatile).")
    print("Now: python calibrate_rubric_article4_scmp.py")
    print("After test: python patch_article4_provider.py cerebras  # restore")
elif mode == "cerebras":
    if CEREBRAS_LINE in src:
        print("Already set to cerebras.")
        sys.exit(0)
    if GROQ_LINE not in src:
        print("FAIL: cannot find groq anchor; manual inspection needed")
        sys.exit(1)
    src = src.replace(GROQ_LINE, CEREBRAS_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: switched back to cerebras (qwen-3-235b).")

import py_compile
py_compile.compile(str(p), doraise=True)
