"""patch_article1_openrouter.py — toggle Article 1 calibration to OpenRouter provider.

Cross-lab bias testing on Article 1 (Reuters wire-service, Trump China policy).
Used as part of free-tier breadth test (post-LENS-019.5).

Usage:
  python patch_article1_openrouter.py openrouter  # switch to OpenRouter
  python patch_article1_openrouter.py cerebras    # restore Cerebras (default)
  python patch_article1_openrouter.py status      # show current

Idempotent. Safe to run multiple times.
"""
import sys
import pathlib

# Try v3 (LENS-019.5 calibration round) first, fall back to plain article1
candidates = [
    pathlib.Path("calibrate_rubric_article1_v3.py"),
    pathlib.Path("calibrate_rubric_article1.py"),
]
p = next((c for c in candidates if c.exists()), None)
if p is None:
    print(f"FAIL: cannot find Article 1 calibration script. Tried: {[str(c) for c in candidates]}")
    sys.exit(1)

print(f"Using calibration script: {p}")

if len(sys.argv) < 2 or sys.argv[1] not in ("openrouter", "cerebras", "status"):
    print(__doc__)
    sys.exit(1)

mode = sys.argv[1]
src = p.read_text(encoding="utf-8")

CEREBRAS_LINE = 'os.environ["S2F_PROVIDER"] = "cerebras"'
OPENROUTER_LINE = 'os.environ["S2F_PROVIDER"] = "openrouter"'

if mode == "status":
    if CEREBRAS_LINE in src:
        print(f"Current: cerebras (qwen-3-235b)")
    elif OPENROUTER_LINE in src:
        print(f"Current: openrouter")
    else:
        print(f"UNKNOWN: no S2F_PROVIDER override found in {p.name}")
    sys.exit(0)

if mode == "openrouter":
    if OPENROUTER_LINE in src:
        print(f"Already set to openrouter ({p.name}).")
        sys.exit(0)
    if CEREBRAS_LINE not in src:
        print(f"FAIL: cannot find cerebras anchor in {p.name}")
        print("This script may not have an S2F_PROVIDER override. Check with:")
        print(f"  grep -n S2F_PROVIDER {p.name}")
        sys.exit(1)
    src = src.replace(CEREBRAS_LINE, OPENROUTER_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print(f"OK: {p.name} will run on OpenRouter.")
elif mode == "cerebras":
    if CEREBRAS_LINE in src:
        print(f"Already set to cerebras ({p.name}).")
        sys.exit(0)
    if OPENROUTER_LINE not in src:
        print(f"FAIL: cannot find openrouter anchor in {p.name}")
        sys.exit(1)
    src = src.replace(OPENROUTER_LINE, CEREBRAS_LINE, 1)
    p.write_text(src, encoding="utf-8")
    print(f"OK: {p.name} restored to cerebras (qwen-3-235b).")

import py_compile
py_compile.compile(str(p), doraise=True)
