"""patch_article3_force_cerebras.py — one-shot, idempotent.
Adds os.environ['S2F_PROVIDER']='cerebras' before load_dotenv()."""
import sys, pathlib

p = pathlib.Path("calibrate_rubric_article3.py")
src = p.read_text(encoding="utf-8")

if 'S2F_PROVIDER' in src and 'os.environ["S2F_PROVIDER"]' in src:
    print("Already patched — no action.")
    sys.exit(0)

needle = "from dotenv import load_dotenv"
if needle not in src:
    print(f"FAIL: anchor not found ({needle!r})")
    sys.exit(1)

inject = (
    '# Force Cerebras for this calibration run (Option R per LENS-019.5)\n'
    'os.environ["S2F_PROVIDER"] = "cerebras"\n\n'
    'from dotenv import load_dotenv'
)
src = src.replace(needle, inject, 1)

# Also add Provider line to banner if missing
banner = 'print(f"Total runs:    {len(LENSES_TO_TEST) * 2}")'
if banner in src and "Provider:" not in src:
    src = src.replace(
        banner,
        banner + '\n    print(f"Provider:      {os.environ.get(\'S2F_PROVIDER\', \'groq\')} (forced by script)")',
        1,
    )

p.write_text(src, encoding="utf-8")

import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: patched and compiles cleanly.")
print("Now run: python calibrate_rubric_article3.py")
