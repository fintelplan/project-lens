"""patch_cerebras_model.py — fix Cerebras model name in lens_framing_rubrics.py
Wrong: llama-3.3-70b
Right: qwen-3-235b-a22b-instruct-2507 (per session record, the available
model on operator's Cerebras free tier)."""
import sys, pathlib

p = pathlib.Path("code/lens_framing_rubrics.py")
src = p.read_text(encoding="utf-8")

if "qwen-3-235b-a22b-instruct-2507" in src:
    print("Already patched.")
    sys.exit(0)

if 'Cerebras(api_key=key), "llama-3.3-70b"' not in src:
    print("FAIL: anchor not found. Manual inspect needed.")
    sys.exit(1)

src = src.replace(
    'Cerebras(api_key=key), "llama-3.3-70b", "cerebras"',
    'Cerebras(api_key=key), "qwen-3-235b-a22b-instruct-2507", "cerebras"'
)
src = src.replace(
    "Using Cerebras provider (model: llama-3.3-70b)",
    "Using Cerebras provider (model: qwen-3-235b-a22b-instruct-2507)"
)

p.write_text(src, encoding="utf-8")

import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: model patched, compiles cleanly.")
print("Now: python calibrate_rubric_article3.py")
