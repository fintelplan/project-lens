"""patch_openrouter_model.py — switch OpenRouter model from defunct Maverick to gpt-oss-120b.

OpenRouter removed meta-llama/llama-4-maverick:free from free tier in early 2026.
404 Not Found on every call. Switch to openai/gpt-oss-120b:free which is:
  - Currently in the free tier (verified April 2026 via openrouter.ai/collections/free-models)
  - Different training lineage from qwen-3 (OpenAI vs Alibaba) — needed for bias test
  - Comparable scale (117B MoE vs qwen-3 235B)
  - 131K context (fits our ~25K v3.1 prompt easily)
  - Native function calling and structured output support

Usage:
  python patch_openrouter_model.py             # apply (Maverick -> gpt-oss-120b)
  python patch_openrouter_model.py --revert    # revert (gpt-oss-120b -> Maverick)
"""
import sys
import pathlib

p = pathlib.Path("code/lens_framing_rubrics.py")
if not p.exists():
    print("FAIL: code/lens_framing_rubrics.py not found")
    sys.exit(1)

src = p.read_text(encoding="utf-8")
REVERT = "--revert" in sys.argv

OLD_MODEL = 'model = "meta-llama/llama-4-maverick:free"'
NEW_MODEL = 'model = "openai/gpt-oss-120b:free"'

if REVERT:
    if NEW_MODEL not in src:
        print("Already reverted (Maverick model present or no model line found).")
        sys.exit(0)
    src = src.replace(NEW_MODEL, OLD_MODEL, 1)
    p.write_text(src, encoding="utf-8")
    print(f"OK: reverted to Maverick (note: still defunct on OpenRouter)")
else:
    if NEW_MODEL in src:
        print("Already patched (gpt-oss-120b active).")
        sys.exit(0)
    if OLD_MODEL not in src:
        print(f"FAIL: anchor not found. Looking for: {OLD_MODEL}")
        print("Inspect with: grep -n 'meta-llama' code/lens_framing_rubrics.py")
        sys.exit(1)
    src = src.replace(OLD_MODEL, NEW_MODEL, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: OpenRouter model switched to openai/gpt-oss-120b:free")

import py_compile
py_compile.compile(str(p), doraise=True)
print("Module compiles cleanly.")
