"""patch_openrouter_model_v2.py — switch OpenRouter model to ANY free-tier model.

Generalizes the v1 single-model patcher. Used for cross-lab bias testing on
Articles 1 and 3 (post-LENS-019.5).

Usage:
  python patch_openrouter_model_v2.py <model_id>     # set model
  python patch_openrouter_model_v2.py status         # show current model
  python patch_openrouter_model_v2.py --list         # show known free models

Verified free-tier models (April 2026):
  openai/gpt-oss-120b:free          — OpenAI lineage (American)
  openai/gpt-oss-20b:free           — OpenAI lineage (American, smaller)
  google/gemma-4-31b-it:free        — Google DeepMind lineage (American)
  google/gemma-4-26b-a4b-it:free    — Google DeepMind lineage (American, MoE)
  nvidia/nemotron-3-super-120b-a12b:free — NVIDIA lineage (American)

Idempotent. Safe to run multiple times.
"""
import sys
import re
import pathlib

KNOWN_MODELS = {
    "gpt-oss-120b": "openai/gpt-oss-120b:free",
    "gpt-oss-20b":  "openai/gpt-oss-20b:free",
    "gemma-4-31b":  "google/gemma-4-31b-it:free",
    "gemma-4-26b":  "google/gemma-4-26b-a4b-it:free",
    "nemotron-3-super": "nvidia/nemotron-3-super-120b-a12b:free",
}

if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print(__doc__)
    sys.exit(0)

if sys.argv[1] == "--list":
    print("Known free-tier model shortcuts:")
    for k, v in KNOWN_MODELS.items():
        print(f"  {k:25} -> {v}")
    print()
    print("Or pass a full OpenRouter model ID directly (e.g. 'tencent/hy3-preview:free').")
    sys.exit(0)

p = pathlib.Path("code/lens_framing_rubrics.py")
if not p.exists():
    print("FAIL: code/lens_framing_rubrics.py not found")
    sys.exit(1)

src = p.read_text(encoding="utf-8")

# Find the current model line in the openrouter branch
# Pattern: model = "..."
MODEL_LINE_RE = re.compile(r'model = "([^"]+)"\s*\n\s*log\.info\(f"Using OpenRouter')

m = MODEL_LINE_RE.search(src)
if not m:
    print("FAIL: cannot find OpenRouter model line in code/lens_framing_rubrics.py")
    print("Inspect with: grep -B 1 -A 1 'Using OpenRouter' code/lens_framing_rubrics.py")
    sys.exit(1)

current_model = m.group(1)

if sys.argv[1] == "status":
    print(f"Current OpenRouter model: {current_model}")
    sys.exit(0)

# Resolve shortcut or use literal
arg = sys.argv[1]
new_model = KNOWN_MODELS.get(arg, arg)

if new_model == current_model:
    print(f"Already set: {new_model}")
    sys.exit(0)

old_line = f'model = "{current_model}"'
new_line = f'model = "{new_model}"'

src = src.replace(old_line, new_line, 1)
p.write_text(src, encoding="utf-8")

import py_compile
py_compile.compile(str(p), doraise=True)

print(f"OK: switched OpenRouter model")
print(f"  from: {current_model}")
print(f"  to:   {new_model}")
