"""
patch_calibrate_scripts_honor_env.py

One-shot patch: makes all 4 article calibration scripts honor the
pre-existing S2F_PROVIDER env var instead of hardcoding 'cerebras'.

This unblocks Ollama (or any other) provider runs on existing articles
without modifying the calibration logic.

Idempotent: safe to run multiple times. Only patches lines that match
the exact original pattern.

Run from /c/school/lens repo root.
"""

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(".").resolve()

# Articles to patch. Adjust filenames if yours differ.
CANDIDATE_FILES = [
    "calibrate_article1_v3.py",
    "calibrate_article3_v3.py",
    "calibrate_article6_v3.py",
    "calibrate_article7_v3.py",
    # Fallbacks in case naming is different:
    "calibrate_article1.py",
    "calibrate_article3.py",
    "calibrate_article6.py",
    "calibrate_article7.py",
]

# The pattern we're replacing.
# Looks for: os.environ["S2F_PROVIDER"] = "cerebras"  (with optional whitespace variations)
OLD_PATTERN = re.compile(
    r'os\.environ\[\s*["\']S2F_PROVIDER["\']\s*\]\s*=\s*["\']cerebras["\']'
)
ALREADY_PATCHED_MARKER = "# PATCHED: honor pre-existing S2F_PROVIDER"

NEW_BLOCK = '''# PATCHED: honor pre-existing S2F_PROVIDER (allows ollama/openrouter override)
if "S2F_PROVIDER" not in os.environ:
    os.environ["S2F_PROVIDER"] = "cerebras"  # default'''


def patch_file(path: Path) -> str:
    """Returns status string: 'patched', 'already_patched', 'no_match', or 'error'."""
    try:
        content = path.read_text(encoding="utf-8")
    except Exception as e:
        return f"error: {e}"

    if ALREADY_PATCHED_MARKER in content:
        return "already_patched"

    match = OLD_PATTERN.search(content)
    if not match:
        return "no_match"

    new_content = OLD_PATTERN.sub(NEW_BLOCK, content, count=1)

    # Backup original
    backup_path = path.with_suffix(path.suffix + ".bak_envpatch")
    backup_path.write_text(content, encoding="utf-8")

    path.write_text(new_content, encoding="utf-8")
    return "patched"


def main():
    if not REPO_ROOT.exists():
        print(f"ERROR: repo root not found at {REPO_ROOT}", file=sys.stderr)
        sys.exit(1)

    found_any = False
    for fname in CANDIDATE_FILES:
        path = REPO_ROOT / fname
        if not path.exists():
            continue
        found_any = True
        status = patch_file(path)
        print(f"{fname}: {status}")

    if not found_any:
        print("WARN: no calibration scripts found at expected paths.")
        print("  Searched:")
        for fname in CANDIDATE_FILES:
            print(f"    - {fname}")
        print("  List actual calibration scripts:")
        for f in sorted(REPO_ROOT.glob("calibrate_article*.py")):
            print(f"    {f.name}")
        sys.exit(2)


if __name__ == "__main__":
    main()
