"""
patch_calibrate_scripts_honor_env_v2.py

CORRECTED filename patterns. Targets calibrate_rubric_article*.py
(not calibrate_article*.py — that was wrong in v1).

Makes these scripts honor the pre-existing S2F_PROVIDER env var instead
of hardcoding 'cerebras', so we can run with S2F_PROVIDER=ollama.

Idempotent: safe to run multiple times. Creates .bak_envpatch backups.

Run from /c/school/lens repo root.
"""

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(".").resolve()

# CORRECTED: actual filenames use _rubric_
TARGET_FILES = [
    "calibrate_rubric_article1_v3.py",        # Article 1 (Reuters wire) — v3 catalog
    "calibrate_rubric_article3.py",            # Article 3 (Asia Times opinion)
    "calibrate_rubric_article6_chosunbiz.py",  # Article 6 (Chosunbiz steno)
    "calibrate_rubric_article7_reuters.py",    # Article 7 (Reuters investigative)
]

OLD_PATTERN = re.compile(
    r'os\.environ\[\s*["\']S2F_PROVIDER["\']\s*\]\s*=\s*["\']cerebras["\']'
)
ALREADY_PATCHED_MARKER = "# PATCHED: honor pre-existing S2F_PROVIDER"

NEW_BLOCK = '''# PATCHED: honor pre-existing S2F_PROVIDER (allows ollama/openrouter override)
if "S2F_PROVIDER" not in os.environ:
    os.environ["S2F_PROVIDER"] = "cerebras"  # default'''


def inspect_file(path: Path) -> dict:
    """Returns dict with status info, doesn't modify."""
    try:
        content = path.read_text(encoding="utf-8")
    except Exception as e:
        return {"status": "error", "error": str(e)}

    if ALREADY_PATCHED_MARKER in content:
        return {"status": "already_patched"}

    matches = OLD_PATTERN.findall(content)
    if not matches:
        # Look for any S2F_PROVIDER assignment for diagnostics
        any_provider = re.findall(
            r'os\.environ\[\s*["\']S2F_PROVIDER["\']\s*\]\s*=\s*["\'](\w+)["\']',
            content,
        )
        return {
            "status": "no_cerebras_match",
            "found_providers": any_provider,
        }

    return {"status": "patchable", "match_count": len(matches)}


def patch_file(path: Path) -> str:
    info = inspect_file(path)
    if info["status"] != "patchable":
        return info["status"]

    content = path.read_text(encoding="utf-8")
    new_content = OLD_PATTERN.sub(NEW_BLOCK, content, count=1)

    backup_path = path.with_suffix(path.suffix + ".bak_envpatch")
    backup_path.write_text(content, encoding="utf-8")

    path.write_text(new_content, encoding="utf-8")
    return "patched"


def main():
    found_any = False
    print(f"Scanning {REPO_ROOT}\n")

    for fname in TARGET_FILES:
        path = REPO_ROOT / fname
        if not path.exists():
            print(f"  MISSING: {fname}")
            continue
        found_any = True
        info = inspect_file(path)
        if info["status"] == "patchable":
            status = patch_file(path)
            print(f"  {fname}: {status} ({info['match_count']} match)")
        elif info["status"] == "already_patched":
            print(f"  {fname}: already_patched (skip)")
        elif info["status"] == "no_cerebras_match":
            providers = info.get("found_providers", [])
            print(f"  {fname}: no cerebras hardcode found")
            if providers:
                print(f"    -> found other S2F_PROVIDER assigns: {providers}")
            else:
                print(f"    -> no S2F_PROVIDER assignments at all (script may use env directly)")
        else:
            print(f"  {fname}: {info['status']}")

    if not found_any:
        print("ERROR: none of the target files exist.")
        sys.exit(2)


if __name__ == "__main__":
    main()
