"""patch_article5_trump_only.py — re-run Article 5 trump_office lens only.

Reason: original Article 5 calibration tonight failed both trump_office runs
to Cerebras 429 queue congestion. xi_office (not_applicable) and khamenei_office
(4 ops detected) returned cleanly and don't need re-running.

This patch swaps LENSES_TO_TEST to ['trump_office'] only — 2 runs instead of 6.
Reduces queue exposure significantly.

Idempotent. Run once. Restore with patch_article5_trump_only.py --restore.
"""
import sys, pathlib

p = pathlib.Path("calibrate_rubric_article5_mee.py")
if not p.exists():
    print("FAIL: calibrate_rubric_article5_mee.py not in cwd")
    sys.exit(1)

src = p.read_text(encoding="utf-8")

# Restore mode
if "--restore" in sys.argv:
    if 'LENSES_TO_TEST = ["trump_office"]' not in src:
        print("Already restored.")
        sys.exit(0)
    src = src.replace(
        'LENSES_TO_TEST = ["trump_office"]',
        'LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]',
        1,
    )
    p.write_text(src, encoding="utf-8")
    print("OK: restored to tri-lens.")
    sys.exit(0)

# Apply mode
if 'LENSES_TO_TEST = ["trump_office"]' in src:
    print("Already patched (trump_office only).")
    sys.exit(0)

anchor = 'LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]'
if anchor not in src:
    print(f"FAIL: anchor not found: {anchor}")
    sys.exit(1)

src = src.replace(anchor, 'LENSES_TO_TEST = ["trump_office"]', 1)
p.write_text(src, encoding="utf-8")

import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: Article 5 calibration restricted to trump_office only (2 runs).")
print("Now: python calibrate_rubric_article5_mee.py")
print("After: python patch_article5_trump_only.py --restore")
