"""patch_catalog_v31.py — switch rubric module to load catalog v3.1.

LENS-019.5 catalog clarity revisions:
  - OP-001 description tightened to distinguish DECOY-SUBJECT pretense from
    incomplete coverage (DO NOT FIRE on missing-coverage cases).
  - OP-016 description tightened to distinguish CONSEQUENCE-attribution
    (the operation) from ACTION-attribution (correct PHI-003 naming).

This patch:
  1. Updates code/lens_framing_rubrics.py CATALOG_PATH constant from v3.json -> v3_1.json
  2. Updates docstring comment references from v3 to v3.1
  3. Verifies module compiles cleanly

Idempotent. Run once. v3.json stays in repo as history (LR-088 versioned-filenames).

Prerequisite: data/lens-OPS-001_catalog_v3_1.json must already be in place
before running this patch.
"""
import sys
import pathlib

REPO_ROOT = pathlib.Path(".")
RUBRIC_FILE = REPO_ROOT / "code" / "lens_framing_rubrics.py"
NEW_CATALOG = REPO_ROOT / "data" / "lens-OPS-001_catalog_v3_1.json"

# Pre-flight: catalog file present?
if not NEW_CATALOG.exists():
    print(f"FAIL: {NEW_CATALOG} not found.")
    print("Move the v3.1 catalog file to data/ first.")
    sys.exit(1)

# Pre-flight: rubric module present?
if not RUBRIC_FILE.exists():
    print(f"FAIL: {RUBRIC_FILE} not found.")
    sys.exit(1)

src = RUBRIC_FILE.read_text(encoding="utf-8")

# Idempotency check
if 'CATALOG_PATH = "data/lens-OPS-001_catalog_v3_1.json"' in src:
    print("Already patched — module loads v3.1.")
    sys.exit(0)

# Anchor check
anchor = 'CATALOG_PATH = "data/lens-OPS-001_catalog_v3.json"'
if anchor not in src:
    print(f"FAIL: anchor not found in {RUBRIC_FILE}:")
    print(f"  expected: {anchor}")
    sys.exit(1)

# Apply patch — single line constant change
src_new = src.replace(
    anchor,
    'CATALOG_PATH = "data/lens-OPS-001_catalog_v3_1.json"',
    1,
)

# Update docstring references (cosmetic but accurate)
src_new = src_new.replace(
    "Catalog of 29 operations loaded from data/lens-OPS-001_catalog_v3.json",
    "Catalog of 29 operations loaded from data/lens-OPS-001_catalog_v3_1.json",
    1,
)
src_new = src_new.replace(
    "Data: data/lens-OPS-001_catalog_v3.json must be present at module init",
    "Data: data/lens-OPS-001_catalog_v3_1.json must be present at module init",
    1,
)

RUBRIC_FILE.write_text(src_new, encoding="utf-8")

# Verify compile
import py_compile
py_compile.compile(str(RUBRIC_FILE), doraise=True)
print(f"OK: {RUBRIC_FILE} now loads v3.1 catalog, compiles cleanly.")

# Verify catalog actually loads
import json
cat = json.loads(NEW_CATALOG.read_text(encoding="utf-8"))
print(f"OK: catalog {cat['catalog_id']} {cat['catalog_version']} "
      f"with {len(cat['operations'])} operations.")
print()
print("Now run: python calibrate_rubric_article3.py")
