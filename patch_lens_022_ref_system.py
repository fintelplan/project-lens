"""
patch_lens_022_ref_system.py
LENS-022: Ref system redesign

What this does:
  1. Copies new lens_ref_system.py into code/
  2. Adds Refs step to .github/workflows/lens-manage-analyze.yml
  3. Deletes lens-ref-free.yml and lens-ref-sonnet.yml

Run from repo root:
  python patch_lens_022_ref_system.py
"""

import os, shutil, sys

ROOT = os.path.abspath(os.path.dirname(__file__) if "__file__" in dir() else ".")

# ── Step 1: Copy new lens_ref_system.py ──────────────────────────────────────
src = os.path.join(ROOT, "lens_ref_system.py")
dst = os.path.join(ROOT, "..", "code", "lens_ref_system.py")
dst = os.path.normpath(dst)

if not os.path.exists(src):
    print(f"ERROR: {src} not found — run from the patch folder")
    sys.exit(1)

shutil.copy2(src, dst)
print(f"✅ Copied lens_ref_system.py → {dst}")

# ── Step 2: Patch lens-manage-analyze.yml ────────────────────────────────────
yml_path = os.path.normpath(os.path.join(ROOT, "..", ".github", "workflows", "lens-manage-analyze.yml"))

with open(yml_path, encoding="utf-8") as f:
    content = f.read()

# Check if already patched
if "Run Article Reference Export" in content:
    print("⚠️  lens-manage-analyze.yml already has Refs step — skipping")
else:
    # The new step to append after S2 step
    refs_step = """
      - name: Run Article Reference Export (S1 + S2 — after S2 completes)
        env:
          SUPABASE_URL:         ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          TELEGRAM_BOT_TOKEN:   ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID:     ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          python code/lens_ref_system.py s1
          python code/lens_ref_system.py s2
          echo "Reference export complete"
"""

    # Insert after the S3 step block (end of file essentially, after last step)
    # Find the S3 step echo line and append after it
    S3_MARKER = '          echo "System 3 complete"'
    if S3_MARKER in content:
        content = content.replace(S3_MARKER, S3_MARKER + refs_step)
        with open(yml_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✅ Added Refs step to lens-manage-analyze.yml")
    else:
        print(f"ERROR: Could not find S3 marker in yml — manual patch needed")
        print(f"       Add refs step after S3 echo in {yml_path}")
        sys.exit(1)

# ── Step 3: Delete old ref workflows ─────────────────────────────────────────
for old_yml in ["lens-ref-free.yml", "lens-ref-sonnet.yml"]:
    path = os.path.normpath(os.path.join(ROOT, "..", ".github", "workflows", old_yml))
    if os.path.exists(path):
        os.remove(path)
        print(f"✅ Deleted {old_yml}")
    else:
        print(f"⚠️  {old_yml} not found — already deleted?")

print("\nDone. Now:")
print("  git add -A")
print('  git commit -m "LENS-022: redesign ref system S1/S2 architecture, merge into manage-analyze"')
print("  git push")
