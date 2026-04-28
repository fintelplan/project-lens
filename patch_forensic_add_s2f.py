"""
patch_forensic_add_s2f.py
Wires S2-F operation detections into lens_forensic_report.py

Changes:
1. Adds fetch_s2f_findings() function after fetch_system3_latest()
2. Adds s2f_findings fetch call in run_forensic_report()
3. Adds s2f_findings parameter to build_prompt()
4. Adds S2-F section in the prompt body
"""

REPORT_PATH = "code/lens_forensic_report.py"

with open(REPORT_PATH, "r", encoding="utf-8") as f:
    content = f.read()

# ── Patch 1: Add fetch_s2f_findings() after fetch_system3_latest ──
FETCH_S2F = '''
def fetch_s2f_findings(sb) -> list:
    """Recent S2-F Watch/Clarity/Verification findings (last 45 days).
    Returns HIGH and MEDIUM confidence findings for forensic context.
    Empty list if table doesn't exist or no findings yet.
    """
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
        rows = (
            sb.table("lens_drift_findings")
              .select("state_actor_lens,finding_confidence,finding_phrasing,"
                      "sample_size,evidence_article_ids,created_at,rubric_version")
              .in_("finding_confidence", ["HIGH", "MEDIUM"])
              .gte("created_at", cutoff)
              .order("finding_confidence", desc=True)
              .order("created_at", desc=True)
              .limit(10)
              .execute()
        ).data or []
        log.info(f"S2-F: {len(rows)} Watch/Clarity/Verification findings in last 45d")
        return rows
    except Exception as e:
        log.warning(f"S2-F findings fetch failed (non-critical): {e}")
        return []

'''

TARGET_AFTER = "def fetch_reference_pool(sb) -> list:"
if TARGET_AFTER not in content:
    print("ERROR: insertion point for fetch_s2f_findings not found")
    exit(1)

if "fetch_s2f_findings" in content:
    print("Already patched — fetch_s2f_findings exists")
else:
    content = content.replace(TARGET_AFTER, FETCH_S2F + TARGET_AFTER, 1)
    print("PATCH 1: fetch_s2f_findings() added")

# ── Patch 2: Add s2f_findings to build_prompt signature ──
OLD_SIG = "def build_prompt(macros: list, injections: list, lens_reports: list,\n                 s3_latest: dict, references: list) -> str:"
NEW_SIG = "def build_prompt(macros: list, injections: list, lens_reports: list,\n                 s3_latest: dict, references: list,\n                 s2f_findings: list = None) -> str:"

if OLD_SIG in content:
    content = content.replace(OLD_SIG, NEW_SIG, 1)
    print("PATCH 2: build_prompt signature updated")
elif "s2f_findings: list = None" in content:
    print("PATCH 2: already applied")
else:
    print("ERROR: build_prompt signature not found")
    exit(1)

# ── Patch 3: Add S2-F section inside build_prompt, before references ──
S2F_SECTION = '''
    # ── S2-F Pretense Operation Findings (Watch/Clarity/Verification) ──
    if s2f_findings:
        s2f_text = "S2-F PRETENSE OPERATION FINDINGS (Watch/Clarity/Verification cadence):\\n"
        s2f_text += "These are multi-article pattern findings from the operations-based detector.\\n"
        s2f_text += "HIGH confidence = 30-45 day verified pattern. MEDIUM = developing pattern.\\n\\n"
        for f in s2f_findings:
            lens = f.get("state_actor_lens", "unknown")
            conf = f.get("finding_confidence", "?")
            phrasing = (f.get("finding_phrasing") or "")[:400]
            sample = f.get("sample_size", 0)
            created = (f.get("created_at") or "")[:10]
            s2f_text += f"[{conf}] {lens} | {sample} articles | {created}\\n"
            s2f_text += f"{phrasing}\\n\\n"
        parts.append(s2f_text)
    else:
        parts.append("S2-F PRETENSE OPERATION FINDINGS: No Watch/Clarity/Verification findings yet "
                     "(S2-F pipeline is new — findings will accumulate over 7-45 days).\\n")

'''

# Insert before the references section in build_prompt
TARGET_REFS = "    # ── Reference pool (Path B) ──"

if TARGET_REFS not in content:
    print("WARNING: reference insertion point not found — skipping patch 3")
elif "S2-F PRETENSE OPERATION FINDINGS" in content:
    print("PATCH 3: already applied")
else:
    content = content.replace(TARGET_REFS, S2F_SECTION + TARGET_REFS, 1)
    print("PATCH 3: S2-F section added to prompt")

# ── Patch 4: Wire fetch + pass in run_forensic_report ──
OLD_FETCH = "    s3_latest = fetch_system3_latest(sb)\n    references = fetch_reference_pool(sb)"
NEW_FETCH  = "    s3_latest = fetch_system3_latest(sb)\n    s2f_findings = fetch_s2f_findings(sb)\n    references = fetch_reference_pool(sb)"

if OLD_FETCH in content:
    content = content.replace(OLD_FETCH, NEW_FETCH, 1)
    print("PATCH 4a: s2f_findings fetched in run_forensic_report")
elif "s2f_findings = fetch_s2f_findings(sb)" in content:
    print("PATCH 4a: already applied")
else:
    print("ERROR: fetch insertion point not found")
    exit(1)

OLD_CALL = "    prompt = build_prompt(macros, injections, lens_reports, s3_latest, references)"
NEW_CALL  = "    prompt = build_prompt(macros, injections, lens_reports, s3_latest, references, s2f_findings)"

if OLD_CALL in content:
    content = content.replace(OLD_CALL, NEW_CALL, 1)
    print("PATCH 4b: s2f_findings passed to build_prompt")
elif "s2f_findings)" in content:
    print("PATCH 4b: already applied")
else:
    print("ERROR: build_prompt call not found")
    exit(1)

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("\nAll patches applied. Verify:")
print("grep -n 'fetch_s2f_findings\\|s2f_findings\\|S2-F PRETENSE' code/lens_forensic_report.py | head -15")
