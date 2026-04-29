"""
patch_forensic_s2f_full_detail.py
Adds fetch_s2f_full_detail() to lens_forensic_report.py.

This replaces the minimal S2-F section (just finding counts) with:
  A: All drift findings (HIGH/MEDIUM) with full phrasing — 45 days
  B: All article detections last 24h — every op with evidence + reasoning + alt
  C: Cross-lens intersection analysis — which ops appear across multiple lenses
  D: PHI-003 apparatus-people separation score per lens
"""

REPORT_PATH = "code/lens_forensic_report.py"

with open(REPORT_PATH, "r", encoding="utf-8") as f:
    content = f.read()

# ── New fetch function ──
NEW_FETCH = '''
def fetch_s2f_full_detail(sb) -> dict:
    """Fetch full S2-F intelligence: drift findings + per-article operation detail.

    Returns dict with:
      drift_findings: list of HIGH/MEDIUM drift findings (last 45 days)
      detections_24h: list of all article detections (last 24h) with full ops
      cross_lens_ops: dict of op_id -> list of lenses that detected it
      phi003_scores:  dict of lens -> count of OP-024/025/026 (apparatus-people ops)
    """
    from datetime import timedelta
    result = {
        "drift_findings": [],
        "detections_24h": [],
        "cross_lens_ops": {},
        "phi003_scores": {},
    }
    try:
        # A: Drift findings last 45 days
        cutoff_45d = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
        rows = sb.table("lens_drift_findings") \\
            .select("state_actor_lens,finding_confidence,finding_phrasing,"
                    "sample_size,evidence_article_ids,created_at,rubric_version") \\
            .in_("finding_confidence", ["HIGH", "MEDIUM"]) \\
            .gte("created_at", cutoff_45d) \\
            .order("finding_confidence", desc=True) \\
            .order("created_at", desc=True) \\
            .limit(20) \\
            .execute().data or []
        result["drift_findings"] = rows
        log.info(f"S2-F drift: {len(rows)} HIGH/MEDIUM findings in last 45d")
    except Exception as e:
        log.warning(f"S2-F drift fetch failed: {e}")

    try:
        # B: Article detections last 24h with full operation detail
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        rows = sb.table("lens_operation_detections") \\
            .select("raw_article_id,voice_name,voice_type,state_actor_lens,"
                    "stage_filter,operations_detected,operation_count,"
                    "early_warning_count,post_suspect_count,confidence,"
                    "food_for_thought,provider,scored_at") \\
            .gte("scored_at", cutoff_24h) \\
            .eq("not_applicable", False) \\
            .order("confidence", desc=True) \\
            .order("operation_count", desc=True) \\
            .limit(50) \\
            .execute().data or []
        result["detections_24h"] = rows
        log.info(f"S2-F detections: {len(rows)} article detections in last 24h")

        # C: Cross-lens intersection — which ops appear across multiple lenses
        cross_lens = {}
        for det in rows:
            lens = det.get("state_actor_lens", "")
            ops = det.get("operations_detected") or []
            if isinstance(ops, str):
                import json as _json
                try:
                    ops = _json.loads(ops)
                except Exception:
                    ops = []
            for op in ops:
                op_id = op.get("id", "")
                if op_id:
                    if op_id not in cross_lens:
                        cross_lens[op_id] = set()
                    cross_lens[op_id].add(lens)
        # Keep only ops that appear in 2+ lenses
        result["cross_lens_ops"] = {
            k: sorted(v) for k, v in cross_lens.items() if len(v) >= 2
        }

        # D: PHI-003 scores — count of apparatus-people ops per lens
        phi003_ops = {"OP-024", "OP-025", "OP-026"}
        phi003 = {}
        for det in rows:
            lens = det.get("state_actor_lens", "")
            ops = det.get("operations_detected") or []
            if isinstance(ops, str):
                import json as _json
                try:
                    ops = _json.loads(ops)
                except Exception:
                    ops = []
            count = sum(1 for op in ops if op.get("id") in phi003_ops)
            if lens not in phi003:
                phi003[lens] = 0
            phi003[lens] += count
        result["phi003_scores"] = phi003

    except Exception as e:
        log.warning(f"S2-F detection fetch failed: {e}")

    return result

'''

# Insert before fetch_reference_pool
TARGET = "def fetch_reference_pool(sb) -> list:"
if "fetch_s2f_full_detail" in content:
    print("ALREADY PATCHED — fetch_s2f_full_detail exists")
elif TARGET not in content:
    print("ERROR: insertion point not found")
    exit(1)
else:
    content = content.replace(TARGET, NEW_FETCH + TARGET, 1)
    print("PATCH 1: fetch_s2f_full_detail() added")

# ── Update build_prompt to use full detail ──
# Replace old s2f_findings parameter with s2f_detail
OLD_SIG = "                 s2f_findings: list = None) -> str:"
NEW_SIG = "                 s2f_findings: list = None,\n                 s2f_detail: dict = None) -> str:"

if OLD_SIG in content:
    content = content.replace(OLD_SIG, NEW_SIG, 1)
    print("PATCH 2: build_prompt signature updated")
elif "s2f_detail: dict = None" in content:
    print("PATCH 2: already applied")
else:
    print("ERROR: build_prompt signature not found")

# ── Replace old S2-F section in prompt with full detail section ──
OLD_S2F_BLOCK = """    # ── S2-F Pretense Operation Findings (Watch/Clarity/Verification) ──
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
        pass  # s2f_text already set above
    else:
        s2f_text = "S2-F PRETENSE OPERATION FINDINGS: No Watch/Clarity/Verification findings yet (S2-F pipeline is new — findings will accumulate over 7-45 days).\\n"
    # ── Reference pool (Path B) ──"""

NEW_S2F_BLOCK = """    # ── S2-F Full Intelligence (A: Drift findings + B: Detection detail + C: Cross-lens + D: PHI-003) ──
    import json as _s2f_json
    s2f_text = "S2-F PRETENSE OPERATION INTELLIGENCE:\\n"
    s2f_text += "=" * 60 + "\\n\\n"

    detail = s2f_detail or {}
    drift = detail.get("drift_findings", [])
    detections = detail.get("detections_24h", [])
    cross_lens = detail.get("cross_lens_ops", {})
    phi003 = detail.get("phi003_scores", {})

    # A: Drift findings
    if drift:
        s2f_text += f"PART A — PATTERN FINDINGS ({len(drift)} HIGH/MEDIUM confirmed patterns, last 45 days):\\n\\n"
        for f in drift:
            lens = f.get("state_actor_lens", "unknown")
            conf = f.get("finding_confidence", "?")
            phrasing = (f.get("finding_phrasing") or "")
            sample = f.get("sample_size", 0)
            created = (f.get("created_at") or "")[:10]
            s2f_text += f"[{conf}] {lens} | {sample} articles | {created}\\n"
            s2f_text += f"{phrasing}\\n\\n"
    else:
        s2f_text += "PART A — PATTERN FINDINGS: None yet (pipeline accumulates over 7-45 days).\\n\\n"

    # B: Per-article operation detail
    if detections:
        s2f_text += f"PART B — OPERATION DETAIL ({len(detections)} article detections, last 24h):\\n\\n"
        for det in detections:
            lens = det.get("state_actor_lens", "")
            voice = det.get("voice_name", "")
            stage = det.get("stage_filter", "")
            conf = det.get("confidence", 0)
            ops = det.get("operations_detected") or []
            fft = det.get("food_for_thought", "")
            if isinstance(ops, str):
                try:
                    ops = _s2f_json.loads(ops)
                except Exception:
                    ops = []
            s2f_text += f"DETECTION: {lens} | {voice} | {stage} | conf={conf:.2f} | {len(ops)} ops\\n"
            for op in ops:
                op_id = op.get("id", "")
                op_name = op.get("name", "")
                evidence = (op.get("evidence_phrase") or "")[:200]
                reasoning = (op.get("reasoning") or "")[:300]
                alt = ""
                alts = op.get("alternative_hypotheses_considered") or []
                if alts:
                    alt = alts[0][:150] if isinstance(alts[0], str) else ""
                s2f_text += f"  {op_id}: {op_name}\\n"
                s2f_text += f"    EVIDENCE: {evidence}\\n"
                s2f_text += f"    REASONING: {reasoning}\\n"
                if alt:
                    s2f_text += f"    ALT: {alt}\\n"
            if fft:
                s2f_text += f"  FOOD FOR THOUGHT: {fft}\\n"
            s2f_text += "\\n"
    else:
        s2f_text += "PART B — OPERATION DETAIL: No article detections in last 24h yet.\\n\\n"

    # C: Cross-lens intersection
    if cross_lens:
        s2f_text += f"PART C — CROSS-LENS OPERATIONS ({len(cross_lens)} ops detected across multiple lenses):\\n"
        for op_id, lenses in sorted(cross_lens.items()):
            s2f_text += f"  {op_id}: detected in {', '.join(lenses)}\\n"
        s2f_text += "\\n"
    else:
        s2f_text += "PART C — CROSS-LENS: No cross-lens patterns yet.\\n\\n"

    # D: PHI-003 apparatus-people separation scores
    if phi003:
        s2f_text += "PART D — PHI-003 APPARATUS-PEOPLE SEPARATION (OP-024/025/026 counts per lens):\\n"
        for lens, count in sorted(phi003.items()):
            flag = " ⚠️ HIGH" if count >= 3 else ""
            s2f_text += f"  {lens}: {count} apparatus-collapse detections{flag}\\n"
        s2f_text += "\\n"

    # ── Reference pool (Path B) ──"""

if OLD_S2F_BLOCK in content:
    content = content.replace(OLD_S2F_BLOCK, NEW_S2F_BLOCK, 1)
    print("PATCH 3: S2-F full detail section replaced")
elif "PART A — PATTERN FINDINGS" in content:
    print("PATCH 3: already applied")
else:
    print("ERROR: S2-F block not found — manual check needed")

# ── Update run_forensic_report to fetch + pass full detail ──
OLD_FETCH = "    s2f_findings = fetch_s2f_findings(sb)\n    references = fetch_reference_pool(sb)"
NEW_FETCH2 = "    s2f_findings = fetch_s2f_findings(sb)\n    s2f_detail = fetch_s2f_full_detail(sb)\n    references = fetch_reference_pool(sb)"

if OLD_FETCH in content:
    content = content.replace(OLD_FETCH, NEW_FETCH2, 1)
    print("PATCH 4a: s2f_detail fetched in run_forensic_report")
elif "s2f_detail = fetch_s2f_full_detail" in content:
    print("PATCH 4a: already applied")
else:
    print("ERROR: fetch insertion point not found")

OLD_CALL = "    prompt = build_prompt(macros, injections, lens_reports, s3_latest, references, s2f_findings)"
NEW_CALL2 = "    prompt = build_prompt(macros, injections, lens_reports, s3_latest, references, s2f_findings, s2f_detail)"

if OLD_CALL in content:
    content = content.replace(OLD_CALL, NEW_CALL2, 1)
    print("PATCH 4b: s2f_detail passed to build_prompt")
elif "s2f_detail)" in content:
    print("PATCH 4b: already applied")
else:
    print("ERROR: build_prompt call not found")

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("\nAll patches applied.")
print("Verify: grep -n 'fetch_s2f_full_detail\\|PART A\\|PART B\\|PART C\\|PART D' code/lens_forensic_report.py | head -10")
