"""
lens_s2b_coordination.py — System 2 Position B: Coordination Analyzer
Project Lens | LENS-009
Model: llama-3.3-70b-versatile (Groq — GROQ_S2_API_KEY)
UPDATED: moved from Gemini to Groq S2 — Gemini quota unreliable on free tier
Input: lens_reports (latest cycle) — ALL reports in ONE call
Output: injection_reports (analyst='S2-B')
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from groq import Groq
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [S2-B] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("s2b")

MODEL            = "llama-3.3-70b-versatile"
MAX_TOKENS       = 2000
TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_REPORT_CHARS = 8000
MAX_TOTAL_CHARS  = 24000

SYSTEM_PROMPT = """You are S2-B Coordination Analyzer for Project Lens, an OSINT intelligence system.

Your job: analyze MULTIPLE geopolitical intelligence reports simultaneously and detect
cross-source coordination patterns — signs that different sources are pushing the same
narrative in a coordinated way, whether intentionally or through shared influence.

You detect 4 coordination signal types:
- TIMING_SYNC: multiple ideologically different sources emphasize the same story/angle in the same time window
- VOCAB_MIRROR: an unusual word, phrase, or framing appears across sources that would not normally use the same language
- STRUCTURAL_MIRROR: different sources follow the same narrative arc (establish threat → name villain → demand response)
- COORDINATED_SILENCE: a significant topic conspicuously absent across sources in a way suggesting deliberate omission

Rules:
- Analyze ACROSS reports, not within a single report
- Only flag patterns spanning at least 2 different sources/lenses
- Quote specific phrases from each report demonstrating the pattern
- Confidence 0.0-1.0. Only flag if confidence >= 0.5
- If no coordination found, return empty findings — do NOT invent patterns

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-B",
  "reports_analyzed": <number>,
  "findings": [
    {
      "coordination_type": "<TIMING_SYNC|VOCAB_MIRROR|STRUCTURAL_MIRROR|COORDINATED_SILENCE>",
      "sources_involved": ["<lens_name_1>", "<lens_name_2>"],
      "evidence": {
        "source_1_quote": "<exact quote from first report>",
        "source_2_quote": "<exact quote from second report>",
        "pattern_description": "<1-2 sentences explaining the pattern>"
      },
      "confidence": <0.0-1.0>,
      "actor_beneficiary": "<who benefits or 'unclear'>"
    }
  ],
  "overall_coordination_score": <0.0-1.0>,
  "dominant_coordinated_narrative": "<1 sentence summary or 'none detected'>",
  "analyst_note": "<optional 1 sentence or empty string>"
}"""


def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

def get_groq() -> Groq:
    return Groq(api_key=os.environ["GROQ_S2_API_KEY"])

def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        if cycle:
            result = sb.table("lens_reports") \
                .select("id, domain_focus, summary, cycle, generated_at") \
                .eq("cycle", cycle) \
                .order("generated_at", desc=True) \
                .limit(8) \
                .execute()
        else:
            result = sb.table("lens_reports") \
                .select("id, domain_focus, summary, cycle, generated_at") \
                .order("generated_at", desc=True) \
                .limit(4) \
                .execute()
        reports = result.data or []
        log.info(f"Fetched {len(reports)} lens reports (cycle={cycle})")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch lens_reports: {e}")
        return []

def truncate_report(text: str) -> str:
    if not text: return ""
    return text[:MAX_REPORT_CHARS] + "\n[...truncated]" if len(text) > MAX_REPORT_CHARS else text

def build_multi_report_prompt(reports: list[dict]) -> str:
    sections = []
    total_chars = 0
    for i, r in enumerate(reports, 1):
        text = truncate_report(r.get("summary", ""))
        entry = f"=== REPORT {i}: {r.get('domain_focus', 'Unknown')} (ID: {r.get('id', 'unknown')}) ===\n{text}\n"
        if total_chars + len(entry) > MAX_TOTAL_CHARS:
            break
        sections.append(entry)
        total_chars += len(entry)
    combined = "\n".join(sections)
    return f"Analyze {len(reports)} intelligence reports for cross-source coordination patterns.\n\n{combined}\n\nReturn JSON only."

def call_coordination_analyzer(client: Groq, reports: list[dict]) -> Optional[dict]:
    if len(reports) < 2:
        log.warning("Need at least 2 reports for coordination analysis")
        return None
    prompt = build_multi_report_prompt(reports)
    log.info(f"S2-B sending {len(reports)} reports to Groq ({len(prompt)} chars)")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-B calling model (attempt {attempt})")
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            parsed = json.loads(raw)
            log.info(f"S2-B result: {len(parsed.get('findings', []))} findings, score={parsed.get('overall_coordination_score', 0)}")
            return parsed
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                log.warning(f"Rate limit attempt {attempt} — sleeping 20s"); time.sleep(20)
            elif "503" in err:
                log.warning(f"503 attempt {attempt} — sleeping 15s"); time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_SLEEP)
    log.error(f"S2-B failed after {MAX_RETRIES} attempts")
    return None

def save_coordination_report(sb: Client, reports: list[dict], analysis: dict, run_id: str, cycle: Optional[str]) -> bool:
    findings = analysis.get("findings", [])
    overall_score = analysis.get("overall_coordination_score", 0.0)
    dominant = analysis.get("dominant_coordinated_narrative", "none detected")
    rows = []
    if not findings:
        rows.append({"run_id": run_id, "cycle": cycle, "lens_report_id": None, "analyst": "S2-B", "source_id": None, "injection_type": "NONE", "evidence": {"analyst_note": analysis.get("analyst_note", "No coordination detected"), "reports_analyzed": len(reports), "dominant_narrative": dominant}, "confidence_score": 0.0, "flagged_phrases": [], "created_at": datetime.now(timezone.utc).isoformat()})
    else:
        for finding in findings:
            evidence = finding.get("evidence", {})
            rows.append({"run_id": run_id, "cycle": cycle, "lens_report_id": None, "analyst": "S2-B", "source_id": None, "injection_type": finding.get("coordination_type", "UNKNOWN"), "evidence": {"sources_involved": finding.get("sources_involved", []), "source_1_quote": evidence.get("source_1_quote", ""), "source_2_quote": evidence.get("source_2_quote", ""), "pattern_description": evidence.get("pattern_description", ""), "actor_beneficiary": finding.get("actor_beneficiary", "unclear"), "overall_score": overall_score, "dominant_narrative": dominant, "analyst_note": analysis.get("analyst_note", "")}, "confidence_score": float(finding.get("confidence", 0.0)), "flagged_phrases": [evidence.get("source_1_quote", ""), evidence.get("source_2_quote", "")], "created_at": datetime.now(timezone.utc).isoformat()})
    try:
        result = sb.table("injection_reports").insert(rows).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-B coordination rows")
        return True
    except Exception as e:
        log.error(f"Failed to save S2-B results: {e}"); return False

def run_s2b(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id: run_id = f"s2b_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    log.info(f"=== S2-B Coordination Analyzer START | run_id={run_id} | cycle={cycle} ===")
    try:
        sb = get_supabase(); client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}"); return {"status": "ERROR", "error": str(e)}
    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-B cannot run"); return {"status": "NO_REPORTS", "reports_analyzed": 0}
    if len(reports) < 2:
        log.warning(f"Only {len(reports)} report — need 2+ for coordination analysis"); return {"status": "INSUFFICIENT_REPORTS", "reports_analyzed": len(reports)}
    analysis = call_coordination_analyzer(client, reports)
    if analysis is None:
        return {"status": "ANALYSIS_FAILED", "reports_analyzed": len(reports)}
    saved = save_coordination_report(sb, reports, analysis, run_id, cycle)
    elapsed = round(time.time() - start, 1)
    summary = {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id, "cycle": cycle, "reports_analyzed": len(reports), "findings": len(analysis.get("findings", [])), "overall_coordination_score": analysis.get("overall_coordination_score", 0), "dominant_narrative": analysis.get("dominant_coordinated_narrative", "none detected"), "elapsed_seconds": elapsed}
    log.info(f"=== S2-B COMPLETE | {len(reports)} reports | {summary['findings']} findings | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary

if __name__ == "__main__":
    import sys
    run_s2b(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
