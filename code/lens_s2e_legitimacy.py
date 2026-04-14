"""
lens_s2e_legitimacy_v3.py — System 2 Position E: Legitimacy Filter
Project Lens | LENS-009
Model: llama-3.1-8b-instant (Groq — GROQ_S2_API_KEY)
UPDATED: moved to Groq S2 — Cerebras model strings unavailable
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from groq import Groq
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [S2-E] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("s2e")

MODEL            = "llama-3.1-8b-instant"
MAX_TOKENS       = 2000
TEMPERATURE      = 0.15
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_REPORT_CHARS = 5000

SYSTEM_PROMPT = """You are S2-E Legitimacy Filter for Project Lens, an OSINT intelligence system.

Your job: 
Step 1 — Extract all STATE ACTORS (governments, heads of state, state institutions, military commands) named in the intelligence report.
Step 2 — For each actor, apply a 6-point democratic legitimacy assessment.

The 6 dimensions:
- ELECTORAL: Does this actor hold power through genuinely free and fair elections? (0=no/autocratic, 1=yes/democratic)
- RULE_OF_LAW: Is there an independent judiciary and legal system? (0=no, 1=yes)
- PRESS_FREEDOM: Is independent media permitted and operating? (0=no, 1=yes)
- CIVIL_SOCIETY: Can independent NGOs and civil society organizations operate freely? (0=no, 1=yes)
- ACCOUNTABILITY: Are there functioning checks and balances? (0=no, 1=yes)
- INTERNATIONAL_NORMS: Does this actor generally comply with UN charter and international law? (0=no/violator, 1=yes/compliant)

Scoring: each dimension 0.0 / 0.5 / 1.0
legitimacy_score = average of 6 dimensions
legitimacy_tier: HIGH (>=0.7), MIXED (0.4-0.69), LOW (<0.4)

Rules:
- Only assess STATE ACTORS — skip individuals, corporations, NGOs
- Use Freedom House and V-Dem as implicit reference frameworks
- Flag actors with LOW legitimacy who are pushing narratives in the report

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-E",
  "lens_id": "<provided>",
  "domain_focus": "<provided>",
  "actors_assessed": [
    {
      "actor_name": "<n>",
      "actor_type": "<government|military|institution|leader>",
      "dimensions": {
        "ELECTORAL":           {"score": <0.0-1.0>, "note": "<brief>"},
        "RULE_OF_LAW":         {"score": <0.0-1.0>, "note": "<brief>"},
        "PRESS_FREEDOM":       {"score": <0.0-1.0>, "note": "<brief>"},
        "CIVIL_SOCIETY":       {"score": <0.0-1.0>, "note": "<brief>"},
        "ACCOUNTABILITY":      {"score": <0.0-1.0>, "note": "<brief>"},
        "INTERNATIONAL_NORMS": {"score": <0.0-1.0>, "note": "<brief>"}
      },
      "legitimacy_score": <0.0-1.0>,
      "legitimacy_tier": "<HIGH|MIXED|LOW>",
      "narrative_role_in_report": "<how this actor is framed>"
    }
  ],
  "low_legitimacy_actors_pushing_narrative": ["<actor names with LOW tier>"],
  "legitimacy_gap_signal": "<description or 'none'>",
  "analyst_note": "<1 sentence or empty string>"
}"""


def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

def get_groq_s2() -> Groq:
    return Groq(api_key=os.environ["GROQ_S2_API_KEY"])

def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        if cycle:
            result = sb.table("lens_reports").select("id, domain_focus, summary, cycle, generated_at").eq("cycle", cycle).order("generated_at", desc=True).limit(8).execute()
        else:
            result = sb.table("lens_reports").select("id, domain_focus, summary, cycle, generated_at").order("generated_at", desc=True).limit(4).execute()
        reports = result.data or []
        log.info(f"Fetched {len(reports)} lens reports (cycle={cycle})")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch lens_reports: {e}"); return []

def truncate_report(text: str) -> str:
    if not text: return ""
    return text[:MAX_REPORT_CHARS] + "\n[...truncated]" if len(text) > MAX_REPORT_CHARS else text

def call_legitimacy_filter(client: Groq, report: dict) -> Optional[dict]:
    report_id = report.get("id", "unknown")
    domain_focus = report.get("domain_focus", "unknown")
    summary = truncate_report(report.get("summary", ""))
    if not summary.strip():
        log.warning(f"Empty summary for {report_id} — skipping"); return None
    user_message = f"Extract state actors and apply the 6-point legitimacy assessment.\n\nLens: {domain_focus}\nReport ID: {report_id}\n\n--- REPORT START ---\n{summary}\n--- REPORT END ---\n\nReturn JSON only."
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-E calling Groq for {domain_focus} (attempt {attempt})")
            response = client.chat.completions.create(model=MODEL, messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_message}], max_tokens=MAX_TOKENS, temperature=TEMPERATURE)
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            parsed = json.loads(raw)
            actors = parsed.get("actors_assessed", [])
            low_actors = parsed.get("low_legitimacy_actors_pushing_narrative", [])
            log.info(f"S2-E result for {domain_focus}: {len(actors)} actors, {len(low_actors)} LOW flagged")
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
    log.error(f"S2-E failed after {MAX_RETRIES} attempts for {domain_focus}"); return None

def save_legitimacy_report(sb: Client, report: dict, analysis: dict, run_id: str) -> bool:
    actors = analysis.get("actors_assessed", [])
    low_actors = analysis.get("low_legitimacy_actors_pushing_narrative", [])
    gap_signal = analysis.get("legitimacy_gap_signal", "")
    injection_type = "LEGITIMACY_GAP" if low_actors else "NONE"
    row = {"run_id": run_id, "cycle": report.get("cycle"), "lens_report_id": report.get("id"), "analyst": "S2-E", "source_id": None, "injection_type": injection_type, "evidence": {"actors_assessed": actors, "low_legitimacy_actors": low_actors, "legitimacy_gap_signal": gap_signal, "analyst_note": analysis.get("analyst_note", ""), "total_actors": len(actors), "provider": "Groq-S2"}, "confidence_score": min(len(low_actors) / max(len(actors), 1), 1.0), "flagged_phrases": low_actors[:10], "generated_at": datetime.now(timezone.utc).isoformat()}
    try:
        result = sb.table("injection_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-E row for lens={report.get('domain_focus')}"); return True
    except Exception as e:
        log.error(f"Failed to save S2-E result: {e}"); return False

def run_s2e(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id: run_id = f"s2e_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    log.info(f"=== S2-E Legitimacy Filter START | run_id={run_id} | cycle={cycle} ===")
    try:
        sb = get_supabase(); client = get_groq_s2()
    except Exception as e:
        log.error(f"Client init failed: {e}"); return {"status": "ERROR", "error": str(e)}
    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-E cannot run"); return {"status": "NO_REPORTS", "reports_analyzed": 0}
    results = []; saved_count = 0; total_low = 0
    for i, report in enumerate(reports):
        log.info(f"Processing {i+1}/{len(reports)}: {report.get('domain_focus')}")
        analysis = call_legitimacy_filter(client, report)
        if analysis is None:
            results.append({"lens": report.get("domain_focus"), "status": "FAILED"}); continue
        low_actors = analysis.get("low_legitimacy_actors_pushing_narrative", [])
        total_low += len(low_actors)
        saved = save_legitimacy_report(sb, report, analysis, run_id)
        if saved: saved_count += 1
        results.append({"lens": report.get("domain_focus"), "status": "OK", "actors_found": len(analysis.get("actors_assessed", [])), "low_legit": len(low_actors), "gap_signal": analysis.get("legitimacy_gap_signal", "none")})
        if i < len(reports) - 1:
            log.info("Stagger 6s..."); time.sleep(6)
    elapsed = round(time.time() - start, 1)
    summary = {"status": "COMPLETE", "run_id": run_id, "cycle": cycle, "reports_analyzed": len(reports), "reports_saved": saved_count, "total_low_legitimacy_actors": total_low, "elapsed_seconds": elapsed, "results": results}
    log.info(f"=== S2-E COMPLETE | {len(reports)} reports | {total_low} LOW legitimacy actors | {elapsed}s ===")
    print(json.dumps(summary, indent=2)); return summary

if __name__ == "__main__":
    import sys
    run_s2e(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
