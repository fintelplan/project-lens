"""
lens_s2c_emotion_v4.py — System 2 Position C: Emotion Decoder
Project Lens | LENS-009
Model: llama-3.3-70b (SambaNova — SAMBANOVA_API_KEY)
UPDATED: switched to SambaNova for provider diversity
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional
import requests

from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [S2-C] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("s2c")

MODEL            = "Meta-Llama-3.3-70B-Instruct"
SAMBANOVA_URL    = "https://api.sambanova.ai/v1/chat/completions"
MAX_TOKENS       = 1500
TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_REPORT_CHARS = 6000

SYSTEM_PROMPT = """You are S2-C Emotion Decoder for Project Lens, an OSINT intelligence system.

Your job: analyze a geopolitical intelligence report and decode its emotional architecture —
the sequence of psychological steps used to move a reader from neutral to a desired emotional 
or behavioral state.

You decode a 5-step sequence when present:
- PRIME: establishes an emotional baseline — anxiety, pride, fear, anger, hope — before any facts arrive
- TRIGGER: introduces the threat, enemy, crisis, or shock event that activates the primed emotion
- FRAME: assigns responsibility — who caused this, who is the villain, who must act
- DELIVER: the payload — the specific belief, attitude, or action the reader is being guided toward
- ANCHOR: locks in the emotional state — repetition, identity language, us-vs-them, calls to action

Not every report will have all 5 steps. Map only the steps that are genuinely present.
A report with 5/5 steps clearly present is a strong signal of deliberate emotional engineering.

Rules:
- Quote the specific phrase from the report that demonstrates each step
- sequence_completeness: steps_found / 5
- manipulation_score: 0.0-1.0
- Only flag manipulation_score >= 0.4 as significant

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-C",
  "lens_id": "<provided>",
  "lens_name": "<provided>",
  "sequence": {
    "PRIME":   {"present": true/false, "quote": "<exact quote or empty>", "note": "<brief note>"},
    "TRIGGER": {"present": true/false, "quote": "<exact quote or empty>", "note": "<brief note>"},
    "FRAME":   {"present": true/false, "quote": "<exact quote or empty>", "note": "<brief note>"},
    "DELIVER": {"present": true/false, "quote": "<exact quote or empty>", "note": "<brief note>"},
    "ANCHOR":  {"present": true/false, "quote": "<exact quote or empty>", "note": "<brief note>"}
  },
  "steps_found": <0-5>,
  "sequence_completeness": <0.0-1.0>,
  "emotion_target": "<primary emotion or 'neutral'>",
  "intended_audience_posture": "<what emotional posture is the reader meant to adopt>",
  "manipulation_score": <0.0-1.0>,
  "analyst_note": "<1 sentence summary or empty string>"
}"""


def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

def call_sambanova(messages: list) -> str:
    api_key = os.environ["SAMBANOVA_API_KEY"]
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": MODEL, "messages": messages, "max_tokens": MAX_TOKENS, "temperature": TEMPERATURE}
    response = requests.post(SAMBANOVA_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]

def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        if cycle:
            result = sb.table("lens_reports").select("id, lens_name, report_text, cycle, created_at, source_id").eq("cycle", cycle).order("created_at", desc=True).limit(8).execute()
        else:
            result = sb.table("lens_reports").select("id, lens_name, report_text, cycle, created_at, source_id").order("created_at", desc=True).limit(4).execute()
        reports = result.data or []
        log.info(f"Fetched {len(reports)} lens reports (cycle={cycle})")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch lens_reports: {e}"); return []

def truncate_report(text: str) -> str:
    if not text: return ""
    return text[:MAX_REPORT_CHARS] + "\n[...truncated]" if len(text) > MAX_REPORT_CHARS else text

def call_emotion_decoder(report: dict) -> Optional[dict]:
    report_id = report.get("id", "unknown")
    lens_name = report.get("lens_name", "unknown")
    report_text = truncate_report(report.get("report_text", ""))
    if not report_text.strip():
        log.warning(f"Empty report_text for {report_id} — skipping"); return None
    user_message = f"Decode the emotional architecture of this intelligence report.\n\nLens: {lens_name}\nReport ID: {report_id}\n\n--- REPORT START ---\n{report_text}\n--- REPORT END ---\n\nReturn JSON only."
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-C calling SambaNova for {lens_name} (attempt {attempt})")
            raw = call_sambanova([{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_message}])
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            parsed = json.loads(raw)
            log.info(f"S2-C result for {lens_name}: steps={parsed.get('steps_found', 0)}/5, emotion={parsed.get('emotion_target', '?')}, manipulation={parsed.get('manipulation_score', 0)}")
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
    log.error(f"S2-C failed after {MAX_RETRIES} attempts for {lens_name}"); return None

def save_emotion_report(sb: Client, report: dict, analysis: dict, run_id: str) -> bool:
    manipulation_score = float(analysis.get("manipulation_score", 0.0))
    steps_found = int(analysis.get("steps_found", 0))
    sequence = analysis.get("sequence", {})
    flagged = [v.get("quote", "") for v in sequence.values() if v.get("present") and v.get("quote")]
    injection_type = "EMOTION_SEQUENCE" if steps_found >= 3 else "NONE"
    row = {"run_id": run_id, "cycle": report.get("cycle"), "lens_report_id": report.get("id"), "analyst": "S2-C", "source_id": report.get("source_id"), "injection_type": injection_type, "evidence": {"sequence": sequence, "steps_found": steps_found, "sequence_completeness": analysis.get("sequence_completeness", 0), "emotion_target": analysis.get("emotion_target", "neutral"), "audience_posture": analysis.get("intended_audience_posture", ""), "analyst_note": analysis.get("analyst_note", ""), "provider": "SambaNova"}, "confidence_score": manipulation_score, "flagged_phrases": flagged, "created_at": datetime.now(timezone.utc).isoformat()}
    try:
        result = sb.table("injection_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-C row for lens={report.get('lens_name')}"); return True
    except Exception as e:
        log.error(f"Failed to save S2-C result: {e}"); return False

def run_s2c(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id: run_id = f"s2c_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    log.info(f"=== S2-C Emotion Decoder START | run_id={run_id} | cycle={cycle} ===")
    try:
        sb = get_supabase()
    except Exception as e:
        log.error(f"Client init failed: {e}"); return {"status": "ERROR", "error": str(e)}
    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-C cannot run"); return {"status": "NO_REPORTS", "reports_analyzed": 0}
    results = []; saved_count = 0; total_steps = 0
    for i, report in enumerate(reports):
        log.info(f"Processing {i+1}/{len(reports)}: {report.get('lens_name')}")
        analysis = call_emotion_decoder(report)
        if analysis is None:
            results.append({"lens": report.get("lens_name"), "status": "FAILED"}); continue
        total_steps += int(analysis.get("steps_found", 0))
        saved = save_emotion_report(sb, report, analysis, run_id)
        if saved: saved_count += 1
        results.append({"lens": report.get("lens_name"), "status": "OK", "steps_found": analysis.get("steps_found", 0), "emotion_target": analysis.get("emotion_target", "neutral"), "manipulation_score": analysis.get("manipulation_score", 0)})
        if i < len(reports) - 1:
            log.info("Stagger 6s..."); time.sleep(6)
    elapsed = round(time.time() - start, 1)
    summary = {"status": "COMPLETE", "run_id": run_id, "cycle": cycle, "reports_analyzed": len(reports), "reports_saved": saved_count, "total_steps_found": total_steps, "elapsed_seconds": elapsed, "results": results}
    log.info(f"=== S2-C COMPLETE | {len(reports)} reports | total steps={total_steps} | {elapsed}s ===")
    print(json.dumps(summary, indent=2)); return summary

if __name__ == "__main__":
    import sys
    run_s2c(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
