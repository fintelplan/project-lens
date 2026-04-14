"""
lens_s2c_emotion.py — System 2 Position C: Emotion Decoder
Project Lens | LENS-009
Model: mistral-small-latest (Mistral free tier)
Input: lens_reports (latest cycle)
Output: injection_reports (analyst='S2-C')
Decodes: PRIME → TRIGGER → FRAME → DELIVER → ANCHOR sequence
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from mistralai import Mistral
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-C] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2c")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL            = "mistral-small-latest"
MAX_TOKENS       = 1500
TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_REPORT_CHARS = 6000

# The 5-step emotional manipulation sequence
EMOTION_STEPS = ["PRIME", "TRIGGER", "FRAME", "DELIVER", "ANCHOR"]

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
A report with 1-2 steps may simply be standard journalism.

Rules:
- Quote the specific phrase from the report that demonstrates each step
- Score sequence_completeness: steps_found / 5
- emotion_target: the primary emotion being activated (fear, anger, pride, urgency, despair, hope)
- manipulation_score: 0.0-1.0 — how engineered does this feel vs natural reporting
- Only flag manipulation_score >= 0.4 as significant
- If the report is neutral factual reporting with no emotional architecture, say so clearly

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
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_mistral() -> Mistral:
    return Mistral(api_key=os.environ["MISTRAL_API_KEY"])


def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        if cycle:
            result = sb.table("lens_reports") \
                .select("id, lens_name, report_text, cycle, created_at, source_id") \
                .eq("cycle", cycle) \
                .order("created_at", desc=True) \
                .limit(8) \
                .execute()
        else:
            result = sb.table("lens_reports") \
                .select("id, lens_name, report_text, cycle, created_at, source_id") \
                .order("created_at", desc=True) \
                .limit(4) \
                .execute()

        reports = result.data or []
        log.info(f"Fetched {len(reports)} lens reports (cycle={cycle})")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch lens_reports: {e}")
        return []


def truncate_report(text: str) -> str:
    if not text:
        return ""
    if len(text) > MAX_REPORT_CHARS:
        return text[:MAX_REPORT_CHARS] + "\n[...truncated]"
    return text


def call_emotion_decoder(client: Mistral, report: dict) -> Optional[dict]:
    """Call mistral-small to decode emotional sequence in one lens report."""
    report_id  = report.get("id", "unknown")
    lens_name  = report.get("lens_name", "unknown")
    report_text = truncate_report(report.get("report_text", ""))

    if not report_text.strip():
        log.warning(f"Empty report_text for {report_id} — skipping")
        return None

    user_message = (
        f"Decode the emotional architecture of this intelligence report.\n\n"
        f"Lens: {lens_name}\n"
        f"Report ID: {report_id}\n\n"
        f"--- REPORT START ---\n{report_text}\n--- REPORT END ---\n\n"
        f"Return JSON only."
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-C calling mistral for {lens_name} (attempt {attempt})")
            response = client.chat.complete(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_message},
                ],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )

            raw = response.choices[0].message.content.strip()

            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            log.info(
                f"S2-C result for {lens_name}: "
                f"steps={parsed.get('steps_found', 0)}/5, "
                f"emotion={parsed.get('emotion_target', '?')}, "
                f"manipulation={parsed.get('manipulation_score', 0)}"
            )
            return parsed

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                log.warning(f"Rate limit (429) attempt {attempt} — sleeping 20s")
                time.sleep(20)
            elif "503" in err:
                log.warning(f"503 attempt {attempt} — sleeping 15s")
                time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"S2-C failed after {MAX_RETRIES} attempts for {lens_name}")
    return None


def save_emotion_report(
    sb: Client,
    report: dict,
    analysis: dict,
    run_id: str
) -> bool:
    """Save S2-C emotion decode result to injection_reports table."""
    manipulation_score = float(analysis.get("manipulation_score", 0.0))
    steps_found        = int(analysis.get("steps_found", 0))
    sequence           = analysis.get("sequence", {})

    # Build flagged_phrases from steps that are present
    flagged = [
        v.get("quote", "")
        for v in sequence.values()
        if v.get("present") and v.get("quote")
    ]

    injection_type = "EMOTION_SEQUENCE" if steps_found >= 3 else "NONE"

    row = {
        "run_id":          run_id,
        "cycle":           report.get("cycle"),
        "lens_report_id":  report.get("id"),
        "analyst":         "S2-C",
        "source_id":       report.get("source_id"),
        "injection_type":  injection_type,
        "evidence": {
            "sequence":              sequence,
            "steps_found":           steps_found,
            "sequence_completeness": analysis.get("sequence_completeness", 0),
            "emotion_target":        analysis.get("emotion_target", "neutral"),
            "audience_posture":      analysis.get("intended_audience_posture", ""),
            "analyst_note":          analysis.get("analyst_note", ""),
        },
        "confidence_score": manipulation_score,
        "flagged_phrases":  flagged,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = sb.table("injection_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-C row for lens={report.get('lens_name')}")
        return True
    except Exception as e:
        log.error(f"Failed to save S2-C result: {e}")
        return False


def run_s2c(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    """Main entry point for S2-C Emotion Decoder."""
    start = time.time()
    if not run_id:
        run_id = f"s2c_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-C Emotion Decoder START | run_id={run_id} | cycle={cycle} ===")

    try:
        sb     = get_supabase()
        client = get_mistral()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-C cannot run")
        return {"status": "NO_REPORTS", "reports_analyzed": 0}

    results      = []
    saved_count  = 0
    total_steps  = 0

    for i, report in enumerate(reports):
        log.info(f"Processing {i+1}/{len(reports)}: {report.get('lens_name')}")

        analysis = call_emotion_decoder(client, report)
        if analysis is None:
            results.append({"lens": report.get("lens_name"), "status": "FAILED"})
            continue

        total_steps += int(analysis.get("steps_found", 0))
        saved = save_emotion_report(sb, report, analysis, run_id)
        if saved:
            saved_count += 1

        results.append({
            "lens":              report.get("lens_name"),
            "status":            "OK",
            "steps_found":       analysis.get("steps_found", 0),
            "emotion_target":    analysis.get("emotion_target", "neutral"),
            "manipulation_score": analysis.get("manipulation_score", 0),
        })

        if i < len(reports) - 1:
            log.info("Stagger 6s...")
            time.sleep(6)

    elapsed = round(time.time() - start, 1)

    summary = {
        "status":           "COMPLETE",
        "run_id":           run_id,
        "cycle":            cycle,
        "reports_analyzed": len(reports),
        "reports_saved":    saved_count,
        "total_steps_found": total_steps,
        "elapsed_seconds":  elapsed,
        "results":          results,
    }

    log.info(f"=== S2-C COMPLETE | {len(reports)} reports | "
             f"total steps={total_steps} | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_s2c(cycle=cycle_arg)
