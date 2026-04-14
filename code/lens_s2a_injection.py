"""
lens_s2a_injection.py — System 2 Position A: Injection Tracer
Project Lens | LENS-009
Model: llama-3.3-70b-versatile (Groq)
Input: lens_reports (Supabase, latest cycle)
Output: injection_reports (Supabase)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional
import ast

from groq import Groq
from supabase import create_client, Client

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-A] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2a")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL          = "llama-3.3-70b-versatile"
MAX_TOKENS     = 1500
TEMPERATURE    = 0.2          # low — we want consistent analytical output
MAX_RETRIES    = 2
RETRY_SLEEP    = 8
MAX_REPORT_CHARS = 6000       # truncate long reports before sending

INJECTION_TYPES = [
    "PHRASE_SYNC",      # same unusual phrase across multiple sources
    "EMOTIONAL_PRIME",  # fear/outrage trigger before factual claim
    "SOURCE_LAUNDER",   # state media claim cited as 'analysts say'
    "FALSE_EQUIV",      # both-sides framing to normalize extreme position
    "OVERTON_SHIFT",    # normalizing positions outside democratic consensus
    "FACT_VOID",        # assertion with zero verifiable basis
]

SYSTEM_PROMPT = """You are S2-A Injection Tracer for Project Lens, an OSINT intelligence system.

Your job: analyze a geopolitical intelligence report and identify narrative injection patterns — 
phrases, framings, or claims that appear to have been deliberately inserted to shape perception 
rather than inform.

You detect 6 injection types:
- PHRASE_SYNC: unusual phrase or framing appearing verbatim/near-verbatim across unrelated sources
- EMOTIONAL_PRIME: emotionally loaded language (fear, outrage, urgency) placed before factual claims to bias interpretation
- SOURCE_LAUNDER: state media or anonymous claim cited as if from neutral/expert source ("analysts say", "observers note")
- FALSE_EQUIV: false balance framing that normalizes an extreme or authoritarian position
- OVERTON_SHIFT: language that gradually normalizes positions outside democratic consensus without justification
- FACT_VOID: confident assertion with no verifiable basis, source, or evidence provided

Rules:
- Only flag patterns you can quote directly from the text
- Confidence score: 0.0–1.0. Only flag if confidence >= 0.5
- Be specific — quote the exact phrase, name the type, explain why it qualifies
- If you find nothing suspicious, return an empty findings array — do NOT invent patterns
- You are an analyst, not a censor. Flag for human review, not for removal.

Respond ONLY with a valid JSON object. No preamble. No explanation outside JSON.

Format:
{
  "analyst": "S2-A",
  "lens_id": "<lens_id provided>",
  "findings": [
    {
      "injection_type": "<one of the 6 types>",
      "flagged_phrase": "<exact quoted text from report>",
      "confidence": <0.0-1.0>,
      "explanation": "<1-2 sentence explanation of why this qualifies>",
      "actor_beneficiary": "<who benefits from this injection, or 'unclear'>"
    }
  ],
  "overall_injection_score": <0.0-1.0>,
  "injection_goal": "<1-2 sentences: what strategic belief is this injection trying to install in S1 and the reader — reverse-engineer the goal>",
  "contamination_contribution": "<SURFACE|MODERATE|DEEP — SURFACE=emotional language only, MODERATE=framing distorted, DEEP=core analytical conclusion compromised>",
  "analyst_note": "<optional 1 sentence summary or empty string>"
}"""


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_groq() -> Groq:
    return Groq(api_key=os.environ["GROQ_API_KEY"])


def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    """Fetch the most recent lens_reports. If cycle given, match it. Else take last 4."""
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
    if not text:
        return ""
    if len(text) > MAX_REPORT_CHARS:
        return text[:MAX_REPORT_CHARS] + "\n[...truncated for analysis]"
    return text


def call_injection_tracer(client: Groq, report: dict) -> Optional[dict]:
    """Call llama-3.3-70b to trace injection patterns in one lens report."""
    report_id = report.get("id", "unknown")
    lens_name = report.get("domain_focus", "unknown")
    report_text = truncate_report(report.get("summary", ""))

    if not report_text.strip():
        log.warning(f"Empty report_text for {report_id} — skipping")
        return None

    user_message = f"""Analyze this intelligence report for narrative injection patterns.

Lens: {lens_name}
Report ID: {report_id}

--- REPORT START ---
{report_text}
--- REPORT END ---

Return JSON only."""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-A calling model for {lens_name} (attempt {attempt})")
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )

            raw = response.choices[0].message.content.strip()

            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            log.info(
                f"S2-A result for {lens_name}: "
                f"{len(parsed.get('findings', []))} findings, "
                f"score={parsed.get('overall_injection_score', 0)}"
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
                log.warning(f"Service unavailable (503) attempt {attempt} — sleeping 15s")
                time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"S2-A failed after {MAX_RETRIES} attempts for {lens_name}")
    return None


def save_injection_report(sb: Client, report: dict, analysis: dict, run_id: str) -> bool:
    """Save one injection analysis result to injection_reports table."""
    findings = analysis.get("findings", [])
    overall_score = analysis.get("overall_injection_score", 0.0)

    # Save one row per finding (or one row with no findings to record clean result)
    rows_to_save = []

    if not findings:
        rows_to_save.append({
            "run_id": run_id,
            "cycle": report.get("cycle"),
            "lens_report_id": report.get("id"),
            "analyst": "S2-A",
            "source_id": None,
            "injection_type": "NONE",
            "evidence": {"analyst_note": analysis.get("analyst_note", "No injection patterns detected")},
            "confidence_score": 0.0,
            "flagged_phrases": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    else:
        for finding in findings:
            rows_to_save.append({
                "run_id": run_id,
                "cycle": report.get("cycle"),
                "lens_report_id": report.get("id"),
                "analyst": "S2-A",
                "source_id": None,
                "injection_type": finding.get("injection_type", "UNKNOWN"),
                "evidence": {
                    "explanation": finding.get("explanation", ""),
                    "actor_beneficiary": finding.get("actor_beneficiary", "unclear"),
                    "overall_score": overall_score,
                    "injection_goal": analysis.get("injection_goal", ""),
                    "contamination_contribution": analysis.get("contamination_contribution", "SURFACE"),
                    "analyst_note": analysis.get("analyst_note", ""),
                },
                "confidence_score": float(finding.get("confidence", 0.0)),
                "flagged_phrases": [finding.get("flagged_phrase", "")],
                "created_at": datetime.now(timezone.utc).isoformat(),
            })

    try:
        result = sb.table("injection_reports").insert(rows_to_save).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} injection_report rows for lens={report.get('domain_focus')}")
        return True
    except Exception as e:
        log.error(f"Failed to save injection_reports: {e}")
        return False


def run_s2a(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    """Main entry point for S2-A Injection Tracer."""
    start = time.time()
    if not run_id:
        run_id = f"s2a_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-A Injection Tracer START | run_id={run_id} | cycle={cycle} ===")

    # ── Init clients ──────────────────────────────────────────────────────────
    try:
        sb = get_supabase()
        groq_client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e), "reports_analyzed": 0}

    # ── Fetch reports ─────────────────────────────────────────────────────────
    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-A cannot run")
        return {"status": "NO_REPORTS", "reports_analyzed": 0}

    # ── Analyze each report ───────────────────────────────────────────────────
    results = []
    total_findings = 0
    saved_count = 0

    for i, report in enumerate(reports):
        log.info(f"Processing report {i+1}/{len(reports)}: {report.get('lens_name')}")

        analysis = call_injection_tracer(groq_client, report)

        if analysis is None:
            log.warning(f"Skipping save — analysis failed for {report.get('lens_name')}")
            results.append({"lens": report.get("lens_name"), "status": "FAILED"})
            continue

        findings_count = len(analysis.get("findings", []))
        total_findings += findings_count

        saved = save_injection_report(sb, report, analysis, run_id)
        if saved:
            saved_count += 1

        results.append({
            "lens": report.get("lens_name"),
            "status": "OK",
            "findings": findings_count,
            "score": analysis.get("overall_injection_score", 0),
        })

        # Stagger between API calls — avoid 429
        if i < len(reports) - 1:
            log.info("Stagger 6s between reports...")
            time.sleep(6)

    elapsed = round(time.time() - start, 1)

    summary = {
        "status": "COMPLETE",
        "run_id": run_id,
        "cycle": cycle,
        "reports_analyzed": len(reports),
        "reports_saved": saved_count,
        "total_findings": total_findings,
        "elapsed_seconds": elapsed,
        "results": results,
    }

    log.info(f"=== S2-A COMPLETE | {len(reports)} reports | {total_findings} findings | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary


# ── Syntax validation target ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_s2a(cycle=cycle_arg)
