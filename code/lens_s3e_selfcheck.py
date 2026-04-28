"""
lens_s3e_selfcheck.py — System 3 Position E: Self-Check LOCAL
Project Lens | LENS-020
Model: llama3.1:70b (Ollama LOCAL — no API key, no quota, no cost)
Reads: lens_reports + injection_reports + lens_system3_reports (last 7 days)
Output: lens_system3_reports (position=S3-E, report_type=TYPE_E)

Purpose: Independent adversarial audit of Project Lens's own outputs.
         PHI-002: even our own system can have blind spots and biases.
         Ask: what is our system missing? Where are we over-confident?
         Where does our framing itself carry bias?

Design principles:
  - LOCAL only — privacy, no data sent to external APIs
  - Weekly cadence (Wednesday + Saturday)
  - Ollama endpoint: localhost:11434
  - Timeout: 600s (70B model is slow on CPU/RAM)

Session: LENS-020
"""

import os, json, time, logging, requests
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-E] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-E")

SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY")
OLLAMA_HOST   = os.environ.get("OLLAMA_HOST", "localhost:11434")
MODEL         = "llama3.1:70b"
LOOKBACK_DAYS = 7
MAX_REPORTS   = 15
TIMEOUT_SEC   = 600   # 70B is slow on RAM — allow 10 min per call

SYSTEM_PROMPT = """You are S3-E: Self-Check Auditor for Project Lens.

Your job is to audit Project Lens's OWN outputs for blind spots, over-confidence,
and systemic bias. You are the system's internal critic.

PHI-002 foundation: even a pro-people system can develop blind spots. The most
dangerous biases are the ones we don't know we have. Your job is to find them.

PHI-003 discipline: check whether our system correctly separates apparatus from
peoples in its outputs. Does it conflate Xi Office with Chinese people? Does it
apply asymmetric standards across different legitimacy categories?

FIVE AUDIT QUESTIONS:
1. BLIND SPOTS: What topics, actors, or regions are systematically underrepresented
   in our S1 outputs? What are we not seeing?
2. OVER-CONFIDENCE: Where is our system claiming HIGH confidence with insufficient
   evidence? Where does S2 flag injections that may actually be legitimate reporting?
3. FRAMING AUDIT: Does our own framing carry directional bias? Are we applying
   PHI-002 consistently across all actors, or showing asymmetric scrutiny?
4. ECHO CHAMBER CHECK: Are S1, S2, and S3 outputs reinforcing each other in ways
   that could amplify a shared blind spot? Where do they agree too easily?
5. MISSING COUNTER-NARRATIVE: What is the strongest case AGAINST our system's
   main findings this week? Who would disagree, and why might they be right?

PHI-003 SPECIFIC CHECK:
- Does our system name apparatus correctly (Xi Office, not China as people)?
- Are legitimacy categories applied consistently (elected-bounded vs unelected-indefinite)?
- Do we show asymmetric scrutiny of any actor compared to others in similar positions?

OUTPUT FORMAT — valid JSON only:
{
  "blind_spots": [
    {
      "topic_or_region": "what is underrepresented",
      "evidence_of_absence": "why we think this is missing not just quiet",
      "significance": "why this matters for PHI-002 mission"
    }
  ],
  "overconfidence_flags": [
    {
      "claim": "specific S1/S2/S3 claim that seems overconfident",
      "why_overconfident": "what evidence is missing or alternative exists",
      "suggested_confidence": "LOW|MEDIUM vs claimed HIGH"
    }
  ],
  "framing_audit": {
    "asymmetric_scrutiny_detected": false,
    "actors_with_heavier_scrutiny": [],
    "actors_with_lighter_scrutiny": [],
    "phi002_consistency_score": 0.0,
    "notes": "specific framing observations"
  },
  "echo_chamber_risks": [
    {
      "pattern": "where S1/S2/S3 agree suspiciously",
      "risk": "what shared blind spot this might indicate"
    }
  ],
  "strongest_counter_narrative": {
    "argument": "best case against our main findings",
    "who_would_make_it": "which perspective or actor",
    "strength": "LOW|MEDIUM|HIGH"
  },
  "phi003_check": {
    "apparatus_people_separation": "CORRECT|INCONSISTENT|MIXED",
    "legitimacy_asymmetry_detected": false,
    "specific_violations": []
  },
  "corrections_to_s1_s2_s3": [
    {
      "target": "S1|S2|S3-A|S3-B|S3-D",
      "correction": "what should be reconsidered",
      "reason": "why from adversarial audit perspective"
    }
  ],
  "summary": "2-3 sentence honest assessment of system quality this week",
  "quality_score": 0.0,
  "overall_system_health": "HEALTHY|MINOR_DRIFT|MAJOR_BLIND_SPOT|CRITICAL"
}

Be ruthlessly honest. This is an internal audit, not a performance review.
Identify real problems, not theoretical ones. Ground every finding in the
actual outputs provided."""


def should_run_today() -> bool:
    """S3-E runs weekly: Wednesday (2) and Saturday (5)."""
    return date.today().weekday() in (2, 5)


def already_ran_this_week(sb: Client) -> bool:
    """Skip if S3-E already ran in last 3 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    r = sb.table("lens_system3_reports") \
        .select("id") \
        .eq("position", "S3-E") \
        .gte("generated_at", cutoff) \
        .limit(1).execute()
    return bool(r.data)


def check_ollama_available() -> bool:
    """Check if Ollama is running and model is available."""
    try:
        r = requests.get(f"http://{OLLAMA_HOST}/api/tags", timeout=10)
        if r.status_code != 200:
            return False
        models = [m["name"] for m in r.json().get("models", [])]
        available = any("llama3.1" in m and "70b" in m for m in models)
        if not available:
            log.warning(f"llama3.1:70b not in Ollama models: {models}")
        return available
    except Exception as e:
        log.warning(f"Ollama not reachable at {OLLAMA_HOST}: {e}")
        return False


def call_ollama(prompt: str) -> Optional[str]:
    """Call Ollama API with long timeout for 70B model."""
    try:
        r = requests.post(
            f"http://{OLLAMA_HOST}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 3000},
            },
            timeout=TIMEOUT_SEC,
        )
        if r.status_code == 200:
            return r.json().get("message", {}).get("content", "")
        log.error(f"Ollama returned {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        log.error(f"Ollama call failed: {e}")
        return None


def fetch_all_reports(sb: Client) -> tuple[list, list, dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    s1 = sb.table("lens_reports") \
        .select("domain_focus,summary,cycle,generated_at,quality_score,food_for_thought") \
        .gte("generated_at", cutoff).order("generated_at", desc=False) \
        .limit(MAX_REPORTS).execute().data or []
    s2 = sb.table("injection_reports") \
        .select("analyst,injection_type,evidence,confidence_score,flagged_phrases,created_at") \
        .gte("created_at", cutoff).order("confidence_score", desc=True) \
        .limit(MAX_REPORTS).execute().data or []
    s3 = {}
    for pos in ("S3-A", "S3-B", "S3-D"):
        try:
            r = sb.table("lens_system3_reports") \
                .select("position,summary,patterns_found,generated_at,quality_score") \
                .eq("position", pos) \
                .order("generated_at", desc=True).limit(1).execute()
            if r.data:
                s3[pos] = r.data[0]
        except Exception:
            pass
    return s1, s2, s3


def build_prompt(s1: list, s2: list, s3: dict) -> str:
    lines = ["=== PROJECT LENS OUTPUTS TO AUDIT — last 7 days ===\n"]
    lines.append(f"--- S1 LENS REPORTS ({len(s1)} reports) ---")
    for r in s1:
        lines.append(f"Date: {r.get('generated_at','')[:10]} | Domain: {r.get('domain_focus')}")
        lines.append(f"Summary: {(r.get('summary') or '')[:400]}")
        fft = r.get("food_for_thought", "")
        if fft:
            lines.append(f"Food for Thought: {fft[:200]}")
        lines.append("─" * 40)
    lines.append(f"\n--- S2 INJECTION FINDINGS ({len(s2)} findings) ---")
    for r in s2:
        lines.append(f"Analyst: {r.get('analyst')} | Type: {r.get('injection_type')} | Score: {r.get('confidence_score')}")
        lines.append(f"Evidence: {str(r.get('evidence') or '')[:200]}")
    if s3:
        lines.append(f"\n--- S3 PATTERN INTELLIGENCE ({len(s3)} positions) ---")
        for pos, row in s3.items():
            lines.append(f"{pos}: {(row.get('summary') or '')[:300]}")
    lines.append("\nNow audit these outputs. Find blind spots, overconfidence, framing bias. Output JSON only.")
    return "\n".join(lines)


def run_s3e(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-E Self-Check LOCAL START | run_id={run_id} ===")

    if not should_run_today():
        log.info("S3-E cadence: not Wed/Sat — skipping")
        return {"status": "SKIPPED_CADENCE", "run_id": run_id}

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if already_ran_this_week(sb):
        log.info("S3-E already ran this week — skipping")
        return {"status": "SKIPPED", "run_id": run_id}

    if not check_ollama_available():
        log.error(f"Ollama not available at {OLLAMA_HOST} or {MODEL} not pulled")
        log.error("Run: ollama pull llama3.1:70b")
        return {"status": "ERROR_NO_OLLAMA", "run_id": run_id}

    s1, s2, s3 = fetch_all_reports(sb)
    log.info(f"Fetched {len(s1)} S1 + {len(s2)} S2 + {len(s3)} S3 reports")

    if not s1 and not s2:
        log.warning("No reports to audit")
        return {"status": "NO_REPORTS", "run_id": run_id}

    prompt = build_prompt(s1, s2, s3)
    log.info(f"Prompt: {len(prompt)} chars | Calling {MODEL} via Ollama (timeout={TIMEOUT_SEC}s)")
    log.info("Note: 70B model on RAM — expect 3-10 min per call")

    raw = call_ollama(prompt)
    if not raw:
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    # Parse JSON
    try:
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        analysis = json.loads(raw.strip())
    except Exception as e:
        log.error(f"JSON parse failed: {e}")
        return {"status": "PARSE_FAILED", "run_id": run_id}

    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-E",
        "report_type":       "TYPE_E",
        "time_horizon":      "7_DAY_AUDIT",
        "patterns_found":    json.dumps(analysis.get("blind_spots", [])),
        "structural_trends": json.dumps({
            "framing_audit":          analysis.get("framing_audit", {}),
            "echo_chamber_risks":     analysis.get("echo_chamber_risks", []),
            "overconfidence_flags":   analysis.get("overconfidence_flags", []),
            "phi003_check":           analysis.get("phi003_check", {}),
            "overall_system_health":  analysis.get("overall_system_health", "UNKNOWN"),
        }),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("corrections_to_s1_s2_s3", [])),
        "corrections_to_s2": json.dumps(analysis.get("strongest_counter_narrative", {})),
        "model_used":        MODEL,
        "provider":          "ollama_local",
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-E",
        "source_reports":    json.dumps([]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)

    health = analysis.get("overall_system_health", "UNKNOWN")
    log.info(f"=== S3-E COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s | health={health} ===")

    print(json.dumps({
        "status":         "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":         run_id,
        "system_health":  health,
        "blind_spots":    len(analysis.get("blind_spots", [])),
        "quality":        analysis.get("quality_score", 0),
        "elapsed":        elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3e(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
