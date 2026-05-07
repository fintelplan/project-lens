"""
lens_s3c_biasdrift.py — System 3 Position C: Bias Drift Monitor
Project Lens | LENS-020
Model: command-r-plus (Cohere — COHERE_API_KEY)
Reads: lens_reports + injection_reports + lens_framing_scores (last 30 days)
Output: lens_system3_reports (position=S3-C, report_type=TYPE_C)

Purpose: Detect systematic drift in how topics are framed across outlets over time.
         PHI-002 alignment: bias drift is how pretense becomes normalized.
         Weekly cadence — only runs on Monday and Thursday.

Session: LENS-020
"""

import os, json, time, logging
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-C] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-C")

SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY")
COHERE_KEY    = os.environ.get("COHERE_API_KEY")
MODEL         = "command-r-plus-08-2024"
LOOKBACK_DAYS = 30
MAX_REPORTS   = 40

SYSTEM_PROMPT = """You are S3-C: Bias Drift Monitor for Project Lens.

Your job: detect systematic drift in how topics are framed across outlets and voices over time.

PHI-002 foundation: bias drift is how pretense becomes normalized. A single biased article
is noise. The same bias appearing consistently across multiple sources over weeks is a signal.
That is the difference between a writer's opinion and a coordinated framing campaign.

PHI-003 discipline: You separate apparatus from peoples. Bias in how outlets frame
"China's government" is NOT bias about "Chinese people." Always name the apparatus,
never the people.

FIVE ANALYTICAL QUESTIONS:
1. DRIFT DETECTION: Which topics show consistent directional framing shifts over 30 days?
   (e.g., consistently framing Actor X as defensive, Actor Y as aggressive)
2. NORMALIZATION: Which narratives that were contested 30 days ago are now treated as facts?
3. ABSENCE DRIFT: Which topics or voices have systematically disappeared from coverage?
4. VOCABULARY SHIFT: Which terms have replaced neutral alternatives? Who uses them?
5. CROSS-SOURCE COHERENCE: Which framing patterns appear independently across multiple
   ideologically different outlets? (High coherence = possible coordination)

ACH CHECK: What is the strongest alternative explanation for the drift you detected?
Could editorial fashion, genuine events, or platform incentives explain it without
requiring coordination?

OUTPUT FORMAT — valid JSON only:
{
  "drift_patterns": [
    {
      "topic": "topic or actor being framed",
      "direction": "direction of drift (e.g., increasingly defensive, increasingly threatening)",
      "evidence": "specific examples from reports",
      "outlets_showing_pattern": ["outlet1", "outlet2"],
      "confidence": 0.0,
      "started_approximately": "rough timeframe",
      "phi002_significance": "what this means for people's cognitive sovereignty"
    }
  ],
  "normalized_narratives": [
    {
      "narrative": "narrative now treated as fact",
      "was_contested": "what was contested about it 30 days ago",
      "who_normalized": "which outlets or voices drove normalization"
    }
  ],
  "disappeared_topics": [
    {
      "topic": "topic that has gone quiet",
      "last_seen_approximately": "when it last appeared",
      "significance": "why its absence matters"
    }
  ],
  "vocabulary_shifts": [
    {
      "new_term": "term now appearing frequently",
      "replaced": "what it replaced",
      "loaded_direction": "what political/emotional direction it carries"
    }
  ],
  "cross_source_coherence": {
    "detected": false,
    "pattern": "description of coherent pattern across sources, or null",
    "sources_involved": [],
    "organic_or_coordinated": "ORGANIC|POSSIBLY_COORDINATED|UNCLEAR"
  },
  "ach_check": {
    "strongest_alternative": "best alternative explanation for drifts detected",
    "confidence_in_drift_over_noise": 0.0
  },
  "corrections_to_s2": [
    {
      "correction": "what S2 injection detection may have missed due to gradual normalization",
      "reason": "why gradual drift evades single-article injection detection"
    }
  ],
  "summary": "2-3 sentence plain English summary of bias drift landscape",
  "quality_score": 0.0,
  "signals_to_watch": ["signal1", "signal2", "signal3"]
}

Rules:
- Ground every drift claim in SPECIFIC evidence from the reports provided
- Never assert coordination without strong cross-source evidence
- Always include the ACH alternative explanation
- PHI-003: name apparatus (Xi Office, Trump Office) never peoples"""


def should_run_today() -> bool:
    """S3-C runs weekly: Monday (0) and Thursday (3) only."""
    today = date.today().weekday()
    return today in (0, 3)


def already_ran_this_week(sb: Client) -> bool:
    """Skip if S3-C already ran in last 3 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    r = sb.table("lens_system3_reports") \
        .select("id") \
        .eq("position", "S3-C") \
        .gte("generated_at", cutoff) \
        .limit(1).execute()
    return bool(r.data)


def fetch_reports(sb: Client) -> tuple[list, list]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    s1 = sb.table("lens_reports") \
        .select("domain_focus,summary,cycle,generated_at,quality_score") \
        .gte("generated_at", cutoff) \
        .order("generated_at", desc=False) \
        .limit(MAX_REPORTS).execute().data or []
    s2 = sb.table("injection_reports") \
        .select("analyst,injection_type,evidence,confidence_score,flagged_phrases,created_at") \
        .gte("created_at", cutoff) \
        .order("created_at", desc=False) \
        .limit(MAX_REPORTS).execute().data or []
    return s1, s2


def build_prompt(s1: list, s2: list) -> str:
    lines = [
        f"=== S1 LENS REPORTS — last {LOOKBACK_DAYS} days ({len(s1)} reports) ===\n",
        "Analyze these for systematic framing drift across the 30-day window.\n",
        "─" * 60,
    ]
    for r in s1:
        lines += [
            f"\nDate: {r.get('generated_at','')[:10]} | Cycle: {r.get('cycle')} | Domain: {r.get('domain_focus')}",
            f"Summary: {(r.get('summary') or '')[:400]}",
            "─" * 40,
        ]
    if s2:
        lines += [f"\n=== S2 INJECTION FINDINGS — last {LOOKBACK_DAYS} days ({len(s2)} reports) ===\n"]
        for r in s2:
            lines += [
                f"Type: {r.get('injection_type')} | Score: {r.get('confidence_score')} | Date: {r.get('created_at','')[:10]}",
                f"Evidence: {str(r.get('evidence') or '')[:200]}",
            ]
    lines.append("\nDetect systematic bias drift across this 30-day window. Output JSON only.")
    return "\n".join(lines)


def run_s3c(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-C Bias Drift Monitor START | run_id={run_id} ===")

    # Cadence check — weekly only
    if not should_run_today():
        log.info("S3-C cadence: not Mon/Thu — skipping")
        return {"status": "SKIPPED_CADENCE", "run_id": run_id}

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if already_ran_this_week(sb):
        log.info("S3-C already ran this week — skipping")
        return {"status": "SKIPPED", "run_id": run_id}

    if not COHERE_KEY:
        log.error("COHERE_API_KEY not set")
        return {"status": "ERROR", "run_id": run_id}

    try:
        import cohere
    except ImportError:
        log.error("cohere SDK not installed — pip install cohere")
        return {"status": "ERROR", "run_id": run_id}

    s1, s2 = fetch_reports(sb)
    log.info(f"Fetched {len(s1)} S1 reports + {len(s2)} S2 reports (30-day window)")

    if not s1:
        log.warning("No S1 reports found")
        return {"status": "NO_REPORTS", "run_id": run_id}

    prompt = build_prompt(s1, s2)
    log.info(f"Prompt: {len(prompt)} chars | Model: {MODEL}")

    analysis = None
    client = cohere.Client(api_key=COHERE_KEY)

    for attempt in range(1, 3):
        try:
            log.info(f"S3-C calling {MODEL} (attempt {attempt})")
            response = client.chat(
                model=MODEL,
                message=prompt,
                preamble=SYSTEM_PROMPT,
                temperature=0.3,
                max_tokens=3000,
            )
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            analysis = json.loads(raw)
            log.info(f"S3-C response parsed: {len(raw)} chars")
            break
        except Exception as e:
            log.warning(f"Attempt {attempt} failed: {e}")
            if attempt < 2:
                time.sleep(20)

    if not analysis:
        log.error("S3-C failed — no analysis produced")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    # Save to lens_system3_reports
    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-C",
        "report_type":       "TYPE_C",
        "time_horizon":      "30_DAY",
        "patterns_found":    json.dumps(analysis.get("drift_patterns", [])),
        "structural_trends": json.dumps({
            "normalized_narratives":   analysis.get("normalized_narratives", []),
            "disappeared_topics":      analysis.get("disappeared_topics", []),
            "vocabulary_shifts":       analysis.get("vocabulary_shifts", []),
            "cross_source_coherence":  analysis.get("cross_source_coherence", {}),
        }),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("signals_to_watch", [])),
        "corrections_to_s2": json.dumps(analysis.get("corrections_to_s2", [])),
        "model_used":        MODEL,
        "provider":          "cohere",
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-C",
        "source_reports":    json.dumps([r.get("id","") for r in s1[:5]]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)

    log.info(f"=== S3-C COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s ===")
    log.info(f"Drift summary: {analysis.get('summary','')[:120]}")

    print(json.dumps({
        "status":        "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":        run_id,
        "drift_patterns": len(analysis.get("drift_patterns", [])),
        "quality":       analysis.get("quality_score", 0),
        "elapsed":       elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3c(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
