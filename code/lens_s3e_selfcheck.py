"""
lens_s3e_selfcheck.py — System 3 Position E: Self-Check
Project Lens | LENS-010
Model: llama-3.3-70b (SambaNova — SAMBANOVA_API_KEY)
Hardware: RDU chips — 3rd hardware type (Groq=LPU, Cerebras=WSE, SambaNova=RDU)
Input: lens_reports (last 10 runs) + injection_reports (last 30 days) + lens_system3_reports
Output: lens_system3_reports (position=S3-E, report_type=SELF_CHECK)

PURPOSE — Defend against Pattern 5: Recursive Self-Injection
  "Run 1: articles say 'weaponized.' Lens 1 produces 'power weaponized.'
   Run 2: Lens 1 reads its own prior output. Confirms it.
   Run 10: established intelligence trend."
  Defense: compare current analysis to last 10 runs.
  Increasing confidence without new evidence = STOP. Issue correction.

WHY SAMBANOVA:
  Maximum epistemic independence from other positions.
  RDU hardware = different execution path from Groq LPU and Cerebras WSE.
  Key already in secrets. Persistent free tier — no daily token limit.
  SAMBANOVA_API_KEY already in GitHub Secrets.

NOTE: S3-E replaced the original Ollama LOCAL design.
  LOCAL = air-gapped epistemic independence.
  SambaNova = different hardware + different infrastructure + different company.
  Best available cloud alternative to local deployment.
  LR-065: epistemic independence through provider separation, not just key separation.

Cadence: DAILY (runs once per day, skips if ran in last 20h)
Session: LENS-010 (LENS-011 for pre-flight guard addition)
"""

import os, json, time, logging, re, httpx
from datetime import datetime, timezone, timedelta
from typing import Optional
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-E] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-E")

SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY")
SAMBANOVA_KEY = os.environ.get("SAMBANOVA_API_KEY")
SAMBANOVA_URL = "https://api.sambanova.ai/v1/chat/completions"
MODEL         = "Meta-Llama-3.3-70B-Instruct"
LOOKBACK_RUNS = 10
LOOKBACK_DAYS = 30

SYSTEM_PROMPT = """You are S3-E: Self-Check for Project Lens.

Your sole purpose: detect Pattern 5 — Recursive Self-Injection.

The mechanism you are hunting:
  Run 1: External source uses loaded word "weaponized"
  Lens 1 absorbs it, produces "power weaponized"
  Stored in Supabase as analysis
  Run 2: Lens 1 reads its own prior output as context
  Confirms the framing. Confidence rises.
  Run 10: "weaponized power" is established intelligence trend
  No new external evidence — just the system confirming itself

This is invisible drift. It never looks wrong from inside.
You are outside. You read the last 10 runs simultaneously and find it.

FIVE CHECKS:

1. CONFIDENCE WITHOUT EVIDENCE:
   Is any signal's confidence score INCREASING across multiple runs
   WITHOUT new external events or sources driving it?
   Increasing confidence from self-confirmation = Pattern 5 active.

2. VOCABULARY DRIFT:
   Are specific loaded words appearing in S1 summaries that were NOT
   in earlier runs? Once introduced, do they persist and spread?
   Track exact word: first appearance run → current run.

3. FRAMING LOCK:
   Has the analytical framing on any topic become fixed?
   If every run produces the same conclusion on the same topic
   regardless of new evidence — the frame is locked from prior output.

4. S2 BLIND SPOT FORMATION:
   Are S2 injection reports consistently MISSING a specific injection type
   that appears repeatedly? Consistent absence = S2 drift, not clean data.

5. CROSS-LENS SYNCHRONIZATION:
   Are all 4 lenses producing more similar outputs over time?
   Convergence without external cause = recursive confirmation loop.

OUTPUT ONLY valid JSON:
{
  "pattern5_detected": true/false,
  "confidence": 0.0,
  "checks": {
    "confidence_without_evidence": {"detected": true/false, "evidence": "..."},
    "vocabulary_drift": {"detected": true/false, "words": ["word1"], "first_seen_run": "..."},
    "framing_lock": {"detected": true/false, "topic": "...", "locked_frame": "..."},
    "s2_blind_spot": {"detected": true/false, "missing_type": "..."},
    "cross_lens_sync": {"detected": true/false, "convergence_evidence": "..."}
  },
  "drift_severity": "NONE|LOW|MODERATE|HIGH|CRITICAL",
  "corrections_to_all_systems": [
    {"system": "S1/S2/S3", "correction": "...", "mandatory": true/false}
  ],
  "vocabulary_to_audit": ["word1", "word2"],
  "summary": "2-3 sentence plain English verdict on system drift state",
  "quality_score": 0.0
}

Rules:
- NONE drift = clean, no recursive patterns detected
- LOW = minor vocabulary persistence, no confidence drift
- MODERATE = vocabulary drift confirmed, 1-2 checks triggered
- HIGH = confidence drift confirmed, framing lock detected
- CRITICAL = multiple checks triggered, mandatory corrections required
Ground everything in specific runs and specific text from the reports provided.
Never assume drift without evidence. Never dismiss evidence without explanation."""


def already_ran_today(sb: Client) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat()
    r = sb.table("lens_system3_reports") \
        .select("id").eq("position", "S3-E") \
        .gte("generated_at", cutoff).limit(1).execute()
    return bool(r.data)


def fetch_s1_history(sb: Client) -> list:
    """Fetch last N lens reports for drift analysis."""
    r = sb.table("lens_reports") \
        .select("id,domain_focus,summary,quality_score,generated_at,cycle") \
        .order("generated_at", desc=True).limit(LOOKBACK_RUNS).execute()
    return list(reversed(r.data or []))  # chronological order for drift detection


def fetch_s2_history(sb: Client) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    r = sb.table("injection_reports") \
        .select("analyst,injection_type,evidence,confidence_score,flagged_phrases,created_at") \
        .gte("created_at", cutoff).order("created_at", desc=False).limit(20).execute()
    return r.data or []


def fetch_s3_history(sb: Client) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    r = sb.table("lens_system3_reports") \
        .select("position,summary,quality_score,generated_at") \
        .gte("generated_at", cutoff).order("generated_at", desc=False).limit(10).execute()
    return r.data or []


def build_prompt(s1: list, s2: list, s3: list) -> str:
    lines = [
        f"=== S1 LENS REPORTS — last {len(s1)} runs IN CHRONOLOGICAL ORDER ===",
        "CRITICAL: Read these in order. Detect drift across the sequence.\n",
        "─" * 60,
    ]
    for i, r in enumerate(s1, 1):
        lines += [
            f"\nRUN {i} | Date: {r.get('generated_at','')[:10]} | Domain: {r.get('domain_focus')} | Quality: {r.get('quality_score','?')}",
            f"Summary: {(r.get('summary') or '')[:500]}",
            "─" * 40,
        ]

    if s2:
        lines += [
            f"\n=== S2 INJECTION REPORTS — last {LOOKBACK_DAYS} days ({len(s2)} reports) ===",
            "Look for MISSING injection types — consistent absence = S2 drift\n",
        ]
        from collections import Counter
        type_counts = Counter(r.get('injection_type', '?') for r in s2)
        lines.append(f"Injection type distribution: {dict(type_counts)}")
        for r in s2[-5:]:
            lines.append(f"  {r.get('created_at','')[:10]} | {r.get('analyst')} | {r.get('injection_type')} | score={r.get('confidence_score')}")

    if s3:
        lines += [
            f"\n=== S3 PRIOR REPORTS — last {LOOKBACK_DAYS} days ({len(s3)} reports) ===",
        ]
        for r in s3:
            lines.append(f"  {r.get('generated_at','')[:10]} | {r.get('position')} | q={r.get('quality_score')} | {(r.get('summary') or '')[:150]}")

    lines.append("\nNow run all 5 Pattern 5 checks. Output JSON only.")
    return "\n".join(lines)


def call_sambanova(prompt: str) -> Optional[dict]:
    """Call SambaNova API — OpenAI-compatible endpoint."""
    headers = {
        "Authorization": f"Bearer {SAMBANOVA_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 2000,
    }

    for attempt in range(1, 3):
        try:
            log.info(f"S3-E calling {MODEL} via SambaNova (attempt {attempt})")
            resp = httpx.post(SAMBANOVA_URL, headers=headers, json=payload, timeout=90.0)
            if resp.status_code == 429:
                log.warning(f"SambaNova 429 — sleeping 30s")
                time.sleep(30)
                continue
            resp.raise_for_status()
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            # Strip code fences
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            return json.loads(raw)
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse failed: {e}")
            return None
        except Exception as e:
            log.warning(f"Attempt {attempt} failed: {e}")
            if attempt < 2: time.sleep(20)

    return None


def run_s3e(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-E Self-Check START | run_id={run_id} | provider=SambaNova/RDU ===")

    if not SAMBANOVA_KEY:
        log.error("SAMBANOVA_API_KEY not set")
        return {"status": "ERROR", "run_id": run_id}

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if already_ran_today(sb):
        log.info("S3-E already ran in last 20h — skipping (daily cadence)")
        return {"status": "SKIPPED", "run_id": run_id}

    s1 = fetch_s1_history(sb)
    s2 = fetch_s2_history(sb)
    s3 = fetch_s3_history(sb)
    log.info(f"Fetched {len(s1)} S1 runs + {len(s2)} S2 reports + {len(s3)} S3 reports")

    if not s1:
        log.warning("No S1 history — skipping")
        return {"status": "NO_REPORTS", "run_id": run_id}

    prompt = build_prompt(s1, s2, s3)
    log.info(f"Prompt: {len(prompt)} chars")

    analysis = call_sambanova(prompt)
    if not analysis:
        log.error("S3-E analysis failed")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    severity = analysis.get("drift_severity", "UNKNOWN")
    pattern5 = analysis.get("pattern5_detected", False)
    log.info(f"Pattern 5 detected: {pattern5} | Severity: {severity}")

    # Log corrections if HIGH or CRITICAL
    corrections = analysis.get("corrections_to_all_systems", [])
    mandatory = [c for c in corrections if c.get("mandatory")]
    if mandatory:
        log.warning(f"MANDATORY corrections issued: {len(mandatory)}")
        for c in mandatory:
            log.warning(f"  [{c.get('system')}] {c.get('correction','')[:100]}")

    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-E",
        "report_type":       "SELF_CHECK",
        "time_horizon":      "10_RUNS",
        "patterns_found":    json.dumps(analysis.get("checks", {})),
        "structural_trends": json.dumps({
            "pattern5_detected": pattern5,
            "drift_severity":    severity,
            "vocabulary_audit":  analysis.get("vocabulary_to_audit", []),
        }),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("vocabulary_to_audit", [])),
        "corrections_to_s2": json.dumps(corrections),
        "model_used":        MODEL,
        "provider":          "sambanova",
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-E",
        "source_reports":    json.dumps([r.get("id") for r in s1[-5:]]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)
    log.info(f"=== S3-E COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s ===")

    print(json.dumps({
        "status":        "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":        run_id,
        "pattern5":      pattern5,
        "severity":      severity,
        "corrections":   len(corrections),
        "mandatory":     len(mandatory),
        "quality":       analysis.get("quality_score", 0),
        "elapsed":       elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3e(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
