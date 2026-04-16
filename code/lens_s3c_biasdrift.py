"""
lens_s3c_biasdrift.py
S3-C: Bias Drift Monitor — Capability 3

Detects how S1 AND S2 analytical framing changes over time.
Without this: zero defense against AI Bias Drift (Capability 3).

Model:   command-r-plus (Cohere) — retrieval-augmented architecture
Reads:   lens_reports last 30 days + injection_reports last 30 days
Detects: vocabulary drift, framing lock, confidence without evidence,
         analytical convergence across lenses
Cadence: weekly (skip if ran in last 6 days)
Writes:  lens_system3_reports (position=S3-C, report_type=DRIFT_MONITOR)

PHI-002: S3 matures freely. S4 receives this as food for thought.
"""

import os, sys, json, time, logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import cohere
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-C] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-C")

MODEL       = "command-r-plus"
CADENCE_DAYS = 6          # skip if ran within last 6 days
LOOKBACK_DAYS = 30        # analyze last 30 days
MAX_REPORTS   = 120       # cap for context window
MAX_INJECTION = 80

# ── Supabase + Cohere clients ─────────────────────────────────────────────────
def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
    return create_client(url, key)

def get_cohere():
    api_key = os.environ.get("COHERE_API_KEY", "")
    if not api_key:
        raise RuntimeError("COHERE_API_KEY missing")
    return cohere.ClientV2(api_key=api_key)

# ── Cadence check ─────────────────────────────────────────────────────────────
def already_ran_recently(sb: Client) -> bool:
    """Skip if S3-C ran within last CADENCE_DAYS days."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=CADENCE_DAYS)).isoformat()
        r = sb.table("lens_system3_reports") \
            .select("id, generated_at") \
            .eq("position", "S3-C") \
            .gte("generated_at", cutoff) \
            .limit(1).execute()
        if r.data:
            last = r.data[0].get("generated_at", "?")[:16]
            log.info(f"S3-C ran recently ({last}) — skipping this cycle (cadence={CADENCE_DAYS}d)")
            return True
        return False
    except Exception as e:
        log.warning(f"Cadence check failed: {e} — proceeding anyway")
        return False

# ── Data fetch ────────────────────────────────────────────────────────────────
def fetch_s1_history(sb: Client) -> list:
    """Fetch last 30 days of S1 lens reports for vocabulary + framing analysis."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
        r = sb.table("lens_reports") \
            .select("id, domain_focus, summary, cycle, generated_at, confidence_score") \
            .gte("generated_at", cutoff) \
            .order("generated_at", desc=False) \
            .limit(MAX_REPORTS).execute()
        reports = r.data or []
        log.info(f"Fetched {len(reports)} S1 reports (last {LOOKBACK_DAYS} days)")
        return reports
    except Exception as e:
        log.error(f"S1 history fetch failed: {e}")
        return []

def fetch_s2_history(sb: Client) -> list:
    """Fetch last 30 days of S2 injection reports for drift pattern analysis."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
        r = sb.table("injection_reports") \
            .select("id, analyst, injection_type, confidence_score, flagged_phrases, created_at") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=False) \
            .limit(MAX_INJECTION).execute()
        reports = r.data or []
        log.info(f"Fetched {len(reports)} S2 injection reports (last {LOOKBACK_DAYS} days)")
        return reports
    except Exception as e:
        log.error(f"S2 history fetch failed: {e}")
        return []

# ── Prompt builder ────────────────────────────────────────────────────────────
def build_drift_prompt(s1_reports: list, s2_reports: list) -> str:
    """Build the drift analysis prompt for command-r-plus."""

    # Summarize S1 by time period — early vs recent
    mid = len(s1_reports) // 2
    early_s1  = s1_reports[:mid]
    recent_s1 = s1_reports[mid:]

    def summarize_reports(reports, label):
        lines = [f"\n--- {label} ({len(reports)} reports) ---"]
        for r in reports[-20:]:  # cap at 20 per period
            conf  = r.get("confidence_score", "?")
            focus = r.get("domain_focus", "")[:60]
            summ  = (r.get("summary", "") or "")[:300]
            lines.append(f"[{r.get('cycle','?')}] {focus} | conf={conf}\n{summ}\n")
        return "\n".join(lines)

    def summarize_injections(reports):
        lines = [f"\n--- S2 Injection History ({len(reports)} reports) ---"]
        for r in reports[-20:]:
            analyst = r.get("analyst", "?")
            itype   = r.get("injection_type", "?")
            conf    = r.get("confidence_score", "?")
            phrases = ", ".join((r.get("flagged_phrases") or [])[:3])
            lines.append(f"[{analyst}] {itype} conf={conf} | phrases: {phrases}")
        return "\n".join(lines)

    s1_early_text   = summarize_reports(early_s1,  "EARLY PERIOD (first 15 days)")
    s1_recent_text  = summarize_reports(recent_s1, "RECENT PERIOD (last 15 days)")
    s2_inject_text  = summarize_injections(s2_reports)

    prompt = f"""You are S3-C: Bias Drift Monitor for Project Lens.

Your job is to detect how the analytical framing of System 1 and System 2 has changed over the last 30 days.

This is NOT about whether individual reports are correct.
It is about whether the SYSTEM AS A WHOLE is drifting in a particular analytical direction.

SYSTEM 1 REPORTS — EARLY PERIOD vs RECENT PERIOD:
{s1_early_text}

{s1_recent_text}

SYSTEM 2 INJECTION REPORTS:
{s2_inject_text}

ANALYZE FOR THESE DRIFT PATTERNS:

1. VOCABULARY DRIFT:
   Are specific words or phrases appearing more frequently in recent reports vs early reports?
   Which terms are increasing? Which are decreasing?
   Does this suggest a framing shift?

2. FRAMING LOCK:
   Is System 1 becoming repetitive — applying the same analytical frame to different events?
   Are lenses starting to sound similar to each other?
   Is the same causal structure appearing across unrelated events?

3. CONFIDENCE WITHOUT EVIDENCE:
   Are confidence scores increasing over time WITHOUT corresponding increases in source diversity?
   Is System 1 becoming MORE certain without new hard data supporting it?

4. ANALYTICAL CONVERGENCE:
   Are all 4 lenses converging on the same conclusions more often recently vs before?
   (Note: this is a WARNING sign in adversarial environments — see Pattern 2)

5. S2 INJECTION PATTERN SHIFT:
   Is System 2 detecting the same injection types repeatedly?
   Could System 2 itself be developing a detection bias?

Respond ONLY with valid JSON — no markdown, no preamble:
{{
  "drift_detected": true/false,
  "severity": "NONE|LOW|MODERATE|HIGH",
  "vocabulary_drift": {{
    "increasing_terms": ["term1", "term2"],
    "decreasing_terms": ["term1"],
    "assessment": "one sentence"
  }},
  "framing_lock": {{
    "detected": true/false,
    "pattern": "description or null"
  }},
  "confidence_drift": {{
    "direction": "INCREASING|STABLE|DECREASING",
    "evidence_support": "strong|moderate|weak",
    "concern": "description or null"
  }},
  "analytical_convergence": {{
    "detected": true/false,
    "severity": "LOW|MODERATE|HIGH",
    "note": "description"
  }},
  "s2_pattern_shift": {{
    "detected": true/false,
    "dominant_injection_type": "type or null",
    "note": "description"
  }},
  "overall_assessment": "2-3 sentences — what should S3 consider as food for thought",
  "quality_score": 0.0
}}"""

    log.info(f"Drift prompt built: {len(prompt)} chars")
    return prompt

# ── Cohere call ───────────────────────────────────────────────────────────────
def call_cohere(co, prompt: str) -> Optional[dict]:
    """Call command-r-plus for drift analysis."""
    for attempt in range(1, 4):
        try:
            log.info(f"Calling command-r-plus (attempt {attempt}/3)")
            response = co.chat(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.message.content[0].text.strip()

            # Strip markdown if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            log.info(f"S3-C analysis complete | severity={parsed.get('severity','?')} | drift={parsed.get('drift_detected','?')}")
            return parsed

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse failed attempt {attempt}: {e}")
            if attempt == 3:
                return None
        except Exception as e:
            log.error(f"Cohere call failed attempt {attempt}: {e}")
            if attempt < 3:
                time.sleep(30)
            else:
                return None
    return None

# ── Save result ───────────────────────────────────────────────────────────────
def save_result(sb: Client, analysis: dict, run_id: str,
                s1_count: int, s2_count: int) -> bool:
    try:
        record = {
            "run_id":        run_id,
            "position":      "S3-C",
            "report_type":   "DRIFT_MONITOR",
            "system_tag":    "S3-C",
            "model_used":    MODEL,
            "provider":      "cohere",
            "quality_score": float(analysis.get("quality_score", 0.0)),
            "summary":       analysis.get("overall_assessment", ""),
            "first_domino":  f"Drift severity: {analysis.get('severity','UNKNOWN')}",
            "source_reports": json.dumps({"s1_reports": s1_count, "s2_reports": s2_count}),
            "elapsed_seconds": 0,
        }
        r = sb.table("lens_system3_reports").insert(record).execute()
        saved = bool(r.data)
        log.info(f"S3-C saved={'YES' if saved else 'NO'}")
        return saved
    except Exception as e:
        log.error(f"Save failed: {e}")
        return False

# ── Main ──────────────────────────────────────────────────────────────────────
def run_s3c(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    run_id = run_id or datetime.now(timezone.utc).strftime("S3C-%Y%m%d-%H%M")
    log.info(f"=== S3-C BIAS DRIFT MONITOR START | run_id={run_id} ===")

    try:
        sb = get_supabase()
        co = get_cohere()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # Cadence check — weekly only
    if already_ran_recently(sb):
        return {"status": "SKIPPED", "reason": f"ran within last {CADENCE_DAYS} days"}

    # Fetch history
    s1_reports = fetch_s1_history(sb)
    s2_reports = fetch_s2_history(sb)

    if len(s1_reports) < 10:
        log.warning(f"Only {len(s1_reports)} S1 reports — need 10+ for meaningful drift analysis")
        return {"status": "INSUFFICIENT_DATA",
                "s1_count": len(s1_reports),
                "note": "S3-C needs 10+ S1 reports. Will improve as system matures."}

    # Build prompt and call Cohere
    prompt   = build_drift_prompt(s1_reports, s2_reports)
    analysis = call_cohere(co, prompt)

    if not analysis:
        log.error("S3-C: Cohere call failed — no drift analysis produced")
        return {"status": "ANALYSIS_FAILED"}

    # Save
    saved = save_result(sb, analysis, run_id, len(s1_reports), len(s2_reports))
    elapsed = round(time.time() - start, 1)
    severity = analysis.get("severity", "UNKNOWN")

    log.info(f"=== S3-C COMPLETE | saved={'YES' if saved else 'NO'} | severity={severity} | {elapsed}s ===")
    log.info(f"Assessment: {analysis.get('overall_assessment','')[:120]}")

    return {
        "status":     "OK" if saved else "SAVE_FAILED",
        "severity":   severity,
        "drift":      analysis.get("drift_detected", False),
        "elapsed":    elapsed,
    }

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_s3c()
    print(json.dumps(result, indent=2))
