"""
lens_s2_gap.py
S2-GAP: Gap Analysis — Black Swan + Ostrich Discovery

Compares what S1 saw vs what adversarial sources (S2-D) saw.
Gap = the intelligence. What is hiding behind the noise?

Input:  S1 top 5 signals (lens_reports latest cycle)
        S2-D adversarial findings (injection_reports latest)
Output: injection_reports (injection_type=GAP_ANALYSIS)
        - stories S1 missed
        - stories S1 over-amplified
        - adversary-only stories (what they want believed that S1 ignored)

Model:  llama-3.3-70b via Groq (GROQ_S2_API_KEY)
PHI-002: The gap between what is reported and what is omitted IS the signal.
         Broken Window defense: what is quietly disappearing behind the noise?
"""

import os, sys, json, time, logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from groq import Groq
from supabase import create_client, Client

# ── Quota guard (LR-074) ──────────────────────────────────────────────────────
from lens_quota_guard import guard_check_with_fallback

# ── Response schema validator (I2) ────────────────────────────────────────────
from lens_response_guard import validate_parsed_response, format_validation_for_log

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S2-GAP] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S2-GAP")

MODEL   = "llama-3.3-70b-versatile"
MAX_TOKENS = 1500

SYSTEM_PROMPT = """You are S2-GAP: Gap Analyst for Project Lens.

Your job is to find the intelligence gap between:
- What System 1 (Western/mainstream sources) amplified
- What adversarial state sources (S2-D) pushed

The gap IS the signal. What is quietly disappearing behind the noise?
What is being hidden by the stories everyone is covering?
This is the Broken Window defense: the loud story distracts from the structural move.

You will receive:
1. S1 TOP SIGNALS: what mainstream analytical lenses emphasized this cycle
2. S2-D ADVERSARIAL NARRATIVE: what state media and adversarial sources pushed

ANALYZE FOR:
1. MISSED BY S1: Stories or developments that adversarial sources covered but S1 ignored
   - These are potential blind spots or intentional omissions
   - Ask: why would adversarial sources cover this but mainstream ignore it?

2. OVER-AMPLIFIED BY S1: Stories S1 emphasized heavily that adversarial sources barely mentioned
   - These could be distractions (Broken Window Pattern)
   - Ask: what structural move might be hiding behind this loud story?

3. ADVERSARY-ONLY STORIES: Narratives appearing ONLY in adversarial sources
   - These reveal what the adversary wants believed
   - Ask: what conclusion does this narrative serve?

4. SILENCE ANALYSIS: What is NOT being reported anywhere?
   - What question is no one asking that someone powerful needs no one to ask?

Respond ONLY with valid JSON, no markdown:
{
  "missed_by_s1": [
    {
      "story": "brief description",
      "why_significant": "what this reveals",
      "cui_bono": "who benefits from S1 missing this"
    }
  ],
  "over_amplified_by_s1": [
    {
      "story": "brief description",
      "possible_distraction": "what structural move might be hiding behind this",
      "confidence": 0.0
    }
  ],
  "adversary_only": [
    {
      "narrative": "brief description",
      "strategic_purpose": "what conclusion this narrative serves",
      "target_audience": "who this narrative is aimed at"
    }
  ],
  "silence_analysis": "what is not being reported anywhere that matters",
  "gap_severity": "CRITICAL|HIGH|MODERATE|LOW",
  "key_gap_finding": "one sentence — the most important gap this cycle",
  "quality_score": 0.0
}"""


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
    return create_client(url, key)


def get_groq():
    key = os.environ.get("GROQ_S2_API_KEY") or os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise RuntimeError("GROQ_S2_API_KEY missing")
    return Groq(api_key=key)


def fetch_s1_signals(sb: Client, cycle: Optional[str] = None) -> list:
    """Fetch latest S1 lens reports for gap comparison."""
    try:
        if cycle:
            r = sb.table("lens_reports") \
                .select("domain_focus, summary, cycle, generated_at") \
                .eq("cycle", cycle) \
                .order("generated_at", desc=True) \
                .limit(8).execute()
        else:
            r = sb.table("lens_reports") \
                .select("domain_focus, summary, cycle, generated_at") \
                .order("generated_at", desc=True) \
                .limit(4).execute()
        reports = r.data or []
        log.info(f"Fetched {len(reports)} S1 signals")
        return reports
    except Exception as e:
        log.error(f"S1 fetch failed: {e}")
        return []


def fetch_s2d_findings(sb: Client) -> Optional[dict]:
    """Fetch latest S2-D adversarial narrative report."""
    try:
        r = sb.table("injection_reports") \
            .select("analyst, injection_type, evidence, confidence_score, created_at") \
            .eq("analyst", "S2-D") \
            .order("created_at", desc=True) \
            .limit(1).execute()
        if r.data:
            log.info(f"S2-D findings loaded: {r.data[0].get('created_at','?')[:16]}")
            return r.data[0]
        log.warning("No S2-D findings available — gap analysis will be partial")
        return None
    except Exception as e:
        log.error(f"S2-D fetch failed: {e}")
        return None


def build_gap_prompt(s1_reports: list, s2d: Optional[dict]) -> str:
    s1_text = "\n".join([
        f"[{r.get('domain_focus','?')}] {(r.get('summary','') or '')[:400]}"
        for r in s1_reports
    ])

    if s2d:
        evidence = s2d.get("evidence", {})
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except Exception:
                evidence = {"raw": evidence}
        s2d_text = (
            f"Injection type: {s2d.get('injection_type','?')}\n"
            f"Confidence: {s2d.get('confidence_score','?')}\n"
            f"Evidence: {json.dumps(evidence)[:800]}"
        )
    else:
        s2d_text = "No S2-D adversarial findings available this cycle."

    return (
        f"S1 TOP SIGNALS THIS CYCLE:\n{s1_text}\n\n"
        f"S2-D ADVERSARIAL NARRATIVE THIS CYCLE:\n{s2d_text}\n\n"
        "Identify the intelligence gap between these two perspectives."
    )


def call_groq(client: Groq, prompt: str) -> Optional[dict]:
    for attempt in range(1, 4):
        try:
            log.info(f"Calling {MODEL} (attempt {attempt}/3)")
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                max_tokens=MAX_TOKENS,
                temperature=0.3,
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            parsed = json.loads(raw.strip())

            # ── Response schema validation (I2) ──────────────────────────────
            vr = validate_parsed_response(parsed, "S2-GAP")
            if not vr.valid:
                log.warning(format_validation_for_log(vr))
            log.info(f"Gap analysis complete | severity={parsed.get('gap_severity','?')}")
            return parsed
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse failed attempt {attempt}: {e}")
            if attempt == 3:
                return None
        except Exception as e:
            log.error(f"Groq call failed attempt {attempt}: {e}")
            if attempt < 3:
                time.sleep(15)
            else:
                return None
    return None


def save_gap_analysis(sb: Client, analysis: dict, run_id: str, cycle: Optional[str]) -> bool:
    try:
        record = {
            "run_id":           run_id,
            "analyst":          "S2-GAP",
            "injection_type":   "GAP_ANALYSIS",
            "confidence_score": float(analysis.get("quality_score", 0.0)),
            "flagged_phrases":  [analysis.get("key_gap_finding", "")],
            "evidence": {
                "missed_by_s1":      analysis.get("missed_by_s1", []),
                "over_amplified":    analysis.get("over_amplified_by_s1", []),
                "adversary_only":    analysis.get("adversary_only", []),
                "silence_analysis":  analysis.get("silence_analysis", ""),
                "gap_severity":      analysis.get("gap_severity", "UNKNOWN"),
                "key_gap_finding":   analysis.get("key_gap_finding", ""),
            },
            "cycle": cycle or "auto",
        }
        r = sb.table("injection_reports").insert(record).execute()
        saved = bool(r.data)
        log.info(f"Gap analysis saved={'YES' if saved else 'NO'}")
        return saved
    except Exception as e:
        log.error(f"Save failed: {e}")
        return False


def run_s2_gap(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start   = time.time()
    run_id  = run_id or datetime.now(timezone.utc).strftime("S2G-%Y%m%d-%H%M")
    log.info(f"=== S2-GAP START | run_id={run_id} ===")

    try:
        sb     = get_supabase()
        client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # ── Quota guard pre-flight (LR-074) ───────────────────────────────────────
    quota_guard = guard_check_with_fallback(positions=["S2-GAP"], run_id=run_id, sb=sb)
    skip_result = next((r for r in quota_guard if r.decision == "SKIP" and "S2-GAP" in r.positions), None)
    if skip_result:
        reason = skip_result.reason
        log.warning(f"S2-GAP quota SKIP: {reason}")
        return {"status": "QUOTA_SKIP", "reason": reason}

    s1_reports = fetch_s1_signals(sb, cycle)
    s2d        = fetch_s2d_findings(sb)

    if not s1_reports:
        log.warning("No S1 signals — gap analysis cannot run")
        return {"status": "NO_S1_DATA"}

    prompt   = build_gap_prompt(s1_reports, s2d)
    analysis = call_groq(client, prompt)

    if not analysis:
        return {"status": "ANALYSIS_FAILED"}

    saved   = save_gap_analysis(sb, analysis, run_id, cycle)
    elapsed = round(time.time() - start, 1)
    severity = analysis.get("gap_severity", "UNKNOWN")

    log.info(f"=== S2-GAP COMPLETE | saved={'YES' if saved else 'NO'} | severity={severity} | {elapsed}s ===")
    log.info(f"Key gap: {analysis.get('key_gap_finding','')[:100]}")

    return {
        "status":   "OK" if saved else "SAVE_FAILED",
        "severity": severity,
        "elapsed":  elapsed,
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_s2_gap()
    print(json.dumps(result, indent=2))
