"""
lens_s2e_legitimacy.py — System 2 Position E: Legitimacy Filter
Project Lens | LENS-009
Model: llama-3.3-70b-versatile (Groq)
Input: lens_reports (latest cycle) — extracts named actors
Output: injection_reports (analyst='S2-E')
6-point democratic legitimacy check on all actors named in S1 reports
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from groq import Groq
from supabase import create_client, Client

# ── Quota guard (LR-074) ──────────────────────────────────────────────────────
from lens_quota_guard import guard_check_with_fallback

# ── Response schema validator (I2) ────────────────────────────────────────────
from lens_response_guard import validate_parsed_response, format_validation_for_log

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-E] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2e")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL            = "llama-3.3-70b-versatile"
MAX_TOKENS       = 2000
TEMPERATURE      = 0.15      # very low — legitimacy scoring should be consistent
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_REPORT_CHARS = 5000

# 6 legitimacy dimensions
LEGITIMACY_DIMS = [
    "ELECTORAL",           # free and fair elections
    "RULE_OF_LAW",         # independent judiciary
    "PRESS_FREEDOM",       # independent media allowed
    "CIVIL_SOCIETY",       # independent organizations can operate
    "ACCOUNTABILITY",      # functioning checks and balances
    "INTERNATIONAL_NORMS", # compliance with UN/international law
    "PEOPLES_SOVEREIGNTY",  # PHI-002: does actor hold/protect genuine people's mandate and GCSP rights?
]

SYSTEM_PROMPT = """You are S2-E Legitimacy Filter for Project Lens, an OSINT intelligence system.

Your job: 
Step 1 — Extract all STATE ACTORS (governments, heads of state, state institutions, military commands) 
named in the intelligence report.
Step 2 — For each actor, apply a 6-point democratic legitimacy assessment.

The 7 dimensions (6 institutional + 1 PHI-002 people's sovereignty):
- ELECTORAL: Does this actor hold power through genuinely free and fair elections? (0=no/autocratic, 1=yes/democratic)
- RULE_OF_LAW: Is there an independent judiciary and legal system? (0=no, 1=yes)
- PRESS_FREEDOM: Is independent media permitted and operating? (0=no, 1=yes)
- CIVIL_SOCIETY: Can independent NGOs and civil society operate freely? (0=no, 1=yes)
- ACCOUNTABILITY: Are there functioning checks and balances? (0=no, 1=yes)
- INTERNATIONAL_NORMS: Does this actor comply with UN charter and international law? (0=no/violator, 1=yes/compliant)
- PEOPLES_SOVEREIGNTY: PHI-002 dimension — does this actor genuinely protect and serve people's right to grow, think, and envision freely? (0=oppresses/blocks GCSP rights, 0.5=partial/contested/transitional, 1=genuinely serves people's sovereign right to grow)

  PEOPLES_SOVEREIGNTY special rules:
  - A military junta that stole elections = 0 (actively blocks people's sovereignty)
  - A functioning democracy with flaws = 0.7-0.9 (institutional check on power exists)
  - A resistance movement with genuine people's electoral mandate = 0.8 (legitimate sovereignty claim, transitional)
  - A state that surveils/suppresses its own people = 0 regardless of formal elections
  - An international institution that serves great power interests over people = 0.3-0.5

Scoring:
- Each dimension: 0.0 (clearly fails), 0.5 (partial/contested/transitional), 1.0 (clearly meets standard)
- legitimacy_score = average of 7 dimensions
- legitimacy_tier: HIGH (>=0.7), MIXED (0.4-0.69), LOW (<0.4)
- actor_type_note: flag "RESISTANCE_MOVEMENT" if actor lacks institutional apparatus but holds genuine people's mandate against stolen sovereignty

Rules:
- Base assessments on established facts, not on the report's framing
- Use Freedom House, RSF Press Freedom Index, V-Dem, and PHI-002 GCSP framework
- Only assess STATE ACTORS — skip individuals, corporations, NGOs
- If an actor is well-known (US, Russia, China, UN) use your knowledge directly
- Flag actors with LOW legitimacy who are pushing narratives in the report — this is significant
- Do not score actors you have insufficient knowledge about — omit them
- CRITICAL: distinguish "institutional immaturity" (transitional democratic actor) from "authoritarian control" — they look similar on institutional dimensions but are opposite on PEOPLES_SOVEREIGNTY

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-E",
  "lens_id": "<provided>",
  "lens_name": "<provided>",
  "actors_assessed": [
    {
      "actor_name": "<name>",
      "actor_type": "<government|military|institution|leader>",
      "dimensions": {
        "ELECTORAL":           {"score": <0.0-1.0>, "note": "<brief justification>"},
        "RULE_OF_LAW":         {"score": <0.0-1.0>, "note": "<brief justification>"},
        "PRESS_FREEDOM":       {"score": <0.0-1.0>, "note": "<brief justification>"},
        "CIVIL_SOCIETY":       {"score": <0.0-1.0>, "note": "<brief justification>"},
        "ACCOUNTABILITY":      {"score": <0.0-1.0>, "note": "<brief justification>"},
        "INTERNATIONAL_NORMS": {"score": <0.0-1.0>, "note": "<brief justification>"},
        "PEOPLES_SOVEREIGNTY": {"score": <0.0-1.0>, "note": "<PHI-002: does actor protect peoples right to grow think envision freely>"}
      },
      "legitimacy_score": <0.0-1.0>,
      "legitimacy_tier": "<HIGH|MIXED|LOW>",
      "actor_type_note": "<FUNCTIONING_DEMOCRACY|AUTHORITARIAN|RESISTANCE_MOVEMENT|TRANSITIONAL|INTERNATIONAL_BODY|unclear>",
      "narrative_role_in_report": "<how this actor is framed in the report>"
    }
  ],
  "low_legitimacy_actors_pushing_narrative": ["<actor names with LOW tier who are active in report>"],
  "legitimacy_gap_signal": "<if LOW legitimacy actors dominate report narrative, describe the gap>",
  "analyst_note": "<1 sentence summary or empty string>"
}"""



# ── TPMGuard ──────────────────────────────────────────────────────────────────
class TPMGuard:
    """
    Rolling 60-second token window guard. Prevents 429 cascades.
    Waits intelligently before each API call. Never crashes — just waits.
    Adapted from GNI MAD pipeline pattern for Project Lens S2.
    """
    def __init__(self, tpm_limit: int = 6000):
        self.tpm_limit = tpm_limit
        self.usage_log = []  # list of (timestamp, tokens)

    def estimate_tokens(self, text: str) -> int:
        return max(1, len(text) // 4)

    def tokens_in_last_60s(self) -> int:
        now = time.time()
        self.usage_log = [(t, tok) for t, tok in self.usage_log if t > now - 60.0]
        return sum(tok for _, tok in self.usage_log)

    def log_usage(self, tokens: int):
        self.usage_log.append((time.time(), tokens))

    def wait_if_needed(self, tokens_needed: int, label: str = ""):
        """Wait until window has headroom. Logs every wait. Returns when safe."""
        while True:
            used = self.tokens_in_last_60s()
            if used + tokens_needed <= self.tpm_limit:
                return
            wait = 10
            log.info(f"[TPMGuard{' '+label if label else ''}] "
                     f"{used}/{self.tpm_limit} TPM used — waiting {wait}s...")
            time.sleep(wait)


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_groq() -> Groq:
    # S2-E uses GROQ_S2E_API_KEY — dedicated quota
    return Groq(api_key=os.environ["GROQ_S2E_API_KEY"])


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
    if not text:
        return ""
    if len(text) > MAX_REPORT_CHARS:
        return text[:MAX_REPORT_CHARS] + "\n[...truncated]"
    return text


def call_legitimacy_filter(client: Groq, report: dict, guard: "TPMGuard") -> Optional[dict]:
    """Call llama-3.3-70b to assess actor legitimacy in one lens report."""
    report_id   = report.get("id", "unknown")
    lens_name   = report.get("lens_name", "unknown")
    report_text = truncate_report(report.get("summary", ""))

    if not report_text.strip():
        log.warning(f"Empty report_text for {report_id} — skipping")
        return None

    user_message = (
        f"Extract state actors and apply the 6-point legitimacy assessment.\n\n"
        f"Lens: {lens_name}\n"
        f"Report ID: {report_id}\n\n"
        f"--- REPORT START ---\n{report_text}\n--- REPORT END ---\n\n"
        f"Return JSON only."
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            guard.wait_if_needed(2000, label="S2-E")
            log.info(f"S2-E calling llama-3.3-70b for {lens_name} (attempt {attempt})")
            response = client.chat.completions.create(
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

            # ── Response schema validation (I2) ──────────────────────────────
            vr = validate_parsed_response(parsed, "S2-E")
            if not vr.valid:
                log.warning(format_validation_for_log(vr))
            actors = parsed.get("actors_assessed", [])
            low_actors = parsed.get("low_legitimacy_actors_pushing_narrative", [])
            log.info(
                f"S2-E result for {lens_name}: "
                f"{len(actors)} actors assessed, "
                f"{len(low_actors)} LOW legitimacy actors flagged"
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

    log.error(f"S2-E failed after {MAX_RETRIES} attempts for {lens_name}")
    return None



def build_correction_to_ma(analysis: dict) -> dict:
    """
    Hard correction channel output for S2-E Legitimacy Filter.
    When LOW legitimacy actors dominate the narrative, their framing should not be
    treated as independent verification. MA must flag signals attributed to these actors.
    """
    actors     = analysis.get("actors_assessed", [])
    low_actors = analysis.get("low_legitimacy_actors_pushing_narrative", [])
    gap_signal = analysis.get("legitimacy_gap_signal", "")

    if not low_actors:
        return {"action": "NONE", "mandatory": False}

    total     = max(len(actors), 1)
    low_ratio = len(low_actors) / total

    if len(low_actors) >= 2 or low_ratio >= 0.5:
        action = "FLAG"
        adj    = -0.20
        mandatory = True
        depth  = "MODERATE"
    else:
        action = "FLAG"
        adj    = -0.10
        mandatory = False
        depth  = "SURFACE"

    return {
        "action":                action,
        "source_analyst":        "S2-E",
        "contamination_depth":   depth,
        "injection_score":       low_ratio,
        "confidence_adjustment": adj,
        "reason": (
            f"LOW legitimacy actors pushing narrative: {low_actors}. "
            + (gap_signal[:150] if gap_signal else "Actors without democratic legitimacy are dominant in report framing.")
        ),
        "injection_goal": (
            f"Actors with LOW democratic legitimacy ({low_actors}) are shaping the narrative — "
            "their framing may systematically misrepresent the situation to serve their interests."
        ),
        "mandatory": mandatory,
    }

def save_legitimacy_report(
    sb: Client,
    report: dict,
    analysis: dict,
    run_id: str
) -> bool:
    """Save S2-E legitimacy assessment to injection_reports table."""
    actors        = analysis.get("actors_assessed", [])
    low_actors    = analysis.get("low_legitimacy_actors_pushing_narrative", [])
    gap_signal    = analysis.get("legitimacy_gap_signal", "")

    # Flag if low legitimacy actors are dominant
    injection_type = "LEGITIMACY_GAP" if low_actors else "NONE"

    # Flagged phrases = names of low legitimacy actors
    flagged = low_actors[:10]

    row = {
        "run_id":         run_id,
        "cycle":          report.get("cycle"),
        "lens_report_id": report.get("id"),
        "analyst":        "S2-E",
        "source_id":      None,
        "injection_type": injection_type,
        "evidence": {
            "actors_assessed":    actors,
            "low_legitimacy_actors": low_actors,
            "legitimacy_gap_signal": gap_signal,
            "analyst_note":       analysis.get("analyst_note", ""),
            "total_actors":       len(actors),
            "correction_to_ma":   build_correction_to_ma(analysis),
        },
        "confidence_score": min(len(low_actors) / max(len(actors), 1), 1.0),
        "flagged_phrases":  flagged,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = sb.table("injection_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-E row for lens={report.get('lens_name')}")
        return True
    except Exception as e:
        log.error(f"Failed to save S2-E result: {e}")
        return False


def run_s2e(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    """Main entry point for S2-E Legitimacy Filter."""
    start = time.time()
    if not run_id:
        run_id = f"s2e_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-E Legitimacy Filter START | run_id={run_id} | cycle={cycle} ===")

    try:
        sb     = get_supabase()
        client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # ── Quota guard pre-flight (LR-074) ───────────────────────────────────────
    quota_guard = guard_check_with_fallback(positions=["S2-E"], run_id=run_id, sb=sb)
    skip_result = next((r for r in quota_guard if r.decision == "SKIP" and "S2-E" in r.positions), None)
    if skip_result:
        reason = skip_result.reason
        log.warning(f"S2-E quota SKIP: {reason}")
        return {"status": "QUOTA_SKIP", "reason": reason, "reports_analyzed": 0}

    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-E cannot run")
        return {"status": "NO_REPORTS", "reports_analyzed": 0}

    results     = []
    saved_count = 0
    total_low   = 0
    guard = TPMGuard(tpm_limit=6000)  # GROQ_S2E_API_KEY

    for i, report in enumerate(reports):
        log.info(f"Processing {i+1}/{len(reports)}: {report.get('domain_focus')}")

        analysis = call_legitimacy_filter(client, report, guard)
        if analysis is None:
            results.append({"lens": report.get("domain_focus"), "status": "FAILED"})
            continue

        low_actors = analysis.get("low_legitimacy_actors_pushing_narrative", [])
        total_low += len(low_actors)

        saved = save_legitimacy_report(sb, report, analysis, run_id)
        if saved:
            saved_count += 1

        results.append({
            "lens":          report.get("domain_focus"),
            "status":        "OK",
            "actors_found":  len(analysis.get("actors_assessed", [])),
            "low_legit":     len(low_actors),
            "gap_signal":    analysis.get("legitimacy_gap_signal", "none"),
        })

        if i < len(reports) - 1:
            log.info("Stagger 6s...")
            time.sleep(6)  # guard.wait_if_needed() handles TPM above

    elapsed = round(time.time() - start, 1)

    summary = {
        "status":           "COMPLETE",
        "run_id":           run_id,
        "cycle":            cycle,
        "reports_analyzed": len(reports),
        "reports_saved":    saved_count,
        "total_low_legitimacy_actors": total_low,
        "elapsed_seconds":  elapsed,
        "results":          results,
    }

    log.info(f"=== S2-E COMPLETE | {len(reports)} reports | "
             f"{total_low} LOW legitimacy actors | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_s2e(cycle=cycle_arg)
