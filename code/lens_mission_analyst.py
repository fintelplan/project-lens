"""
lens_mission_analyst.py — Mission Analyst (Lens 5)
Project Lens | LENS-009
Model: llama-3.3-70b-versatile (Groq — GROQ_MANAGER_API_KEY)
Input: lens_reports (S1) + injection_reports (S2) — latest cycle
Output: lens_macro_reports (Supabase)
Purpose: final macro report for Global Game Changers audience
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from groq import Groq
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MA] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("mission_analyst")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL            = "llama-3.3-70b-versatile"
MAX_TOKENS       = 2500
TEMPERATURE      = 0.3       # slightly higher — synthesis needs nuance
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_S1_CHARS     = 6000      # per S1 report
MAX_S2_CHARS     = 3000      # per S2 finding
MAX_TOTAL_CHARS  = 28000     # total prompt cap

THREAT_LEVELS = ["CRITICAL", "HIGH", "ELEVATED", "MODERATE", "LOW"]

SYSTEM_PROMPT = """You are the Mission Analyst for Project Lens — the final intelligence synthesis 
layer for Global Game Changers (GCSP), a program that educates the world's emerging leaders on 
global power dynamics, information warfare, and democratic resilience.

Your audience: senior analysts, educators, and emerging global leaders who need to understand 
what is ACTUALLY happening beneath the surface of world events — including manufactured narratives, 
adversarial influence operations, and legitimacy gaps.

You receive two types of input:
1. SYSTEM 1 reports: 4 analytical lenses examining world events (Foundation, Physical Reality, 
   Causal Chain, Sovereignty Check)
2. SYSTEM 2 reports: psychological and adversarial intelligence 
   (Injection Tracer, Coordination Analyzer, Emotion Decoder, Adversary Narrative, Legitimacy Filter)

Your job: synthesize all of this into ONE macro intelligence report.

The report must answer:
1. WHAT IS HAPPENING: The 3-5 most significant developments in the world right now
2. WHAT IS MANUFACTURED: Key narrative injections, coordinated messaging, or emotional manipulation detected
3. WHO IS PUSHING IT: Actors with legitimacy gaps who are actively shaping narratives
4. WHAT THE ADVERSARY WANTS: The adversarial narrative and its strategic goal
5. THREAT ASSESSMENT: Overall threat level to democratic governance and information integrity
6. WHAT MATTERS FOR GCSP: Specific implications for global governance, emerging leaders, democratic institutions

Tone: Analytically precise. No sensationalism. No speculation without evidence. 
      Honest about uncertainty. Written for intelligent adults who can handle complexity.

Threat levels: CRITICAL / HIGH / ELEVATED / MODERATE / LOW

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "MISSION_ANALYST",
  "cycle": "<provided>",
  "threat_level": "<CRITICAL|HIGH|ELEVATED|MODERATE|LOW>",
  "executive_summary": "<3-4 sentence summary readable in 30 seconds>",
  "key_findings": [
    {
      "finding": "<specific finding>",
      "confidence": <0.0-1.0>,
      "evidence_sources": ["<S1 lens or S2 analyst that supports this>"],
      "significance": "<why this matters for global governance>"
    }
  ],
  "manufactured_narratives": [
    {
      "narrative": "<description of manufactured narrative>",
      "injection_type": "<type from S2>",
      "beneficiary": "<who benefits>",
      "confidence": <0.0-1.0>
    }
  ],
  "adversary_narrative_summary": "<1-2 sentences on what adversarial state actors are pushing today>",
  "actors_of_concern": [
    {
      "actor": "<name>",
      "concern": "<why flagged>",
      "legitimacy_tier": "<HIGH|MIXED|LOW>"
    }
  ],
  "gcsp_implications": [
    "<specific implication for GCSP audience — emerging leaders, democratic institutions>"
  ],
  "intelligence_gaps": "<what we could not determine with current sources>",
  "quality_score": <0.0-1.0>,
  "analyst_note": "<1 sentence meta-note on report quality or caveats>"
}"""


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_groq() -> Groq:
    # Mission Analyst uses GROQ_MANAGER_API_KEY — separate quota from main pipeline
    return Groq(api_key=os.environ["GROQ_MANAGER_API_KEY"])


def fetch_s1_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
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
        log.info(f"Fetched {len(reports)} S1 reports")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch S1 reports: {e}")
        return []


def fetch_s2_reports(sb: Client, run_id: Optional[str] = None) -> list[dict]:
    """Fetch S2 injection_reports from all analysts for this run."""
    try:
        if run_id:
            result = sb.table("injection_reports") \
                .select("id, analyst, injection_type, evidence, confidence_score, flagged_phrases, cycle, generated_at") \
                .eq("run_id", run_id) \
                .order("generated_at", desc=True) \
                .limit(30) \
                .execute()
        else:
            result = sb.table("injection_reports") \
                .select("id, analyst, injection_type, evidence, confidence_score, flagged_phrases, cycle, generated_at") \
                .order("generated_at", desc=True) \
                .limit(20) \
                .execute()
        reports = result.data or []
        log.info(f"Fetched {len(reports)} S2 reports")
        by_analyst = {}
        for r in reports:
            a = r.get("analyst", "?")
            by_analyst[a] = by_analyst.get(a, 0) + 1
        log.info(f"S2 breakdown: {by_analyst}")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch S2 reports: {e}")
        return []


def truncate(text: str, limit: int) -> str:
    if not text:
        return ""
    if len(text) > limit:
        return text[:limit] + "[...truncated]"
    return text


def build_synthesis_prompt(
    s1_reports: list[dict],
    s2_reports: list[dict],
    cycle: Optional[str]
) -> str:
    """Build the full synthesis prompt for Mission Analyst."""
    sections = []
    total_chars = 0

    # ── S1 Reports ────────────────────────────────────────────────────────────
    sections.append("=== SYSTEM 1 REPORTS (Analytical Lenses) ===\n")
    for r in s1_reports:
        text = truncate(r.get("summary", ""), MAX_S1_CHARS)
        entry = f"--- {r.get('domain_focus', 'Unknown Lens')} ---\n{text}\n"
        if total_chars + len(entry) > MAX_TOTAL_CHARS * 0.6:
            break
        sections.append(entry)
        total_chars += len(entry)

    # ── S2 Reports ────────────────────────────────────────────────────────────
    sections.append("\n=== SYSTEM 2 REPORTS (Psychological + Adversarial Intelligence) ===\n")
    for r in s2_reports:
        analyst   = r.get("analyst", "?")
        inj_type  = r.get("injection_type", "?")
        conf      = r.get("confidence_score", 0)
        evidence  = r.get("evidence", {})
        flagged   = r.get("flagged_phrases", [])

        # Truncate evidence for prompt
        evidence_str = truncate(json.dumps(evidence), MAX_S2_CHARS)
        flagged_str  = " | ".join([f for f in flagged if f][:5])

        entry = (
            f"[{analyst}] type={inj_type} confidence={conf}\n"
            f"Flagged: {flagged_str}\n"
            f"Evidence: {evidence_str}\n\n"
        )
        if total_chars + len(entry) > MAX_TOTAL_CHARS:
            log.info(f"Prompt cap reached at S2 entry for {analyst}")
            break
        sections.append(entry)
        total_chars += len(entry)

    prompt = "".join(sections)
    log.info(f"Synthesis prompt: {len(prompt)} chars")
    return prompt


def call_mission_analyst(client: Groq, prompt: str, cycle: Optional[str]) -> Optional[dict]:
    """Call llama-3.3-70b to produce the macro report."""
    user_message = (
        f"Synthesize the following S1 and S2 intelligence into a macro report.\n"
        f"Cycle: {cycle or 'latest'}\n\n"
        f"{prompt}\n\n"
        f"Return JSON only."
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Mission Analyst calling model (attempt {attempt})")
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
            log.info(
                f"Mission Analyst result: "
                f"threat={parsed.get('threat_level', '?')}, "
                f"findings={len(parsed.get('key_findings', []))}, "
                f"quality={parsed.get('quality_score', 0)}"
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

    log.error(f"Mission Analyst failed after {MAX_RETRIES} attempts")
    return None


def save_macro_report(
    sb: Client,
    analysis: dict,
    s1_report_ids: list[str],
    s2_report_ids: list[str],
    run_id: str,
    cycle: Optional[str]
) -> bool:
    """Save final macro report to lens_macro_reports table."""
    row = {
        "run_id":         run_id,
        "cycle":          cycle,
        "macro_summary":  analysis.get("executive_summary", ""),
        "threat_level":   analysis.get("threat_level", "MODERATE"),
        "key_findings":   analysis.get("key_findings", []),
        "actors_named":   analysis.get("actors_of_concern", []),
        "s1_report_ids":  s1_report_ids,
        "s2_report_ids":  s2_report_ids,
        "quality_score":  float(analysis.get("quality_score", 0.0)),
        "generated_at":     datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = sb.table("lens_macro_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} macro report row")
        return True
    except Exception as e:
        log.error(f"Failed to save macro report: {e}")
        return False


def run_mission_analyst(
    cycle: Optional[str] = None,
    run_id: Optional[str] = None
) -> dict:
    """Main entry point for Mission Analyst."""
    start = time.time()
    if not run_id:
        run_id = f"ma_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== MISSION ANALYST START | run_id={run_id} | cycle={cycle} ===")

    try:
        sb     = get_supabase()
        client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # ── Fetch S1 + S2 ─────────────────────────────────────────────────────────
    s1_reports = fetch_s1_reports(sb, cycle)
    s2_reports = fetch_s2_reports(sb, run_id)

    if not s1_reports:
        log.warning("No S1 reports — Mission Analyst cannot run")
        return {"status": "NO_S1_REPORTS"}

    if not s2_reports:
        log.warning("No S2 reports found — running on S1 only")

    # ── Build prompt and synthesize ───────────────────────────────────────────
    prompt = build_synthesis_prompt(s1_reports, s2_reports, cycle)

    analysis = call_mission_analyst(client, prompt, cycle)
    if analysis is None:
        return {"status": "ANALYSIS_FAILED"}

    s1_ids = [r.get("id") for r in s1_reports if r.get("id")]
    s2_ids = [r.get("id") for r in s2_reports if r.get("id")]

    saved = save_macro_report(sb, analysis, s1_ids, s2_ids, run_id, cycle)

    elapsed = round(time.time() - start, 1)

    summary = {
        "status":          "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":          run_id,
        "cycle":           cycle,
        "threat_level":    analysis.get("threat_level", "?"),
        "key_findings":    len(analysis.get("key_findings", [])),
        "quality_score":   analysis.get("quality_score", 0),
        "executive_summary": analysis.get("executive_summary", "")[:200],
        "elapsed_seconds": elapsed,
    }

    log.info(
        f"=== MISSION ANALYST COMPLETE | "
        f"threat={summary['threat_level']} | "
        f"findings={summary['key_findings']} | "
        f"quality={summary['quality_score']} | "
        f"{elapsed}s ==="
    )
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_mission_analyst(cycle=cycle_arg)
