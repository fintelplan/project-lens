"""
lens_mission_analyst.py — Mission Analyst (Lens 5)
Project Lens | LENS-010
Model: llama-3.3-70b-versatile (Groq — GROQ_MA_API_KEY)
Input: lens_reports (S1) + injection_reports (S2) — latest cycle
Output: lens_macro_reports (Supabase)

LENS-010 additions:
  - apply_s2_corrections(): reads correction_to_ma from S2 reports
    Mandatory corrections are prepended as HARD CONSTRAINTS before LLM call.
    The LLM cannot synthesize without them — they are presented as ground truth.
  - contamination_depth: overall depth rating from S2 corrections
  - s2_corrections_applied: structured list saved to macro report
"""

import os
import json
import time
import logging
from collections import deque
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
TEMPERATURE      = 0.3
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_S1_CHARS     = 6000
MAX_S2_CHARS     = 3000
MAX_TOTAL_CHARS  = 28000

# ── TPMGuard ──────────────────────────────────────────────────────────────────
class TPMGuard:
    def __init__(self, tpm_limit: int = 6000):
        self.tpm_limit = tpm_limit
        self._log: deque = deque()

    def tokens_in_last_60s(self) -> int:
        now = time.time()
        self._log = deque((t, n) for t, n in self._log if now - t < 60)
        return sum(n for _, n in self._log)

    def log_usage(self, tokens: int):
        self._log.append((time.time(), tokens))

    def wait_if_needed(self, tokens_needed: int, label: str = ""):
        while self.tokens_in_last_60s() + tokens_needed > self.tpm_limit:
            used = self.tokens_in_last_60s()
            log.info(f"[TPMGuard{' ' + label if label else ''}] {used}/{self.tpm_limit} TPM — waiting 10s...")
            time.sleep(10)

_tpm = TPMGuard(6000)

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are the Mission Analyst for Project Lens — the final intelligence synthesis
layer for Global Game Changers (GCSP), a program that educates the world's emerging leaders on
global power dynamics, information warfare, and democratic resilience.

Your audience: senior analysts, educators, and emerging global leaders who need to understand
what is ACTUALLY happening beneath the surface of world events — including manufactured narratives,
adversarial influence operations, and legitimacy gaps.

You receive three types of input:
1. MANDATORY S2 CORRECTIONS — apply these FIRST, before reading anything else.
   These are confirmed injection artifacts. They are non-negotiable ground truth.
   Do not rationalize them away. If a correction says DOWNGRADE a signal, downgrade it.
2. SYSTEM 1 reports: 4 analytical lenses examining world events
3. SYSTEM 2 reports: psychological and adversarial intelligence

Your job: synthesize all of this into ONE macro intelligence report.

The report must answer:
1. WHAT IS HAPPENING: The 3-5 most significant developments (after corrections applied)
2. WHAT IS MANUFACTURED: Key narrative injections, coordinated messaging, emotional manipulation
3. WHO IS PUSHING IT: Actors with legitimacy gaps who are actively shaping narratives
4. WHAT THE ADVERSARY WANTS: The adversarial narrative and its strategic goal
5. THREAT ASSESSMENT: Overall threat level to democratic governance and information integrity
6. WHAT MATTERS FOR GCSP: Specific implications for global governance, emerging leaders
7. CUI BONO SYNTHESIS: Who benefits from the OVERALL PATTERN of injections?
   - S2-A found injection method X — who benefits from this method being used?
   - S2-B found coordination pattern — who benefits from this coordination?
   - S2-C found emotional framing — who benefits from this emotional state?
   - S2-D found adversary narrative — who benefits from this narrative being accepted?
   - Is the SAME ACTOR appearing across multiple S2 positions?
   - Convergence of beneficiary = most reliable attribution possible.
   - Five positions pointing at same actor = confirmed intelligence, not inference.

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
    "<specific implication for GCSP audience>"
  ],
  "intelligence_gaps": "<what we could not determine with current sources>",
  "quality_score": <0.0-1.0>,
  "analyst_note": "<1 sentence meta-note on report quality or caveats>",
  "cui_bono_synthesis": {
    "primary_beneficiary": "<actor or category who benefits most from today's injection pattern>",
    "convergence": "<CONFIRMED|PROBABLE|UNCLEAR — based on how many S2 positions point to same actor>",
    "evidence": "<which S2 positions support this attribution>",
    "note": "<1 sentence — what this tells us about the strategic intent behind today's information environment>"
  }
}"""


# ── Database helpers ──────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def get_groq() -> Groq:
    return Groq(api_key=os.environ["GROQ_MA_API_KEY"])


def fetch_s1_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        q = sb.table("lens_reports").select(
            "id, domain_focus, summary, cycle, generated_at"
        )
        if cycle:
            q = q.eq("cycle", cycle).order("generated_at", desc=True).limit(8)
        else:
            q = q.order("generated_at", desc=True).limit(4)
        reports = q.execute().data or []
        log.info(f"Fetched {len(reports)} S1 reports")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch S1 reports: {e}")
        return []


def fetch_s2_reports(sb: Client, run_id: Optional[str] = None) -> list[dict]:
    try:
        q = sb.table("injection_reports").select(
            "id, analyst, injection_type, evidence, confidence_score, flagged_phrases, cycle, created_at"
        )
        if run_id:
            q = q.eq("run_id", run_id).order("created_at", desc=True).limit(30)
        else:
            q = q.order("created_at", desc=True).limit(20)
        reports = q.execute().data or []
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
    return text[:limit] + "[...truncated]" if len(text) > limit else text


# ── Hard correction channel receiver ─────────────────────────────────────────
def apply_s2_corrections(s2_reports: list[dict]) -> tuple[list[dict], str]:
    """
    Extract correction_to_ma blocks from S2 reports.
    Returns:
        corrections:        list of correction dicts (mandatory ones first)
        contamination_depth: overall SURFACE / MODERATE / DEEP based on worst correction
    """
    corrections = []
    depth_scores = {"SURFACE": 0, "MODERATE": 1, "DEEP": 2}
    worst_depth = "SURFACE"

    for report in s2_reports:
        evidence = report.get("evidence") or {}
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except Exception:
                evidence = {}

        corr = evidence.get("correction_to_ma")
        if not corr or not isinstance(corr, dict):
            continue
        if corr.get("action") == "NONE":
            continue

        corrections.append({
            "analyst":             report.get("analyst", "S2-?"),
            "action":              corr.get("action", "FLAG"),
            "contamination_depth": corr.get("contamination_depth", "SURFACE"),
            "injection_score":     corr.get("injection_score", 0.0),
            "confidence_adjustment": corr.get("confidence_adjustment", 0.0),
            "reason":              corr.get("reason", ""),
            "injection_goal":      corr.get("injection_goal", "none detected"),
            "mandatory":           corr.get("mandatory", False),
        })

        # Track worst contamination depth seen
        depth = corr.get("contamination_depth", "SURFACE")
        if depth_scores.get(depth, 0) > depth_scores.get(worst_depth, 0):
            worst_depth = depth

    # Sort: mandatory first, then by injection_score descending
    corrections.sort(key=lambda c: (not c["mandatory"], -c.get("injection_score", 0)))

    if corrections:
        log.info(
            f"apply_s2_corrections: {len(corrections)} corrections "
            f"({sum(1 for c in corrections if c['mandatory'])} mandatory), "
            f"worst_depth={worst_depth}"
        )
    return corrections, worst_depth


def format_corrections_for_prompt(corrections: list[dict]) -> str:
    """
    Format mandatory corrections as a hard-constraint block for the synthesis prompt.
    This goes BEFORE S1 reports so the LLM reads it first.
    """
    if not corrections:
        return ""

    mandatory   = [c for c in corrections if c["mandatory"]]
    advisory    = [c for c in corrections if not c["mandatory"]]

    lines = [
        "=== MANDATORY S2 CORRECTIONS — APPLY BEFORE SYNTHESIZING ===",
        "These corrections are confirmed injection artifacts. They are NON-NEGOTIABLE.",
        "You MUST apply them. Do not rationalize them away.",
        "",
    ]

    for i, c in enumerate(mandatory, 1):
        adj = c.get("confidence_adjustment", 0)
        adj_str = f"{adj:+.0%}" if adj else ""
        lines.append(
            f"CORRECTION {i} [{c['action']}] — from {c['analyst']} "
            f"(contamination: {c['contamination_depth']}, confidence adjustment: {adj_str})"
        )
        lines.append(f"  Reason: {c['reason']}")
        if c.get("injection_goal") and c["injection_goal"] != "none detected":
            lines.append(f"  Injection goal was: {c['injection_goal']}")
        lines.append("")

    if advisory:
        lines.append("=== ADVISORY S2 FLAGS (consider, not mandatory) ===")
        for c in advisory:
            lines.append(f"[{c['action']}] {c['analyst']}: {c['reason'][:120]}")
        lines.append("")

    return "\n".join(lines)



# ── S3 Context fetch ──────────────────────────────────────────────────────────
def fetch_s3_context(sb) -> dict:
    """
    FIX-3: Fetch latest S3-A and S3-D reports for MA context.
    Architecture doc Table 7: MA receives S2-corrected, S3-contextualized intelligence.
    S3 context = pattern intelligence (7-day) + structural trends (30-day).
    """
    ctx = {"s3a": None, "s3d": None}
    try:
        r = sb.table("lens_system3_reports") \
            .select("position, report_type, summary, first_domino, generated_at") \
            .eq("position", "S3-A") \
            .order("generated_at", desc=True) \
            .limit(1).execute()
        if r.data:
            ctx["s3a"] = r.data[0]
            log.info(f"S3-A context loaded: {r.data[0].get('generated_at','?')[:16]}")
    except Exception as e:
        log.warning(f"S3-A context fetch failed: {e}")
    try:
        r = sb.table("lens_system3_reports") \
            .select("position, report_type, summary, first_domino, generated_at") \
            .eq("position", "S3-D") \
            .order("generated_at", desc=True) \
            .limit(1).execute()
        if r.data:
            ctx["s3d"] = r.data[0]
            log.info(f"S3-D context loaded: {r.data[0].get('generated_at','?')[:16]}")
    except Exception as e:
        log.warning(f"S3-D context fetch failed: {e}")
    if not ctx["s3a"] and not ctx["s3d"]:
        log.info("No S3 context available yet — MA proceeds without it (normal on first runs)")
    return ctx

# ── Synthesis prompt ──────────────────────────────────────────────────────────
def build_synthesis_prompt(
    s1_reports: list[dict],
    s2_reports: list[dict],
    corrections: list[dict],
    cycle: Optional[str],
    s3_context: dict = None,
) -> str:
    sections = []
    total_chars = 0

    # ── Mandatory corrections FIRST (hard channel) ────────────────────────────
    corrections_block = format_corrections_for_prompt(corrections)
    if corrections_block:
        sections.append(corrections_block + "\n")
        total_chars += len(corrections_block)

    # ── S1 Reports ────────────────────────────────────────────────────────────
    sections.append("=== SYSTEM 1 REPORTS (Analytical Lenses — READ AFTER APPLYING CORRECTIONS) ===\n")
    for r in s1_reports:
        text  = truncate(r.get("summary", ""), MAX_S1_CHARS)
        entry = f"--- {r.get('domain_focus', 'Unknown Lens')} ---\n{text}\n"
        if total_chars + len(entry) > MAX_TOTAL_CHARS * 0.6:
            break
        sections.append(entry)
        total_chars += len(entry)

    # ── S2 Reports ────────────────────────────────────────────────────────────
    sections.append("\n=== SYSTEM 2 REPORTS (Psychological + Adversarial Intelligence) ===\n")
    for r in s2_reports:
        analyst  = r.get("analyst", "?")
        inj_type = r.get("injection_type", "?")
        conf     = r.get("confidence_score", 0)
        evidence = r.get("evidence", {})
        flagged  = r.get("flagged_phrases", [])

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

    # ── S3 Context (Pattern Intelligence + Structural Trends) ──────────────────
    if s3_context:
        s3_section = "\n=== SYSTEM 3 CONTEXT (Pattern Intelligence + Structural Trends) ===\n"
        s3_section += "(Food for thought — S3 matures freely, read as background context)\n"
        s3a = s3_context.get("s3a")
        s3d = s3_context.get("s3d")
        if s3a:
            s3a_sum  = truncate(s3a.get("summary", ""), 1200)
            s3a_dom  = s3a.get("first_domino", "")
            s3a_date = s3a.get("generated_at", "")[:16]
            s3_section += f"\n[S3-A PATTERN — 7-day window @ {s3a_date}]\n{s3a_sum}\n"
            if s3a_dom:
                s3_section += f"First Domino (what becomes inevitable if patterns continue):\n{s3a_dom}\n"
        if s3d:
            s3d_sum  = truncate(s3d.get("summary", ""), 1200)
            s3d_dom  = s3d.get("first_domino", "")
            s3d_date = s3d.get("generated_at", "")[:16]
            s3_section += f"\n[S3-D STRUCTURAL — 30-day window @ {s3d_date}]\n{s3d_sum}\n"
            if s3d_dom:
                s3_section += f"Structural First Domino:\n{s3d_dom}\n"
        if total_chars + len(s3_section) <= MAX_TOTAL_CHARS:
            sections.append(s3_section)
            total_chars += len(s3_section)
            log.info(f"S3 context added to MA prompt ({len(s3_section)} chars)")
        else:
            log.info("S3 context skipped — prompt cap reached")

    prompt = "".join(sections)
    log.info(f"Synthesis prompt: {len(prompt)} chars ({len(corrections)} corrections prepended)")
    return prompt


# ── Core analysis ─────────────────────────────────────────────────────────────
def call_mission_analyst(client: Groq, prompt: str, cycle: Optional[str]) -> Optional[dict]:
    user_message = (
        f"Synthesize the following intelligence into a macro report.\n"
        f"Cycle: {cycle or 'latest'}\n\n"
        f"{prompt}\n\n"
        f"Return JSON only."
    )

    _tpm.wait_if_needed(3000, label="MA")

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
            if response.usage:
                _tpm.log_usage(response.usage.total_tokens)

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
                log.warning(f"Rate limit (429) attempt {attempt} — sleeping 20s"); time.sleep(20)
            elif "503" in err:
                log.warning(f"503 attempt {attempt} — sleeping 15s"); time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"Mission Analyst failed after {MAX_RETRIES} attempts")
    return None


# ── Save to Supabase ──────────────────────────────────────────────────────────
def save_macro_report(
    sb: Client,
    analysis: dict,
    s1_report_ids: list[str],
    s2_report_ids: list[str],
    corrections: list[dict],
    contamination_depth: str,
    run_id: str,
    cycle: Optional[str],
) -> bool:
    row = {
        "run_id":               run_id,
        "cycle":                cycle,
        "threat_level":         analysis.get("threat_level", "MODERATE"),
        "executive_summary":    analysis.get("executive_summary", ""),
        "key_findings":         analysis.get("key_findings", []),
        "manufactured_narratives": analysis.get("manufactured_narratives", []),
        "adversary_narrative_summary": analysis.get("adversary_narrative_summary", ""),
        "actors_of_concern":    analysis.get("actors_of_concern", []),
        "gcsp_implications":    analysis.get("gcsp_implications", []),
        "contamination_depth":  contamination_depth,
        "s2_corrections_applied": [
            {
                "analyst":    c["analyst"],
                "action":     c["action"],
                "mandatory":  c["mandatory"],
                "reason":     c["reason"][:200],
            }
            for c in corrections
        ],
        "quality_score":        float(analysis.get("quality_score", 0.0)),
        "s1_report_ids":        s1_report_ids,
        "s2_report_ids":        s2_report_ids,
        "generated_at":         datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = sb.table("lens_macro_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} macro report row (contamination_depth={contamination_depth})")
        return saved > 0
    except Exception as e:
        log.error(f"Failed to save macro report: {e}")
        return False


# ── Entry point ───────────────────────────────────────────────────────────────
def run_mission_analyst(
    cycle: Optional[str] = None,
    run_id: Optional[str] = None,
) -> dict:
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

    s1_reports = fetch_s1_reports(sb, cycle)
    s2_reports = fetch_s2_reports(sb, run_id)

    if not s1_reports:
        log.warning("No S1 reports — Mission Analyst cannot run")
        return {"status": "NO_S1_REPORTS"}

    if not s2_reports:
        log.warning("No S2 reports — running on S1 only (no corrections)")

    # ── HARD CORRECTION CHANNEL — extract before synthesis ───────────────────
    corrections, contamination_depth = apply_s2_corrections(s2_reports)
    mandatory_count = sum(1 for c in corrections if c["mandatory"])
    log.info(
        f"Corrections extracted: {len(corrections)} total, "
        f"{mandatory_count} mandatory, overall_depth={contamination_depth}"
    )

    # ── Build prompt with corrections prepended ───────────────────────────────
    # FIX-3: fetch S3 context and pass to MA — closes S1→S2→S3→MA circuit
    s3_context = fetch_s3_context(sb)
    prompt = build_synthesis_prompt(s1_reports, s2_reports, corrections, cycle, s3_context)

    analysis = call_mission_analyst(client, prompt, cycle)
    if analysis is None:
        return {"status": "ANALYSIS_FAILED"}

    s1_ids = [r.get("id") for r in s1_reports if r.get("id")]
    s2_ids = [r.get("id") for r in s2_reports if r.get("id")]

    saved = save_macro_report(
        sb, analysis, s1_ids, s2_ids,
        corrections, contamination_depth,
        run_id, cycle,
    )

    elapsed = round(time.time() - start, 1)
    summary = {
        "status":               "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":               run_id,
        "cycle":                cycle,
        "threat_level":         analysis.get("threat_level", "?"),
        "key_findings":         len(analysis.get("key_findings", [])),
        "quality_score":        analysis.get("quality_score", 0),
        "contamination_depth":  contamination_depth,
        "corrections_applied":  len(corrections),
        "mandatory_corrections": mandatory_count,
        "executive_summary":    analysis.get("executive_summary", "")[:200],
        "elapsed_seconds":      elapsed,
    }
    log.info(
        f"=== MISSION ANALYST COMPLETE | "
        f"threat={summary['threat_level']} | "
        f"depth={contamination_depth} | "
        f"corrections={len(corrections)} ({mandatory_count} mandatory) | "
        f"{elapsed}s ==="
    )
    print(json.dumps(summary, indent=2))

    # ── Telegram daily brief (fires after every successful MA run) ──────────
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from lens_telegram import send_daily_brief, send_critical_alert
        send_daily_brief(run_id=run_id)
        if summary.get("threat_level") in ("CRITICAL", "HIGH"):
            send_critical_alert(
                reason=f"Mission Analyst threat={summary['threat_level']}",
                signal=summary.get("executive_summary","")[:400],
                threat=summary.get("threat_level","HIGH")
            )
    except Exception as _te:
        log.warning(f"Telegram alert failed (non-critical): {_te}")

    return summary


if __name__ == "__main__":
    import sys
    run_mission_analyst(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
