"""
lens_s2b_coordination.py — System 2 Position B: Coordination Analyzer
Project Lens | LENS-009
Model: gemini-1.5-flash (Google — GEMINI_API_KEY)
Context: 1,000,000 tokens — holds ALL reports simultaneously
Guard: GeminiRPMGuard (15 RPM free tier) + AFC disabled
FIXED: gemini-1.5-flash per architecture doc Table 4 per architecture book
Input: lens_reports (latest cycle)
Output: injection_reports (analyst='S2-B')
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from google import genai
from google.genai import types as genai_types
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-B] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2b")

MODEL            = "gemini-2.0-flash"
MAX_TOKENS       = 2000
TEMPERATURE      = 0.2
MAX_RETRIES      = 3
RETRY_SLEEP      = 15
MAX_REPORT_CHARS = 8000
MAX_TOTAL_CHARS  = 800000   # 1M context — use it fully


# ── GeminiRPMGuard ────────────────────────────────────────────────────────────
class GeminiRPMGuard:
    """
    Requests-Per-Minute guard for Gemini free tier.
    gemini-2.0-flash: 15 RPM limit.
    Tracks requests not tokens — Gemini TPM is 1M (generous).
    The real constraint is RPM.
    """
    RPM_LIMITS = {
        "gemini-2.0-flash": 15,
        "gemini-1.5-flash": 15,
        "gemini-1.5-pro":    2,
        "gemini-2.5-flash": 10,
        "gemini-2.0-flash": 15,
    }

    def __init__(self, model: str):
        self.rpm_limit = self.RPM_LIMITS.get(model, 10)
        self.req_log   = []  # list of timestamps

    def requests_in_last_60s(self) -> int:
        now = time.time()
        self.req_log = [t for t in self.req_log if t > now - 60.0]
        return len(self.req_log)

    def log_request(self):
        self.req_log.append(time.time())

    def wait_if_needed(self, label=""):
        """Wait until RPM window has a free slot. Never crashes."""
        while True:
            used = self.requests_in_last_60s()
            if used < self.rpm_limit:
                return
            wait = 10
            tag = " " + label if label else ""
            log.info(f"[GeminiRPMGuard{tag}] {used}/{self.rpm_limit} RPM — waiting {wait}s...")
            time.sleep(wait)


# ── Clients ───────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def get_gemini():
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


# ── Data fetch ────────────────────────────────────────────────────────────────
def fetch_raw_articles(sb: Client, cycle: Optional[str] = None) -> list:
    """
    FIX-2: Read raw articles NOT lens_reports.
    Architecture doc Table 4: S2-B must hold ALL raw articles simultaneously.
    Coordination detection requires source timing — not post-analysis summaries.
    """
    from datetime import datetime, timedelta, timezone
    try:
        # Fetch last 6 hours of raw articles — covers current + previous cycle
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        result = sb.table("lens_raw_articles") \
            .select("id, title, content, source_name, domain, published_at, collected_at") \
            .gte("collected_at", cutoff) \
            .order("collected_at", desc=False) \
            .limit(200) \
            .execute()
        articles = result.data or []
        log.info(f"Fetched {len(articles)} raw articles (last 6h) for coordination analysis")
        return articles
    except Exception as e:
        log.error(f"Failed to fetch lens_raw_articles: {e}")
        return []


def truncate_report(text: str) -> str:
    if not text:
        return ""
    return text[:MAX_REPORT_CHARS] + "\n[...truncated]" if len(text) > MAX_REPORT_CHARS else text


def build_prompt(reports: list) -> str:
    """
    FIX-2: Build prompt from raw articles (title + content + source + timing).
    S2-B detects coordination by seeing which sources published similar
    framing at similar times — requires raw source data, not summaries.
    """
    sections = []
    total_chars = 0
    for i, r in enumerate(reports, 1):
        title   = (r.get("title", "") or "")[:200]
        body    = truncate_report(r.get("content", "") or "")
        source  = r.get("source_name", "Unknown")
        domain  = r.get("domain", "")
        pub_at  = r.get("published_at", r.get("collected_at", ""))[:19] if r.get("published_at") or r.get("collected_at") else "unknown"
        entry   = f"=== ARTICLE {i}: [{source}] [{domain}] @ {pub_at} ===\nHEADLINE: {title}\n{body}\n"
        if total_chars + len(entry) > MAX_TOTAL_CHARS:
            break
        sections.append(entry)
        total_chars += len(entry)
    combined = "\n".join(sections)
    pct = (total_chars / MAX_TOTAL_CHARS) * 100
    log.info(f"S2-B prompt: {len(sections)} raw articles, {total_chars} chars ({pct:.1f}% of 1M context)")
    return f"Analyze {len(reports)} intelligence reports for cross-source coordination patterns.\n\n{combined}\n\nReturn JSON only."


# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are S2-B Coordination Analyzer for Project Lens, an OSINT intelligence system.

Your job: analyze MULTIPLE geopolitical intelligence reports simultaneously and detect
cross-source coordination patterns — signs that different sources are pushing the same
narrative in a coordinated way, whether intentionally or through shared influence.

You detect 4 coordination signal types:
- TIMING_SYNC: multiple ideologically different sources emphasize the same story in the same time window
- VOCAB_MIRROR: an unusual word or framing appears across sources that would not normally use the same language
- STRUCTURAL_MIRROR: different sources follow the same narrative arc (threat → villain → demanded response)
- COORDINATED_SILENCE: a significant topic conspicuously absent across sources suggesting deliberate omission

Rules:
- Analyze ACROSS reports, not within a single report
- Only flag patterns spanning at least 2 different sources
- Quote specific phrases from each report demonstrating the pattern
- Confidence 0.0-1.0. Only flag if confidence >= 0.5
- If no coordination found, return empty findings — do NOT invent patterns
- You have 1M context — use it to compare ALL reports simultaneously

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-B",
  "reports_analyzed": <number>,
  "findings": [
    {
      "coordination_type": "<TIMING_SYNC|VOCAB_MIRROR|STRUCTURAL_MIRROR|COORDINATED_SILENCE>",
      "sources_involved": ["<lens_1>", "<lens_2>"],
      "evidence": {
        "source_1_quote": "<exact quote from first report>",
        "source_2_quote": "<exact quote from second report>",
        "pattern_description": "<1-2 sentences explaining the coordination pattern>"
      },
      "confidence": <0.0-1.0>,
      "actor_beneficiary": "<who benefits or 'unclear'>"
    }
  ],
  "overall_coordination_score": <0.0-1.0>,
  "dominant_coordinated_narrative": "<1 sentence summary or 'none detected'>",
  "analyst_note": "<optional 1 sentence or empty string>"
}"""


# ── API call ──────────────────────────────────────────────────────────────────
def call_coordination_analyzer(client, reports: list, rpm_guard: GeminiRPMGuard) -> Optional[dict]:
    if len(reports) < 2:
        log.warning("Need at least 2 reports for coordination analysis")
        return None

    user_prompt  = build_prompt(reports)
    full_content = SYSTEM_PROMPT + "\n\n" + user_prompt

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rpm_guard.wait_if_needed(label="S2-B")
            log.info(f"S2-B calling gemini-1.5-flash (attempt {attempt})")

            response = client.models.generate_content(
                model=MODEL,
                contents=full_content,
                config=genai_types.GenerateContentConfig(
                    max_output_tokens=MAX_TOKENS,
                    temperature=TEMPERATURE,
                    automatic_function_calling=genai_types.AutomaticFunctionCallingConfig(
                        disable=True
                    ),
                )
            )

            rpm_guard.log_request()
            raw = response.text.strip()

            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            findings = len(parsed.get("findings", []))
            score    = parsed.get("overall_coordination_score", 0)
            narrative = parsed.get("dominant_coordinated_narrative", "none")[:60]
            log.info(f"S2-B result: {findings} findings, score={score}, narrative='{narrative}'")
            return parsed

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                wait = 30 * attempt   # escalating: 30s, 60s, 90s
                log.warning(f"Gemini 429 attempt {attempt} — sleeping {wait}s")
                time.sleep(wait)
            elif "503" in err or "500" in err:
                log.warning(f"Gemini server error attempt {attempt} — sleeping 20s")
                time.sleep(20)
            elif "404" in err:
                log.error(f"Model not found: {MODEL} — check model name")
                return None
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"S2-B failed after {MAX_RETRIES} attempts")
    return None


# ── Save ──────────────────────────────────────────────────────────────────────

def build_correction_to_ma(analysis: dict) -> dict:
    """
    Hard correction channel output for S2-B Coordination Analyzer.
    When coordination is detected, MA must downgrade signals from coordinated sources.
    Pattern 2 (Manufactured Consensus) is the most dangerous cross-lens contaminator.
    """
    score    = analysis.get("overall_coordination_score", 0.0)
    findings = analysis.get("findings", [])
    dominant = analysis.get("dominant_coordinated_narrative", "none detected")

    if score < 0.3 or not findings:
        return {"action": "NONE", "mandatory": False}

    types = list({f.get("coordination_type", "") for f in findings})

    if score >= 0.65 or len(findings) >= 3:
        action = "DOWNGRADE"
        adj    = -0.40
        mandatory = True
        depth  = "MODERATE"
    else:
        action = "FLAG"
        adj    = -0.15
        mandatory = False
        depth  = "SURFACE"

    return {
        "action":                action,
        "source_analyst":        "S2-B",
        "contamination_depth":   depth,
        "injection_score":       score,
        "confidence_adjustment": adj,
        "reason": (
            f"Coordination detected across {len(findings)} signal(s): {types}. "
            f"Dominant coordinated narrative: '{dominant[:100]}'. "
            "Signals from coordinated sources may reflect manufactured consensus, not independent verification."
        ),
        "injection_goal": (
            f"Manufacture appearance of independent consensus around: '{dominant[:80]}'"
            if dominant != "none detected" else "Coordination pattern without clear dominant narrative."
        ),
        "mandatory": mandatory,
    }

def save_coordination_report(
    sb: Client, reports: list, analysis: dict, run_id: str, cycle: Optional[str]
) -> bool:
    findings      = analysis.get("findings", [])
    overall_score = analysis.get("overall_coordination_score", 0.0)
    dominant      = analysis.get("dominant_coordinated_narrative", "none detected")
    rows = []

    if not findings:
        rows.append({
            "run_id": run_id, "cycle": cycle, "lens_report_id": None,
            "analyst": "S2-B", "source_id": None, "injection_type": "NONE",
            "evidence": {
                "analyst_note":      analysis.get("analyst_note", "No coordination detected"),
                "reports_analyzed":  len(reports),
                "dominant_narrative": dominant,
                "model": MODEL, "context": "1M",
                "correction_to_ma": build_correction_to_ma(analysis),
            },
            "confidence_score": 0.0, "flagged_phrases": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    else:
        for finding in findings:
            ev = finding.get("evidence", {})
            rows.append({
                "run_id": run_id, "cycle": cycle, "lens_report_id": None,
                "analyst": "S2-B", "source_id": None,
                "injection_type": finding.get("coordination_type", "UNKNOWN"),
                "evidence": {
                    "sources_involved":    finding.get("sources_involved", []),
                    "source_1_quote":      ev.get("source_1_quote", ""),
                    "source_2_quote":      ev.get("source_2_quote", ""),
                    "pattern_description": ev.get("pattern_description", ""),
                    "actor_beneficiary":   finding.get("actor_beneficiary", "unclear"),
                    "overall_score":       overall_score,
                    "dominant_narrative":  dominant,
                    "model": MODEL, "context": "1M",
                    "analyst_note": analysis.get("analyst_note", ""),
                    "correction_to_ma": build_correction_to_ma(analysis),
                },
                "confidence_score": float(finding.get("confidence", 0.0)),
                "flagged_phrases":  [ev.get("source_1_quote", ""), ev.get("source_2_quote", "")],
                "created_at": datetime.now(timezone.utc).isoformat(),
            })

    try:
        result = sb.table("injection_reports").insert(rows).execute()
        saved  = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-B rows (gemini-1.5-flash, 1M context)")
        return True
    except Exception as e:
        log.error(f"Failed to save S2-B results: {e}")
        return False


# ── Entry point ───────────────────────────────────────────────────────────────
def run_s2b(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = f"s2b_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-B Coordination Analyzer START | run_id={run_id} | cycle={cycle} ===")
    log.info("S2-B: waiting 180s for Gemini RPM window from S1 to clear...")
    time.sleep(180)

    try:
        sb     = get_supabase()
        client = get_gemini()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    reports = fetch_raw_articles(sb, cycle)  # FIX-2: raw articles not summaries
    if not reports:
        log.warning("No raw articles found (last 6h) — S2-B cannot run")
        return {"status": "NO_RAW_ARTICLES", "reports_analyzed": 0}
    if len(reports) < 2:
        log.warning(f"Only {len(reports)} raw articles — need 2+")
        return {"status": "INSUFFICIENT_ARTICLES", "reports_analyzed": len(reports)}

    rpm_guard = GeminiRPMGuard(model=MODEL)
    analysis  = call_coordination_analyzer(client, reports, rpm_guard)

    if analysis is None:
        return {"status": "ANALYSIS_FAILED", "reports_analyzed": len(reports)}

    saved   = save_coordination_report(sb, reports, analysis, run_id, cycle)
    elapsed = round(time.time() - start, 1)

    summary = {
        "status":                     "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":                     run_id,
        "cycle":                      cycle,
        "reports_analyzed":           len(reports),
        "findings":                   len(analysis.get("findings", [])),
        "overall_coordination_score": analysis.get("overall_coordination_score", 0),
        "dominant_narrative":         analysis.get("dominant_coordinated_narrative", "none detected"),
        "model":                      MODEL,
        "elapsed_seconds":            elapsed,
    }
    log.info(f"=== S2-B COMPLETE | {len(reports)} reports | {summary['findings']} findings | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    run_s2b(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
