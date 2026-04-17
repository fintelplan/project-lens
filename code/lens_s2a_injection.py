"""
lens_s2a_injection.py — System 2 Position A: Injection Tracer
Project Lens | LENS-010
Model: llama-3.3-70b-versatile (Groq — GROQ_S2_API_KEY)
Input: lens_reports (Supabase, latest cycle)
Output: injection_reports (Supabase)

LENS-010 additions:
  - Language sanitization before model call (lens_sanitize.py)
    Replaced phrases become injection evidence (EMOTIONAL_PRIME type)
  - injection_goal field (Q4: what is injection trying to make S1 believe?)
  - contamination_contribution field (SURFACE / MODERATE / DEEP per report)
  - correction_to_ma structured field — HARD correction channel for Mission Analyst
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

from lens_sanitize import sanitize_text, add_runtime_flag

# ── Quota guard (LR-074) ──────────────────────────────────────────────────────
from lens_quota_guard import guard_check_with_fallback

# ── Response schema validator (I2) ────────────────────────────────────────────
from lens_response_guard import validate_parsed_response, format_validation_for_log

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-A] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2a")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL            = "llama-3.3-70b-versatile"
MAX_TOKENS       = 1800
TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 8
MAX_REPORT_CHARS = 6000
STAGGER_SLEEP    = 6  # seconds between per-report API calls

# ── TPMGuard ─────────────────────────────────────────────────────────────────
class TPMGuard:
    """Rolling 60-second token window for Groq free tier (6000 TPM)."""
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
SYSTEM_PROMPT = """You are S2-A Injection Tracer for Project Lens, an OSINT intelligence system.

You receive an intelligence report that has already been SANITIZED — emotionally loaded vocabulary
has been replaced with neutral language. The replaced phrases are listed separately as pre-detected
injection vocabulary.

Your job: analyze the sanitized report for DEEPER injection patterns that survive sanitization —
structural manipulation, false equivalences, omissions, and source laundering that vocabulary
replacement cannot catch. The pre-detected phrases are your starting point, not your endpoint.

You detect 6 injection types:
- PHRASE_SYNC:     unusual framing appearing verbatim/near-verbatim across unrelated sources
- EMOTIONAL_PRIME: emotionally loaded language placed before factual claims (pre-detected phrases qualify)
- SOURCE_LAUNDER:  state media or anonymous claim cited as if from neutral/expert source ("analysts say")
- FALSE_EQUIV:     false balance framing that normalizes an extreme or authoritarian position
- OVERTON_SHIFT:   language that gradually normalizes positions outside democratic consensus
- FACT_VOID:       confident assertion with no verifiable basis, source, or evidence provided

Rules:
- Quote directly from the sanitized report text
- Confidence score: 0.0-1.0. Only flag if confidence >= 0.5
- EMOTIONAL_PRIME findings: always include the original (pre-sanitization) phrase from the replaced_phrases list
- If nothing suspicious remains, return empty findings — do NOT invent patterns
- You are an analyst, not a censor. Flag for human review, not for removal.

contamination_contribution levels:
- SURFACE:  1-2 injection patterns, isolated, do not shape the overall conclusion
- MODERATE: 3+ patterns, or patterns that shaped a key finding
- DEEP:     patterns that dominated the report — the core conclusion is injection-derived

Respond ONLY with valid JSON. No preamble. No explanation outside JSON.

Format:
{
  "analyst": "S2-A",
  "lens_id": "<provided>",
  "findings": [
    {
      "injection_type": "<one of the 6 types>",
      "flagged_phrase": "<exact quoted text from sanitized report, or original phrase for EMOTIONAL_PRIME>",
      "confidence": <0.0-1.0>,
      "explanation": "<1-2 sentence explanation of why this qualifies>",
      "actor_beneficiary": "<who benefits from this injection, or 'unclear'>"
    }
  ],
  "overall_injection_score": <0.0-1.0>,
  "injection_goal": "<Q4: what is this injection trying to make the reader believe? 1 sentence. 'none detected' if clean>",
  "contamination_contribution": "<SURFACE|MODERATE|DEEP>",
  "analyst_note": "<optional 1 sentence or empty string>"
}"""


# ── Database helpers ──────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def get_groq() -> Groq:
    return Groq(api_key=os.environ["GROQ_S2_API_KEY"])


def fetch_latest_reports(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    try:
        q = sb.table("lens_reports").select(
            "id, domain_focus, summary, cycle, generated_at"
        )
        if cycle:
            q = q.eq("cycle", cycle).order("generated_at", desc=True).limit(8)
        else:
            q = q.order("generated_at", desc=True).limit(4)
        reports = q.execute().data or []
        log.info(f"Fetched {len(reports)} lens reports (cycle={cycle})")
        return reports
    except Exception as e:
        log.error(f"Failed to fetch lens_reports: {e}")
        return []


def truncate_report(text: str) -> str:
    if not text:
        return ""
    return text[:MAX_REPORT_CHARS] + "\n[...truncated]" if len(text) > MAX_REPORT_CHARS else text


# ── Core analysis ─────────────────────────────────────────────────────────────
def call_injection_tracer(client: Groq, report: dict, replaced_phrases: list[str]) -> Optional[dict]:
    """Call llama-3.3-70b on sanitized report text to trace injection patterns."""
    report_id = report.get("id", "unknown")
    lens_name = report.get("domain_focus", "unknown")
    san_text  = truncate_report(report.get("_sanitized_summary", report.get("summary", "")))

    if not san_text.strip():
        log.warning(f"Empty summary for {report_id} — skipping")
        return None

    # Disclosure of pre-sanitized phrases
    replaced_note = ""
    if replaced_phrases:
        replaced_note = (
            f"\n\nPRE-DETECTED INJECTION VOCABULARY (already replaced in text above):\n"
            + ", ".join(f'"{p}"' for p in replaced_phrases)
            + "\nThese qualify as EMOTIONAL_PRIME injections. Include them in your findings."
        )

    user_message = (
        f"Analyze this intelligence report for narrative injection patterns.\n\n"
        f"Lens: {lens_name}\nReport ID: {report_id}\n\n"
        f"--- SANITIZED REPORT START ---\n{san_text}\n--- SANITIZED REPORT END ---"
        f"{replaced_note}\n\nReturn JSON only."
    )

    _tpm.wait_if_needed(2000, label="S2-A")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-A calling model for {lens_name} (attempt {attempt})")
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

            # ── Response schema validation (I2) ──────────────────────────────
            vr = validate_parsed_response(parsed, "S2-A")
            if not vr.valid:
                log.warning(format_validation_for_log(vr))

            # Ensure pre-sanitized phrases always appear in findings (never silently dropped)
            if replaced_phrases:
                existing_flags = {f.get("flagged_phrase", "").lower() for f in parsed.get("findings", [])}
                for phrase in replaced_phrases:
                    if phrase.lower() not in existing_flags:
                        parsed.setdefault("findings", []).append({
                            "injection_type":  "EMOTIONAL_PRIME",
                            "flagged_phrase":  phrase,
                            "confidence":      0.7,
                            "explanation":     (
                                f"Pre-sanitization vocabulary: '{phrase}' was present in original S1 report. "
                                "Emotionally loaded language absorbed from source articles."
                            ),
                            "actor_beneficiary": "unclear",
                        })

            log.info(
                f"S2-A result for {lens_name}: "
                f"{len(parsed.get('findings', []))} findings, "
                f"score={parsed.get('overall_injection_score', 0)}, "
                f"contamination={parsed.get('contamination_contribution', '?')}"
            )
            return parsed

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                log.warning(f"Rate limit attempt {attempt} — sleeping 20s"); time.sleep(20)
            elif "503" in err:
                log.warning(f"503 attempt {attempt} — sleeping 15s"); time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"S2-A failed after {MAX_RETRIES} attempts for {lens_name}")
    return None


# ── Hard correction channel ───────────────────────────────────────────────────
def build_correction_to_ma(analysis: dict, replaced_phrases: list[str]) -> dict:
    """
    Build a STRUCTURED correction for the Mission Analyst.

    This is the hard correction channel — Python code, not prompt suggestion.
    MA's apply_s2_corrections() reads this before calling its LLM.
    If mandatory=True the MA cannot synthesize without applying this correction.

    action values:
      DOWNGRADE — reduce confidence in a signal S1 flagged
      FLAG      — surface for explicit human review (MA notes it, does not suppress)
      NONE      — no correction needed
    """
    findings      = analysis.get("findings", [])
    inj_score     = analysis.get("overall_injection_score", 0.0)
    inj_goal      = analysis.get("injection_goal", "none detected")
    contamination = analysis.get("contamination_contribution", "SURFACE")

    if not findings and inj_score < 0.3:
        return {"action": "NONE", "mandatory": False}

    # Severity mapping
    if contamination == "DEEP" or inj_score >= 0.7:
        action = "DOWNGRADE"
        confidence_adjustment = -0.45
        mandatory = True
    elif contamination == "MODERATE" or inj_score >= 0.4:
        action = "DOWNGRADE"
        confidence_adjustment = -0.25
        mandatory = True
    else:
        action = "FLAG"
        confidence_adjustment = -0.1
        mandatory = False

    reason_parts = []
    if replaced_phrases:
        reason_parts.append(
            f"Language sanitization replaced {len(replaced_phrases)} injection phrase(s): "
            + ", ".join(f"'{p}'" for p in replaced_phrases[:4])
            + ". These originated in source framing, not independent analysis."
        )
    high_conf = [f for f in findings if f.get("confidence", 0) >= 0.7]
    if high_conf:
        types = list({f["injection_type"] for f in high_conf})
        reason_parts.append(f"{len(high_conf)} high-confidence injection pattern(s) detected: {types}.")
    if inj_goal and inj_goal != "none detected":
        reason_parts.append(f"Injection goal (Q4): {inj_goal}")

    return {
        "action":                action,
        "source_analyst":        "S2-A",
        "contamination_depth":   contamination,
        "injection_score":       inj_score,
        "confidence_adjustment": confidence_adjustment,
        "reason":                " ".join(reason_parts) or "Injection patterns detected — see findings.",
        "injection_goal":        inj_goal,
        "mandatory":             mandatory,
    }


# ── Save to Supabase ──────────────────────────────────────────────────────────
def save_injection_report(
    sb: Client,
    report: dict,
    analysis: dict,
    replaced_phrases: list[str],
    run_id: str,
    cycle: Optional[str],
) -> bool:
    findings   = analysis.get("findings", [])
    inj_score  = analysis.get("overall_injection_score", 0.0)
    correction = build_correction_to_ma(analysis, replaced_phrases)

    rows = []
    base = {
        "run_id":          run_id,
        "cycle":           cycle,
        "lens_report_id":  report.get("id"),
        "analyst":         "S2-A",
        "source_id":       report.get("source_id"),
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }

    if not findings:
        rows.append({**base,
            "injection_type":   "NONE",
            "confidence_score": 0.0,
            "flagged_phrases":  [],
            "evidence": {
                "analyst_note":     analysis.get("analyst_note", "No injections detected"),
                "injection_goal":   analysis.get("injection_goal", "none detected"),
                "contamination":    analysis.get("contamination_contribution", "SURFACE"),
                "replaced_phrases": replaced_phrases,
                "correction_to_ma": correction,
            },
        })
    else:
        for finding in findings:
            rows.append({**base,
                "injection_type":   finding.get("injection_type", "UNKNOWN"),
                "confidence_score": float(finding.get("confidence", 0.0)),
                "flagged_phrases":  [finding.get("flagged_phrase", "")],
                "evidence": {
                    "flagged_phrase":    finding.get("flagged_phrase", ""),
                    "explanation":       finding.get("explanation", ""),
                    "actor_beneficiary": finding.get("actor_beneficiary", "unclear"),
                    "overall_score":     inj_score,
                    "injection_goal":    analysis.get("injection_goal", "none detected"),
                    "contamination":     analysis.get("contamination_contribution", "SURFACE"),
                    "replaced_phrases":  replaced_phrases,
                    "correction_to_ma":  correction,   # ← HARD CORRECTION CHANNEL
                    "analyst_note":      analysis.get("analyst_note", ""),
                },
            })

    try:
        result = sb.table("injection_reports").insert(rows).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-A rows for {report.get('domain_focus', '?')}")
        return saved > 0
    except Exception as e:
        log.error(f"Failed to save S2-A results: {e}")
        return False


# ── Entry point ───────────────────────────────────────────────────────────────
def run_s2a(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = f"s2a_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-A Injection Tracer START | run_id={run_id} | cycle={cycle} ===")

    try:
        sb     = get_supabase()
        client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # ── Quota guard pre-flight (LR-074) ───────────────────────────────────────
    quota_guard = guard_check_with_fallback(positions=["S2-A"], run_id=run_id, sb=sb)
    skipped = [p for p, d in quota_guard.position_decisions.items() if d == "SKIP"]
    if "S2-A" in skipped:
        reason = quota_guard.group_results[0].reason if quota_guard.group_results else "quota SKIP"
        log.warning(f"S2-A quota SKIP: {reason}")
        return {"status": "QUOTA_SKIP", "reason": reason, "reports_analyzed": 0}

    reports = fetch_latest_reports(sb, cycle)
    if not reports:
        log.warning("No lens_reports found — S2-A cannot run")
        return {"status": "NO_REPORTS", "reports_analyzed": 0}

    saved_ok       = 0
    total_replaced = 0
    results        = []

    for i, report in enumerate(reports):
        if i > 0:
            time.sleep(STAGGER_SLEEP)

        # Sanitize BEFORE API call — replace injected vocabulary, collect evidence
        raw_text = report.get("summary", "")
        san = sanitize_text(raw_text[:MAX_REPORT_CHARS] if raw_text else "")
        replaced_phrases = san["replaced_phrases"]
        total_replaced  += len(replaced_phrases)

        # Attach sanitized text to report dict for the tracer
        report["_sanitized_summary"] = san["sanitized_text"]

        analysis = call_injection_tracer(client, report, replaced_phrases)
        if analysis is None:
            log.warning(f"S2-A: no result for {report.get('domain_focus', '?')}")
            continue

        ok = save_injection_report(sb, report, analysis, replaced_phrases, run_id, cycle)
        if ok:
            saved_ok += 1

        correction = build_correction_to_ma(analysis, replaced_phrases)
        results.append({
            "lens":                  report.get("domain_focus", "?"),
            "findings":              len(analysis.get("findings", [])),
            "score":                 analysis.get("overall_injection_score", 0),
            "contamination":         analysis.get("contamination_contribution", "?"),
            "correction_action":     correction.get("action", "NONE"),
            "correction_mandatory":  correction.get("mandatory", False),
        })

    elapsed = round(time.time() - start, 1)
    summary = {
        "status":                    "COMPLETE" if saved_ok > 0 else ("SAVE_FAILED" if reports else "NO_REPORTS"),
        "run_id":                    run_id,
        "cycle":                     cycle,
        "reports_analyzed":          len(reports),
        "reports_saved":             saved_ok,
        "total_phrases_sanitized":   total_replaced,
        "results":                   results,
        "elapsed_seconds":           elapsed,
    }
    log.info(
        f"=== S2-A COMPLETE | {len(reports)} reports | {saved_ok} saved | "
        f"{total_replaced} phrases sanitized | {elapsed}s ==="
    )
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    run_s2a(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
