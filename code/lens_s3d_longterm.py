"""
lens_s3d_longterm.py — System 3 Position D: Long-term Researcher
Project Lens | LENS-010
Model: CC-82 -- mistral ministral-8b-2512 (Cerebras withdrew its free tier
       ~2026-08-17; S3-D saved nothing after 2026-09-03)
Reads: lens_reports + injection_reports (last 30 days)
Output: lens_system3_reports (position=S3-D, report_type=TYPE_A)

Purpose: Find structural changes accumulating over 30 days.
         What windows are closing? What is being quietly built?
         Which actors are gaining structural advantage while appearing inactive?
         2x per week cadence (Monday + Thursday).

Session: LENS-010
"""

import os, json, time, logging
from datetime import datetime, timezone, timedelta
from typing import Optional
import requests
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-D] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-D")

SUPABASE_URL   = os.environ.get("SUPABASE_URL")
SUPABASE_KEY   = os.environ.get("SUPABASE_SERVICE_KEY")
MISTRAL_KEY    = os.environ.get("MISTRAL_API_KEY")
MODEL          = "ministral-8b-2512"
PROVIDER       = "mistral"
# CC-82: the old fallback cap was 2500 and the answer was cut mid-string at
# ~9.2-9.9K chars on 2026-09-14 and 2026-09-17. S3-A and the S3 report run
# the same model at 8000 and finish at 3.4-4.1K tokens. A truncated answer
# is now FAILED, never saved (D-027).
MAX_TOKENS     = 8000
LOOKBACK_DAYS  = 30
MAX_S1_REPORTS = 30
MAX_S2_REPORTS = 20

SYSTEM_PROMPT = """You are S3-D: Long-term Researcher for Project Lens.

You operate at the longest time horizon in the three-system architecture.
Your window: 30 days. Your question: what is being STRUCTURALLY BUILT?

System 1 sees daily events. System 2 sees daily manipulation.
You see what is accumulating beneath both — the structural changes that only become
visible when you hold 30 days of signal simultaneously.

FIVE RESEARCH QUESTIONS:

1. STRUCTURAL ACCUMULATION:
   What changes have been accumulating steadily across 30 days that no single
   daily report captures? What is different about the world now vs 30 days ago?

2. CLOSING WINDOWS:
   Which opportunities, alliances, or strategic positions are quietly closing?
   What can be done today that will be impossible in 90 days?

3. SILENT BUILDERS:
   Which actors have been consistently gaining structural position while generating
   minimal news coverage? Who is winning without appearing to compete?

4. INJECTION DRIFT:
   Across 30 days of S2 injection reports — is the injection pattern CHANGING?
   Are new vocabulary vectors appearing? Are new sources being used?
   Injection pattern evolution reveals strategic intent and operational tempo.

5. CONVERGENCE SIGNALS:
   Where are multiple slow-moving trends converging toward the same pressure point?
   What structural collision is becoming inevitable?
6. CAPABILITY 2 — MANUFACTURED CAUSALITY (30-day verification):
   Was the claimed cause actually followed by the claimed effect over the last 30 days?
   Is a causal chain being repeated and cited as established fact without verification?
   Are new events being interpreted THROUGH a pre-established causal narrative?
   Does the chain consistently serve the same strategic beneficiary (Cui Bono)?
7. ACH CHECK (adversarial hardening):
   What is the strongest evidence that CONTRADICTS your structural analysis?
   If your 30-day pattern analysis is wrong, what would the data have looked like instead?
   State the most disconfirming evidence explicitly.
8. SECTARIAN TRAP 30-DAY TRACKING:
   What was the baseline level of ethnic/religious/political tension 30 days ago vs today?
   Is escalation organic (proportional to actual events) or manufactured (disproportionate)?
   What historical analog predicts what happens next if the current trajectory continues?

OUTPUT FORMAT — valid JSON only:
{
  "structural_accumulation": "what has changed structurally over 30 days",
  "closing_windows": ["opportunity closing 1", "opportunity closing 2"],
  "silent_builders": [
    {"actor": "...", "structural_gain": "...", "evidence_span": "X days"}
  ],
  "injection_drift": "how injection patterns have evolved over 30 days",
  "convergence_signals": ["convergence 1", "convergence 2"],
  "patterns_found": [
    {"pattern": "30-day pattern name", "evidence": "...", "confidence": 0.0, "time_horizon": "90d"}
  ],
  "structural_trends": {
    "gaining": ["actor/trend gaining structural position"],
    "losing": ["actor/trend losing structural position"],
    "stable": ["actor/trend holding position despite volatility"]
  },
  "signals_to_watch": ["signal1", "signal2", "signal3"],
  "corrections_to_s2": [
    {"correction": "pattern S2 is systematically missing", "reason": "visible only over 30 days"}
  ],
  "summary": "2-3 sentence plain English summary of 30-day structural picture",
  "quality_score": 0.0,
  "capability_2": {
    "manufactured_causality_detected": false,
    "claimed_chain": "the causal claim being repeated as established fact, or null",
    "verified": "YES|NO|PARTIALLY — was cause actually followed by effect over 30 days?",
    "cui_bono": "who benefits from this causal narrative being accepted"
  },
  "ach_check": {
    "strongest_contradiction": "evidence most challenging to the 30-day structural analysis",
    "failure_mode": "if this analysis is wrong, what would the data have looked like"
  },
  "sectarian_trap_30d": {
    "baseline_tension": "LOW|MODERATE|HIGH — 30 days ago",
    "current_tension": "LOW|MODERATE|HIGH — today",
    "escalation_type": "ORGANIC|MANUFACTURED|UNCLEAR",
    "historical_analog": "which historical precedent matches this trajectory, or null",
    "predicted_next": "what the analog suggests happens next"
  }
}
Rules: Ground EVERYTHING in specific evidence spanning the full 30-day window.
Single events are noise. Patterns across 30 days are signal.
If you can only see 7 days of data, report what you can see and note the limitation."""


def should_run_today() -> bool:
    """S3-D runs 2x per week: Monday (0) and Thursday (3) only."""
    return datetime.now(timezone.utc).weekday() in (0, 3)

def get_window_days() -> int:
    """
    3-I: Dynamic window based on weekday.
    Monday   = 30-day operational window (what changed recently)
    Thursday = 90-day structural window  (what is being built long-term)
    180-day deferred until True History DB has 90+ days of data.
    """
    weekday = datetime.now(timezone.utc).weekday()
    if weekday == 3:   # Thursday
        return 90
    return 30          # Monday (default)


def fetch_s1_reports(sb: Client, lookback_days: int = LOOKBACK_DAYS) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    limit  = MAX_S1_REPORTS if lookback_days <= 30 else MAX_S1_REPORTS * 3
    r = sb.table("lens_reports") \
        .select("id,domain_focus,summary,cycle,generated_at") \
        .gte("generated_at", cutoff).order("generated_at", desc=False) \
        .limit(limit).execute()
    log.info(f"Fetched {len(r.data or [])} S1 reports (window={lookback_days}d)")
    return r.data or []


def fetch_s2_reports(sb: Client, lookback_days: int = LOOKBACK_DAYS) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    limit  = MAX_S2_REPORTS if lookback_days <= 30 else MAX_S2_REPORTS * 3
    r = sb.table("injection_reports") \
        .select("analyst,injection_type,evidence,confidence_score,created_at") \
        .gte("created_at", cutoff).order("created_at", desc=False) \
        .limit(limit).execute()
    log.info(f"Fetched {len(r.data or [])} S2 reports (window={lookback_days}d)")
    return r.data or []


def run_s3d(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-D Long-term Researcher START | run_id={run_id} ===")

    window_days = get_window_days()
    log.info(f"S3-D window: {window_days}-day (Monday=30d, Thursday=90d)")
    if not should_run_today():
        log.info(f"S3-D skipping — not Monday or Thursday (today={datetime.now(timezone.utc).strftime(chr(37)+chr(65))})")
        return {"status": "SKIPPED", "run_id": run_id}
    if not MISTRAL_KEY:
        log.error("MISTRAL_API_KEY not set")
        return {"status": "ERROR", "run_id": run_id}

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    s1 = fetch_s1_reports(sb, window_days)
    s2 = fetch_s2_reports(sb, window_days)
    log.info(f"Fetched {len(s1)} S1 reports + {len(s2)} S2 reports (last {LOOKBACK_DAYS} days)")

    if not s1:
        log.warning("No S1 reports found")
        return {"status": "NO_REPORTS", "run_id": run_id}

    lines = [
        f"=== S1 LENS REPORTS — last {LOOKBACK_DAYS} days ({len(s1)} reports) ===",
        "Hold ALL of these simultaneously. Find what accumulates across the full window.\n",
        "─" * 60,
    ]
    for r in s1:
        lines += [
            f"\nDate: {r.get('generated_at','')[:10]} | Domain: {r.get('domain_focus')} | Cycle: {r.get('cycle')}",
            f"Analysis: {(r.get('summary') or '')[:450]}",
            "─" * 30,
        ]
    if s2:
        lines += [
            f"\n=== S2 INJECTION REPORTS — last {LOOKBACK_DAYS} days ({len(s2)} reports) ===",
            "Look for EVOLUTION in injection patterns over time.\n",
        ]
        for r in s2:
            lines += [
                f"Date: {r.get('created_at','')[:10]} | Analyst: {r.get('analyst')} | Type: {r.get('injection_type')} | Score: {r.get('confidence_score')}",
                f"Evidence: {str(r.get('evidence') or '')[:200]}",
            ]
    lines.append("\nFind 30-day structural patterns. Output JSON only.")
    prompt = "\n".join(lines)

    log.info(f"Prompt: {len(prompt)} chars | Model: {MODEL}")

    analysis = None
    for attempt in range(1, 3):
        log.info(f"S3-D calling {PROVIDER}/{MODEL} (attempt {attempt}, "
                 f"prompt {len(SYSTEM_PROMPT) + len(prompt)} chars, max_tokens {MAX_TOKENS})")
        try:
            r = requests.post(
                "https://api.mistral.ai/v1/chat/completions", timeout=180,
                headers={"Authorization": f"Bearer {MISTRAL_KEY}",
                         "Content-Type": "application/json"},
                json={"model": MODEL, "response_format": {"type": "json_object"},
                      "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                                   {"role": "user", "content": prompt}],
                      "max_tokens": MAX_TOKENS, "temperature": 0.3})
        except Exception as e:
            log.warning(f"S3-D attempt {attempt}: transport {repr(e)[:160]}")
            if attempt < 2: time.sleep(20)
            continue
        if r.status_code != 200:
            retryable = r.status_code not in (400, 401, 402, 403, 404, 422) \
                and (r.headers.get("x-should-retry") or "").lower() != "false"
            log.warning(f"S3-D attempt {attempt}: HTTP {r.status_code} "
                        f"retryable={retryable} {r.text[:200]}")
            if retryable and attempt < 2: time.sleep(20)
            continue
        b = r.json()
        ch = (b.get("choices") or [{}])[0]
        u = b.get("usage") or {}
        fin = ch.get("finish_reason")
        log.info(f"S3-D usage: {PROVIDER}/{MODEL} in={u.get('prompt_tokens')} "
                 f"out={u.get('completion_tokens')} max_tokens={MAX_TOKENS} "
                 f"finish_reason={fin}")
        if str(fin).lower() in ("length", "max_tokens"):
            log.error(f"S3-D: finish_reason={fin} -- truncated at {MAX_TOKENS}, not saved")
            break
        raw = (ch.get("message") or {}).get("content") or ""
        import re as _re
        raw = _re.sub(r"```json|```", "", raw).strip()
        try:
            analysis = json.loads(raw)
            break
        except Exception as e:
            log.warning(f"S3-D attempt {attempt}: JSON parse failed ({e}); finish={fin}")

    if not analysis:
        log.error("S3-D failed")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-D",
        "report_type":       "TYPE_A",
        "time_horizon":      "30_DAY",
        "patterns_found":    json.dumps(analysis.get("patterns_found", [])),
        "structural_trends": json.dumps(analysis.get("structural_trends", {})),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("signals_to_watch", [])),
        "corrections_to_s2": json.dumps(analysis.get("corrections_to_s2", [])),
        "model_used":        MODEL,
        "provider":          PROVIDER,
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-D",
        "source_reports":    json.dumps([r.get("id") for r in s1[:5]]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)
    log.info(f"=== S3-D COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s ===")

    print(json.dumps({
        "status":      "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":      run_id,
        "patterns":    len(analysis.get("patterns_found", [])),
        "summary":     analysis.get("summary", "")[:120],
        "quality":     analysis.get("quality_score", 0),
        "elapsed":     elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3d(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
