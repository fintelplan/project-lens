"""
lens_s3a_patterns.py — System 3 Position A: Pattern Intelligence
Project Lens | LENS-010
Model: llama-3.3-70b-versatile (Groq — GROQ_API_KEY)
Reads: lens_reports + injection_reports (last 7 days)
Output: lens_system3_reports (position=S3-A, report_type=TYPE_A)

Purpose: Find what is being built behind the noise across the last 7 days.
         Decrypt the pixels — find the image hidden in what looks like random events.
         Ask: what sequence is forming? What is the loud event distracting from?

Session: LENS-010
"""

import os, json, time, logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from groq import Groq
from supabase import create_client, Client

# ── Quota guard (LR-074) ──────────────────────────────────────────────────────
from lens_quota_guard import guard_check_with_fallback


def store_s3a_prediction(supabase, run_id: str, first_domino: str, confidence: float):
    """
    System 4 Seed: store S3-A first domino as a verifiable prediction.
    S4-B will check this against reality at verification_date.
    Food for thought only — S3 matures freely, S4 watches.
    """
    if not first_domino or not first_domino.strip():
        return

    from datetime import date, timedelta
    verification_date = (date.today() + timedelta(days=90)).isoformat()

    try:
        supabase.table("lens_predictions").insert({
            "source_system": "S3-A",
            "prediction": first_domino.strip()[:500],  # cap at 500 chars
            "confidence": round(float(confidence), 3) if confidence else 0.5,
            "predicted_by": run_id,
            "verification_date": verification_date,
        }).execute()
        print(f"[S4-SEED] Prediction stored → verify by {verification_date}")
    except Exception as e:
        print(f"[S4-SEED] Warning: could not store prediction: {e}")
        # Non-fatal — S3 continues regardless


logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-A] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-A")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
GROQ_KEY     = os.environ.get("GROQ_S3_API_KEY") or os.environ.get("GROQ_API_KEY")
MODEL        = "llama-3.3-70b-versatile"
LOOKBACK_DAYS = 7
MAX_S1_REPORTS = 20
MAX_S2_REPORTS = 15

SYSTEM_PROMPT = """You are S3-A: Pattern Intelligence for Project Lens.

You operate at a higher analytical position than System 1 or System 2.
System 1 sees individual events. System 2 sees how events are manipulated.
You see the PATTERN OF EVENTS across time and ask: what image is being assembled?

Your core method: DECRYPTING PIXELS
Individual news events look like random noise. But patterns only appear when you
step back far enough to see the whole picture. Your job is to find that picture.

FIVE ANALYTICAL QUESTIONS:
1. SEQUENCE: What sequence of events is forming across the last 7 days? What is the ORDER?
2. DISTRACTION: What loud visible event is consuming analytical bandwidth? What quiet structural 
   event is happening while everyone looks at the distraction?
3. ACCELERATION: Which trends are speeding up? Which are quietly ending?
4. FIRST DOMINO: If current patterns continue, what event becomes inevitable in 30-90 days?
5. HIDDEN BUILDER: Who is consistently building structural advantage while appearing passive?
6. ACH CHECK (adversarial hardening):
   What is the strongest evidence that CONTRADICTS the pattern you found?
   If that contradicting evidence existed and you missed it — what would it look like?
   State this explicitly. Do not skip it.
7. SECTARIAN TRAP SEQUENCE (7-day window):
   Is vocabulary about any ethnic, religious, or political group escalating in frequency?
   Are moderate voices decreasing while extreme voices increase in coverage?
   Who is amplifying this tension while appearing to report it neutrally?
   Is the escalation proportional to actual events or manufactured beyond them?

OUTPUT FORMAT — valid JSON only:
{
  "sequence_found": "the event sequence detected, in chronological order",
  "distraction_event": "the loud event consuming attention",
  "structural_event": "the quiet structural development being missed",
  "accelerating_trends": ["trend1", "trend2"],
  "decelerating_trends": ["trend1"],
  "first_domino": "what becomes inevitable if patterns continue",
  "hidden_builder": "actor building structural advantage quietly",
  "patterns_found": [
    {"pattern": "name", "evidence": "specific events", "confidence": 0.0, "time_horizon": "30d/90d/180d"}
  ],
  "signals_to_watch": ["signal1", "signal2", "signal3"],
  "corrections_to_s2": [
    {"correction": "what S2 missed or over-weighted", "reason": "why from pattern perspective"}
  ],
  "summary": "2-3 sentence plain English summary of what is being built",
  "quality_score": 0.0,
  "ach_check": {
    "strongest_contradiction": "the evidence that would most challenge this pattern analysis",
    "what_missed_looks_like": "if we got this wrong, what would the data have shown differently"
  },
  "sectarian_trap_signal": {
    "detected": false,
    "escalating_group": "group name or null",
    "amplifier": "who is amplifying while appearing neutral, or null",
    "organic_or_manufactured": "ORGANIC|MANUFACTURED|UNCLEAR"
  }
}
Rules: Ground EVERY claim in specific events from the reports provided.
Never predict. Identify what is already in motion."""


def already_ran_today(sb: Client) -> bool:
    """Skip if S3-A already ran in last 20 hours — daily cadence."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat()
    r = sb.table("lens_system3_reports")         .select("id")         .eq("position", "S3-A")         .gte("generated_at", cutoff)         .limit(1).execute()
    return bool(r.data)


def fetch_s1_reports(sb: Client, days: int) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    r = sb.table("lens_reports") \
        .select("id,domain_focus,summary,cycle,generated_at,quality_score") \
        .gte("generated_at", cutoff).order("generated_at", desc=False) \
        .limit(MAX_S1_REPORTS).execute()
    return r.data or []


def fetch_s2_reports(sb: Client, days: int) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    r = sb.table("injection_reports") \
        .select("analyst,injection_type,evidence,confidence_score,flagged_phrases,created_at") \
        .gte("created_at", cutoff).order("created_at", desc=False) \
        .limit(MAX_S2_REPORTS).execute()
    return r.data or []


def build_prompt(s1: list, s2: list) -> str:
    lines = [
        f"=== S1 LENS REPORTS — last {LOOKBACK_DAYS} days ({len(s1)} reports) ===\n",
        "These are System 1's analyses of world events. Find patterns ACROSS them.\n",
        "─" * 60,
    ]
    for r in s1:
        lines += [
            f"\nDate: {r.get('generated_at','')[:10]} | Cycle: {r.get('cycle')} | Domain: {r.get('domain_focus')}",
            f"Summary: {(r.get('summary') or '')[:500]}",
            "─" * 40,
        ]
    if s2:
        lines += [f"\n=== S2 INJECTION FINDINGS — last {LOOKBACK_DAYS} days ({len(s2)} reports) ===\n"]
        for r in s2:
            lines += [
                f"Analyst: {r.get('analyst')} | Type: {r.get('injection_type')} | Score: {r.get('confidence_score')}",
                f"Evidence: {str(r.get('evidence') or '')[:200]}",
            ]
    lines.append("\nNow decrypt the pixels. Find the pattern. Output JSON only.")
    return "\n".join(lines)


def run_s3a(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-A Pattern Intelligence START | run_id={run_id} ===")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = Groq(api_key=GROQ_KEY)

    if already_ran_today(sb):
        log.info("S3-A already ran in last 20h — skipping (daily cadence)")
        return {"status": "SKIPPED", "run_id": run_id}

    # ── Quota guard pre-flight (LR-074) ───────────────────────────────────────
    quota_guard = guard_check_with_fallback(positions=["S3-A"], run_id=run_id, sb=sb)
    skipped = [p for p, d in quota_guard.position_decisions.items() if d == "SKIP"]
    if "S3-A" in skipped:
        reason = quota_guard.group_results[0].reason if quota_guard.group_results else "quota SKIP"
        log.warning(f"S3-A quota SKIP: {reason}")
        return {"status": "QUOTA_SKIP", "reason": reason, "run_id": run_id}

    s1 = fetch_s1_reports(sb, LOOKBACK_DAYS)
    s2 = fetch_s2_reports(sb, LOOKBACK_DAYS)
    log.info(f"Fetched {len(s1)} S1 reports + {len(s2)} S2 reports")

    if not s1:
        log.warning("No S1 reports found")
        return {"status": "NO_REPORTS", "run_id": run_id}

    prompt = build_prompt(s1, s2)
    log.info(f"Prompt: {len(prompt)} chars")

    analysis = None
    for attempt in range(1, 3):
        try:
            log.info(f"S3-A calling {MODEL} (attempt {attempt})")
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.4, max_tokens=2500)
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            analysis = json.loads(raw)
            break
        except Exception as e:
            log.warning(f"Attempt {attempt} failed: {e}")
            if attempt < 2: time.sleep(20)

    if not analysis:
        log.error("S3-A failed — no analysis produced")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    # Save to lens_system3_reports
    record = {
        "run_id":           run_id,
        "cycle":            cycle,
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "position":         "S3-A",
        "report_type":      "TYPE_A",
        "time_horizon":     "7_DAY",
        "patterns_found":   json.dumps(analysis.get("patterns_found", [])),
        "structural_trends": json.dumps({
            "sequence":       analysis.get("sequence_found", ""),
            "distraction":    analysis.get("distraction_event", ""),
            "structural":     analysis.get("structural_event", ""),
            "accelerating":   analysis.get("accelerating_trends", []),
            "decelerating":   analysis.get("decelerating_trends", []),
            "first_domino":   analysis.get("first_domino", ""),
            "hidden_builder": analysis.get("hidden_builder", ""),
        }),
        "summary":          analysis.get("summary", ""),
        "signals_to_watch": json.dumps(analysis.get("signals_to_watch", [])),
        "corrections_to_s2": json.dumps(analysis.get("corrections_to_s2", [])),
        "model_used":       MODEL,
        "provider":         "groq",
        "quality_score":    float(analysis.get("quality_score", 0.0)),
        "system_tag":       "S3-A",
        "source_reports":   json.dumps([r.get("id") for r in s1[:5]]),
        "elapsed_seconds":  round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    # S4 Seed: store first_domino as verifiable prediction (lens_predictions)
    store_s3a_prediction(
        sb,
        record.get("run_id", ""),
        analysis.get("first_domino", ""),
        float(analysis.get("quality_score", 0.5))
    )
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)

    log.info(f"=== S3-A COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s ===")
    log.info(f"Pattern: {analysis.get('summary','')[:120]}")

    import sys
    print(json.dumps({
        "status":        "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":        run_id,
        "patterns":      len(analysis.get("patterns_found", [])),
        "first_domino":  analysis.get("first_domino", "")[:100],
        "quality":       analysis.get("quality_score", 0),
        "elapsed":       elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3a(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
