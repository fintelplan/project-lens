"""
lens_s3a_patterns.py — System 3 Position A: Pattern Intelligence
Project Lens | LENS-010
Model: registry role s3a_patterns (lens_models.ROLES) -- CC-74: Cohere primary, ministral-8b fallback
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
import requests
from lens_models import assert_model_known, fit_max_tokens, wire, get_role
from lens_window_sample import sample_window, newest_third, window_span   # CC-99
from supabase import create_client, Client

# ── Quota guard (LR-074) ──────────────────────────────────────────────────────
from lens_quota_guard import guard_check_with_fallback

# ── Response schema validator (I2) ────────────────────────────────────────────
from lens_response_guard import validate_parsed_response, format_validation_for_log


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
PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s3a_patterns")
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
4. FIRST DOMINO: What cause, already in motion, starts the chain you see? Name the cause, not the
   ending. Hold it as a hypothesis to watch: give one observable sign that would confirm it within
   30-90 days and one that would disconfirm it. Never say it will happen or is inevitable.
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
  "first_domino": "the cause already in motion (a hypothesis, not a forecast) -- confirmed if: <sign>; disconfirmed if: <sign>",
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
    """CC-99 (LENS-045): the whole window, evenly in time -- not its oldest rows.

    `order asc` + `limit 20` read the OLDEST 20 reports of the 7 days. At ~8 S1
    reports a day that was about the first two and a half days, so S3-A's first
    question -- what SEQUENCE is forming across the last 7 days -- never saw the
    last four. S3-D had the same defect (CC-93/94/96); this is its sampler.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows, total = sample_window(sb, "lens_reports",
                                "id,domain_focus,summary,cycle,generated_at,quality_score",
                                "generated_at", cutoff, MAX_S1_REPORTS)
    log.info(f"S1 window={days}d: {total} reports in window, {len(rows)} sampled evenly "
             f"in time ({newest_third(rows, 'generated_at')} in the newest third)"
             f"{window_span(rows, 'generated_at')}")
    return rows


def fetch_s2_reports(sb: Client, days: int) -> list:
    """CC-99: the same fix. injection_reports held 1,169 rows in 30 days
    (LENS-044), so ~270 in 7; the oldest 15 were the window's first hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows, total = sample_window(sb, "injection_reports",
                                "id,analyst,injection_type,evidence,confidence_score,flagged_phrases,created_at",
                                "created_at", cutoff, MAX_S2_REPORTS)
    log.info(f"S2 window={days}d: {total} reports in window, {len(rows)} sampled evenly "
             f"in time ({newest_third(rows, 'created_at')} in the newest third)"
             f"{window_span(rows, 'created_at')}")
    return rows


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


# -- CC-74 (LENS-042): two legs, both named by the registry -----------------
# Cerebras withdrew its free tier (~2026-08-17) and this file only ever called
# Cerebras, so S3-A saved nothing for a month. Primary and fallback now come
# from ROLES["s3a_patterns"]. Every attempt logs HTTP status, usage and
# finish_reason; a response that says not to retry is not retried (R11).
_NO_RETRY = {400, 401, 402, 403, 404, 422}


def _parse_json_text(raw):
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def _post_leg(provider, model, key, max_tokens, prompt):
    """One HTTP call. Returns (status, text, finish, usage, retryable, err)."""
    if provider == "cohere":
        url = "https://api.cohere.com/v2/chat"
    elif provider == "mistral":
        url = "https://api.mistral.ai/v1/chat/completions"
    else:
        return 0, "", None, {}, False, "no HTTP leg for provider " + str(provider)
    body = {"model": model,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                         {"role": "user", "content": prompt}],
            "temperature": 0.4, "max_tokens": max_tokens,
            "response_format": {"type": "json_object"}}
    r = requests.post(url, json=body, timeout=180,
                      headers={"Authorization": "Bearer " + key,
                               "Content-Type": "application/json"})
    if r.status_code != 200:
        hdr = (r.headers.get("x-should-retry") or "").lower()
        retryable = r.status_code not in _NO_RETRY and hdr != "false"
        return r.status_code, "", None, {}, retryable, r.text[:200]
    b = r.json()
    if provider == "cohere":
        parts = (b.get("message") or {}).get("content") or []
        text = "".join(p.get("text", "") for p in parts)
        finish = b.get("finish_reason")
        u = (b.get("usage") or {}).get("tokens") or {}
        usage = {"in": u.get("input_tokens"), "out": u.get("output_tokens")}
    else:
        ch = (b.get("choices") or [{}])[0]
        text = (ch.get("message") or {}).get("content") or ""
        finish = ch.get("finish_reason")
        u = b.get("usage") or {}
        usage = {"in": u.get("prompt_tokens"), "out": u.get("completion_tokens")}
    return 200, text, finish, usage, True, None


def _generate(prompt, prompt_chars, max_tokens):
    """Primary then fallback. Returns (analysis or None, provider, model)."""
    role = get_role("s3a_patterns")
    legs = [(role["provider"], role["model"], role["key_env"], max_tokens)]
    if role.get("fb_provider"):
        legs.append((role["fb_provider"], role["fb_model"], role["fb_key_env"],
                     fit_max_tokens(prompt_chars, role["max_out"],
                                    role["fb_provider"], role["fb_model"])))
    for n, (prov, model, key_env, mt) in enumerate(legs):
        tag = "primary" if n == 0 else "FALLBACK"
        key = os.environ.get(key_env, "")
        if not key:
            log.error(f"S3-A {tag} {prov}/{model}: {key_env} not set -- leg skipped")
            continue
        assert_model_known(prov, model)
        for attempt in range(1, 3):
            log.info(f"S3-A {tag} calling {prov}/{model} (attempt {attempt}, "
                     f"prompt {prompt_chars} chars, max_tokens {mt})")
            try:
                status, text, finish, usage, retryable, err = _post_leg(
                    prov, model, key, mt, prompt)
            except Exception as e:
                status, text, finish, usage, retryable, err = (
                    0, "", None, {}, True, repr(e)[:200])
            if status != 200:
                log.warning(f"S3-A {tag} {prov}/{model} attempt {attempt}: "
                            f"HTTP {status} retryable={retryable} {err}")
                if retryable and attempt < 2:
                    time.sleep(20)
                    continue
                try:   # CC-104 (item 11): the leg is given up -- record it once
                    from lens_provider_refusal import record_refusal
                    record_refusal(prov, model, status=status or None, text=err)
                except Exception:
                    pass
                break
            log.info(f"S3-A {tag} usage: {prov}/{model} in={usage.get('in')} "
                     f"out={usage.get('out')} max_tokens={mt} "
                     f"finish_reason={finish}")
            try:
                analysis = _parse_json_text(text)
            except Exception as e:
                log.warning(f"S3-A {tag} {prov}/{model} attempt {attempt}: "
                            f"JSON parse failed ({e}); finish_reason={finish}")
                if attempt < 2:
                    continue
                break
            vr = validate_parsed_response(analysis, "S3-A")
            if not vr.valid:
                log.warning(format_validation_for_log(vr))
            return analysis, prov, model
    return None, None, None


def run_s3a(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-A Pattern Intelligence START | run_id={run_id} ===")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    if not os.environ.get(KEY_ENV, ""):
        log.error(KEY_ENV + " missing -- primary leg will be skipped")

    if already_ran_today(sb):
        log.info("S3-A already ran in last 20h — skipping (daily cadence)")
        return {"status": "SKIPPED", "run_id": run_id}

    # ── Quota guard pre-flight (LR-074) ───────────────────────────────────────
    quota_guard = guard_check_with_fallback(positions=["S3-A"], run_id=run_id, sb=sb)
    skip_result = next((r for r in quota_guard if r.decision == "SKIP" and "S3-A" in r.positions), None)
    if skip_result:
        reason = skip_result.reason
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
    prompt_chars = len(SYSTEM_PROMPT) + len(prompt)
    max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, PROVIDER, MODEL)

    analysis, used_provider, used_model = _generate(prompt, prompt_chars, max_tokens)

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
        "first_domino":     analysis.get("first_domino", ""),   # CC-126: the column was never written; the Brief and the S3 message read it
        "signals_to_watch": json.dumps(analysis.get("signals_to_watch", [])),
        "corrections_to_s2": json.dumps(analysis.get("corrections_to_s2", [])),
        "model_used":       used_model,
        "provider":         used_provider,
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
