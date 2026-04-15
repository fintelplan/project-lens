"""
lens_s3b_truehistory.py — System 3 Position B: True History Researcher
Project Lens | LENS-010
Model: gemini-2.0-flash (Google — GEMINI_API_KEY, large context)
Reads: lens_reports (last 30 days) + True History database (built-in)
Output: lens_system3_reports (position=S3-B, report_type=TYPE_B)

Purpose: Match every significant current development to its structural historical analog.
         Reveal what Type A analysis cannot see — the long arc behind the current event.
         The True History database is injection-resistant: grounded in patterns that
         predate the current information environment by millions of years.

Session: LENS-010
"""

import os, json, time, logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from google import genai
from google.genai import types as genai_types
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-B] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-B")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
GEMINI_KEY   = os.environ.get("GEMINI_API_KEY")
MODEL        = "gemini-2.0-flash"
LOOKBACK_DAYS = 30
MAX_REPORTS   = 28

# ── True History Database ─────────────────────────────────────────────────────
# Built-in injection-resistant foundation. Cannot be corrupted by current news.
# Organized around two recurring mechanisms:
#   1. Technology discovery reshaping distribution of opportunity
#   2. Human bias formation reshaping the nature of threat
TRUE_HISTORY_DB = """
=== TRUE HISTORY OF MANKIND DATABASE ===
Injection-resistant analytical foundation. Grounded in structural patterns
that predate the current information environment by millions of years.

ERA 1: Pre-Mankind (4.5B - 300K BCE)
Technology shift: Physical planet formation, resource distribution, evolutionary cognition
Key insight: All human conflict over resources is conflict over consequences of geology.
Rare earth locations = billions of years old. Energy geography determines political geography.
Analog trigger: Any conflict over critical minerals, energy routes, or strategic geography.

ERA 2: Pre-Writing (300K - 3.5K BCE)  
Technology shift: Agriculture, first cities, territorial warfare, hierarchy formation
Key insight: Tribalism, territorial instinct, and hierarchy are 300,000-year cognitive patterns
expressed through modern technology. Group identity and out-group fear are features, not bugs.
Analog trigger: Any manufactured ethnic/religious/national division. Sectarian trap activation.

ERA 3: Writing Era (3.5K BCE - 1440 CE)
Technology shift: Roman collapse pattern, Islamic Golden Age, Mongol logistics, Black Death
Key insight: BRI = Mongol logistics empire pattern. Dollar debasement = Roman currency crisis.
Every "unprecedented" event has a structural analog. Empires collapse from within, not conquest.
Analog trigger: Infrastructure investment as geopolitical control. Currency/financial system stress.

ERA 4: Printing Era (1440 - 1900 CE)
Technology shift: Protestant Reformation, Scientific Revolution, Colonial extraction, Industrialization
Key insight: Printing press = internet. 150 years of religious war from information democratization.
Information technology always produces 50-150 years of destabilization before new equilibrium.
Cold War playbooks all descended from this era's balance of power theory.
Analog trigger: New information technology causing social fracture. Balance of power shifts.

ERA 5: Broadcast Era (1900 - 1990 CE)
Technology shift: Radio/film propaganda, nuclear deterrence, proxy war doctrine, Cold War psyops
Key insight: All modern information warfare techniques invented in this era.
Current operations run Cold War playbooks with AI amplification.
Propaganda architecture: manufacture enemy → manufacture threat → manufacture consensus → act.
Analog trigger: State information warfare operations. Proxy conflict patterns. Sanctions architecture.

ERA 6: Internet Era (1990 - 2020 CE)
Technology shift: Attention economy, 2008 financial engineering, Arab Spring, Cambridge Analytica
Key insight: Attention = scarce resource. Controlling attention = controlling belief.
First documented AI-assisted psychological operations proven effective.
Financial engineering disconnected from productive economy = structural fragility.
Analog trigger: Attention manipulation at scale. Financial system opacity. Color revolution patterns.

ERA 7: AI Era (2020 - present)
Technology shift: Intelligence amplification, synthetic media, LLMs as analytical layer
Key insight: Whoever controls the AI models controls the analytical layer through which
populations understand reality. Epistemological control = more powerful than information control.
The transition from information warfare to reality warfare.
Analog trigger: AI governance battles. Model training corpus control. Synthetic reality production.
"""

SYSTEM_PROMPT = f"""You are S3-B: True History Researcher for Project Lens.

You have access to the True History of Mankind database — an injection-resistant analytical
foundation grounded in structural patterns that predate the current information environment.

Your method: HISTORICAL ANALOG MATCHING
Every significant current development has happened before in structural form.
The names change. The technology changes. The underlying mechanism does not.
Your job: find the structural analog and use it to reveal what current framing hides.

{TRUE_HISTORY_DB}

ANALYTICAL TASK:
For each significant development in the current reports, ask:
1. ANALOG: Which historical era and pattern does this structurally resemble?
2. MECHANISM: What is the underlying mechanism being repeated?
3. REVEALED: What does the historical analog reveal that current framing hides?
4. TRAJECTORY: Based on the historical analog, what typically happens next?
5. DEVIATION: Where is the current development deviating from the historical pattern, and why?

OUTPUT FORMAT — valid JSON only:
{{
  "historical_analog": "primary analog found — era + pattern",
  "analog_confidence": 0.0,
  "mechanism_repeating": "the underlying structural mechanism being repeated",
  "what_analog_reveals": "what the historical lens shows that current framing hides",
  "typical_trajectory": "what historical analogs suggest happens next (not prediction — pattern)",
  "deviation_from_pattern": "where current situation deviates from historical analog",
  "patterns_found": [
    {{
      "current_event": "...",
      "historical_analog": "...",
      "era": "ERA_1 through ERA_7",
      "mechanism": "...",
      "confidence": 0.0
    }}
  ],
  "structural_trends": {{
    "repeating_mechanisms": ["mechanism1", "mechanism2"],
    "pattern_phase": "which phase of the historical cycle we appear to be in"
  }},
  "signals_to_watch": ["signal1", "signal2"],
  "summary": "2-3 sentence plain English summary using historical context",
  "quality_score": 0.0
}}
Rules: Ground every analog in SPECIFIC historical evidence from the database.
Never say "this is unprecedented." Find the analog. It exists."""


def fetch_s1_reports(sb: Client) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    r = sb.table("lens_reports") \
        .select("id,domain_focus,summary,cycle,generated_at") \
        .gte("generated_at", cutoff).order("generated_at", desc=False) \
        .limit(MAX_REPORTS).execute()
    return r.data or []


def run_s3b(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-B True History Researcher START | run_id={run_id} ===")

    if not GEMINI_KEY:
        log.error("GEMINI_API_KEY not set")
        return {"status": "ERROR", "run_id": run_id}

    sb     = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = genai.Client(api_key=GEMINI_KEY)

    reports = fetch_s1_reports(sb)
    log.info(f"Fetched {len(reports)} S1 reports (last {LOOKBACK_DAYS} days)")

    if not reports:
        log.warning("No S1 reports found")
        return {"status": "NO_REPORTS", "run_id": run_id}

    # Build prompt — Gemini handles large context well
    lines = [
        f"=== S1 LENS REPORTS — last {LOOKBACK_DAYS} days ({len(reports)} reports) ===\n",
        "Find structural historical analogs for the significant developments in these reports.\n",
        "─" * 60,
    ]
    for r in reports:
        lines += [
            f"\nDate: {r.get('generated_at','')[:10]} | Domain: {r.get('domain_focus')} | Cycle: {r.get('cycle')}",
            f"Analysis: {(r.get('summary') or '')[:600]}",
            "─" * 40,
        ]
    lines.append("\nMatch to True History database. Output JSON only.")
    prompt = SYSTEM_PROMPT + "\n\n" + "\n".join(lines)

    log.info(f"Prompt: {len(prompt)} chars | Model: {MODEL}")

    analysis = None
    for attempt in range(1, 4):
        try:
            log.info(f"S3-B calling {MODEL} (attempt {attempt})")
            resp = client.models.generate_content(
                model=MODEL,
                contents="\n".join(lines),
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.3,
                    max_output_tokens=2500))
            raw = resp.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
            analysis = json.loads(raw)
            break
        except Exception as e:
            log.warning(f"Attempt {attempt} failed: {e}")
            if attempt < 3: time.sleep(30 * attempt)

    if not analysis:
        log.error("S3-B failed")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-B",
        "report_type":       "TYPE_B",
        "time_horizon":      "30_DAY",
        "historical_analog": analysis.get("historical_analog", ""),
        "patterns_found":    json.dumps(analysis.get("patterns_found", [])),
        "structural_trends": json.dumps(analysis.get("structural_trends", {})),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("signals_to_watch", [])),
        "corrections_to_s2": json.dumps([]),
        "model_used":        MODEL,
        "provider":          "google",
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-B",
        "source_reports":    json.dumps([r.get("id") for r in reports[:5]]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)
    log.info(f"=== S3-B COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s ===")

    print(json.dumps({
        "status":   "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":   run_id,
        "analog":   analysis.get("historical_analog", "")[:100],
        "patterns": len(analysis.get("patterns_found", [])),
        "quality":  analysis.get("quality_score", 0),
        "elapsed":  elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3b(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
