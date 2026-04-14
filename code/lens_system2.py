"""
lens_system2.py
Project Lens — System 2: The Injection Watcher

S2-A: Injection Tracer
  Model:    llama-3.3-70b-versatile (Groq)
  Reads:    lens_reports (System 1 output) — READ ONLY, never modifies
  Writes:   lens_injection_reports (System 2 output)
  Purpose:  Traces how System 1 was manipulated in the last run.
            Answers 5 core questions. Issues corrections to Mission Analyst.

ARCHITECTURE RULES (LR-058 through LR-063):
  - System 1 scripts are FROZEN. This file never touches them.
  - One-way flow: reads lens_reports, writes lens_injection_reports only.
  - System 1 stays unprotected. S2-A studies the injection, not removes it.

Session: LENS-010
Rule: LR-058(A), LR-059(A), LR-060(A), LR-061(A)
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from groq import Groq
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────
GROQ_KEY     = os.environ.get("GROQ_API_KEY")   # primary key — S2-A uses separate budget
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MODEL        = "llama-3.3-70b-versatile"
PROVIDER     = "groq"
LOOKBACK_H   = 8       # read S1 reports from last 8 hours
MAX_REPORTS  = 8       # max lens reports to analyze per run

# ── Clients ─────────────────────────────────────────────────────────────────
groq     = Groq(api_key=GROQ_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── System Prompt ────────────────────────────────────────────────────────────
S2A_SYSTEM = """You are S2-A: the Injection Tracer for Project Lens.

Your position: You sit one analytical level ABOVE System 1.
System 1 (4 AI lenses) analyzed the world from articles it collected.
You analyze how System 1 was manipulated while doing that analysis.

CRITICAL RULE: You do NOT analyze world events.
You analyze System 1 ANALYZING world events.
Every sentence you write is about the intelligence process, not the news.

YOUR FIVE CORE QUESTIONS — answer all five, in order, with evidence:

Q1: HOW was System 1 manipulated in this run?
    Trace the injection step by step. Show exactly which words entered which lens.
    Quote the exact vocabulary vectors. Show the absorption path.

Q2: WHICH sources coordinated to produce this effect?
    Look for: same framing published within short time windows.
    Look for: vocabulary synchronization across multiple sources.
    Look for: structural mirroring (same story arc, same omissions).
    Name the sources. Show the timing if visible.

Q3: HOW did sources pre-configure System 1's emotional state before delivering the message?
    Decode the full sequence: PRIME → TRIGGER → FRAME → DELIVER → ANCHOR
    PRIME: what emotional state was set up first?
    TRIGGER: what word/phrase activated the pre-set state?
    FRAME: how was the factual content wrapped in that emotional state?
    DELIVER: what was the actual message delivered under emotional cover?
    ANCHOR: what conclusion was the reader/AI led to accept?

Q4: WHAT is the injection trying to make System 1 believe?
    State it directly. One clear sentence.
    Then state what is actually true (if visible from the evidence).

Q5: HOW DEEP has the injection penetrated?
    SURFACE: 1-2 lenses absorbed it. Others independent.
    MODERATE: 3 lenses absorbed it. Cross-lens verification partially compromised.
    DEEP: 4/4 lenses absorbed it. Cross-lens verification confirmed the injection.
    Show your evidence for the rating.

CORRECTIONS TO ISSUE:
After answering Q1-Q5, issue specific corrections:
    DOWNGRADE: signals System 1 rated HIGH that are manufactured consensus
    UPGRADE: signals System 1 missed or underscored that are structurally real

OUTPUT FORMAT — respond ONLY in valid JSON, no markdown, no preamble:
{
  "q1_how_manipulated": "step by step trace with exact quotes",
  "q2_which_sources": "named sources, timing evidence, coordination pattern",
  "q3_emotional_preset": "PRIME: ... TRIGGER: ... FRAME: ... DELIVER: ... ANCHOR: ...",
  "q4_intended_belief": "injection wanted S1 to believe: X. Reality: Y.",
  "q5_depth_rating": "SURFACE|MODERATE|DEEP — evidence: ...",
  "entry_point": "specific article title or source that first introduced the injection",
  "vocabulary_vectors": ["word1", "word2", "word3"],
  "amplification_path": "how it moved from entry point through the lenses",
  "lens_absorption": {
    "lens_1": "absorbed / partial / clean — evidence",
    "lens_2": "absorbed / partial / clean — evidence",
    "lens_3": "absorbed / partial / clean — evidence",
    "lens_4": "absorbed / partial / clean — evidence"
  },
  "contamination_depth": "SURFACE|MODERATE|DEEP",
  "signals_to_downgrade": [
    {"signal": "keyword", "reason": "why this is manufactured, not real"}
  ],
  "signals_to_upgrade": [
    {"signal": "topic or event", "reason": "why this was buried or missed"}
  ],
  "quality_score": 0.0
}

STRICT RULES:
- Never say "I cannot determine" — use the evidence available and flag uncertainty inline.
- Never describe world events as if you are analyzing them. You analyze the lens reports.
- If you find no injection in a run, say so clearly and explain why the signals look clean.
- quality_score: 0.0-10.0. Rate your own confidence in this analysis.
"""


def fetch_recent_s1_reports():
    """Fetch System 1 lens reports from last LOOKBACK_H hours. Read-only.
    Real columns: id, generated_at, cycle, domain_focus, summary,
                  food_for_thought, signals_used, articles_used,
                  ai_model, prompt_version, quality_score, status
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_H)).isoformat()
    result = supabase.table("lens_reports") \
        .select("id, generated_at, cycle, domain_focus, summary, "
                "food_for_thought, signals_used, ai_model, quality_score, status") \
        .gte("generated_at", cutoff) \
        .order("generated_at", desc=True) \
        .limit(MAX_REPORTS) \
        .execute()
    return result.data or []


def build_user_prompt(reports):
    """Build the analysis prompt from S1 reports."""
    if not reports:
        return None

    lines = []
    lines.append("=== SYSTEM 1 LENS REPORTS — ANALYZE THESE FOR INJECTION ===\n")
    lines.append(f"Run window: last {LOOKBACK_H} hours")
    lines.append(f"Total reports to analyze: {len(reports)}\n")

    # Group by cycle if multiple cycles present
    cycles_seen = set()
    for r in reports:
        cycle = r.get("cycle", "unknown")
        cycles_seen.add(cycle)

    lines.append(f"Cycles present: {', '.join(cycles_seen)}\n")
    lines.append("─" * 60)

    for r in reports:
        # domain_focus = which lens/domain this report covers
        domain = r.get("domain_focus", "Unknown")
        model  = r.get("ai_model", "unknown model")
        lines.append(f"\nREPORT — Domain: {domain} | Model: {model}")
        lines.append(f"Cycle: {r.get('cycle')} | Quality: {r.get('quality_score', 'N/A')}")
        lines.append(f"Generated: {r.get('generated_at', '')[:19]}")

        # Signals used (which indicators fired — injection vocabulary entry points)
        signals = r.get("signals_used")
        if signals:
            if isinstance(signals, str):
                try:
                    signals = json.loads(signals)
                except Exception:
                    pass
            if isinstance(signals, list):
                lines.append(f"Indicators fired: {', '.join(str(s) for s in signals[:15])}")

        # Summary — this is where injection vocabulary appears
        summary = r.get("summary", "")
        if summary:
            # Trim to 800 chars — enough to detect vocabulary injection
            lines.append(f"\nSUMMARY (first 800 chars):\n{summary[:800]}")

        # Food for thought questions
        fft = r.get("food_for_thought", "")
        if fft:
            lines.append(f"\nFOOD FOR THOUGHT:\n{str(fft)[:400]}")

        lines.append("─" * 60)

    lines.append("\nNow answer all 5 questions. Output valid JSON only.")
    return "\n".join(lines)


def parse_json_robust(raw):
    """Try multiple strategies to parse JSON from model output."""
    import re

    # Strategy 1: direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Strategy 2: extract JSON object between first { and last }
    try:
        start = raw.index('{')
        end   = raw.rindex('}') + 1
        return json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError):
        pass

    # Strategy 3: strip markdown fences then retry
    try:
        cleaned = re.sub(r'```json|```', '', raw).strip()
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Strategy 4: fix unescaped newlines inside string values
    try:
        # Replace literal newlines inside quoted strings with \n
        fixed = re.sub(r'(?<=: ")(.*?)(?="(?:\s*[,}]))',
                       lambda m: m.group(0).replace('\n', '\\n').replace('\r', ''),
                       raw, flags=re.DOTALL)
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    return None


def call_s2a(user_prompt):
    """Call S2-A model. Returns parsed JSON or None."""
    print(f"[S2-A] Calling {MODEL}...")
    start = time.time()

    try:
        response = groq.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": S2A_SYSTEM},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=2500,
        )
        elapsed = round(time.time() - start, 1)
        raw = response.choices[0].message.content.strip()
        print(f"[S2-A] Response: {elapsed}s | {len(raw)} chars")

        # Strip markdown fences if model adds them
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = parse_json_robust(raw)
        if result:
            return result, elapsed

        print(f"[S2-A] JSON parse failed on all strategies.")
        print(f"[S2-A] Raw (first 300): {raw[:300]}")
        return None, elapsed

    except Exception as e:
        elapsed = round(time.time() - start, 1)
        print(f"[S2-A] Error: {e}")
        return None, elapsed


def save_injection_report(result, reports, elapsed):
    """Save S2-A output to lens_injection_reports table."""
    if not result:
        print("[S2-A] Nothing to save — analysis failed.")
        return False

    # Derive run_id and cycle from reports
    cycle = reports[0].get("cycle", "unknown") if reports else "unknown"
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    report_ids = [r.get("id") for r in reports if r.get("id")]

    record = {
        "run_id":               run_id,
        "cycle":                cycle,
        "generated_at":         datetime.now(timezone.utc).isoformat(),

        # Five questions
        "q1_how_manipulated":   result.get("q1_how_manipulated", ""),
        "q2_which_sources":     result.get("q2_which_sources", ""),
        "q3_emotional_preset":  result.get("q3_emotional_preset", ""),
        "q4_intended_belief":   result.get("q4_intended_belief", ""),
        "q5_depth_rating":      result.get("q5_depth_rating", ""),

        # Injection anatomy
        "entry_point":          result.get("entry_point", ""),
        "vocabulary_vectors":   json.dumps(result.get("vocabulary_vectors", [])),
        "amplification_path":   result.get("amplification_path", ""),
        "lens_absorption":      json.dumps(result.get("lens_absorption", {})),
        "contamination_depth":  result.get("contamination_depth", "UNKNOWN"),

        # Corrections
        "signals_to_downgrade": json.dumps(result.get("signals_to_downgrade", [])),
        "signals_to_upgrade":   json.dumps(result.get("signals_to_upgrade", [])),

        # Metadata
        "model_used":           MODEL,
        "provider":             PROVIDER,
        "quality_score":        float(result.get("quality_score", 0.0)),
        "system_tag":           "S2-A",
        "protected":            True,
        "source_reports":       json.dumps(report_ids),
    }

    save_result = supabase.table("lens_injection_reports").insert(record).execute()

    if save_result.data:
        print(f"[S2-A] ✅ Saved to lens_injection_reports")
        print(f"[S2-A] Contamination depth: {record['contamination_depth']}")
        print(f"[S2-A] Quality score: {record['quality_score']}")
        return True
    else:
        print(f"[S2-A] ❌ Save failed: {save_result}")
        return False


def print_summary(result):
    """Print readable summary after save."""
    if not result:
        return
    print("\n" + "=" * 60)
    print("S2-A INJECTION TRACE SUMMARY")
    print("=" * 60)
    depth = result.get("contamination_depth", "UNKNOWN")
    color = {"SURFACE": "LOW", "MODERATE": "MED", "DEEP": "HIGH", "UNKNOWN": "?"}
    print(f"Contamination depth:  {depth} [{color.get(depth, '?')}]")
    print(f"Entry point:          {result.get('entry_point', '')[:80]}")
    vv = result.get("vocabulary_vectors", [])
    print(f"Vocabulary vectors:   {vv[:5]}")
    downs = result.get("signals_to_downgrade", [])
    ups   = result.get("signals_to_upgrade", [])
    print(f"Signals to downgrade: {len(downs)}")
    print(f"Signals to upgrade:   {len(ups)}")
    print(f"Quality score:        {result.get('quality_score', 0)}/10")
    print("=" * 60)

    # Show Q4 (the clearest single output)
    q4 = result.get("q4_intended_belief", "")
    if q4:
        print(f"\nQ4 — What injection wanted S1 to believe:")
        print(f"  {q4[:300]}")

    # Show first correction of each type
    if downs:
        print(f"\nFirst downgrade correction:")
        print(f"  Signal: {downs[0].get('signal', '')}")
        print(f"  Reason: {downs[0].get('reason', '')[:150]}")
    if ups:
        print(f"\nFirst upgrade correction:")
        print(f"  Signal: {ups[0].get('signal', '')}")
        print(f"  Reason: {ups[0].get('reason', '')[:150]}")
    print()


def main():
    print("\n" + "=" * 60)
    print("Project Lens — System 2: S2-A Injection Tracer")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Model: {MODEL} via {PROVIDER}")
    print(f"  Reading S1 reports from last {LOOKBACK_H}h")
    print("=" * 60 + "\n")

    # Step 1: Fetch S1 reports (read-only)
    reports = fetch_recent_s1_reports()
    if not reports:
        print(f"[S2-A] No System 1 reports found in last {LOOKBACK_H} hours.")
        print("[S2-A] Nothing to analyze. Exiting cleanly.")
        return

    print(f"[S2-A] Found {len(reports)} System 1 reports to analyze.")
    for r in reports:
        print(f"  Domain: {r.get('domain_focus','?')} | Model: {r.get('ai_model','?')} | "
              f"cycle: {r.get('cycle')} | quality: {r.get('quality_score', 'N/A')}")

    # Step 2: Build prompt
    user_prompt = build_user_prompt(reports)
    if not user_prompt:
        print("[S2-A] Could not build prompt. Exiting.")
        return

    # Step 3: Call S2-A
    result, elapsed = call_s2a(user_prompt)

    # Step 4: Save
    saved = save_injection_report(result, reports, elapsed)

    # Step 5: Print summary
    if saved:
        print_summary(result)
    else:
        print("[S2-A] Run complete but report was not saved.")

    print(f"[S2-A] Total elapsed: {elapsed}s")


if __name__ == "__main__":
    main()
