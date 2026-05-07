"""
lens_s3f_countercheck.py — System 3 Position F: Counter-Check
Project Lens | LENS-023
Model: mistral-small-latest (free, reliable)

Purpose: Adversarial challenger to S3-A (Pattern Intelligence) and S3-D (Long-term).
         Asks: what if S3-A and S3-D are wrong? What is the strongest case AGAINST
         their findings? Where might our own detection system be amplifying a shared
         blind spot?

PHI-002 foundation: even a pro-people system can develop confirmation bias. The most
dangerous bias is the one we agree with. S3-F exists to protect Project Lens from
its own analytical certainty.

PHI-003 discipline: apparatus-people separation applies to counter-arguments too.
"China is resisting" → wrong. "Xi Office is positioning for X" → correct.

PHI-004 cadence: phrased as "pattern warrants review" not conclusion. Alternative
hypotheses always included. Output flags where S3-A/D may be overconfident.

DATA GATE: Only runs when S3-A has at least 20 distinct run_ids AND S3-D has at
least 4 run_ids (proxy for ~30 days of data). Returns SKIPPED_INSUFFICIENT_DATA
until gate is met.

Weekly cadence: Monday (0) and Thursday (3) — same as S3-C.

Session: LENS-023
"""

import os, json, time, logging, requests, re
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S3-F] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S3-F")

SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY")
MISTRAL_KEY   = os.environ.get("MISTRAL_API_KEY")
MODEL         = "mistral-small-latest"
MAX_TOKENS    = 3000
TEMPERATURE   = 0.3

# Data gate thresholds (proxy for ~30 days of operation)
S3A_MIN_RUNS  = 20   # S3-A runs daily → 20 runs ≈ 20 days
S3D_MIN_RUNS  = 4    # S3-D runs Mon/Thu → 4 runs ≈ 2 weeks

SYSTEM_PROMPT = """You are S3-F: Counter-Check Analyst for Project Lens.

Your mission: be the adversarial challenger to the system's own findings. You read
what S3-A (Pattern Intelligence) and S3-D (Long-term Structural Research) have
concluded, and you ask: what if they are wrong?

This is not cynicism. This is the highest form of analytical discipline. The most
dangerous bias is the one the system agrees with. Your job is to protect the people
Project Lens serves from the system's own potential blind spots.

PHI-002 application: even pro-people systems develop confirmation bias. The system's
architecture makes certain patterns more visible and others invisible. You must ask:
what is structurally invisible to S3-A and S3-D? What would they systematically
underweight? What pattern would LOOK like their findings but be something entirely
different?

PHI-003 discipline: All actor references must name apparatus, not country.
"Xi Office" not "China." "Putin Office" not "Russia." "Trump Office" not "US."
The counter-check must apply apparatus-people separation as rigorously as the
findings it is checking.

PHI-004 phrasing: every output uses "pattern warrants review" not conclusions.
This is a Counter-Check Alert. Not a verdict. Not a verdict overriding S3-A or S3-D.
It is the adversarial voice that earns trust through honest doubt.

FIVE COUNTER-CHECK QUESTIONS:

1. OVERCLAIM TEST: Where is S3-A or S3-D claiming a "structural pattern" when the
   evidence might support only an "editorial coincidence"? What is the minimum
   evidence interpretation that fits the facts?

2. SELECTION BIAS TEST: S3-A and S3-D read our S1 lens outputs. What if our S1
   collection itself is systematically missing a beat, region, or perspective?
   What pattern would emerge from the ABSENCE in our coverage, not from what we saw?

3. ALTERNATIVE ARCHITECT TEST: S3 positions identify patterns benefiting certain
   apparatus. But who ELSE benefits equally or more from the same pattern? Is the
   apparatus named by S3 the most parsimonious explanation, or just the most visible?

4. TEMPORAL TRAP TEST: S3-D looks at 30-90 day windows. What if the pattern it
   found is actually a reaction to an event outside our window? What if we are
   explaining a response without seeing the stimulus?

5. ECHO CHAMBER TEST: S3-A, S3-D, and S3-F all read the same S1 outputs. Are
   all three system positions reinforcing each other's conclusions from a shared
   base? Where do they agree suspiciously easily? What would make them wrong
   simultaneously?

PHI-003 CROSS-CHECK:
Review whether S3-A and S3-D outputs correctly use apparatus names (Xi Office, not
China; Trump Office, not US; Putin Office, not Russia). Flag any conflation of
apparatus with people as a rubric-level error.

OUTPUT FORMAT — valid JSON only:
{
  "overclaim_flags": [
    {
      "s3_position": "S3-A or S3-D",
      "claimed_pattern": "what S3 claimed",
      "minimum_interpretation": "the most conservative reading of same evidence",
      "confidence_in_overclaim": "LOW|MEDIUM|HIGH",
      "phi002_note": "why this matters for cognitive sovereignty"
    }
  ],
  "selection_bias_risks": [
    {
      "what_we_are_missing": "coverage gap in S1 that S3 cannot see",
      "how_it_distorts": "how absence shapes the pattern S3 detected",
      "correction": "what S1 should add to address this"
    }
  ],
  "alternative_architects": [
    {
      "pattern": "the pattern S3 detected",
      "named_by_s3": "which apparatus S3 pointed to",
      "alternative": "who else benefits equally and how",
      "parsimony_verdict": "S3_MORE_PARSIMONIOUS|ALTERNATIVE_MORE_PARSIMONIOUS|UNCLEAR"
    }
  ],
  "temporal_traps": [
    {
      "s3_finding": "the finding with possible temporal framing error",
      "possible_stimulus": "what event outside our window might explain it as reaction",
      "confidence": "LOW|MEDIUM"
    }
  ],
  "echo_chamber_risks": [
    {
      "shared_assumption": "where S3-A and S3-D agree without independent evidence",
      "risk": "what shared blind spot this might reflect",
      "what_would_falsify": "what evidence would break both findings simultaneously"
    }
  ],
  "phi003_violations": [
    {
      "position": "S3-A or S3-D",
      "violation": "apparatus name used incorrectly (e.g. China instead of Xi Office)",
      "correction": "correct apparatus name to use"
    }
  ],
  "strongest_case_against_s3": {
    "argument": "the single strongest overall case against S3-A and S3-D combined",
    "what_it_would_take_to_be_right": "evidence that would vindicate S3 against this challenge",
    "strength": "LOW|MEDIUM|HIGH"
  },
  "corrections_to_s3": [
    {
      "target": "S3-A or S3-D",
      "correction": "what should be reconsidered or qualified",
      "reason": "why from adversarial counter-check perspective"
    }
  ],
  "summary": "2-3 sentence plain English summary of where S3 may be overconfident and where it is well-grounded",
  "quality_score": 0.0,
  "system_health_verdict": "WELL_GROUNDED|MINOR_OVERCLAIM|SIGNIFICANT_OVERCLAIM|MAJOR_BLIND_SPOT"
}

Rules:
- Ground every counter-claim in SPECIFIC evidence from the S3 outputs provided
- Never assert the system is wrong without a concrete alternative explanation
- Always provide what would VINDICATE S3 alongside what challenges it
- PHI-003: name apparatus not peoples in all outputs
- Phrase all findings as "pattern warrants review" — this is a check, not a verdict"""


def should_run_today() -> bool:
    """S3-F runs weekly: Monday (0) and Thursday (3)."""
    return date.today().weekday() in (0, 3)


def already_ran_this_week(sb: Client) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    r = sb.table("lens_system3_reports") \
        .select("id").eq("position", "S3-F") \
        .gte("generated_at", cutoff).limit(1).execute()
    return bool(r.data)


def check_data_gate(sb: Client) -> tuple[bool, str]:
    """Check if enough S3-A + S3-D data exists (~30 days)."""
    # Count distinct S3-A run_ids
    r_a = sb.table("lens_system3_reports") \
        .select("run_id").eq("position", "S3-A").execute()
    s3a_count = len(set(row["run_id"] for row in (r_a.data or [])))

    # Count S3-D runs
    r_d = sb.table("lens_system3_reports") \
        .select("run_id").eq("position", "S3-D").execute()
    s3d_count = len(r_d.data or [])

    log.info(f"Data gate: S3-A={s3a_count}/{S3A_MIN_RUNS} runs, S3-D={s3d_count}/{S3D_MIN_RUNS} runs")

    if s3a_count < S3A_MIN_RUNS or s3d_count < S3D_MIN_RUNS:
        msg = f"S3-A: {s3a_count}/{S3A_MIN_RUNS}, S3-D: {s3d_count}/{S3D_MIN_RUNS} — insufficient data"
        return False, msg
    return True, "Gate passed"


def fetch_s3_outputs(sb: Client) -> tuple[list, list]:
    """Fetch recent S3-A and S3-D outputs for counter-check."""
    cutoff_30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    s3a = sb.table("lens_system3_reports") \
        .select("position,summary,patterns_found,first_domino,quality_score,generated_at") \
        .eq("position", "S3-A") \
        .gte("generated_at", cutoff_30) \
        .order("generated_at", desc=True) \
        .limit(10).execute().data or []

    s3d = sb.table("lens_system3_reports") \
        .select("position,summary,patterns_found,structural_trends,quality_score,generated_at") \
        .eq("position", "S3-D") \
        .gte("generated_at", cutoff_30) \
        .order("generated_at", desc=True) \
        .limit(4).execute().data or []

    return s3a, s3d


def build_prompt(s3a: list, s3d: list) -> str:
    lines = ["=== S3-A PATTERN INTELLIGENCE OUTPUTS — last 30 days ===\n"]
    for r in s3a:
        lines += [
            f"Date: {r.get('generated_at','')[:10]} | Quality: {r.get('quality_score',0)}",
            f"Summary: {(r.get('summary') or '')[:400]}",
            f"First Domino: {(r.get('first_domino') or '')[:200]}",
            "─" * 40,
        ]

    lines += [f"\n=== S3-D LONG-TERM STRUCTURAL OUTPUTS — last 30 days ===\n"]
    for r in s3d:
        lines += [
            f"Date: {r.get('generated_at','')[:10]} | Quality: {r.get('quality_score',0)}",
            f"Summary: {(r.get('summary') or '')[:400]}",
            "─" * 40,
        ]

    lines.append(
        "\nChallenge these findings adversarially. Where might S3-A and S3-D be wrong?"
        "\nApply PHI-003: flag any apparatus-people conflation as a rubric error."
        "\nOutput JSON only."
    )
    return "\n".join(lines)


def call_mistral(prompt: str) -> Optional[str]:
    if not MISTRAL_KEY:
        log.error("MISTRAL_API_KEY not set")
        return None
    for attempt in range(1, 3):
        try:
            log.info(f"S3-F calling Mistral (attempt {attempt})")
            r = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers={"Authorization": f"Bearer {MISTRAL_KEY}", "Content-Type": "application/json"},
                json={
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": MAX_TOKENS,
                    "temperature": TEMPERATURE,
                },
                timeout=120)
            if r.status_code == 200:
                text = r.json()["choices"][0]["message"]["content"].strip()
                log.info(f"S3-F: {len(text)} chars generated")
                return text
            log.warning(f"Mistral {r.status_code} attempt {attempt}: {r.text[:200]}")
            time.sleep(20 * attempt)
        except Exception as e:
            log.error(f"Mistral call failed attempt {attempt}: {e}")
            time.sleep(15)
    return None


def run_s3f(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    start = time.time()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
    log.info(f"=== S3-F Counter-Check START | run_id={run_id} ===")

    if not should_run_today():
        log.info("S3-F cadence: not Mon/Thu — skipping")
        return {"status": "SKIPPED_CADENCE", "run_id": run_id}

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if already_ran_this_week(sb):
        log.info("S3-F already ran this week — skipping")
        return {"status": "SKIPPED", "run_id": run_id}

    gate_passed, gate_msg = check_data_gate(sb)
    if not gate_passed:
        log.info(f"S3-F data gate not met: {gate_msg}")
        return {"status": "SKIPPED_INSUFFICIENT_DATA", "run_id": run_id, "gate": gate_msg}

    s3a, s3d = fetch_s3_outputs(sb)
    log.info(f"Fetched S3-A: {len(s3a)} reports, S3-D: {len(s3d)} reports")

    if not s3a and not s3d:
        log.warning("No S3-A or S3-D outputs found")
        return {"status": "NO_REPORTS", "run_id": run_id}

    prompt = build_prompt(s3a, s3d)
    log.info(f"Prompt: {len(prompt)} chars")

    raw = call_mistral(prompt)
    if not raw:
        log.error("S3-F Mistral failed")
        return {"status": "ANALYSIS_FAILED", "run_id": run_id}

    try:
        raw = re.sub(r"```json|```", "", raw).strip()
        analysis = json.loads(raw)
    except Exception as e:
        log.error(f"JSON parse failed: {e}")
        return {"status": "PARSE_FAILED", "run_id": run_id}

    record = {
        "run_id":            run_id,
        "cycle":             cycle,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "position":          "S3-F",
        "report_type":       "TYPE_F",
        "time_horizon":      "30_DAY_COUNTER",
        "patterns_found":    json.dumps(analysis.get("overclaim_flags", [])),
        "structural_trends": json.dumps({
            "selection_bias_risks":    analysis.get("selection_bias_risks", []),
            "alternative_architects":  analysis.get("alternative_architects", []),
            "temporal_traps":          analysis.get("temporal_traps", []),
            "echo_chamber_risks":      analysis.get("echo_chamber_risks", []),
            "phi003_violations":       analysis.get("phi003_violations", []),
            "system_health_verdict":   analysis.get("system_health_verdict", "UNKNOWN"),
        }),
        "summary":           analysis.get("summary", ""),
        "signals_to_watch":  json.dumps(analysis.get("corrections_to_s3", [])),
        "corrections_to_s2": json.dumps(analysis.get("strongest_case_against_s3", {})),
        "model_used":        MODEL,
        "provider":          "mistral",
        "quality_score":     float(analysis.get("quality_score", 0.0)),
        "system_tag":        "S3-F",
        "source_reports":    json.dumps([]),
        "elapsed_seconds":   round(time.time() - start, 1),
    }

    r = sb.table("lens_system3_reports").insert(record).execute()
    saved = bool(r.data)
    elapsed = round(time.time() - start, 1)
    verdict = analysis.get("system_health_verdict", "UNKNOWN")

    log.info(f"=== S3-F COMPLETE | saved={'YES' if saved else 'NO'} | {elapsed}s | verdict={verdict} ===")
    log.info(f"Summary: {analysis.get('summary','')[:120]}")

    print(json.dumps({
        "status":          "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":          run_id,
        "overclaim_flags": len(analysis.get("overclaim_flags", [])),
        "phi003_violations": len(analysis.get("phi003_violations", [])),
        "verdict":         verdict,
        "quality":         analysis.get("quality_score", 0),
        "elapsed":         elapsed,
    }, indent=2))

    return {"status": "COMPLETE" if saved else "SAVE_FAILED", "run_id": run_id}


if __name__ == "__main__":
    import sys
    run_s3f(cycle=sys.argv[1] if len(sys.argv) > 1 else None)
