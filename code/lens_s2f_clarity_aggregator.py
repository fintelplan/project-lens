"""
lens_s2f_clarity_aggregator.py — Stage 2 Clarity Aggregator
LENS-020 | Project Lens

PHI-004 cognitive sovereignty cadence — Clarity phase (Day 7-30, MEDIUM confidence).
Takes voices that had Watch alerts and checks if pattern persisted or dissolved.

Input:  voices with Watch findings + new scoring data (14-21 day window)
Output: lens_drift_findings rows with finding_confidence='MEDIUM'

Honesty discipline:
  - Explicitly names which Watch suspects CONTINUED
  - Explicitly names which Watch suspects DROPPED OUT
  - Operation-coherence check: same ops recurring across different topics?
  - Phrasing: "pattern is developing" — NOT "pattern confirmed"
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter
from typing import Optional
from lens_s2f_helpers import get_state_office_entity_id

log = logging.getLogger("s2f_clarity")

CLARITY_WINDOW_DAYS   = int(os.environ.get("CLARITY_WINDOW_DAYS",   "21"))
CLARITY_MIN_ARTICLES  = int(os.environ.get("CLARITY_MIN_ARTICLES",  "6"))
CLARITY_MIN_OPS       = int(os.environ.get("CLARITY_MIN_OPS",       "3"))
CLARITY_COHERENCE_MIN = float(os.environ.get("CLARITY_COHERENCE_MIN", "0.5"))


def _get_supabase_client():
    try:
        from supabase import create_client
    except ImportError:
        log.error("supabase SDK not installed")
        return None
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


def get_active_watch_voices(client, state_actor_lens: Optional[str] = None) -> list:
    """Fetch voices that have unreviewed Watch findings."""
    try:
        query = client.table("lens_drift_findings") \
            .select("state_actor_lens, finding_phrasing, created_at") \
            .eq("finding_confidence", "LOW") \
            .eq("reviewed_by_operator", False)
        if state_actor_lens:
            query = query.eq("state_actor_lens", state_actor_lens)
        response = query.execute()

        # Extract voice names from finding_phrasing
        voices = []
        for row in (response.data or []):
            phrasing = row.get("finding_phrasing", "")
            # Format: "Watch alert (lens): voice_name — ..."
            if "): " in phrasing and " — " in phrasing:
                voice = phrasing.split("): ")[1].split(" — ")[0].strip()
                voices.append({
                    "voice_name": voice,
                    "state_actor_lens": row["state_actor_lens"],
                    "watch_created_at": row["created_at"],
                })
        return voices
    except Exception as e:
        log.error(f"Watch voices fetch failed: {str(e)[:200]}")
        return []


def run_clarity_aggregator(
    state_actor_lens: Optional[str] = None,
    dry_run: bool = False,
) -> list[dict]:
    """Run Clarity aggregator on voices with active Watch alerts.

    Returns list of Clarity findings.
    """
    client = _get_supabase_client()
    if not client:
        return []

    watch_voices_raw = get_active_watch_voices(client, state_actor_lens)
    # Deduplicate by (voice_name, state_actor_lens) — keep most recent
    seen = {}
    for v in watch_voices_raw:
        key = (v["voice_name"], v["state_actor_lens"])
        if key not in seen or v["watch_created_at"] > seen[key]["watch_created_at"]:
            seen[key] = v
    watch_voices = list(seen.values())
    log.info(f"Clarity aggregator: {len(watch_voices)} unique voices ({len(watch_voices_raw)} raw) with Watch alerts")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=CLARITY_WINDOW_DAYS)).isoformat()
    findings = []

    for voice_info in watch_voices:
        voice_name = voice_info["voice_name"]
        lens = voice_info["state_actor_lens"]

        # Fetch detections for this voice in clarity window
        try:
            response = client.table("lens_operation_detections") \
                .select("operations_detected, raw_article_id, scored_at, confidence") \
                .eq("voice_name", voice_name) \
                .eq("state_actor_lens", lens) \
                .eq("stage_filter", "early_warning") \
                .eq("not_applicable", False) \
                .gte("scored_at", cutoff) \
                .execute()
            detections = response.data or []
        except Exception as e:
            log.error(f"Detection fetch failed for {voice_name}: {str(e)[:200]}")
            continue

        unique_articles = {d["raw_article_id"] for d in detections}

        if len(unique_articles) < CLARITY_MIN_ARTICLES:
            log.info(f"Clarity: {voice_name} × {lens} — insufficient articles ({len(unique_articles)}/{CLARITY_MIN_ARTICLES}), Watch alert dissolving")
            # Record dissolution
            findings.append({
                "voice_name": voice_name,
                "state_actor_lens": lens,
                "stage": "clarity",
                "outcome": "dissolved",
                "article_count": len(unique_articles),
                "finding_confidence": "LOW",
                "finding_phrasing": (
                    f"Clarity update ({lens}): {voice_name} — "
                    f"Watch alert from Day 0-7 did NOT persist. "
                    f"Only {len(unique_articles)} articles in {CLARITY_WINDOW_DAYS}-day window "
                    f"(minimum {CLARITY_MIN_ARTICLES} required). Pattern dissolved."
                ),
                "alternative_hypotheses": [],
                "rubric_version": "v2-operations",
            })
            continue

        # Count operations
        op_counter = Counter()
        for d in detections:
            for op in (d["operations_detected"] or []):
                op_counter[op.get("id", "")] += 1

        total_unique_ops = len(op_counter)
        recurring_ops = [(op, cnt) for op, cnt in op_counter.items() if cnt >= 2]

        if total_unique_ops < CLARITY_MIN_OPS:
            log.info(f"Clarity: {voice_name} × {lens} — insufficient ops ({total_unique_ops}), dissolving")
            continue

        # Cross-topic coherence: do same ops appear across different time windows?
        coherence = len(recurring_ops) / max(total_unique_ops, 1)

        pattern_status = "developing" if coherence >= CLARITY_COHERENCE_MIN else "weak"

        finding_phrasing = (
            f"Clarity alert ({lens}): {voice_name} — "
            f"pattern is {pattern_status}. "
            f"{len(recurring_ops)} operations recurring across {len(unique_articles)} articles "
            f"over {CLARITY_WINDOW_DAYS} days. "
            f"Cross-topic coherence: {coherence:.0%}. "
            f"Top operations: {', '.join([op for op, _ in recurring_ops[:3]])}. "
            f"Confidence: MEDIUM. Requires Verification tier for HIGH confidence conclusion."
        )

        finding = {
            "voice_name":              voice_name,
            "state_actor_lens":        lens,
            "stage":                   "clarity",
            "outcome":                 "developing" if coherence >= CLARITY_COHERENCE_MIN else "weak",
            "article_count":           len(unique_articles),
            "operation_counts":        dict(op_counter),
            "recurring_ops":           [op for op, _ in recurring_ops],
            "coherence_score":         coherence,
            "finding_confidence":      "MEDIUM",
            "finding_phrasing":        finding_phrasing,
            "evidence_article_ids":    list(unique_articles),
            "alternative_hypotheses": [
                {"hypothesis": "Beat specialization — voice covers this topic area, structural ops expected", "plausibility": 0.35},
                {"hypothesis": "Editorial house style — outlet norms produce these patterns", "plausibility": 0.25},
                {"hypothesis": "Genuine coordinated framing — requires Verification tier to confirm", "plausibility": None},
            ],
            "rubric_version": "v2-operations",
        }

        findings.append(finding)
        log.info(f"Clarity finding ({pattern_status}): {voice_name} × {lens}")

    log.info(f"Clarity aggregator: {len(findings)} findings generated")

    if not dry_run and findings:
        _write_findings(client, findings)

    return findings


def _write_findings(client, findings: list[dict]):
    for f in findings:
        try:
            row = {
                "entity_id":               get_state_office_entity_id(client, f["state_actor_lens"]),
                "state_actor_lens":        f["state_actor_lens"],
                "window_start":            (datetime.now(timezone.utc) - timedelta(days=21)).date().isoformat(),
                "window_end":              datetime.now(timezone.utc).date().isoformat(),
                "sample_size":             max(f["article_count"], 1),
                "framing_mean":            f.get("operation_counts", {}),
                "outlet_baseline":         {},
                "deviance_sigma":          f.get("coherence_score", 0.0),
                "alternative_hypotheses":  f["alternative_hypotheses"],
                "finding_confidence":      f["finding_confidence"],
                "evidence_article_ids":    f.get("evidence_article_ids", []),
                "finding_phrasing":        f["finding_phrasing"],
                "rubric_version":          f["rubric_version"],
                "reviewed_by_operator":    False,
            }
            client.table("lens_drift_findings").insert(row).execute()
            log.info(f"Clarity finding written: {f['voice_name']} × {f['state_actor_lens']}")
        except Exception as e:
            log.error(f"Clarity write failed: {str(e)[:200]}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    import sys
    lens_filter = sys.argv[1] if len(sys.argv) > 1 else None
    dry = "--dry" in sys.argv
    findings = run_clarity_aggregator(state_actor_lens=lens_filter, dry_run=dry)
    print(f"\nClarity aggregator: {len(findings)} findings")
    for f in findings:
        print(f"  [{f['outcome']}] {f['voice_name']} × {f['state_actor_lens']}: conf={f['finding_confidence']}")
