"""
lens_s2f_watch_aggregator.py — Stage 1 Watch Aggregator
LENS-020 | Project Lens

PHI-004 cognitive sovereignty cadence — Watch phase (Day 0-7, LOW confidence).
Scans recent operation detections for emerging patterns.

Trigger: Watch alert fires when a voice × lens shows:
  - ≥2 instances of the SAME operation across different articles, OR
  - ≥3 DIFFERENT operations across different articles

Phrasing discipline: "pattern may be forming" — NEVER "pattern confirmed".
Alternative hypotheses always included per LR-083.

Output: lens_drift_findings rows with finding_confidence='LOW'.
"""

import os
import logging
from datetime import datetime, timezone
from collections import Counter
from typing import Optional

log = logging.getLogger("s2f_watch")

# ── Thresholds (operator-tunable via env) ──────────────────────────────────
WATCH_WINDOW_DAYS   = int(os.environ.get("WATCH_WINDOW_DAYS",   "10"))
WATCH_MIN_ARTICLES  = int(os.environ.get("WATCH_MIN_ARTICLES",  "3"))
WATCH_SAME_OP_MIN   = int(os.environ.get("WATCH_SAME_OP_MIN",   "2"))   # ≥N of same op
WATCH_DIFF_OPS_MIN  = int(os.environ.get("WATCH_DIFF_OPS_MIN",  "3"))   # ≥N different ops


def _get_supabase_client():
    try:
        from supabase import create_client
from lens_s2f_helpers import get_state_office_entity_id
    except ImportError:
        log.error("supabase SDK not installed")
        return None
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


def run_watch_aggregator(
    state_actor_lens: Optional[str] = None,
    dry_run: bool = False,
) -> list[dict]:
    """Run Watch aggregator across all voices (or one lens).

    Args:
        state_actor_lens: filter to one lens (e.g. 'xi_office') or None for all
        dry_run: if True, compute findings but don't write to DB

    Returns:
        List of finding dicts (written or not depending on dry_run)
    """
    client = _get_supabase_client()
    if not client:
        log.error("No Supabase client — cannot run Watch aggregator")
        return []

    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=WATCH_WINDOW_DAYS)).isoformat()

    # ── Fetch recent detections ──
    log.info(f"Watch aggregator: fetching detections (last {WATCH_WINDOW_DAYS} days)")
    try:
        query = client.table("lens_operation_detections") \
            .select("voice_name, voice_type, state_actor_lens, operations_detected, raw_article_id, scored_at, confidence") \
            .eq("stage_filter", "early_warning") \
            .eq("not_applicable", False) \
            .gte("scored_at", cutoff)
        if state_actor_lens:
            query = query.eq("state_actor_lens", state_actor_lens)
        response = query.execute()
        detections = response.data or []
    except Exception as e:
        log.error(f"Fetch failed: {str(e)[:200]}")
        return []

    log.info(f"Watch aggregator: {len(detections)} detections fetched")

    # ── Group by (voice_name, state_actor_lens) ──
    grouped = {}
    for d in detections:
        key = (d["voice_name"], d["state_actor_lens"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(d)

    findings = []

    for (voice_name, lens), rows in grouped.items():
        # Need minimum article count
        unique_articles = {r["raw_article_id"] for r in rows}
        if len(unique_articles) < WATCH_MIN_ARTICLES:
            continue

        # Count operations across all articles
        op_counter = Counter()
        evidence_articles = {}
        for row in rows:
            for op in (row["operations_detected"] or []):
                op_id = op.get("id", "")
                op_counter[op_id] += 1
                if op_id not in evidence_articles:
                    evidence_articles[op_id] = []
                evidence_articles[op_id].append(row["raw_article_id"])

        # Check thresholds
        same_op_hits = [(op, cnt) for op, cnt in op_counter.items() if cnt >= WATCH_SAME_OP_MIN]
        diff_ops_count = len(op_counter)

        if not same_op_hits and diff_ops_count < WATCH_DIFF_OPS_MIN:
            continue

        # Build finding
        trigger_reason = []
        triggered_ops = []
        if same_op_hits:
            for op_id, cnt in same_op_hits:
                trigger_reason.append(f"{op_id} detected {cnt}x across {len(evidence_articles.get(op_id, []))} articles")
                triggered_ops.append(op_id)
        if diff_ops_count >= WATCH_DIFF_OPS_MIN:
            trigger_reason.append(f"{diff_ops_count} different operations detected")

        finding_phrasing = (
            f"Watch alert ({lens}): {voice_name} — "
            f"pattern may be forming. "
            f"{'; '.join(trigger_reason)}. "
            f"Observed across {len(unique_articles)} articles in {WATCH_WINDOW_DAYS}-day window. "
            f"Confidence: LOW. Alternative hypothesis: editorial consistency or source specialization."
        )

        finding = {
            "voice_name":              voice_name,
            "state_actor_lens":        lens,
            "stage":                   "watch",
            "window_days":             WATCH_WINDOW_DAYS,
            "article_count":           len(unique_articles),
            "triggered_operations":    triggered_ops,
            "operation_counts":        dict(op_counter),
            "evidence_article_ids":    list(unique_articles),
            "finding_confidence":      "LOW",
            "finding_phrasing":        finding_phrasing,
            "alternative_hypotheses":  [
                {"hypothesis": "Editorial consistency — voice consistently covers this beat with same framing style", "plausibility": 0.4},
                {"hypothesis": "Source specialization — outlet's audience expects this framing angle", "plausibility": 0.3},
                {"hypothesis": "Topic structure — article genre drives operation pattern, not voice intent", "plausibility": 0.2},
            ],
            "rubric_version":          "v2-operations",
            "created_at":              datetime.now(timezone.utc).isoformat(),
        }

        findings.append(finding)
        log.info(f"Watch finding: {voice_name} × {lens} — {finding_phrasing[:100]}...")

    log.info(f"Watch aggregator: {len(findings)} findings generated")

    # ── Write to DB ──
    if not dry_run and findings:
        _write_findings(client, findings)

    return findings


def _write_findings(client, findings: list[dict]):
    """Write Watch findings to lens_drift_findings."""
    for f in findings:
        try:
            row = {
                "entity_id":               get_state_office_entity_id(client, f["state_actor_lens"]),
                "state_actor_lens":         f["state_actor_lens"],
                "window_start":             (datetime.now(timezone.utc)).date().isoformat(),
                "window_end":               (datetime.now(timezone.utc)).date().isoformat(),
                "sample_size":              f["article_count"],
                "framing_mean":             f["operation_counts"],
                "outlet_baseline":          {},
                "deviance_sigma":           0.0,
                "alternative_hypotheses":   f["alternative_hypotheses"],
                "finding_confidence":       f["finding_confidence"],
                "evidence_article_ids":     f["evidence_article_ids"],
                "finding_phrasing":         f["finding_phrasing"],
                "rubric_version":           f["rubric_version"],
                "reviewed_by_operator":     False,
            }
            client.table("lens_drift_findings").insert(row).execute()
            log.info(f"Finding written: {f['voice_name']} × {f['state_actor_lens']}")
        except Exception as e:
            log.error(f"Finding write failed: {str(e)[:200]}")


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()
    lens_filter = sys.argv[1] if len(sys.argv) > 1 else None
    dry = "--dry" in sys.argv
    findings = run_watch_aggregator(state_actor_lens=lens_filter, dry_run=dry)
    print(f"\nWatch aggregator complete: {len(findings)} findings")
    for f in findings:
        print(f"  {f['voice_name']} × {f['state_actor_lens']}: {f['finding_phrasing'][:120]}...")
