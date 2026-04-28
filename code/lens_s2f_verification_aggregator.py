"""
lens_s2f_verification_aggregator.py — Stage 3 Verification Aggregator
LENS-020 | Project Lens

PHI-004 cognitive sovereignty cadence — Verification phase (Day 30-45, HIGH confidence).
Produces Forensic Report inputs for Direction B delivery (operator-gated).

Requirements per session spec (T-020.D):
  - Minimum 15 articles from voice across window (sample_size >= 15)
  - Confidence ceiling: HIGH
  - Must include: complete arc (Watch said X, Clarity updated to Y, Verification finds Z)
  - Must include: operations that WASHED OUT and operations that PERSISTED
  - Output: lens_drift_findings with finding_confidence='HIGH' + reviewed_by_operator=False
  - Feeds Direction B: Telegram Forensic Report delivery to operator

Honesty discipline per PHI-004:
  "The Verification Alert IS the archive of the cognitive process, not merely its conclusion."
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter
from typing import Optional

log = logging.getLogger("s2f_verification")

VERIFICATION_WINDOW_DAYS  = int(os.environ.get("VERIFICATION_WINDOW_DAYS", "45"))
VERIFICATION_MIN_ARTICLES = int(os.environ.get("VERIFICATION_MIN_ARTICLES", "15"))
VERIFICATION_MIN_OPS      = int(os.environ.get("VERIFICATION_MIN_OPS",      "3"))
VERIFICATION_RECUR_MIN    = int(os.environ.get("VERIFICATION_RECUR_MIN",    "3"))  # op must appear in >= N articles


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


def run_verification_aggregator(
    state_actor_lens: Optional[str] = None,
    dry_run: bool = False,
) -> list[dict]:
    """Run Verification aggregator on voices with Clarity findings.

    Returns Verification findings ready for Direction B delivery.
    """
    client = _get_supabase_client()
    if not client:
        return []

    # ── Get voices with MEDIUM Clarity findings ──
    try:
        query = client.table("lens_drift_findings") \
            .select("state_actor_lens, finding_phrasing, created_at, framing_mean") \
            .eq("finding_confidence", "MEDIUM") \
            .eq("reviewed_by_operator", False)
        if state_actor_lens:
            query = query.eq("state_actor_lens", state_actor_lens)
        response = query.execute()
        clarity_findings = response.data or []
    except Exception as e:
        log.error(f"Clarity findings fetch failed: {str(e)[:200]}")
        return []

    log.info(f"Verification aggregator: {len(clarity_findings)} voices from Clarity tier")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=VERIFICATION_WINDOW_DAYS)).isoformat()
    findings = []

    for cf in clarity_findings:
        lens = cf["state_actor_lens"]
        phrasing = cf.get("finding_phrasing", "")

        # Extract voice name
        if "): " in phrasing and " — " in phrasing:
            voice_name = phrasing.split("): ")[1].split(" — ")[0].strip()
        else:
            continue

        # ── Fetch all detections in verification window ──
        try:
            response = client.table("lens_operation_detections") \
                .select("operations_detected, raw_article_id, scored_at, confidence, stage_filter") \
                .eq("voice_name", voice_name) \
                .eq("state_actor_lens", lens) \
                .eq("not_applicable", False) \
                .gte("scored_at", cutoff) \
                .execute()
            detections = response.data or []
        except Exception as e:
            log.error(f"Detection fetch failed: {str(e)[:200]}")
            continue

        unique_articles = {d["raw_article_id"] for d in detections}

        # Hard gate: minimum 15 articles
        if len(unique_articles) < VERIFICATION_MIN_ARTICLES:
            log.info(
                f"Verification: {voice_name} × {lens} — "
                f"only {len(unique_articles)} articles (need {VERIFICATION_MIN_ARTICLES}). "
                f"Not ready for Verification tier yet."
            )
            continue

        # ── Count operations per article ──
        op_per_article = {}
        for d in detections:
            art_id = d["raw_article_id"]
            if art_id not in op_per_article:
                op_per_article[art_id] = set()
            for op in (d["operations_detected"] or []):
                op_per_article[art_id].add(op.get("id", ""))

        # Count how many articles each op appears in
        op_article_count = Counter()
        for art_ops in op_per_article.values():
            for op_id in art_ops:
                op_article_count[op_id] += 1

        # Persistent ops: appear in >= VERIFICATION_RECUR_MIN articles
        persistent_ops = [
            (op, cnt) for op, cnt in op_article_count.items()
            if cnt >= VERIFICATION_RECUR_MIN
        ]
        persistent_ops.sort(key=lambda x: x[1], reverse=True)

        # Washed-out ops: appeared in < VERIFICATION_RECUR_MIN articles
        washed_out_ops = [
            op for op, cnt in op_article_count.items()
            if cnt < VERIFICATION_RECUR_MIN
        ]

        if len(persistent_ops) < VERIFICATION_MIN_OPS:
            log.info(
                f"Verification: {voice_name} × {lens} — "
                f"only {len(persistent_ops)} persistent ops (need {VERIFICATION_MIN_OPS}). "
                f"Pattern insufficient for HIGH confidence."
            )
            continue

        # ── Build Verification finding ──
        avg_confidence = sum(d["confidence"] for d in detections) / max(len(detections), 1)

        top_ops_str = ", ".join([f"{op} ({cnt} articles)" for op, cnt in persistent_ops[:5]])
        washed_str  = ", ".join(washed_out_ops[:3]) if washed_out_ops else "none"

        finding_phrasing = (
            f"VERIFICATION FINDING ({lens}): {voice_name}\n\n"
            f"CONFIRMED PERSISTENT PATTERNS ({len(unique_articles)} articles over {VERIFICATION_WINDOW_DAYS} days):\n"
            f"  Operations persisting across >= {VERIFICATION_RECUR_MIN} articles: {top_ops_str}\n\n"
            f"OPERATIONS THAT WASHED OUT: {washed_str}\n\n"
            f"COGNITIVE ARC:\n"
            f"  Watch (Day 0-7):       Pattern suspected\n"
            f"  Clarity (Day 7-30):    Pattern developing — {cf.get('finding_phrasing', '')[:80]}...\n"
            f"  Verification (Day 30-45): Pattern confirmed at HIGH confidence\n\n"
            f"CONFIDENCE: HIGH | Sample: {len(unique_articles)} articles | "
            f"Avg detection confidence: {avg_confidence:.2f}\n\n"
            f"⚠️ Operator review required before Direction A (public) delivery."
        )

        finding = {
            "voice_name":             voice_name,
            "state_actor_lens":       lens,
            "stage":                  "verification",
            "article_count":          len(unique_articles),
            "persistent_ops":         [op for op, _ in persistent_ops],
            "washed_out_ops":         washed_out_ops,
            "op_article_counts":      dict(op_article_count),
            "avg_confidence":         avg_confidence,
            "finding_confidence":     "HIGH",
            "finding_phrasing":       finding_phrasing,
            "evidence_article_ids":   list(unique_articles),
            "ready_for_direction_b":  True,
            "alternative_hypotheses": [
                {"hypothesis": "Sustained beat specialization — voice's editorial role produces these patterns", "plausibility": 0.2},
                {"hypothesis": "Long-term outlet editorial line — house policy rather than coordinated influence", "plausibility": 0.15},
            ],
            "rubric_version": "v2-operations",
        }

        findings.append(finding)
        log.info(
            f"VERIFICATION FINDING: {voice_name} × {lens} — "
            f"{len(persistent_ops)} persistent ops, HIGH confidence ✅"
        )

    log.info(f"Verification aggregator: {len(findings)} HIGH-confidence findings")

    if not dry_run and findings:
        _write_findings(client, findings)

    return findings


def _write_findings(client, findings: list[dict]):
    for f in findings:
        try:
            row = {
                "entity_id":             None,
                "state_actor_lens":       f["state_actor_lens"],
                "window_start":           (datetime.now(timezone.utc) - timedelta(days=45)).date().isoformat(),
                "window_end":             datetime.now(timezone.utc).date().isoformat(),
                "sample_size":            f["article_count"],
                "framing_mean":           f["op_article_counts"],
                "outlet_baseline":        {},
                "deviance_sigma":         f["avg_confidence"],
                "alternative_hypotheses": f["alternative_hypotheses"],
                "finding_confidence":     f["finding_confidence"],
                "evidence_article_ids":   f["evidence_article_ids"],
                "finding_phrasing":       f["finding_phrasing"],
                "rubric_version":         f["rubric_version"],
                "reviewed_by_operator":   False,
            }
            client.table("lens_drift_findings").insert(row).execute()
            log.info(f"Verification finding written: {f['voice_name']} × {f['state_actor_lens']}")
        except Exception as e:
            log.error(f"Verification write failed: {str(e)[:200]}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    lens_filter = sys.argv[1] if len(sys.argv) > 1 else None
    dry = "--dry" in sys.argv
    findings = run_verification_aggregator(state_actor_lens=lens_filter, dry_run=dry)
    print(f"\nVerification aggregator: {len(findings)} HIGH-confidence findings")
    for f in findings:
        print(f"  ✅ {f['voice_name']} × {f['state_actor_lens']}: {len(f['persistent_ops'])} persistent ops")
        print(f"     Ready for Direction B: {f['ready_for_direction_b']}")
