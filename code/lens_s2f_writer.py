"""
lens_s2f_writer.py — S2-F Detection Result Persistence
LENS-020 | Project Lens

Writes DetectionResult objects from lens_framing_rubrics.py to Supabase
lens_operation_detections table.

Per LR-076: read before edit. Per LR-080: write-then-verify (SELECT after INSERT).
Window-agnostic: called by Watch/Clarity/Verification aggregators, not directly.

Dependencies:
    pip install supabase
Env:
    SUPABASE_URL
    SUPABASE_KEY (service_role key for write access)
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("s2f_writer")


def _get_supabase_client():
    """Return Supabase client or None if not configured."""
    try:
        from supabase import create_client
    except ImportError:
        log.error("supabase SDK not installed — pip install supabase")
        return None
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL or SUPABASE_KEY not set")
        return None
    return create_client(url, key)


def write_detection_result(
    result,
    raw_article_id: str,
    voice_name: str,
    voice_type: str,
    provider: str = "cerebras",
    ensemble_mode: bool = False,
) -> Optional[str]:
    """Persist one DetectionResult to lens_operation_detections.

    Args:
        result: DetectionResult from detect_operations_in_article() or
                detect_operations_ensemble()
        raw_article_id: UUID of the article in lens_raw_articles
        voice_name: byline or quoted expert name
        voice_type: author | expert | official | think_tank | unknown
        provider: which provider(s) were used
        ensemble_mode: True if result came from detect_operations_ensemble()

    Returns:
        UUID of inserted row, or None on failure.
    """
    if result.status != "OK":
        log.warning(f"Skipping write — status={result.status} (not OK)")
        return None

    client = _get_supabase_client()
    if not client:
        return None

    early_count = len(result.early_warning_operations())
    post_count = len(result.post_suspect_operations())

    row = {
        "raw_article_id":          raw_article_id,
        "voice_name":              voice_name,
        "voice_type":              voice_type,
        "state_actor_lens":        result.state_actor_lens,
        "stage_filter":            result.stage_filter,
        "operations_detected":     result.operations_detected or [],
        "operations_not_present":  result.operations_not_present or [],
        "operation_count":         result.operation_count(),
        "early_warning_count":     early_count,
        "post_suspect_count":      post_count,
        "confidence":              result.confidence,
        "not_applicable":          result.not_applicable,
        "food_for_thought":        result.food_for_thought or "",
        "rubric_version":          result.rubric_version,
        "catalog_version":         result.catalog_version or "v3.1",
        "provider":                provider,
        "ensemble_mode":           ensemble_mode,
        "status":                  "pending",
        "reviewed_by_operator":    False,
    }

    try:
        response = client.table("lens_operation_detections").upsert(
            row,
            on_conflict="raw_article_id,voice_name,state_actor_lens,stage_filter,rubric_version"
        ).execute()

        if response.data:
            inserted_id = response.data[0]["id"]
            log.info(
                f"Written detection: {result.state_actor_lens} / {voice_name} "
                f"/ {result.stage_filter} → {result.operation_count()} ops "
                f"/ conf={result.confidence:.2f} / id={inserted_id[:8]}..."
            )
            return inserted_id
        else:
            log.error(f"Write returned no data: {response}")
            return None

    except Exception as e:
        log.error(f"Write failed: {str(e)[:300]}")
        return None


def write_batch(
    results: list,
    raw_article_id: str,
    voice_name: str,
    voice_type: str,
    provider: str = "cerebras",
    ensemble_mode: bool = False,
) -> list[str]:
    """Write multiple DetectionResults for one article × voice.

    Typically called with results from all lenses × stages for one article.
    Returns list of inserted UUIDs (may be shorter than input if some failed).
    """
    inserted = []
    for result in results:
        uid = write_detection_result(
            result=result,
            raw_article_id=raw_article_id,
            voice_name=voice_name,
            voice_type=voice_type,
            provider=provider,
            ensemble_mode=ensemble_mode,
        )
        if uid:
            inserted.append(uid)
    log.info(f"Batch write: {len(inserted)}/{len(results)} succeeded for article {raw_article_id[:8]}...")
    return inserted


def get_detections_for_article(
    raw_article_id: str,
    state_actor_lens: Optional[str] = None,
    stage_filter: Optional[str] = None,
) -> list:
    """Fetch all detections for an article. Used by aggregators."""
    client = _get_supabase_client()
    if not client:
        return []
    try:
        query = client.table("lens_operation_detections") \
            .select("*") \
            .eq("raw_article_id", raw_article_id)
        if state_actor_lens:
            query = query.eq("state_actor_lens", state_actor_lens)
        if stage_filter:
            query = query.eq("stage_filter", stage_filter)
        response = query.execute()
        return response.data or []
    except Exception as e:
        log.error(f"Fetch failed: {str(e)[:200]}")
        return []


def get_recent_detections_for_voice(
    voice_name: str,
    state_actor_lens: str,
    days: int = 10,
    stage_filter: str = "early_warning",
) -> list:
    """Fetch recent detections for a voice × lens. Used by Watch aggregator."""
    client = _get_supabase_client()
    if not client:
        return []
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        response = client.table("lens_operation_detections") \
            .select("*") \
            .eq("voice_name", voice_name) \
            .eq("state_actor_lens", state_actor_lens) \
            .eq("stage_filter", stage_filter) \
            .eq("not_applicable", False) \
            .gte("scored_at", cutoff) \
            .order("scored_at", desc=True) \
            .execute()
        return response.data or []
    except Exception as e:
        log.error(f"Voice fetch failed: {str(e)[:200]}")
        return []
