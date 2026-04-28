"""
lens_s2f_scoring_cron.py — S2-F Scoring Cron Entry Point
LENS-020 | Project Lens

Called by lens-s2f-scoring.yml GitHub Actions workflow.
Fetches recent articles from Supabase, scores each via ensemble detector,
writes DetectionResult to lens_operation_detections.

Env vars:
    SUPABASE_URL, SUPABASE_KEY
    CEREBRAS_API_KEY, CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID
    S2F_LENSES          comma-separated lenses (default: xi_office,trump_office,khamenei_office)
    S2F_LOOKBACK_HOURS  hours to look back for new articles (default: 3)
    S2F_MAX_ARTICLES    max articles per run (default: 20, cost control)
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2F-CRON] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("s2f_cron")


def get_recent_articles(client, lookback_hours: int, max_articles: int) -> list:
    """Fetch articles collected in the last N hours, not yet scored by S2-F."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    try:
        # Get recent articles
        response = client.table("lens_raw_articles") \
            .select("id, title, content, source_name, author, collected_at") \
            .gte("collected_at", cutoff) \
            .order("collected_at", desc=True) \
            .limit(max_articles) \
            .execute()
        articles = response.data or []
        log.info(f"Fetched {len(articles)} articles from last {lookback_hours}h")
        return articles
    except Exception as e:
        log.error(f"Article fetch failed: {str(e)[:200]}")
        return []


def already_scored(client, article_id: str, lens: str, stage: str) -> bool:
    """Check if article × lens × stage already has a detection result."""
    try:
        response = client.table("lens_operation_detections") \
            .select("id") \
            .eq("raw_article_id", article_id) \
            .eq("state_actor_lens", lens) \
            .eq("stage_filter", stage) \
            .limit(1) \
            .execute()
        return len(response.data or []) > 0
    except Exception:
        return False


def main():
    # ── Config ──
    lenses = os.environ.get("S2F_LENSES", "xi_office,trump_office,khamenei_office").split(",")
    lookback_hours = int(os.environ.get("S2F_LOOKBACK_HOURS", "3"))
    max_articles = int(os.environ.get("S2F_MAX_ARTICLES", "20"))

    log.info(f"S2-F scoring cron starting: lenses={lenses}, lookback={lookback_hours}h, max={max_articles}")

    # ── Supabase ──
    try:
        from supabase import create_client
    except ImportError:
        log.error("supabase not installed")
        sys.exit(1)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        log.error("SUPABASE_URL or SUPABASE_KEY not set")
        sys.exit(1)
    client = create_client(url, key)

    # ── Import rubric modules ──
    from lens_framing_rubrics import detect_operations_ensemble
    from lens_s2f_writer import write_detection_result

    # ── Fetch articles ──
    articles = get_recent_articles(client, lookback_hours, max_articles)
    if not articles:
        log.info("No articles to score — exiting")
        return

    # ── Score each article ──
    scored = 0
    skipped = 0
    failed = 0

    for article in articles:
        article_id = article["id"]
        title = article.get("title", "")
        body = article.get("content", "")
        source = article.get("source_name", "unknown")
        voice_name = article.get("author") or source
        voice_type = "author" if article.get("author") else "unknown"

        if not body or len(body) < 400:
            log.info(f"Skip (too short): {title[:60]}")
            skipped += 1
            continue

        for lens in lenses:
            # Check if already scored
            if already_scored(client, article_id, lens, "early_warning"):
                log.info(f"Skip (already scored): {title[:40]} × {lens}")
                skipped += 1
                continue

            log.info(f"Scoring: {title[:60]} × {lens}")
            try:
                result = detect_operations_ensemble(
                    article_title=title,
                    article_body=body,
                    article_source=source,
                    voice_name=voice_name,
                    voice_type=voice_type,
                    state_actor_lens=lens,
                    stage_filter="early_warning",
                )

                uid = write_detection_result(
                    result=result,
                    raw_article_id=article_id,
                    voice_name=voice_name,
                    voice_type=voice_type,
                    provider="ensemble",
                    ensemble_mode=True,
                )

                if uid:
                    scored += 1
                    log.info(f"  → {result.operation_count()} ops / conf={result.confidence:.2f}")
                else:
                    failed += 1

            except Exception as e:
                log.error(f"  → Error: {str(e)[:200]}")
                failed += 1

    log.info(f"S2-F cron complete: scored={scored} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    if not os.environ.get("GITHUB_ACTIONS"):
        from dotenv import load_dotenv
        load_dotenv()
    main()
