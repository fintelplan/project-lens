"""
lens_s2f_scoring_cron.py — S2-F Scoring Cron Entry Point
LENS-020 | Project Lens

Called by lens-s2f-scoring.yml GitHub Actions workflow.
Fetches recent articles from Supabase, scores each via ensemble detector,
writes DetectionResult to lens_operation_detections.

Env vars:
    SUPABASE_URL, SUPABASE_KEY
    CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID   (CC-95: CEREBRAS_API_KEY is no longer read)
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


def _paged(make_query, page=1000):
    """All rows of a query, page by page (PostgREST returns at most 1000 per call)."""
    out, start = [], 0
    while True:
        rows = make_query().range(start, start + page - 1).execute().data or []
        out += rows
        if len(rows) < page:
            return out
        start += page


def get_recent_articles(client, lookback_hours: int, max_articles: int):
    """Pick up to max_articles articles from the last N hours to score (CC-122).

    Before CC-122 this took the newest max_articles rows by collected_at and the scoring loop
    dropped the short ones afterwards. The collector writes each batch from parallel threads
    within about a second, so the newest rows were whichever source finished last: 975 of
    1,093 detections in 45 days were RT, 4 voices in all, while 200-285 long articles per
    window went unread; on Sep 24 the newest eight were TASS teasers and nothing was scored
    under a green run. LENS-020 v4 gives the Watch tier "every article, every cron run".
    Now lens_s2f_selection.select_articles: long enough first, not yet scored, one article per
    source per round, the source scored longest ago first -- S2-D's LENS-017 B-2 round-robin,
    carried across runs. max_articles still bounds the provider calls; the lookback can be
    wide because scored articles are excluded. Returns None when the database read fails.
    """
    from lens_s2f_selection import (select_articles, report_line, last_scored_by,
                                    MIN_BODY_CHARS, RECENT_DAYS)
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=lookback_hours)).isoformat()
    since = (now - timedelta(days=RECENT_DAYS)).isoformat()
    try:
        rows = _paged(lambda: client.table("lens_raw_articles")
                      .select("id, title, content, source_name, author, collected_at")
                      .gte("collected_at", cutoff).order("collected_at", desc=True))
        recent = _paged(lambda: client.table("lens_operation_detections")
                        .select("voice_name, scored_at").gte("scored_at", since))
        last = last_scored_by(recent)
        long_ids = [r["id"] for r in rows if len(r.get("content") or "") >= MIN_BODY_CHARS]
        scored_ids = set()
        for k in range(0, len(long_ids), 100):
            got = client.table("lens_operation_detections").select("raw_article_id") \
                .in_("raw_article_id", long_ids[k:k + 100]).eq("stage_filter", "early_warning") \
                .execute().data or []
            scored_ids.update(g["raw_article_id"] for g in got)
    except Exception as e:
        log.error(f"Article selection failed: {str(e)[:200]}")
        return None
    picked, report = select_articles(rows, last, max_articles, exclude_ids=scored_ids)
    log.info(report_line(report, lookback_hours))
    if rows and not picked:
        log.warning(f"S2-F: nothing to score -- {report['window']} articles in the window, "
                    f"{report['long']} long enough, {report['already_scored']} of those already scored")
    return picked


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
    except Exception as e:
        # CC-101: any error here read as 'not scored', silently, and the article
        # was scored again. The behaviour stays; the silence goes.
        from lens_framing_rubrics import _redact
        log.warning(f"already_scored check failed, scoring anyway: {_redact(str(e))[:160]}")
        return False


def scoring_exit_code(scored: int, failed: int) -> int:
    """CC-85: a run that wrote NOTHING while work failed is not a green run.

    scored=0 failed=24 exited 0 for months and showed a green tick.
    A partially failed run (scored>0) still exits 0 -- see LENS-043 order.
    """
    return 1 if (scored == 0 and failed > 0) else 0


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
    if articles is None:                      # CC-122: a failed read is not 'nothing new'
        log.error("S2-F: article selection failed -- exiting 1")
        sys.exit(1)
    if not articles:
        log.info("No articles to score — exiting")
        return

    # ── Score each article ──
    scored = 0
    skipped = 0
    failed = 0
    quota_skipped = 0   # CC-97: the provider refused for the day; no call was made

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

                if result.status == "SKIP_QUOTA":
                    # CC-97: not scored, not written, and not a new failure --
                    # the refusal that tripped the breaker was counted once, as failed,
                    # so a run that scored nothing still exits 1 (CC-85).
                    quota_skipped += 1
                    log.info("  -> quota-skipped: " + (result.error or "")[:120])
                    continue

                uid = write_detection_result(
                    result=result,
                    raw_article_id=article_id,
                    voice_name=voice_name,
                    voice_type=voice_type,
                    provider=result.provider or "unknown",   # CC-85: the leg that answered
                    ensemble_mode=result.ensemble_mode,      # CC-85: both legs, or nothing
                )

                if uid:
                    scored += 1
                    log.info(f"  → {result.operation_count()} ops / conf={result.confidence:.2f}")
                else:
                    failed += 1

            except Exception as e:
                log.error(f"  → Error: {str(e)[:200]}")
                failed += 1

    log.info(f"S2-F cron complete: scored={scored} skipped={skipped} failed={failed} "
             f"quota_skipped={quota_skipped}")
    _attempted = scored + failed + quota_skipped   # CC-125 (D6, ruled A at LENS-045): say the coverage
    if _attempted:
        _cov = 100 * scored // _attempted
        (log.warning if _cov < 50 else log.info)(
            f"S2F_COVERAGE scored {scored} of {_attempted} attempted scorings ({_cov}%)"
            + (" -- under half; the exit stays green until D2 sets the RED threshold" if _cov < 50 else ""))
    if quota_skipped:
        log.warning(f"S2-F: {quota_skipped} scorings not attempted -- a provider "
                    f"refused for the day (CC-97 breaker)")

    code = scoring_exit_code(scored, failed)
    if code:
        log.error(f"S2-F wrote ZERO rows with {failed} failures -- exiting {code}")
    sys.exit(code)


if __name__ == "__main__":
    if not os.environ.get("GITHUB_ACTIONS"):
        from dotenv import load_dotenv
        load_dotenv()
    main()
