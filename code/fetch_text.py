# ============================================================
# Project Lens — fetch_text.py
# Collects articles from all sources in lens-SRC-001_sources.json
# Saves raw articles to Supabase lens_raw_articles table
# Matches indicators from lens-IND-001_master.json
# Runs via GitHub Actions 4x daily
#
# Ethics: Public sources only (LR-014(E))
# Data: Every article tagged on arrival (LR-003(A))
# ============================================================

import os
import json
import hashlib
import time
import requests
import feedparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from lens_injection_detector import scan_article
from lens_entity_extract import extract_entities_for_article  # LENS-018 T3
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

# ── Load sources and indicators ──────────────────────────────
def load_sources():
    path = os.path.join('data', 'lens-SRC-001_sources.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)['sources']

def load_indicators():
    path = os.path.join('data', 'lens-IND-001_master.json')
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    indicators = []
    for domain_data in data['domains'].values():
        for ind in domain_data['indicators']:
            indicators.append(ind)
    return indicators

# ── Hash URL for deduplication ────────────────────────────────
def hash_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]

# ── Fetch RSS feed ────────────────────────────────────────────
def fetch_feed(source: dict, all_sources: list = None) -> list:
    articles = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; ProjectLens/1.0; +https://github.com/fintelplan/project-lens)'
        }
        response = requests.get(source['rss_url'], headers=headers, timeout=8)
        if not response.ok:
            print(f'  WARNING: {source["name"]} returned {response.status_code}')
            return []

        feed = feedparser.parse(response.content)
        for entry in feed.entries[:20]:
            url = entry.get('link', '')
            if not url:
                continue

            title = entry.get('title', '')
            content = entry.get('summary', '') or entry.get('description', '')

            # Parse published date
            published_at = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    published_at = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                except Exception:
                    published_at = None

            article_candidate = {
                'url': url,
                'url_hash': hash_url(url),
                'title': title[:500] if title else '',
                'content': content[:2000] if content else '',
                'source_id': source['id'],
                'source_name': source['name'],
                'published_at': published_at,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'modality': 'text',
                'language': source.get('language', 'en'),
                'domain': source['domain'],
                'is_verified': False,
                'raw_metadata': json.dumps({
                    'actor': source.get('actor', ''),
                    'tier_coverage': source.get('tier_coverage', []),
                    'tier': source.get('tier', 'TIER2'),
                })
            }
            # ── Injection detection (LENS-005) ───────────────────────────
            scan = scan_article(article_candidate, source_tier=source.get('tier', 'TIER2'))
            if scan['action'] == 'REMOVE':
                # Direct prompt injection — silently drop, never reaches AI
                continue
            if scan['action'] == 'FLAG':
                # Lens-specific attack — include but tag for AI awareness
                article_candidate['injection_flag'] = scan['injection_flag']
                article_candidate['injection_reason'] = scan['reason']
            articles.append(article_candidate)

        print(f'  OK {source["name"]}: {len(articles)} articles')
        if len(articles) == 0 and source.get('reserve_id'):
            rid = source.get('reserve_id')
            rsrc = next((s for s in (all_sources or []) if s['id'] == rid), None)
            if rsrc:
                print(f'  RESERVE: {source["name"]} dead -> {rsrc["name"]}...')
                try:
                    import requests as _rq
                    rf = feedparser.parse(_rq.get(rsrc['rss_url'],timeout=8,headers={'User-Agent':'Mozilla/5.0'}).content)
                    for entry in rf.entries[:20]:
                        u = entry.get('link','')
                        if u: articles.append({'url':u,'url_hash':hash_url(u),'title':entry.get('title','')[:500],'content':(entry.get('summary','') or '')[:2000],'source_id':rsrc['id'],'source_name':rsrc['name']+' [RESERVE]','published_at':None,'collected_at':datetime.now(timezone.utc).isoformat(),'modality':'text','language':'en','domain':source['domain'],'is_verified':False,'raw_metadata':'{}'})
                    print(f'  RESERVE OK: {len(articles)} articles')
                except Exception as re2:
                    print(f'  RESERVE ERROR: {str(re2)[:50]}')

    except Exception as e:
        print(f'  ERROR {source["name"]}: {str(e)[:60]}')

    return articles

# ── Match indicators ──────────────────────────────────────────
def match_indicators(article: dict, indicators: list) -> list:
    """
    Phase 1 keyword matching across ALL indicators regardless of domain.
    Removed strict domain gate — BBC (POWER) can match RESOURCE indicators
    if content mentions energy. Every article gets at least 1 indicator tag.

    Fix: LENS-004 FIX-010 — domain gate removed, keyword matching expanded.
    Phase 2 will use ML classifier (LR-025(M)).
    """
    matches = []
    text = (article.get('title', '') + ' ' + article.get('content', '')).lower()

    for indicator in indicators:
        keywords = ' '.join(indicator.get('what_to_watch', [])).lower() if isinstance(indicator.get('what_to_watch'), list) else indicator.get('what_to_watch', '').lower()
        actor       = indicator.get('actor', '').lower()
        name        = indicator.get('name', '').lower()
        ind_domain  = indicator.get('domain', '')

        # Match 1: actor name appears in article text
        actor_hit = actor and any(
            a.strip() in text
            for a in actor.replace('-', ' ').split()
            if len(a.strip()) > 3  # skip short words like "US" matching "house"
        )

        # Match 2: indicator name keywords appear in article text
        name_words = [w for w in name.split() if len(w) > 4]
        name_hit   = name_words and any(w in text for w in name_words)

        # Match 3: what_to_watch keywords appear in article text
        watch_words = [w for w in keywords.split() if len(w) > 4]
        watch_hit   = watch_words and any(w in text for w in watch_words)

        if actor_hit or name_hit or watch_hit:
            matches.append({
                'indicator_id': indicator['id'],
                'domain':       ind_domain,
                'tier':         indicator['tier'],
                'confidence':   0.5,  # placeholder — ML will improve (LR-025(M))
                'detected_by':  'rule',
                'human_verified': False,
            })

    return matches

# ── Save to Supabase ──────────────────────────────────────────
def save_article(article: dict) -> str | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print('  ERROR: Missing Supabase credentials')
        return None

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates,return=representation',
    }

    try:
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/lens_raw_articles',
            headers=headers,
            json=article,
            timeout=15
        )
        if r.ok and r.json():
            return r.json()[0]['id']
        return None
    except Exception as e:
        print(f'  ERROR saving article: {str(e)[:60]}')
        return None

def save_indicator_matches(article_id: str, matches: list):
    # LENS-008 FIX: bulk POST — was 1 HTTP request per match (2301 requests/run = ~690s)
    # Now: 1 HTTP request per article — ~200 requests/run instead of 2301
    # Root cause of collection 15min timeout. Supabase accepts array for bulk insert.
    if not matches or not article_id:
        return

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates',
    }

    bulk = []
    for match in matches:
        m = dict(match)
        m['article_id'] = article_id
        bulk.append(m)

    try:
        requests.post(
            f'{SUPABASE_URL}/rest/v1/lens_indicator_matches',
            headers=headers,
            json=bulk,
            timeout=15
        )
    except Exception:
        pass

def save_pipeline_run(run_data: dict):
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    }
    try:
        requests.post(
            f'{SUPABASE_URL}/rest/v1/lens_pipeline_runs',
            headers=headers,
            json=run_data,
            timeout=10
        )
    except Exception:
        pass


def save_source_health(health_records: list):
    """LENS-006: Save per-source health to Supabase lens_source_health table."""
    if not health_records:
        return
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates',
    }
    for record in health_records:
        try:
            requests.post(
                f'{SUPABASE_URL}/rest/v1/lens_source_health',
                headers=headers,
                json=record,
                timeout=10
            )
        except Exception:
            pass

# ── Canonical cycle (LENS-014 O1) ───────────────────────────
from lens_cycle import get_cycle  # noqa: E402

# ── Main ──────────────────────────────────────────────────────
def main():
    cycle = get_cycle()

    print('=' * 60)
    print(f'Project Lens — fetch_text.py')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')
    print(f'Cycle: {cycle}')
    print('=' * 60)

    sources = load_sources()
    indicators = load_indicators()
    print(f'Sources loaded: {len(sources)}')
    print(f'Indicators loaded: {len(indicators)}')
    source_fetch_counts = {}  # LENS-005: track per-source article counts
    print()

    total_collected = 0
    total_saved = 0
    total_matches = 0
    errors = []

    run_start = datetime.now(timezone.utc).isoformat()

    # Phase 1: Parallel fetch (LENS-007 FIX-001)
    # Root cause: 41 sources x 15s sequential = ~656s -> timeout
    # Fix: 10 parallel workers -> ~33s fetch time
    # Phase 2 save stays sequential -- no Supabase race conditions
    FETCH_WORKERS = 10

    def fetch_one(src):
        try:
            return src, fetch_feed(src, sources)
        except Exception:
            return src, []

    print(f'[collect] Fetching {len(sources)} sources ({FETCH_WORKERS} workers)...')
    fetched_results = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_one, src): src for src in sources}
        for future in as_completed(futures):
            src, articles = future.result()
            fetched_results[src['id']] = (src, articles)

    print(f'[collect] All fetched. Processing and saving...')
    print()

    # Phase 2: Sequential save -- safe, no race conditions
    for source in sources:
        source, articles = fetched_results.get(source['id'], (source, []))
        source_fetch_counts[source['id']] = len(articles)  # LENS-006: track count

        for article in articles:
            total_collected += 1

            # Match indicators
            matches = match_indicators(article, indicators)
            total_matches += len(matches)

            # Save article
            article_id = save_article(article)
            if article_id:
                total_saved += 1
                save_indicator_matches(article_id, matches)
                # LENS-018 T3: extract entities (fail-safe, never blocks)
                try:
                    extract_entities_for_article(article, article_id)
                except Exception as _entity_exc:
                    # Belt-and-suspenders — module is already internally fail-safe
                    print(f'  WARNING: entity extraction failed: {str(_entity_exc)[:80]}')

    # Save source health to Supabase (LENS-006)
    run_at = datetime.now(timezone.utc).isoformat()
    health_records = []
    sources_map = {s['id']: s for s in sources}
    for source in sources:
        sid = source['id']
        count = source_fetch_counts.get(sid, 0)
        health_records.append({
            'run_at': run_at,
            'cycle': cycle,
            'source_id': sid,
            'source_name': source['name'],
            'domain': source['domain'],
            'tier': source.get('tier', 'TIER2'),
            'articles_count': count,
            'is_dead': count == 0,
            'used_reserve': count == 0 and bool(source.get('reserve_id')),
            'reserve_id': source.get('reserve_id') if count == 0 else None,
        })
    save_source_health(health_records)
    dead_count = sum(1 for r in health_records if r['is_dead'])
    print(f'  Source health saved: {len(health_records)} sources, {dead_count} dead')

    # Save pipeline run record
    save_pipeline_run({
        'cycle': cycle,
        'started_at': run_start,
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'articles_collected': total_collected,
        'indicators_matched': total_matches,
        'reports_generated': 0,
        'status': 'complete',
        'errors': json.dumps(errors),
        'tokens_used': 0
    })

    print()
    print('=' * 60)
    print(f'Done')
    print(f'  Articles fetched:  {total_collected}')
    print(f'  Articles saved:    {total_saved}')
    # Source health summary (LENS-006: now populated correctly)
    dead_sources = [sid for sid, cnt in source_fetch_counts.items() if cnt == 0]
    if dead_sources:
        print(f'  Dead sources: {len(dead_sources)} -> {dead_sources}')
    else:
        print(f'  Source health: all {len(source_fetch_counts)} sources alive')
    print(f'  Duplicate skipped: {total_collected - total_saved}')
    print(f'  Indicator matches: {total_matches}')
    print(f'  End: {datetime.now(timezone.utc).isoformat()}')
    print('=' * 60)

if __name__ == '__main__':
    main()
