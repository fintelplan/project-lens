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
def fetch_feed(source: dict) -> list:
    articles = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; ProjectLens/1.0; +https://github.com/fintelplan/project-lens)'
        }
        response = requests.get(source['rss_url'], headers=headers, timeout=15)
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

            articles.append({
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
                })
            })

        print(f'  OK {source["name"]}: {len(articles)} articles')

    except Exception as e:
        print(f'  ERROR {source["name"]}: {str(e)[:60]}')

    return articles

# ── Match indicators ──────────────────────────────────────────
def match_indicators(article: dict, indicators: list) -> list:
    matches = []
    text = (article.get('title', '') + ' ' + article.get('content', '')).lower()

    for indicator in indicators:
        # Simple keyword matching for Phase 1
        # Phase 2 will use ML classifier (LR-025(M))
        keywords = indicator.get('what_to_watch', '').lower()
        actor = indicator.get('actor', '').lower()
        name = indicator.get('name', '').lower()

        # Check if article domain matches indicator domain
        if article.get('domain') != indicator.get('domain'):
            continue

        # Simple signal: actor name appears in text
        if actor and any(a.strip() in text for a in actor.split('-')):
            matches.append({
                'indicator_id': indicator['id'],
                'domain': indicator['domain'],
                'tier': indicator['tier'],
                'confidence': 0.5,  # placeholder — ML will improve this
                'detected_by': 'rule',
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
    if not matches or not article_id:
        return

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates',
    }

    for match in matches:
        match['article_id'] = article_id
        try:
            requests.post(
                f'{SUPABASE_URL}/rest/v1/lens_indicator_matches',
                headers=headers,
                json=match,
                timeout=10
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

# ── Main ──────────────────────────────────────────────────────
def main():
    cycle_hour = datetime.now(timezone.utc).hour
    if cycle_hour < 10:
        cycle = 'morning'
    elif cycle_hour < 13:
        cycle = 'midday'
    elif cycle_hour < 17:
        cycle = 'afternoon'
    else:
        cycle = 'evening'

    print('=' * 60)
    print(f'Project Lens — fetch_text.py')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')
    print(f'Cycle: {cycle}')
    print('=' * 60)

    sources = load_sources()
    indicators = load_indicators()
    print(f'Sources loaded: {len(sources)}')
    print(f'Indicators loaded: {len(indicators)}')
    print()

    total_collected = 0
    total_saved = 0
    total_matches = 0
    errors = []

    run_start = datetime.now(timezone.utc).isoformat()

    for source in sources:
        print(f'Fetching: {source["name"]}...')
        articles = fetch_feed(source)

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

        time.sleep(1)  # polite delay between sources

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
    print(f'  Duplicate skipped: {total_collected - total_saved}')
    print(f'  Indicator matches: {total_matches}')
    print(f'  End: {datetime.now(timezone.utc).isoformat()}')
    print('=' * 60)

if __name__ == '__main__':
    main()
