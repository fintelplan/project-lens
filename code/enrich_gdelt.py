# ============================================================
# Project Lens — enrich_gdelt.py
# LENS-006 Priority 3: GDELT Enrichment Layer
#
# Queries GDELT API for global coverage counts per domain
# Saves to lens_gdelt_enrichment table in Supabase
# Runs via separate GitHub Actions cron — NEVER blocks main pipeline
#
# Ethics: GDELT is fully public data (LR-014(E))
# Schedule: After analyze runs — 13:45/17:45/21:45/04:45 UTC
# ============================================================

import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')
GDELT_API    = 'https://api.gdeltproject.org/api/v2/doc/doc'

# ── Domain query map ─────────────────────────────────────────
# Each domain gets a focused keyword query for GDELT
DOMAIN_QUERIES = {
    'POWER': {
        'query': 'geopolitics OR "foreign policy" OR sanctions OR diplomacy',
        'label': 'Geopolitical Signals'
    },
    'TECH': {
        'query': '"artificial intelligence" OR semiconductor OR "export controls" OR surveillance',
        'label': 'Technology Signals'
    },
    'FINANCE': {
        'query': '"sovereign debt" OR IMF OR "World Bank" OR "debt crisis" OR "central bank"',
        'label': 'Financial Signals'
    },
    'MILITARY': {
        'query': 'military OR "arms deal" OR conflict OR "defense spending" OR warship',
        'label': 'Military Signals'
    },
    'NARRATIVE': {
        'query': 'disinformation OR propaganda OR "influence operation" OR censorship',
        'label': 'Narrative Signals'
    },
    'NETWORK': {
        'query': '"money laundering" OR "dark money" OR "shell company" OR corruption OR oligarch',
        'label': 'Network Signals'
    },
    'RESOURCE': {
        'query': '"energy security" OR "critical minerals" OR "rare earth" OR "food security"',
        'label': 'Resource Signals'
    },
}

# ── Query GDELT ───────────────────────────────────────────────
def query_gdelt(domain: str, query_info: dict, timespan: str = '24h') -> dict:
    """
    Query GDELT DocSearch API for article volume and tone.
    Returns count + average tone for the domain query.
    """
    params = {
        'query':    query_info['query'],
        'mode':     'TimelineVol',
        'maxrecords': 10,
        'timespan': timespan,
        'format':   'json',
    }

    for attempt in range(3):
        try:
            r = requests.get(GDELT_API, params=params, timeout=20)
            if r.status_code == 429:
                wait = 30 * (2 ** attempt)
                print(f'  429 rate limit — waiting {wait}s (attempt {attempt+1}/3)')
                time.sleep(wait)
                continue
            if not r.ok:
                print(f'  WARNING: GDELT {domain} returned {r.status_code}')
                return {'count': 0, 'avg_tone': None, 'top_themes': []}
            break
        except Exception as e:
            print(f'  ERROR {domain}: {str(e)[:60]}')
            return {'count': 0, 'avg_tone': None, 'top_themes': []}
    else:
        print(f'  FAILED {domain}: 3 attempts exhausted')
        return {'count': 0, 'avg_tone': None, 'top_themes': []}
    try:
        # LENS-008 FIX: detect GitHub IP block before calling .json()
        # GitHub runner IPs get silent-blocked — GDELT returns 200 with empty
        # or non-JSON body. Check both empty body AND invalid JSON explicitly.
        if not r.text.strip():
            print(f'  GDELT_BLOCKED {domain}: empty body (GitHub runner IP blocked)')
            return {'count': 0, 'avg_tone': None, 'top_themes': []}

        try:
            data = r.json()
        except (ValueError, json.JSONDecodeError):
            # Non-JSON body = GitHub IP blocked (HTML error page or garbage)
            preview = repr(r.text[:40])
            print(f'  GDELT_BLOCKED {domain}: invalid body (GitHub IP blocked) — {preview}')
            return {'count': 0, 'avg_tone': None, 'top_themes': []}

        # TimelineVol returns timeline array of {date, value} — sum for total count
        timeline = data.get('timeline', [{}])[0].get('data', [])
        count = int(sum(p.get('value', 0) for p in timeline))
        avg_tone = None   # not available in TimelineVol mode
        top_themes = []   # not available in TimelineVol mode

        print(f'  OK {domain}: {count} articles | tone: {avg_tone}')
        return {'count': count, 'avg_tone': avg_tone, 'top_themes': top_themes}

    except Exception as e:
        print(f'  ERROR {domain}: {str(e)[:60]}')
        return {'count': 0, 'avg_tone': None, 'top_themes': []}

# ── Save to Supabase ──────────────────────────────────────────
def save_enrichment(records: list):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print('  ERROR: Missing Supabase credentials')
        return

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates',
    }

    saved = 0
    for record in records:
        try:
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/lens_gdelt_enrichment',
                headers=headers,
                json=record,
                timeout=10
            )
            if r.ok:
                saved += 1
        except Exception as e:
            print(f'  ERROR saving {record["domain"]}: {str(e)[:50]}')

    print(f'  Saved: {saved}/{len(records)} records to Supabase')

# ── Main ──────────────────────────────────────────────────────
def main():
    print('=' * 60)
    print('Project Lens — enrich_gdelt.py')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')
    print('=' * 60)

    records = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for domain, query_info in DOMAIN_QUERIES.items():
        print(f'Querying GDELT: {domain}...')
        result = query_gdelt(domain, query_info, timespan='24h')

        records.append({
            'fetched_at':    fetched_at,
            'domain':        domain,
            'query_term':    query_info['query'],
            'article_count': result['count'],
            'avg_tone':      result['avg_tone'],
            'top_themes':    json.dumps(result['top_themes']),
            'timespan':      '24h',
        })

        time.sleep(3)   # LENS-008: reduced 10s->3s (saves 49s/run, still polite)

    print()
    save_enrichment(records)

    print()
    print('=' * 60)
    print('GDELT Enrichment Summary')
    print('-' * 60)
    for r in sorted(records, key=lambda x: x['article_count'], reverse=True):
        tone_str = f"tone: {r['avg_tone']:.1f}" if r['avg_tone'] is not None else 'tone: N/A'
        print(f"  {r['domain']:<12} {r['article_count']:>4} articles | {tone_str}")
    print('=' * 60)
    print(f'End: {datetime.now(timezone.utc).isoformat()}')
    print('=' * 60)

if __name__ == '__main__':
    main()
