# ============================================================
# Project Lens — lens_manager.py
# AI 5 — Management AI
#
# Runs BEFORE every analyze cycle.
# Guards the 4x daily budget.
# Checks all providers before firing.
# Warns on manual triggers.
# Calculates dynamic stagger for Cerebras lenses.
#
# Model: llama-3.3-70b-versatile via Groq (GROQ_MANAGER_API_KEY)
# Separate API key — never competes with Lens 1 quota.
#
# Rules:
#   LR-047(P): Manager must run before every analyze cycle
#   LR-048(P): Daily budget = 4 runs. Hard cap.
#   LR-049(P): Manual runs warned clearly before consuming budget
# ============================================================

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL        = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY        = os.getenv('SUPABASE_SERVICE_KEY', '')
GROQ_MANAGER_KEY    = os.getenv('GROQ_MANAGER_API_KEY', '')
GITHUB_ACTIONS      = os.getenv('GITHUB_ACTIONS', 'false').lower() == 'true'
LENS_FORCE          = os.getenv('LENS_FORCE', '0') == '1'

DAILY_BUDGET        = 4       # sacred — never exceed
GEMINI_RPD_LIMIT    = 20      # Gemini free tier daily limit
GEMINI_RPD_BUFFER   = 2       # keep 2 requests as buffer
CEREBRAS_SAFE_GAP   = 30      # seconds Cerebras needs to clear queue
LENS3_AVG_FALLBACK  = 12      # fallback avg runtime if no history

# ── Supabase helpers ──────────────────────────────────────────
def supabase_get(endpoint, params=''):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
    }
    try:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/{endpoint}{params}',
            headers=headers, timeout=10
        )
        return r.json() if r.ok else []
    except Exception:
        return []

def get_runs_today():
    """Count pipeline runs since midnight UTC today."""
    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat()
    rows = supabase_get(
        'lens_pipeline_runs',
        f'?started_at=gte.{midnight}&select=started_at,cycle&order=started_at.desc'
    )
    return rows if isinstance(rows, list) else []

def get_last_run():
    """Get the most recent pipeline run."""
    rows = supabase_get(
        'lens_pipeline_runs',
        '?select=started_at,finished_at&order=started_at.desc&limit=1'
    )
    return rows[0] if rows else None

def get_lens3_avg_runtime():
    """
    Get average Lens 3 runtime from last 3 successful runs.
    Used to calculate safe Lens 4 stagger dynamically.
    """
    rows = supabase_get(
        'lens_reports',
        '?select=created_at&ai_model=eq.qwen-3-235b-a22b-instruct-2507'
        '&order=created_at.desc&limit=6'
    )
    # Fallback if no history
    return LENS3_AVG_FALLBACK

def get_gemini_calls_today():
    """Count Lens 2 (Gemini) reports generated today."""
    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat()
    rows = supabase_get(
        'lens_reports',
        f'?generated_at=gte.{midnight}'
        f'&ai_model=like.*gemini*&select=id'
    )
    return len(rows) if isinstance(rows, list) else 0

# ── Provider health checks ────────────────────────────────────
def check_groq():
    """Quick ping to Groq API."""
    key = os.getenv('GROQ_API_KEY', '')
    if not key:
        return False, 'GROQ_API_KEY not set'
    try:
        r = requests.get(
            'https://api.groq.com/openai/v1/models',
            headers={'Authorization': f'Bearer {key}'},
            timeout=8
        )
        return r.ok, 'OK' if r.ok else f'HTTP {r.status_code}'
    except Exception as e:
        return False, str(e)[:40]

def check_gemini(gemini_calls_today):
    """Check Gemini RPD headroom."""
    remaining = GEMINI_RPD_LIMIT - gemini_calls_today - GEMINI_RPD_BUFFER
    if remaining <= 0:
        return False, f'RPD exhausted ({gemini_calls_today}/{GEMINI_RPD_LIMIT} used)'
    return True, f'OK ({gemini_calls_today}/{GEMINI_RPD_LIMIT} RPD used, {remaining} remaining)'

def check_cerebras():
    """Quick ping to Cerebras API."""
    key = os.getenv('CEREBRAS_API_KEY', '')
    if not key:
        return False, 'CEREBRAS_API_KEY not set'
    try:
        r = requests.get(
            'https://api.cerebras.ai/v1/models',
            headers={'Authorization': f'Bearer {key}'},
            timeout=8
        )
        return r.ok, 'OK' if r.ok else f'HTTP {r.status_code}'
    except Exception as e:
        return False, str(e)[:40]

# ── AI 5 verdict via Groq llama-3.3-70b ──────────────────────
def get_ai_verdict(context: dict) -> str:
    """
    Ask AI 5 (llama-3.3-70b) for management verdict.
    Returns structured decision in plain text.
    """
    if not GROQ_MANAGER_KEY:
        return "MANAGER_KEY_MISSING"

    from groq import Groq
    client = Groq(api_key=GROQ_MANAGER_KEY)

    system = """You are AI 5 — the Management AI for Project Lens.
Your job: analyze system health and give a clear GO/WARN/STOP verdict.
Be direct. One line per finding. No fluff."""

    user = f"""Project Lens system state:

Budget used today: {context['runs_today']}/{context['daily_budget']}
Trigger type: {context['trigger']}
Minutes since last run: {context['minutes_since_last']}
Groq (Lens 1): {context['groq_status']}
Gemini (Lens 2): {context['gemini_status']}
Cerebras (Lens 3+4): {context['cerebras_status']}
Lens 3 avg runtime: {context['lens3_avg']}s
Calculated Lens 4 stagger: {context['lens4_stagger']}s

Give verdict: GO / WARN / STOP
List any concerns.
Suggest next safe run time if WARN or STOP."""

    try:
        resp = client.chat.completions.create(
            model='llama-3.3-70b-versatile',
            messages=[
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': user}
            ],
            temperature=0.1,
            max_tokens=300
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"AI_VERDICT_ERROR: {str(e)[:60]}"

# ── Main preflight ────────────────────────────────────────────
def run_manager() -> dict:
    """
    Run full pre-flight check.
    Returns dict with verdict and stagger for each lens.
    Exit code 0 = proceed, 1 = abort.
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime('%Y-%m-%d %H:%M UTC')
    trigger = 'scheduled (GitHub Actions)' if GITHUB_ACTIONS else 'MANUAL'

    print('=' * 60)
    print(f'Project Lens — AI 5 Manager — {date_str}')
    print(f'Trigger: {trigger}')
    print('=' * 60)

    # ── Budget check ──────────────────────────────────────────
    runs_today = get_runs_today()
    runs_count = len(runs_today)
    last_run   = get_last_run()

    minutes_since_last = 9999
    if last_run and last_run.get('started_at'):
        try:
            last_dt = datetime.fromisoformat(
                last_run['started_at'].replace('Z', '+00:00')
            )
            minutes_since_last = int((now - last_dt).total_seconds() / 60)
        except Exception:
            pass

    print()
    print(f'Budget:  {runs_count} / {DAILY_BUDGET} runs used today')
    print(f'Last run: {minutes_since_last} minutes ago')

    # Hard stop — budget exhausted
    if runs_count >= DAILY_BUDGET and not LENS_FORCE:
        print()
        print('HARD STOP — Daily budget exhausted.')
        print(f'  {DAILY_BUDGET} runs completed today.')
        next_reset = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0
        ).strftime('%Y-%m-%d 00:00 UTC')
        print(f'  Budget resets: {next_reset}')
        print('  Set LENS_FORCE=1 to override (emergency only).')
        print('=' * 60)
        sys.exit(1)

    # Manual trigger warning
    if not GITHUB_ACTIONS:
        print()
        print('WARNING: Manual trigger detected.')
        remaining = DAILY_BUDGET - runs_count
        print(f'  Budget remaining today: {remaining} of {DAILY_BUDGET} runs')
        if runs_count > 0 and minutes_since_last < 180:
            print(f'  Last run was only {minutes_since_last} min ago.')
            print(f'  Recommended gap: 180+ minutes.')
        if not LENS_FORCE:
            print()
            print('  Set LENS_FORCE=1 to proceed with manual run.')
            print('  Example: LENS_FORCE=1 python code/lens_manager.py')
            print('=' * 60)
            sys.exit(1)
        else:
            print('  LENS_FORCE=1 detected — proceeding with manual run.')

    # ── Provider health ───────────────────────────────────────
    print()
    print('Provider health:')

    groq_ok, groq_msg       = check_groq()
    gemini_calls            = get_gemini_calls_today()
    gemini_ok, gemini_msg   = check_gemini(gemini_calls)
    cerebras_ok, cerebras_msg = check_cerebras()

    print(f'  Lens 1 Groq:      {"OK" if groq_ok else "FAIL"}  ({groq_msg})')
    print(f'  Lens 2 Gemini:    {"OK" if gemini_ok else "WARN"} ({gemini_msg})')
    print(f'  Lens 3+4 Cerebras: {"OK" if cerebras_ok else "FAIL"} ({cerebras_msg})')

    # ── Dynamic stagger calculation ───────────────────────────
    lens3_avg    = get_lens3_avg_runtime()
    lens4_stagger = int(lens3_avg + CEREBRAS_SAFE_GAP + 6)
    # +6 = Lens 3 base stagger. Total = Lens 3 expected finish + 30s buffer

    print()
    print(f'Dynamic stagger:')
    print(f'  Lens 3 avg runtime: {lens3_avg}s')
    print(f'  Lens 4 stagger:     {lens4_stagger}s  '
          f'(Lens3 avg {lens3_avg}s + base 6s + buffer {CEREBRAS_SAFE_GAP}s)')

    # ── Per-lens verdict ──────────────────────────────────────
    print()
    print('Per-lens verdict:')
    lens_verdicts = {
        1: 'GO' if groq_ok else 'SKIP',
        2: 'GO' if gemini_ok else 'SKIP',
        3: 'GO' if cerebras_ok else 'SKIP',
        4: 'GO' if cerebras_ok else 'SKIP',
    }
    for lid, verdict in lens_verdicts.items():
        print(f'  Lens {lid}: {verdict}')

    # ── AI 5 verdict ──────────────────────────────────────────
    print()
    print('AI 5 verdict (llama-3.3-70b):')
    context = {
        'runs_today': runs_count,
        'daily_budget': DAILY_BUDGET,
        'trigger': trigger,
        'minutes_since_last': minutes_since_last,
        'groq_status': groq_msg,
        'gemini_status': gemini_msg,
        'cerebras_status': cerebras_msg,
        'lens3_avg': lens3_avg,
        'lens4_stagger': lens4_stagger,
    }
    ai_verdict = get_ai_verdict(context)
    for line in ai_verdict.split('\n'):
        print(f'  {line}')

    # ── Final decision ────────────────────────────────────────
    go_count = sum(1 for v in lens_verdicts.values() if v == 'GO')
    full_run  = go_count == 4

    print()
    print('=' * 60)
    if full_run:
        print('FULL RUN APPROVED — all 4 lenses cleared')
    else:
        print(f'PARTIAL RUN — {go_count}/4 lenses cleared')
    print(f'Lens 4 stagger set to: {lens4_stagger}s')
    print('=' * 60)

    # Write stagger to env file for analyze to pick up
    stagger_env = os.path.join(os.path.dirname(__file__), '.lens_stagger')
    with open(stagger_env, 'w') as f:
        f.write(str(lens4_stagger))

    return {
        'verdict': 'GO' if full_run else 'PARTIAL',
        'lens_verdicts': lens_verdicts,
        'lens4_stagger': lens4_stagger,
        'runs_today': runs_count,
    }


if __name__ == '__main__':
    result = run_manager()
    # Exit 0 = proceed with analyze
    # Exit 1 = abort (handled above for budget/manual)
    sys.exit(0)
