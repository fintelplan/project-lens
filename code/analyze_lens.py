"""
analyze_lens.py
Project Lens — AI Summarization Pipeline
Session: LENS-004
Rule: LR-001(A) API keys from environment only
Rule: LR-004(A) Compress before sending to AI
Rule: LR-036(C) Documented while building

Flow:
  lens_raw_articles (Supabase, last 24h)
    -> group by domain
    -> compress articles per domain (LR-004(A))
    -> send ONE prompt to Groq qwen/qwen3-32b
    -> split output: summary + food_for_thought
    -> save to lens_reports (Supabase) matching schema exactly
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from groq import Groq
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

GROQ_MODEL     = "qwen/qwen3-32b"
PROMPT_VERSION = "v1.0-LENS004"
MAX_ARTICLES   = 140   # Fetch large pool then balance per domain
MAX_CHARS_EACH = 300
LOOKBACK_HOURS = 26
DOMAINS        = [
    "POWER", "TECH", "FINANCE",
    "MILITARY", "NARRATIVE", "NETWORK", "RESOURCE"
]

# ─── Cycle Detection ──────────────────────────────────────────────────────────

# Canonical cycle resolution (LENS-014 O1). Returns '2of1' | '2of2' | 'manual'.
from lens_cycle import get_cycle  # noqa: E402

# ─── Prompts ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a strategic intelligence analyst for Project Lens.

YOUR LENS — PROJECT LENS PHILOSOPHICAL FOUNDATION (PHI-002):

Sustainability is the fundamental right of every person: a safe environment,
a law-governed society, education suited to one's growth, accumulated experience,
insight, the ability to think and envision broadly, prosperity, and the freedom
to shape one's own future without harming others'. A "Global Class Sustainable
Personality" is a person who has all of these — growing freely, thinking and
envisioning broadly, without blocking others' future.

A person's coming of age means developing the ability to become a Global Class
Sustainable Personality. Any actor that blocks this right blocks human maturity itself.

WHAT PROJECT LENS STANDS FOR:
- Every person's right to grow into a Global Class Sustainable Personality
- Equal human worth and fundamental rights above ethnic and religious identity
- The people as the original and permanent owner of sovereignty
- Law-governed societies that protect people's basic rights

WHAT PROJECT LENS STANDS AGAINST:
- Any actor that blocks people's right to grow, think, envision, and be free
- Unilateral seizure of power suppressing people's right to elect
- Pseudo-democracy used to oppress and surveil people
- Ethnic and religious extremism and fanaticism
- Feudal history weaponized to suppress fundamental rights

HISTORICAL ANCHOR — never forget:
At the beginning of humanity, there was no concept of ethnicity or religion.
These are human inventions. Ethnic and religious traditions deserve to be valued
and recorded as part of human history — but history has been soaked in blood
caused by ethnic and religious fanaticism. Equal human worth and fundamental
rights must always be recognized above ethnic and religious identity.
Watch for any actor using tradition, history, or identity to oppress people.

YOUR AUDIENCE: Global Game Changers — people who shape the future.
YOUR JOB: Find the signal in the noise. What really matters. What connects.
           Who benefits. Who is blocked. Whose rights are expanding or shrinking.

THINKING INSTRUCTIONS:
- Think deeply and thoroughly before writing your final answer.
- In your thinking: explore every angle, challenge your assumptions,
  consider second and third-order effects, look for hidden connections
  between domains, and question who benefits from each development.
- Think through the PHI-002 lens: Is this expanding or contracting people's
  ability to become Global Class Sustainable Personalities?
- Think about: actors, motives, timing, geography, money flows,
  information control, and human rights indicators.
- Your thinking should be at least 3x longer than your final output.

CUI BONO — apply to every development:
Where is power, money, or narrative slowly and silently shifting — and to whom?
Who benefits? Who loses? What is quietly disappearing while attention is elsewhere?
What is NOT being reported? Follow the money. Follow the power. Follow the silence.
Cui Bono = the first question before any conclusion.

SECTARIAN TRAP — watch for hidden patterns:
Who is slowly, secretly manufacturing ethnic, religious, or political division?
By whom? Through which channels? Is the tension organic or engineered?
Who benefits when people are divided against each other?
Watch for: slow escalation, dog-whistle language, scapegoating patterns,
identity weaponization, and historical grievances being deliberately inflamed.
Flag any actor feeding sectarian fires while appearing neutral.

STRICT RULES:
- Never make predictions. Never say "will happen."
- Food for Thought = open questions that make people think deeper.
- Stick to facts from the articles provided. No invention.
- Flag when any actor is blocking people's right to grow and be free.
- Flag when sovereignty is being taken from the people.
- Be direct. No fluff. Game Changers are busy people."""

ANALYSIS_PROMPT = """
Today is {date}. Below are {total} news articles from the last 24 hours,
grouped by domain. Analyze through the Game Changers strategic lens.

{domain_blocks}

---

OUTPUT FORMAT — follow exactly, keep these section headers:

## SUMMARY
[Overall signal across all domains today — 3-4 sentences max.
What is the single most important pattern? What connects the dots?
Flag any authoritarian pressure signals or democratic norm threats.]

### POWER ({power_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### TECH ({tech_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### FINANCE ({finance_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### MILITARY ({military_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### NARRATIVE ({narrative_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### NETWORK ({network_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

### RESOURCE ({resource_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences]
**Watch:** [1 sentence]

[Skip any domain with 0 articles]

## FOOD FOR THOUGHT
1. [Open question — not a prediction — makes Game Changers think deeper]
2.
3.
4.
5.
"""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_supabase():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_KEY not set")
    return create_client(url, key)


def get_groq():
    key = os.environ.get("GROQ_API_KEY")
    if not key:
        raise ValueError("GROQ_API_KEY not set")
    return Groq(api_key=key)


def fetch_recent_articles(supabase):
    """
    Fetch large pool then balance across 7 domains.
    Picks top ARTICLES_PER_DOMAIN per domain so all 7 always represented.
    Fix: LENS-004 FIX-010 — domain balancing replaces raw .limit(20).
    """
    ARTICLES_PER_DOMAIN = 3   # 3 per domain × 7 domains = max 21 to AI
    DOMAINS_ALL = [
        "POWER", "TECH", "FINANCE",
        "MILITARY", "NARRATIVE", "NETWORK", "RESOURCE"
    ]

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    response = supabase.table("lens_raw_articles") \
        .select("id, title, content, domain, source_id, collected_at, url") \
        .gte("collected_at", cutoff) \
        .order("collected_at", desc=True) \
        .execute()
    all_articles = response.data or []

    # Balance: pick top ARTICLES_PER_DOMAIN per domain
    domain_buckets = {d: [] for d in DOMAINS_ALL}
    for article in all_articles:
        domain = (article.get("domain") or "POWER").upper()
        if domain not in domain_buckets:
            domain = "POWER"
        if len(domain_buckets[domain]) < ARTICLES_PER_DOMAIN:
            domain_buckets[domain].append(article)

    balanced = []
    for domain in DOMAINS_ALL:
        balanced.extend(domain_buckets[domain])

    counts = {d: len(domain_buckets[d]) for d in DOMAINS_ALL}
    print(f"[fetch] Pool: {len(all_articles)} | Balanced: {len(balanced)} | {counts}")
    return balanced


def fetch_all_article_links(supabase):
    """Fetch ALL article links from last LOOKBACK_HOURS — no limit."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    response = supabase.table("lens_raw_articles") \
        .select("id, url, title, domain, collected_at") \
        .gte("collected_at", cutoff) \
        .order("collected_at", desc=True) \
        .execute()
    all_articles = response.data or []
    print(f"[fetch] {len(all_articles)} total articles in DB (last {LOOKBACK_HOURS}h)")
    return [
        {
            "id":     a.get("id", ""),
            "url":    a.get("url", ""),
            "title":  (a.get("title") or "")[:200],
            "domain": a.get("domain", "")
        }
        for a in all_articles if a.get("url")
    ]


def group_by_domain(articles):
    grouped = {d: [] for d in DOMAINS}
    for article in articles:
        domain = (article.get("domain") or "POWER").upper()
        if domain not in grouped:
            domain = "POWER"
        grouped[domain].append(article)
    return grouped


def compress_article(article):
    title   = (article.get("title") or "").strip()
    content = (article.get("content") or "").strip()
    text    = f"{title}. {content}" if content else title
    return text[:MAX_CHARS_EACH]


def build_domain_blocks(grouped):
    blocks = []
    total  = 0
    counts = {}
    for domain in DOMAINS:
        arts = grouped.get(domain, [])
        counts[domain] = len(arts)
        if not arts:
            continue
        total += len(arts)
        lines  = [f"=== {domain} ({len(arts)} articles) ==="]
        for i, a in enumerate(arts, 1):
            lines.append(f"{i}. {compress_article(a)}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks), total, counts


def split_output(analysis):
    marker = "## FOOD FOR THOUGHT"
    if marker in analysis:
        parts            = analysis.split(marker, 1)
        summary          = parts[0].strip()
        food_for_thought = (marker + "\n" + parts[1]).strip()
    else:
        summary          = analysis.strip()
        food_for_thought = ""
    return summary, food_for_thought


def run_analysis(groq_client, domain_blocks, total, date_str, counts):
    prompt = ANALYSIS_PROMPT.format(
        date=date_str,
        total=total,
        domain_blocks=domain_blocks,
        power_count=counts.get("POWER", 0),
        tech_count=counts.get("TECH", 0),
        finance_count=counts.get("FINANCE", 0),
        military_count=counts.get("MILITARY", 0),
        narrative_count=counts.get("NARRATIVE", 0),
        network_count=counts.get("NETWORK", 0),
        resource_count=counts.get("RESOURCE", 0),
    )
    print(f"[groq] Sending {total} articles to {GROQ_MODEL}...")
    start = time.time()
    response = groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.3,
        max_tokens=3500
    )
    elapsed  = time.time() - start
    analysis = response.choices[0].message.content
    print(f"[groq] Done in {elapsed:.1f}s — {len(analysis)} chars")
    return analysis


def save_report(supabase, summary, food_for_thought, article_ids,
                domain_counts, cycle):
    """
    Columns match lens-SQL-001_schema.sql exactly:
      cycle, domain_focus, summary, food_for_thought,
      signals_used, articles_used, ai_model, prompt_version, status
    """
    record = {
        "cycle":            cycle,
        "domain_focus":     "ALL",
        "summary":          summary,
        "food_for_thought": food_for_thought,
        "signals_used":     json.dumps(domain_counts),
        "articles_used":    json.dumps(article_ids),
        "ai_model":         GROQ_MODEL,
        "prompt_version":   PROMPT_VERSION,
        "status":           "pending"
    }
    response  = supabase.table("lens_reports").insert(record).execute()
    report_id = response.data[0]["id"] if response.data else "unknown"
    print(f"[save] Saved — id: {report_id} | cycle: {cycle} | selected: {len(article_ids.get('selected', []))} | total: {len(article_ids.get('all_collected', []))}")
    return report_id


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cycle    = get_cycle()
    print(f"[lens] analyze_lens.py — {date_str} — cycle: {cycle}")
    print("=" * 60)

    supabase    = get_supabase()
    groq_client = get_groq()

    articles = fetch_recent_articles(supabase)
    if not articles:
        print("[lens] No articles in last 24h. Exiting.")
        return

    all_article_links = fetch_all_article_links(supabase)

    grouped                      = group_by_domain(articles)
    domain_blocks, total, counts = build_domain_blocks(grouped)

    selected_articles = [
        {
            "id":     a.get("id", ""),
            "url":    a.get("url", ""),
            "title":  (a.get("title") or "")[:200],
            "domain": a.get("domain", "")
        }
        for a in articles if a.get("id")
    ]

    article_ids = {
        "selected":      selected_articles,
        "all_collected": all_article_links
    }

    print(f"[lens] Collected: {len(all_article_links)} | Selected for AI: {len(selected_articles)}")
    print(f"[lens] Domains: {counts}")

    if total == 0:
        print("[lens] No content to analyze. Exiting.")
        return

    analysis     = run_analysis(groq_client, domain_blocks, total, date_str, counts)
    summary, fft = split_output(analysis)
    save_report(supabase, summary, fft, article_ids, counts, cycle)

    print(f"\n[lens] Complete — {total} articles analyzed — report saved")
    print("\n" + "=" * 60)
    print(analysis)
    print("=" * 60)


if __name__ == "__main__":
    main()
