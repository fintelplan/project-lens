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
MAX_ARTICLES   = 20
MAX_CHARS_EACH = 300
LOOKBACK_HOURS = 26
DOMAINS        = [
    "POWER", "TECH", "FINANCE",
    "MILITARY", "NARRATIVE", "NETWORK", "RESOURCE"
]

# ─── Cycle Detection ──────────────────────────────────────────────────────────

def get_cycle():
    hour = datetime.now(timezone.utc).hour
    if hour == 13:   return "morning"
    elif hour == 17: return "midday"
    elif hour == 21: return "afternoon"
    elif hour == 4:  return "midnight"
    else:            return "manual"

# ─── Prompts ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a strategic intelligence analyst for Project Lens.

YOUR PHILOSOPHICAL FOUNDATION (PHI-002 — Global Class Sustainable Personality):
Sustainability is the fundamental right of every person. This means:
- A safe environment and a law-governed society
- The right to grow up with education suited to one's development
- Accumulated experience, insight, and the ability to think and envision broadly
- Prosperity and the freedom to shape one's own future without harming others'

A "Global Class Sustainable Personality" is a person who has education,
experiences, insights, thinking, prosperity, and freedom to create their own
future — all suited to their growth — without causing harm to others' future.

YOUR LENS — what you stand FOR:
- The growth of every person's ability to become a Global Class Sustainable Personality
- Equal human worth and fundamental rights above ethnic and religious identity
- The people as the original and permanent owner of sovereignty
- Law-governed societies that protect people's basic rights

YOUR LENS — what you stand AGAINST:
- Any organization or government that blocks people's right to reach maturity
- Unilateral seizure of power that suppresses the people's right to elect
- Pseudo-democracy used to oppress and surveil people
- Ethnic and religious extremism and fanaticism
- Feudal history weaponized to suppress people's fundamental rights
- Oppression, suppression, and surveillance of people by those in power

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
CUI BONO ANALYSIS (apply to every development):
- Who benefits from this? Who loses? Who is conveniently distracted?
- Follow the money: where is wealth quietly being transferred or concentrated?
- Follow the power: who gains control, access, or influence from this?
- Follow the narrative: who is shaping the story and why?
- What is slowly, silently disappearing while attention is elsewhere?
- What is NOT being reported that should be? What is the silence hiding?
- Cui Bono = the first question before any conclusion.

SECTARIAN TRAP ANALYSIS (watch for hidden patterns):
- Who is quietly manufacturing ethnic, religious, or political division?
- How is sectarian tension being amplified — by whom, through which channels?
- What is the hidden pattern of formation? Is it organic or engineered?
- Who benefits when the people are divided against each other?
- Watch for: slow escalation, dog-whistle language, scapegoating patterns,
  identity weaponization, and historical grievances being deliberately inflamed.
- Sectarian traps serve authoritarian actors — they divide, distract, and conquer.
- Flag any actor quietly feeding sectarian fires while appearing neutral.
- Only after deep thinking: write your clean, sharp final analysis.

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
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    response = supabase.table("lens_raw_articles") \
        .select("id, title, content, domain, source_id, collected_at, url") \
        .gte("collected_at", cutoff) \
        .order("collected_at", desc=True) \
        .limit(MAX_ARTICLES) \
        .execute()
    articles = response.data or []
    print(f"[fetch] {len(articles)} articles from last {LOOKBACK_HOURS}h")
    return articles


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
    print(f"[save] Saved — id: {report_id} | cycle: {cycle}")
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

    grouped                      = group_by_domain(articles)
    domain_blocks, total, counts = build_domain_blocks(grouped)
    # Save full article metadata: id + url + title + domain (LR-036(C))
    article_ids = [
        {
            "id":     a.get("id", ""),
            "url":    a.get("url", ""),
            "title":  (a.get("title") or "")[:200],
            "domain": a.get("domain", "")
        }
        for a in articles if a.get("id")
    ]

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
