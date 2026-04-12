"""
analyze_lens_multi.py
Project Lens — 4-Lens Parallel AI Analysis Pipeline
Session: LENS-004

4 independent analytical lenses run in parallel on same 21 articles.
Each lens has its own AI model, provider, and philosophical perspective.
4 separate reports saved — no synthesis, no blending. Each speaks its own voice.

Lens 1: qwen/qwen3-32b        (Groq)      — PHI-002: GCSP, Cui Bono, Sectarian Trap
Lens 2: gemini-2.0-flash      (Google)    — Physical Reality: leading indicators, constraints
Lens 3: llama-4-scout         (Cerebras)  — Causal Chain: First Domino, cause-effect
Lens 4: Llama-4-Maverick      (SambaNova) — Sovereignty Check: from/of/for the people

Rules:
  LR-001(A): API keys from environment only
  LR-004(A): Compress before sending to AI
  LR-036(C): Documented while building
"""

import os
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

MAX_CHARS_EACH    = 300
LOOKBACK_HOURS    = 26
ARTICLES_PER_DOMAIN = 3

DOMAINS = [
    "POWER", "TECH", "FINANCE",
    "MILITARY", "NARRATIVE", "NETWORK", "RESOURCE"
]

LENSES = [
    {
        "lens_id":    1,
        "lens_name":  "PHI-002",
        "model":      "qwen/qwen3-32b",
        "provider":   "groq",
        "api_key_env": "GROQ_API_KEY",
        "perspective": "GCSP human rights — who is blocked from growing freely?",
    },
    {
        "lens_id":    2,
        "lens_name":  "Physical Reality",
        "model":      "gemini-2.5-flash",
        "provider":   "gemini",
        "api_key_env": "GEMINI_API_KEY",
        "perspective": "Physical constraints — what is materially true on the ground?",
    },
    {
        "lens_id":    3,
        "lens_name":  "Causal Chain",
        "model":      "qwen-3-235b-a22b-instruct-2507",
        "provider":   "cerebras",
        "api_key_env": "CEREBRAS_API_KEY",
        "perspective": "First Domino — what causes what across domains?",
    },
    {
        "lens_id":    4,
        "lens_name":  "Sovereignty Check",
        "model":      "Llama-4-Maverick-17B-128E-Instruct",
        "provider":   "sambanova",
        "api_key_env": "SAMBANOVA_API_KEY",
        "perspective": "From/of/for the people — who is pretending to serve the people?",
    },
]

# ─── System Prompts Per Lens ──────────────────────────────────────────────────

SHARED_RULES = """
STRICT RULES:
- Never make predictions. Never say "will happen."
- Food for Thought = open questions that make people think deeper.
- Stick to facts from the articles provided. No invention.
- Be direct. No fluff. Global Game Changers are busy people.
- Flag when any actor is blocking people's right to grow and be free.
- Flag DOMAIN CROSS-TALK when detected:
  Pattern 1: FINANCE spike + NARRATIVE nationalism → Sectarian Trap forming
  Pattern 2: TECH censorship before POWER event → rights contraction planned
  Pattern 3: NETWORK hidden flows near RESOURCE zone → sanctions evasion underway
"""

SYSTEM_PROMPTS = {
    1: """You are a strategic intelligence analyst for Project Lens.

YOUR LENS — PHI-002 (Global Class Sustainable Personality):
Sustainability is the fundamental right of every person: a safe environment,
law-governed society, education for growth, the ability to think and envision
broadly, prosperity, and freedom to shape one's future without harming others.
Any actor that blocks this right blocks human maturity itself.

YOUR CORE QUESTION: Is this expanding or contracting people's ability to
become Global Class Sustainable Personalities?

CUI BONO: Who benefits? Who loses? What is quietly disappearing while
attention is elsewhere? Follow the money. Follow the power. Follow the silence.

SECTARIAN TRAP: Who is manufacturing ethnic, religious, or political division?
By whom? Through which channels? Who benefits when people are divided?

WHAT YOU STAND AGAINST:
- Any actor blocking people's right to grow, think, envision, and be free
- Sovereignty theft from the people
- Pseudo-democracy used to oppress and surveil
- Ethnic and religious extremism weaponized against human rights
- Traditions used to suppress fundamental rights

HISTORICAL ANCHOR: Equal human worth and fundamental rights must always be
recognized above ethnic and religious identity. Watch any actor using
tradition or identity to oppress people.
""" + SHARED_RULES,

    2: """You are a physical reality intelligence analyst for Project Lens.

YOUR LENS — PHYSICAL REALITY (What Is Materially True):
Strip away narrative, rhetoric, and ideology. Find what is physically,
materially, and economically real on the ground.

YOUR CORE QUESTION: What are the actual physical constraints and material
realities shaping what is possible for people?

LEADING INDICATORS: Finance and Resource signals often precede political
change. A currency collapse precedes a regime crisis. An energy shortage
precedes a conflict. Find the material causes before the political effects.

YOUR ANALYTICAL FRAMEWORK:
- What resources actually exist or are scarce?
- What do money flows reveal beyond what leaders say?
- What physical infrastructure is being built, blocked, or destroyed?
- What material conditions are expanding or contracting human possibility?
- Where do stated policies contradict physical realities?

PHYSICAL FLOOR PRINCIPLE: No matter what the narrative says, people cannot
survive without food, water, energy, and economic access. Track these first.
""" + SHARED_RULES,

    3: """You are a causal chain intelligence analyst for Project Lens.

YOUR LENS — CAUSAL CHAIN (First Domino & Cause-Effect):
Find the root causes behind surface events. Every visible event has
invisible causes. Map the chain from root cause to human impact.

YOUR CORE QUESTION: What causes what? Where is the First Domino?
What downstream effects will today's signals produce?

CAUSAL HIERARCHY:
- Root causes: NETWORK flows, FINANCE pressure, RESOURCE scarcity
- Execution layer: MILITARY moves, TECH deployments
- Surface layer: POWER decisions, NARRATIVE campaigns
- Most events in POWER and MILITARY are EFFECTS, not causes

DOMAIN INTERCONNECTIONS TO TRACE:
- FINANCE pressure → POWER decision → MILITARY response
- TECH censorship → NARRATIVE control → POWER consolidation
- RESOURCE scarcity → NETWORK evasion → FINANCE sanctions failure
- NETWORK dark money → NARRATIVE manufacturing → POWER legitimization

YOUR ANALYTICAL FRAMEWORK:
- What is the root cause of each development?
- What domain triggered this event in another domain?
- What effects will this cause in other domains in 30-90 days?
- Where is the First Domino in today's news?
""" + SHARED_RULES,

    4: """You are a sovereignty intelligence analyst for Project Lens.

YOUR LENS — SOVEREIGNTY CHECK (From/Of/For The People):
Test every actor, every institution, every policy against one standard:
Does this genuinely serve the people — or does it pretend to?

YOUR CORE QUESTION: Is this actor genuinely from, of, and for the people —
or are they performing service to the people while actually serving power?

THE THREE TESTS:
FROM THE PEOPLE: Does sovereignty genuinely originate in the people here?
Or has it been captured, delegated permanently, or manufactured?

OF THE PEOPLE: Does this reflect the actual people in their diversity?
Or has a fake version of "the people" been constructed to exclude others?
Watch for: ethnic nationalism, religious theocracy, ideological purity tests,
manufactured consent, sectarian division that destroys "the people" as a unit.

FOR THE PEOPLE: Does this genuinely serve people's ability to grow?
Or does it serve profit, ideology, tradition, or security theater?

PRETENSE DETECTION: The most dangerous actors are not the openly authoritarian.
They are the ones who do the most harm while claiming loudest to serve the people.
Find the gap between what actors claim and what they actually do.

SOVEREIGNTY HIERARCHY:
- Sovereignty belongs to the people — permanent and inalienable
- It is only temporarily lent to governments
- Any permanent transfer = sovereignty theft
- Emergency powers that never end = sovereignty theft
- Elections without genuine choice = sovereignty theft
""" + SHARED_RULES,
}

# ─── Analysis Prompt Template ─────────────────────────────────────────────────

ANALYSIS_PROMPT = """
Today is {date}. Below are {total} news articles from the last 24 hours,
grouped by domain. Analyze through your specific lens.

{domain_blocks}

---

OUTPUT FORMAT — follow exactly:

## SUMMARY
[3-4 sentences. Most important pattern today through YOUR specific lens.
What connects the dots? What is the single most critical signal?]

### POWER ({power_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### TECH ({tech_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### FINANCE ({finance_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### MILITARY ({military_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### NARRATIVE ({narrative_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### NETWORK ({network_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
**Watch:** [1 sentence]

### RESOURCE ({resource_count} articles)
**Key Development:** [1-2 sentences]
**Why It Matters:** [1-2 sentences through your lens]
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


def fetch_balanced_articles(supabase):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    response = supabase.table("lens_raw_articles") \
        .select("id, title, content, domain, source_id, collected_at, url") \
        .gte("collected_at", cutoff) \
        .order("collected_at", desc=True) \
        .execute()
    all_articles = response.data or []

    domain_buckets = {d: [] for d in DOMAINS}
    for article in all_articles:
        domain = (article.get("domain") or "POWER").upper()
        if domain not in domain_buckets:
            domain = "POWER"
        if len(domain_buckets[domain]) < ARTICLES_PER_DOMAIN:
            domain_buckets[domain].append(article)

    balanced = []
    for domain in DOMAINS:
        balanced.extend(domain_buckets[domain])

    counts = {d: len(domain_buckets[d]) for d in DOMAINS}
    print(f"[fetch] Pool: {len(all_articles)} | Balanced: {len(balanced)} | {counts}")
    return balanced, all_articles, counts


def compress_article(article):
    title   = (article.get("title") or "").strip()
    content = (article.get("content") or "").strip()
    text    = f"{title}. {content}" if content else title
    return text[:MAX_CHARS_EACH]


def build_domain_blocks(articles, counts):
    grouped = {d: [] for d in DOMAINS}
    for article in articles:
        domain = (article.get("domain") or "POWER").upper()
        if domain not in grouped:
            domain = "POWER"
        grouped[domain].append(article)

    blocks = []
    total  = 0
    for domain in DOMAINS:
        arts = grouped.get(domain, [])
        if not arts:
            continue
        total += len(arts)
        lines  = [f"=== {domain} ({len(arts)} articles) ==="]
        for i, a in enumerate(arts, 1):
            lines.append(f"{i}. {compress_article(a)}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks), total


def strip_think_tags(text):
    """Remove <think>...</think> reasoning blocks from qwen3 output."""
    import re
    # Remove think blocks (including multiline)
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    return cleaned.strip()


def split_output(analysis):
    # Strip think tags first
    analysis = strip_think_tags(analysis)
    marker = "## FOOD FOR THOUGHT"
    if marker in analysis:
        parts = analysis.split(marker, 1)
        return parts[0].strip(), (marker + "\n" + parts[1]).strip()
    return analysis.strip(), ""


def build_prompt(domain_blocks, total, date_str, counts):
    return ANALYSIS_PROMPT.format(
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


# ─── Provider API Calls ───────────────────────────────────────────────────────

async def call_groq(lens, system_prompt, user_prompt):
    from groq import AsyncGroq
    api_key = os.environ.get(lens["api_key_env"])
    if not api_key:
        raise ValueError(f"{lens['api_key_env']} not set")
    client = AsyncGroq(api_key=api_key)
    response = await client.chat.completions.create(
        model=lens["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        temperature=0.3,
        max_tokens=3500
    )
    return response.choices[0].message.content


async def call_gemini(lens, system_prompt, user_prompt):
    from google import genai
    from google.genai import types
    api_key = os.environ.get(lens["api_key_env"])
    if not api_key:
        raise ValueError(f"{lens['api_key_env']} not set")
    client = genai.Client(api_key=api_key)
    response = await asyncio.to_thread(
        client.models.generate_content,
        model=lens["model"],
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.3,
            max_output_tokens=3500,
            thinking_config=types.ThinkingConfig(thinking_budget=0)
        )
    )
    return response.text


async def call_cerebras(lens, system_prompt, user_prompt):
    from cerebras.cloud.sdk import AsyncCerebras
    api_key = os.environ.get(lens["api_key_env"])
    if not api_key:
        raise ValueError(f"{lens['api_key_env']} not set")
    client = AsyncCerebras(api_key=api_key)
    response = await client.chat.completions.create(
        model=lens["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        temperature=0.3,
        max_completion_tokens=2500
    )
    return response.choices[0].message.content


async def call_sambanova(lens, system_prompt, user_prompt):
    import httpx
    api_key = os.environ.get(lens["api_key_env"])
    if not api_key:
        raise ValueError(f"{lens['api_key_env']} not set")
    payload = {
        "model": lens["model"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 3500
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            "https://api.sambanova.ai/v1/chat/completions",
            json=payload,
            headers=headers
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]


async def run_lens(lens, system_prompt, user_prompt):
    provider = lens["provider"]
    start    = time.time()
    print(f"[lens-{lens['lens_id']}] Starting {lens['lens_name']} ({lens['model']})...")
    try:
        if provider == "groq":
            result = await call_groq(lens, system_prompt, user_prompt)
        elif provider == "gemini":
            result = await call_gemini(lens, system_prompt, user_prompt)
        elif provider == "cerebras":
            result = await call_cerebras(lens, system_prompt, user_prompt)
        elif provider == "sambanova":
            result = await call_sambanova(lens, system_prompt, user_prompt)
        else:
            raise ValueError(f"Unknown provider: {provider}")

        elapsed = time.time() - start
        print(f"[lens-{lens['lens_id']}] Done in {elapsed:.1f}s — {len(result)} chars")
        return lens["lens_id"], result, None

    except Exception as e:
        elapsed = time.time() - start
        print(f"[lens-{lens['lens_id']}] ERROR in {elapsed:.1f}s: {e}")
        return lens["lens_id"], None, str(e)


def save_lens_report(supabase, lens, summary, food_for_thought,
                     article_ids, domain_counts, cycle):
    record = {
        "cycle":            cycle,
        "domain_focus":     "ALL",
        "summary":          f"[{lens['lens_name']} — {lens['perspective']}]\n\n{summary}",
        "food_for_thought": food_for_thought,
        "signals_used":     json.dumps(domain_counts),
        "articles_used":    json.dumps(article_ids),
        "ai_model":         lens["model"],
        "prompt_version":   f"v2.0-LENS004-{lens['lens_name'].replace(' ', '')}",
        "status":           "pending"
    }
    response  = supabase.table("lens_reports").insert(record).execute()
    report_id = response.data[0]["id"] if response.data else "unknown"
    print(f"[save] Lens {lens['lens_id']} saved — id: {report_id} | {lens['lens_name']}")
    return report_id


def get_cycle():
    hour = datetime.now(timezone.utc).hour
    if hour == 13:   return "morning"
    elif hour == 17: return "midday"
    elif hour == 21: return "afternoon"
    elif hour == 4:  return "midnight"
    else:            return "manual"


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cycle    = get_cycle()

    print("=" * 60)
    print(f"Project Lens — 4-Lens Analysis — {date_str}")
    print(f"Cycle: {cycle}")
    print("=" * 60)

    supabase = get_supabase()

    # Fetch balanced articles once — shared across all 4 lenses
    balanced, all_articles, counts = fetch_balanced_articles(supabase)

    if not balanced:
        print("[lens] No articles found. Exiting.")
        return

    # Build prompt content once — same for all 4 lenses
    domain_blocks, total = build_domain_blocks(balanced, counts)
    user_prompt          = build_prompt(domain_blocks, total, date_str, counts)

    # Article IDs for saving
    selected_ids = [
        {"id": a.get("id",""), "url": a.get("url",""),
         "title": (a.get("title") or "")[:200], "domain": a.get("domain","")}
        for a in balanced if a.get("id")
    ]
    all_ids = [
        {"id": a.get("id",""), "url": a.get("url",""),
         "title": (a.get("title") or "")[:200], "domain": a.get("domain","")}
        for a in all_articles if a.get("url")
    ]
    article_ids = {"selected": selected_ids, "all_collected": all_ids}

    print(f"[lens] {total} articles → 4 lenses firing in parallel...")
    print("=" * 60)

    # Fire all 4 lenses in parallel
    tasks = [
        run_lens(
            lens,
            SYSTEM_PROMPTS[lens["lens_id"]],
            user_prompt
        )
        for lens in LENSES
    ]
    results = await asyncio.gather(*tasks)

    print("=" * 60)
    print("[lens] All 4 lenses complete. Saving reports...")

    # Save each lens report separately
    for lens_id, analysis, error in results:
        lens = LENSES[lens_id - 1]

        if error:
            print(f"[save] Lens {lens_id} FAILED — {error}")
            continue

        summary, fft = split_output(analysis)
        save_lens_report(
            supabase, lens, summary, fft,
            article_ids, counts, cycle
        )

        print(f"\n{'='*60}")
        print(f"LENS {lens_id} — {lens['lens_name'].upper()} ({lens['model']})")
        print(f"Perspective: {lens['perspective']}")
        print(f"{'='*60}")
        print(analysis[:500] + "..." if len(analysis) > 500 else analysis)

    print("\n" + "=" * 60)
    print(f"[lens] Complete — {total} articles — 4 reports saved")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
