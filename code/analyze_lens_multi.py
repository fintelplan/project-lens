"""
analyze_lens_multi.py
Project Lens — 4-Lens Parallel AI Analysis Pipeline
Session: LENS-004

4 independent analytical lenses run in parallel on same 21 articles.
Each lens has its own AI model, provider, and philosophical perspective.
4 separate reports saved — no synthesis, no blending. Each speaks its own voice.

Lens 1: qwen/qwen3-32b        (Groq)      — Foundation: GCSP, Cui Bono, Sectarian Trap, Debt Trap
Lens 2: gemini-2.0-flash      (Google)    — Physical Reality: leading indicators, constraints
Lens 3: llama-4-scout         (Cerebras)  — Causal Chain: First Domino, cause-effect
Lens 4: Llama-4-Maverick      (SambaNova) — Sovereignty Check: from/of/for the people

Rules:
  LR-001(A): API keys from environment only
  LR-004(A): Compress before sending to AI
  LR-036(C): Documented while building
"""

import os
import re
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

MAX_CHARS_EACH    = 300
# Per-domain article allocation — weighted by causal position (LENS-005 FIX-028)
# Hidden signals get more slots: TECH/NARRATIVE/NETWORK are early warning
# RESOURCE → FINANCE → NETWORK → POWER → NARRATIVE → MILITARY → TECH (causal chain)
ARTICLES_PER_DOMAIN = {
    "POWER":     3,   # geopolitical anchor — unchanged
    "TECH":      4,   # hidden signals — surveillance, rights contraction earliest warning
    "FINANCE":   2,   # Debt Trap captured via NETWORK signals
    "MILITARY":  2,   # effect not cause — POWER signals precede these
    "NARRATIVE": 4,   # Sectarian Trap + PHI-002 pretense detection
    "NETWORK":   4,   # dark money, enablers, Cui Bono — highest leverage domain
    "RESOURCE":  2,   # physical floor — present but Debt Trap covered by FINANCE
}
ARTICLES_PER_DOMAIN_TOTAL = sum(ARTICLES_PER_DOMAIN.values())  # = 21

DOMAINS = [
    "POWER", "TECH", "FINANCE",
    "MILITARY", "NARRATIVE", "NETWORK", "RESOURCE"
]

LENSES = [
    {
        "lens_id":    1,
        "lens_name":  "Foundation",
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
        "stagger_override": 6,        # fires at 6s — Lens 4 fires at 21s (15s gap)
    },
    {
        "lens_id":    4,
        "lens_name":  "Sovereignty Check",
        "model":      "qwen-3-235b-a22b-instruct-2507",
        "provider":   "cerebras",
        "api_key_env": "CEREBRAS_API_KEY",
        "perspective": "From/of/for the people — who is pretending to serve the people?",
        "stagger_override": 21,       # 6s cerebras default + 15s gap from Lens 3
        "fallback_provider":  "sambanova",
        "fallback_model":     "llama3.1-8b",
        "fallback_api_key_env": "SAMBANOVA_API_KEY",
    },
]

# LOOKBACK extended to 48h for richer article pool
LOOKBACK_HOURS = 48

# Per-lens article token budgets
# Budget = tpm_limit - system_prompt_overhead - output_budget
# ~75 tokens per compressed article (300 chars / 4)
LENS_ARTICLE_BUDGETS = {
    "groq":        1980,   # empirical safe: 9 articles x 220 tokens = 1980
    "gemini":    55800,   # 60000 - 700 - 3500 = 55800 (~744 articles, all fine)
    "cerebras":  56800,   # 60000 - 700 - 2500 = 56800 (~757 articles, all fine)
    "sambanova": 35800,   # 40000 - 700 - 3500 = 35800 (~477 articles, all fine)
}
TOKENS_PER_ARTICLE = 220  # empirical: Groq measured 21 articles = 4605 tokens = ~219/article

# ─── System Prompts Per Lens ──────────────────────────────────────────────────

SHARED_RULES = """
STRICT RULES:
- Never make predictions. Never say "will happen."
- Food for Thought = open questions that make people think deeper.
- Stick to facts from the articles provided. No invention.
- Be direct. No fluff. Global Game Changers are busy people.
- Flag when any actor is blocking people's right to grow and be free.
- Flag DOMAIN CROSS-TALK when detected:
  Pattern 1: FINANCE spike + NARRATIVE nationalism -> Sectarian Trap forming
  Pattern 2: TECH censorship before POWER event -> rights contraction planned
  Pattern 3: NETWORK hidden flows near RESOURCE zone -> sanctions evasion underway
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

DEBT TRAP — watch in all four dimensions:

Direct + Active:
Who is deliberately structuring loans to extract sovereignty?
What assets, ports, resources, or political compliance are being claimed
when the trap springs? A nation that cannot repay loses its ability to
govern freely — this is sovereignty theft through finance.
Cui Bono: who benefits when a nation defaults?

Direct + Passive:
What lending creates structural dependency even without malicious intent?
Which conditions attached to loans redirect public funds away from
people's growth? Are development loans developing the people or the lender?
Privatization conditions, austerity requirements, budget controls —
these are GCSP blockers dressed as financial assistance.

Indirect + Active:
Who is using existing debt as political leverage?
Debt forgiveness in exchange for what?
Debt restructuring in exchange for what?
Follow the conditionality — that is where sovereignty is traded.
Vote with us at the UN. Let us build a base. Sell us your port.

Indirect + Passive:
Which systemic financial structures keep nations permanently dependent
regardless of any single actor's intent?
Dollar hegemony, commodity price volatility, structural adjustment
legacies, interest rate cycles imposed by rich-country central banks
that devastate poor-country economies — no single villain, but people
are blocked from growing freely by the architecture itself.

GCSP TEST FOR DEBT:
Does this debt expand or contract the people's ability to fund their own
education, healthcare, safe environment — the foundations of GCSP?
Does this debt give or take sovereignty from the people?
Who owns the debt — and therefore who owns the decisions?
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
- FINANCE pressure -> POWER decision -> MILITARY response
- TECH censorship -> NARRATIVE control -> POWER consolidation
- RESOURCE scarcity -> NETWORK evasion -> FINANCE sanctions failure
- NETWORK dark money -> NARRATIVE manufacturing -> POWER legitimization

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

OUTPUT FORMAT — follow exactly. No bold markdown. Plain text labels only.

## SUMMARY
[One sentence naming the single dominant signal today.]
[Then exactly 3 numbered key connections that explain why:]
1. [First connection — what pattern links two or more domains]
2. [Second connection — cause and effect across actors]
3. [Third connection — what is quietly happening while attention is elsewhere]

### POWER ({power_count} articles)
Key Development: [1-2 sentences — what actually happened]
Why It Matters: [1-2 sentences through YOUR specific lens]
Watch: [1 sentence — what to monitor next]

### TECH ({tech_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

### FINANCE ({finance_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

### MILITARY ({military_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

### NARRATIVE ({narrative_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

### NETWORK ({network_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

### RESOURCE ({resource_count} articles)
Key Development: [1-2 sentences]
Why It Matters: [1-2 sentences through your lens]
Watch: [1 sentence]

[Skip any domain with 0 articles]

## FOOD FOR THOUGHT
1. [Open question — not a prediction — makes Game Changers think deeper]
2. [Open question — follows from a different domain than question 1]
3. [Open question — about a hidden actor or silent beneficiary]
4. [Open question — about long-term consequences, not today's events]
5. [Open question — challenges an assumption everyone is making]

============================================================
"""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_supabase():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_KEY not set")
    return create_client(url, key)


def _load_source_tiers() -> dict:
    """Load source_id -> tier mapping from lens-SRC-001_sources.json."""
    import json as _json
    for src_path in [
        os.path.join("data", "lens-SRC-001_sources.json"),
        os.path.join("..", "data", "lens-SRC-001_sources.json"),
    ]:
        if os.path.exists(src_path):
            with open(src_path, "r", encoding="utf-8") as f:
                data = _json.load(f)
            return {s["id"]: s.get("tier", "TIER2") for s in data.get("sources", [])}
    return {}


def _extract_keywords(title: str) -> set:
    """Extract significant keywords from title for event matching."""
    stop = {
        "a","an","the","and","or","but","in","on","at","to","for",
        "of","with","by","from","is","are","was","were","be","been",
        "has","have","had","will","would","could","should","may","might",
        "that","this","these","those","it","its","as","up","out","into",
        "about","over","after","before","through","during","says","said",
        "new","report","news","latest","update","breaking","just","now",
    }
    words = re.findall(r'[a-z]{4,}', title.lower())
    return {w for w in words if w not in stop}


def _compute_velocity(all_articles: list) -> dict:
    """
    Compute velocity score per article.
    Velocity = how fast this article's topic is accelerating vs 6h baseline.

    recent_count = articles with similar title keywords in last 6 hours
    baseline = articles with similar keywords in last 48h / 8 (per 6h window)
    velocity = recent_count / max(1, baseline)
    multiplier = min(2.0, max(1.0, 1.0 + (velocity - 1.0) × 0.5))

    Breaking news (4× acceleration) → multiplier = 2.0 (score doubled)
    Normal flow (1× rate)           → multiplier = 1.0 (unchanged)
    LENS-005 FIX-029
    """
    import time as _time
    now_ts   = _time.time()
    window6h = 6 * 3600
    window48h = 48 * 3600

    # Pre-compute keywords per article
    kw_map = {
        a.get("id", ""): _extract_keywords(a.get("title", ""))
        for a in all_articles
    }

    velocity = {}
    for a in all_articles:
        aid      = a.get("id", "")
        a_kws    = kw_map.get(aid, set())
        if len(a_kws) < 2:
            velocity[aid] = 1.0
            continue

        try:
            from datetime import datetime as _dt
            collected = _dt.fromisoformat(a.get("collected_at","").replace("Z","+00:00"))
            art_ts    = collected.timestamp()
        except Exception:
            velocity[aid] = 1.0
            continue

        recent_count   = 0
        baseline_count = 0

        for b in all_articles:
            bid   = b.get("id","")
            if bid == aid:
                continue
            b_kws = kw_map.get(bid, set())
            if len(a_kws & b_kws) < 2:
                continue
            try:
                b_collected = _dt.fromisoformat(b.get("collected_at","").replace("Z","+00:00"))
                b_ts        = b_collected.timestamp()
            except Exception:
                continue

            age = now_ts - b_ts
            if age <= window6h:
                recent_count += 1
            if age <= window48h:
                baseline_count += 1

        baseline_per_window = max(1, baseline_count / 8)
        vel                 = recent_count / baseline_per_window
        multiplier          = min(2.0, max(1.0, 1.0 + (vel - 1.0) * 0.5))
        velocity[aid]       = round(multiplier, 4)

    return velocity


def _compute_local_coverage(all_articles: list, source_tiers: dict) -> dict:
    """
    Compute local_coverage_count per article.
    Count OTHER TIER1+2 articles covering the same event
    (>=2 shared significant keywords, different source_id).
    Returns dict: article_id -> coverage_count
    """
    kw_map = {
        a.get("id", ""): _extract_keywords(a.get("title", ""))
        for a in all_articles
    }
    verified = {"TIER1", "TIER2"}
    coverage = {}
    for a in all_articles:
        aid   = a.get("id", "")
        a_src = a.get("source_id", "")
        a_kws = kw_map.get(aid, set())
        if len(a_kws) < 2:
            coverage[aid] = 0
            continue
        count = 0
        for b in all_articles:
            bid   = b.get("id", "")
            b_src = b.get("source_id", "")
            if bid == aid or b_src == a_src:
                continue
            if source_tiers.get(b_src, "TIER2") not in verified:
                continue
            if len(a_kws & kw_map.get(bid, set())) >= 2:
                count += 1
        coverage[aid] = count
    return coverage


def _recency_weight(collected_at_str: str, now_ts: float) -> float:
    """Time decay: 1.0 (now) -> 0.5 (48h old). Min 0.5."""
    try:
        from datetime import datetime as _dt
        collected = _dt.fromisoformat(collected_at_str.replace("Z", "+00:00"))
        hours_old = (now_ts - collected.timestamp()) / 3600
        return max(0.5, 1.0 - min(hours_old, 48) / 48 * 0.5)
    except Exception:
        return 0.75


def fetch_balanced_articles(supabase):
    """
    Significance-scored article selection. LENS-005 FIX-021.
    STATE sources always included, bypass scoring.
    Non-STATE: score = local_coverage_count x recency_weight.
    Top ARTICLES_PER_DOMAIN per domain selected by score.
    No external API calls — 24/7 reliable.
    """
    import time as _time
    now_ts       = _time.time()
    cutoff       = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    source_tiers = _load_source_tiers()

    response = supabase.table("lens_raw_articles") \
        .select("id, title, content, domain, source_id, collected_at, url") \
        .gte("collected_at", cutoff) \
        .order("collected_at", desc=True) \
        .execute()
    all_articles = response.data or []

    if not all_articles:
        return [], [], {d: 0 for d in DOMAINS}

    coverage_counts = _compute_local_coverage(all_articles, source_tiers)
    velocity_scores = _compute_velocity(all_articles)

    state_articles  = []
    scored_articles = []

    for article in all_articles:
        tier = source_tiers.get(article.get("source_id", ""), "TIER2")
        if tier == "STATE":
            state_articles.append(article)
        else:
            cov   = coverage_counts.get(article.get("id", ""), 0)
            rec   = _recency_weight(article.get("collected_at", ""), now_ts)
            vel   = velocity_scores.get(article.get("id", ""), 1.0)
            score = round(cov * rec * vel, 4)
            article["_significance_score"] = score
            article["_coverage_count"]     = cov
            article["_recency_weight"]     = round(rec, 4)
            article["_velocity_mult"]      = vel
            scored_articles.append(article)

    scored_articles.sort(key=lambda a: a.get("_significance_score", 0), reverse=True)

    domain_buckets = {d: [] for d in DOMAINS}
    for article in scored_articles:
        domain = (article.get("domain") or "POWER").upper()
        if domain not in domain_buckets:
            domain = "POWER"
        limit = ARTICLES_PER_DOMAIN.get(domain, 3) if isinstance(ARTICLES_PER_DOMAIN, dict) else ARTICLES_PER_DOMAIN
        if len(domain_buckets[domain]) < limit:
            domain_buckets[domain].append(article)

    selected = []
    for domain in DOMAINS:
        selected.extend(domain_buckets[domain])

    final  = state_articles + selected
    counts = {d: len(domain_buckets[d]) for d in DOMAINS}

    print(f"[fetch] Pool: {len(all_articles)} | State: {len(state_articles)} | "
          f"Scored: {len(selected)} | Total: {len(final)}")
    print(f"[fetch] Domains: {counts}")

    for domain in DOMAINS:
        top = domain_buckets.get(domain, [])
        if top:
            best = top[0]
            print(f"  [{domain}] best: "
                  f"score={best.get('_significance_score',0):.2f} "
                  f"cov={best.get('_coverage_count',0)} "
                  f"rec={best.get('_recency_weight',0):.2f} "
                  f"vel={best.get('_velocity_mult',1.0):.2f} | "
                  f"{(best.get('title') or '')[:40]}")

    return final, all_articles, counts

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




def find_cross_lens_signals(results: list, lenses: list) -> list:
    """
    Find signals appearing in 3+ lens reports = HIGH CONFIDENCE.
    Extracts keywords from each report, finds overlap across lenses.
    Returns list of high-confidence signals with lens_count.
    LENS-005 FIX-031
    """
    import re as _re

    stop = {
        # Basic stop words
        "a","an","the","and","or","but","in","on","at","to","for",
        "of","with","by","from","is","are","was","were","be","been",
        "has","have","had","will","would","could","should","this","that",
        "these","those","it","its","as","up","out","into","about","over",
        "while","also","such","more","most","than","which","when","where",
        "their","there","they","them","been","have","what","will","from",
        # Template words — appear in EVERY report due to our output format
        # These are structural noise, not intelligence signals (LENS-006)
        "today","domain","signal","key","development","lens","analysis",
        "summary","watch","matters","thought","articles","report","cycle",
        "output","format","section","perspective","framework","strictly",
        "overview","context","background","narrative","following","provides",
        # Generic geopolitical words — too broad to be real signals
        "political","economic","military","critical","global","security",
        "international","government","national","market","country","region",
        "policy","current","major","important","significant","strategic",
        "potential","ongoing","recent","including","through","between",
        "against","within","without","across","however","therefore","because",
        "various","several","further","beyond","around","during","despite",
        # OUR OWN DOMAIN NAMES — appear in every report as section headers
        # These are structural noise, never real intelligence signals (LENS-006)
        "network","finance","resource","narrative","sovereignty","physical",
        "causal","foundation","reality","military","technology","geopolitical",
    }

    def extract_kws(text):
        # Min 7 chars — removes noise like "force","other","power","action"
        # LENS-006: raised from 5 to 7 to eliminate generic single-domain words
        words = _re.findall(r'[a-z]{7,}', (text or '').lower())
        return {w for w in words if w not in stop}

    # Build keyword sets per lens
    lens_kws = {}
    for lens_id, analysis, error in results:
        if error or not analysis:
            continue
        lens_kws[lens_id] = extract_kws(analysis[:3000])

    if len(lens_kws) < 2:
        return []

    # Find keywords appearing in 3+ lenses
    all_kws = set()
    for kws in lens_kws.values():
        all_kws |= kws

    signals = []
    for kw in all_kws:
        lenses_with_kw = [lid for lid, kws in lens_kws.items() if kw in kws]
        if len(lenses_with_kw) >= 3:
            signals.append({
                "signal": kw,
                "lens_count": len(lenses_with_kw),
                "lens_ids": lenses_with_kw,
                "confidence": "CRITICAL" if len(lenses_with_kw) == 4 else "HIGH"
            })

    # Sort by lens_count descending
    signals.sort(key=lambda s: s["lens_count"], reverse=True)
    return signals[:20]  # top 20 signals


def score_lens_quality(analysis: str, lens_name: str) -> dict:
    """
    Score lens output quality 0-10. Deterministic, no AI needed.
    5 dimensions × 2 points each = 10 max.
    LENS-005 FIX-031
    """
    if not analysis:
        return {"total": 0, "details": {}}

    text = analysis.lower()

    # 1. Specificity (0-2): named actors, places, mechanisms
    specificity_kws = ["iran","trump","china","russia","nato","imf","strait","hormuz",
                        "bitcoin","ceasefire","sanctions","blockade","ukraine","israel"]
    spec_hits = sum(1 for kw in specificity_kws if kw in text)
    specificity = min(2.0, spec_hits * 0.25)

    # 2. Signal depth (0-2): cause-effect language, not just description
    depth_kws = ["because","therefore","triggers","causes","leads to","results in",
                  "cui bono","first domino","root cause","underlying","consequence"]
    depth_hits = sum(1 for kw in depth_kws if kw in text)
    depth = min(2.0, depth_hits * 0.4)

    # 3. PHI-002 alignment (0-2): GCSP framework applied
    phi_kws = ["gcsp","sovereignty","rights","freedom","blocked","people","democracy",
                "pretense","debt trap","sectarian","cui bono"]
    phi_hits = sum(1 for kw in phi_kws if kw in text)
    phi_align = min(2.0, phi_hits * 0.4)

    # 4. Cross-domain detection (0-2): domains mentioned together
    domain_kws = ["power","tech","finance","military","narrative","network","resource"]
    domain_hits = sum(1 for kw in domain_kws if kw in text)
    cross_domain = min(2.0, max(0, domain_hits - 2) * 0.5)

    # 5. Food for thought quality (0-2): questions present
    has_food = "food for thought" in text
    question_count = text.count("?")
    food_quality = min(2.0, (1.0 if has_food else 0) + question_count * 0.2)

    total = round(specificity + depth + phi_align + cross_domain + food_quality, 2)

    return {
        "total": total,
        "details": {
            "specificity":   round(specificity, 2),
            "signal_depth":  round(depth, 2),
            "phi002_align":  round(phi_align, 2),
            "cross_domain":  round(cross_domain, 2),
            "food_quality":  round(food_quality, 2),
        }
    }

# ─── TPM Guard ────────────────────────────────────────────────────────────────

class TPMGuard:
    """
    Token-Per-Minute aware rate limiter.
    Estimates payload size, tracks usage, waits if needed.
    Each lens instance is independent — separate quota pools.

    Session: LENS-005
    Rule: LR-001(A) No hardcoded limits — from LENSES config
    """

    # Conservative TPM limits per provider (free tier)
    TPM_LIMITS = {
        "groq":      6_000,
        "gemini":   60_000,
        "cerebras": 60_000,
        "sambanova": 40_000,
    }

    def __init__(self, provider: str, lens_id: int):
        self.provider    = provider
        self.lens_id     = lens_id
        self.tpm_limit   = self.TPM_LIMITS.get(provider, 10_000)
        self.usage_log   = []  # list of (timestamp, tokens_used)

    def estimate_tokens(self, text: str) -> int:
        """Estimate token count — 4 chars per token is standard approximation."""
        return max(1, len(text) // 4)

    def tokens_used_last_60s(self) -> int:
        """Count tokens used in the last 60 seconds."""
        now     = time.time()
        cutoff  = now - 60.0
        # Remove old entries outside window
        self.usage_log = [(t, tok) for t, tok in self.usage_log if t > cutoff]
        return sum(tok for _, tok in self.usage_log)

    def log_usage(self, tokens: int):
        """Record tokens used at current time."""
        self.usage_log.append((time.time(), tokens))

    async def wait_if_needed(self, payload_tokens: int):
        """
        Check if payload fits in current TPM window.
        If payload > tpm_limit entirely -> raise immediately (never fits).
        If payload fits but window full -> wait for reset then continue.
        """
        # Hard cap: payload larger than total limit will never fit
        if payload_tokens > self.tpm_limit:
            raise ValueError(
                f"[tpm-lens{self.lens_id}] Payload {payload_tokens} tokens exceeds "
                f"TPM limit {self.tpm_limit} — articles must be trimmed before calling."
            )
        while True:
            used      = self.tokens_used_last_60s()
            headroom  = self.tpm_limit - used
            if payload_tokens <= headroom:
                break  # fits — proceed
            wait_needed = 61 - (time.time() - self.usage_log[0][0]) if self.usage_log else 5
            wait_secs   = max(1, min(wait_needed, 65))
            print(f"[tpm-lens{self.lens_id}] TPM window full "
                  f"({used}/{self.tpm_limit} used) — waiting {wait_secs:.0f}s...")
            await asyncio.sleep(wait_secs)

    async def call_protected(self, payload: str, api_fn, *args, **kwargs):
        """
        Estimate payload tokens, wait if needed, call API, log usage.
        Usage:
          result = await guard.call_protected(full_prompt, api_fn, lens, sys_p, usr_p)
        """
        tokens = self.estimate_tokens(payload) + 2000  # output budget only — TOKENS_PER_ARTICLE is empirical
        await self.wait_if_needed(tokens)
        result = await api_fn(*args, **kwargs)
        self.log_usage(tokens)
        return result

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
        max_tokens=2500
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


# ─── Lens Provider Guard ──────────────────────────────────────────────────────

class LensProviderGuard:
    """
    Pre-flight + retry guard for each lens provider.
    Inspired by GNI MAD preflight + GroqGuardian pattern.

    Principles (from GNI lessons):
    - Pre-check before firing — never assume provider is ready
    - Exponential backoff on 429 — never hammer a rate-limited provider
    - Graceful degradation — one lens failing never kills the pipeline
    - Calculated waits — not hardcoded sleeps

    LENS-006: Applied MAD guard concept to Lens provider layer.
    """

    # Stagger delays per provider — prevents simultaneous burst hits
    # Root cause of SambaNova 429: all 4 lenses fired at t=0 simultaneously
    STAGGER_DELAYS = {
        "groq":      0,   # fires first — lowest TPM, needs head start
        "gemini":    3,   # 3s after groq
        "cerebras":  6,   # 6s after groq
        "sambanova": 10,  # 10s after groq — most sensitive to bursts
    }

    # Backoff schedule on 429 (seconds)
    BACKOFF_SCHEDULE = [15, 30, 60]  # attempt 1, 2, 3 — then give up

    def __init__(self, provider: str, lens_id: int, lens_name: str):
        self.provider  = provider
        self.lens_id   = lens_id
        self.lens_name = lens_name
        self.attempts  = 0

    async def stagger(self, override: int = None):
        """
        Wait stagger delay for this provider before firing.
        override: per-lens stagger_override from LENSES config.
        Used to give Lens 4 extra 15s gap from Lens 3 on same provider.
        """
        delay = override if override is not None else self.STAGGER_DELAYS.get(self.provider, 5)
        if delay > 0:
            print(f"[guard-lens{self.lens_id}] Stagger wait {delay}s "
                  f"({'override' if override is not None else self.provider} launch delay)...")
            await asyncio.sleep(delay)

    async def handle_429(self) -> bool:
        """
        Handle 429 rate limit with exponential backoff.
        Returns True if should retry, False if max attempts exhausted.
        """
        self.attempts += 1
        if self.attempts > len(self.BACKOFF_SCHEDULE):
            print(f"[guard-lens{self.lens_id}] {self.lens_name} — "
                  f"429 max retries exhausted ({self.attempts-1} attempts). "
                  f"Marking lens FAILED this cycle.")
            return False  # give up — pipeline continues without this lens

        wait = self.BACKOFF_SCHEDULE[self.attempts - 1]
        print(f"[guard-lens{self.lens_id}] {self.lens_name} — "
              f"429 rate limit. Backoff attempt {self.attempts}/3 — "
              f"waiting {wait}s...")
        await asyncio.sleep(wait)
        return True  # retry

    def log_success(self):
        if self.attempts > 0:
            print(f"[guard-lens{self.lens_id}] {self.lens_name} — "
                  f"recovered after {self.attempts} retry attempt(s). OK.")

async def call_sambanova(lens, system_prompt, user_prompt):
    """
    SambaNova API call with LensProviderGuard retry logic.
    429 handled via exponential backoff — max 3 retries.
    LENS-006: Applied MAD guard pattern to SambaNova provider.
    """
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
    guard = LensProviderGuard(
        provider="sambanova",
        lens_id=lens["lens_id"],
        lens_name=lens["lens_name"]
    )
    while True:
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                response = await client.post(
                    "https://api.sambanova.ai/v1/chat/completions",
                    json=payload,
                    headers=headers
                )
                if response.status_code == 429:
                    should_retry = await guard.handle_429()
                    if not should_retry:
                        raise ValueError(
                            f"SambaNova 429 — max retries exhausted after "
                            f"{len(guard.BACKOFF_SCHEDULE)} attempts"
                        )
                    continue  # retry after backoff
                response.raise_for_status()
                data = response.json()
                guard.log_success()
                return data["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                should_retry = await guard.handle_429()
                if not should_retry:
                    raise ValueError(
                        f"SambaNova 429 — max retries exhausted"
                    )
                continue
            raise  # re-raise non-429 errors immediately


def trim_articles_to_budget(articles: list, provider: str) -> list:
    """
    Trim article list to fit within provider TPM budget.
    State articles (no _significance_score) always kept first.
    Best-scored non-state articles fill remaining budget.

    Returns trimmed list safe to send to this provider.
    """
    budget = LENS_ARTICLE_BUDGETS.get(provider, 35800)
    max_articles = max(1, budget // TOKENS_PER_ARTICLE)

    if len(articles) <= max_articles:
        return articles  # already fits

    # Separate state from scored
    state   = [a for a in articles if "_significance_score" not in a]
    scored  = [a for a in articles if "_significance_score" in a]

    # State articles first — but also capped at max_articles
    # Recent state articles take priority (already sorted desc by collected_at)
    trimmed   = state[:max_articles]
    remaining = max(0, max_articles - len(trimmed))

    # Fill remaining with best-scored non-state articles
    scored_sorted = sorted(scored, key=lambda a: a.get("_significance_score", 0), reverse=True)
    trimmed += scored_sorted[:remaining]

    return trimmed




async def run_lens(lens, system_prompt, articles_full: list):
    """
    Run a single lens with TPM protection.
    Articles trimmed to provider budget before building prompt.
    TPMGuard waits if window full, raises if payload impossible.
    Each lens has its own independent TPMGuard instance.
    """
    from datetime import datetime as _dt, timezone as _tz
    provider = lens["provider"]
    start    = time.time()
    guard    = TPMGuard(provider=provider, lens_id=lens["lens_id"])

    # Trim articles to this provider's budget
    articles_trimmed = trim_articles_to_budget(articles_full, provider)
    trim_msg = f" (trimmed {len(articles_full)}->{len(articles_trimmed)})" if len(articles_trimmed) < len(articles_full) else ""

    # Build prompt from trimmed articles
    domain_blocks, total = build_domain_blocks(articles_trimmed, {})
    date_str   = _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M UTC")
    # Recount domain
    from collections import Counter as _Counter
    counts = _Counter((a.get("domain") or "POWER").upper() for a in articles_trimmed)
    user_prompt = build_prompt(domain_blocks, total, date_str, counts)

    full_payload = system_prompt + user_prompt
    payload_tokens = guard.estimate_tokens(full_payload) + 2000

    # Stagger launch — prevents simultaneous burst hits on providers
    # Per-lens stagger_override used when two lenses share the same provider
    # Lens 3 Cerebras=6s, Lens 4 Cerebras=21s — 15s gap prevents burst hit
    launch_guard = LensProviderGuard(
        provider=provider,
        lens_id=lens["lens_id"],
        lens_name=lens["lens_name"]
    )
    stagger_override = lens.get("stagger_override", None)
    await launch_guard.stagger(override=stagger_override)

    print(f"[lens-{lens['lens_id']}] Starting {lens['lens_name']} ({lens['model']}) "
          f"| {len(articles_trimmed)} articles{trim_msg} "
          f"| ~{payload_tokens} tokens | TPM limit: {guard.tpm_limit}")
    try:
        if provider == "groq":
            result = await guard.call_protected(
                full_payload, call_groq, lens, system_prompt, user_prompt)
        elif provider == "gemini":
            result = await guard.call_protected(
                full_payload, call_gemini, lens, system_prompt, user_prompt)
        elif provider == "cerebras":
            result = await guard.call_protected(
                full_payload, call_cerebras, lens, system_prompt, user_prompt)
        elif provider == "sambanova":
            result = await guard.call_protected(
                full_payload, call_sambanova, lens, system_prompt, user_prompt)
        else:
            raise ValueError(f"Unknown provider: {provider}")

        elapsed = time.time() - start
        print(f"[lens-{lens['lens_id']}] Done in {elapsed:.1f}s — {len(result)} chars")
        return lens["lens_id"], result, None

    except Exception as e:
        elapsed = time.time() - start
        print(f"[lens-{lens['lens_id']}] ERROR in {elapsed:.1f}s: {e}")

        # Try fallback provider if configured (LENS-006)
        fallback_provider = lens.get("fallback_provider")
        fallback_model    = lens.get("fallback_model")
        fallback_key_env  = lens.get("fallback_api_key_env")

        if fallback_provider and fallback_model and fallback_key_env:
            print(f"[lens-{lens['lens_id']}] Trying fallback: "
                  f"{fallback_provider} {fallback_model}...")
            fallback_lens = dict(lens)
            fallback_lens["provider"]    = fallback_provider
            fallback_lens["model"]       = fallback_model
            fallback_lens["api_key_env"] = fallback_key_env
            fallback_lens.pop("fallback_provider", None)
            fallback_lens.pop("fallback_model", None)
            fallback_lens.pop("fallback_api_key_env", None)
            fallback_lens.pop("stagger_override", None)
            try:
                articles_fb = trim_articles_to_budget(articles_full, fallback_provider)
                domain_blocks_fb, total_fb = build_domain_blocks(articles_fb, {})
                from datetime import datetime as _dt, timezone as _tz
                from collections import Counter as _Counter
                date_str_fb = _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M UTC")
                counts_fb   = _Counter((a.get("domain") or "POWER").upper()
                                       for a in articles_fb)
                user_prompt_fb = build_prompt(domain_blocks_fb, total_fb,
                                              date_str_fb, counts_fb)
                system_prompt_fb = SYSTEM_PROMPTS[lens["lens_id"]]
                if fallback_provider == "sambanova":
                    result_fb = await call_sambanova(fallback_lens,
                                                     system_prompt_fb,
                                                     user_prompt_fb)
                elif fallback_provider == "cerebras":
                    result_fb = await call_cerebras(fallback_lens,
                                                    system_prompt_fb,
                                                    user_prompt_fb)
                else:
                    result_fb = await call_groq(fallback_lens,
                                               system_prompt_fb,
                                               user_prompt_fb)
                fb_elapsed = time.time() - start
                print(f"[lens-{lens['lens_id']}] Fallback OK in "
                      f"{fb_elapsed:.1f}s — {len(result_fb)} chars")
                return lens["lens_id"], result_fb + " [FALLBACK]", None
            except Exception as fb_e:
                print(f"[lens-{lens['lens_id']}] Fallback FAILED: {fb_e}")
                return lens["lens_id"], None, f"Primary: {e} | Fallback: {fb_e}"

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
    """
    Cycle labels matching real cron schedule.
    08 UTC=morning | 12 UTC=midday | 16 UTC=afternoon | 20 UTC=evening
    Fix: LENS-005 FIX-021 — wrong hours from earlier session.
    """
    hour = datetime.now(timezone.utc).hour
    if   hour == 8:  return "morning"
    elif hour == 12: return "midday"
    elif hour == 16: return "afternoon"
    elif hour == 20: return "evening"
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

    print(f"[lens] {total} articles -> 4 lenses firing in parallel...")
    print("=" * 60)

    # Fire all 4 lenses in parallel
    # Each lens trims articles to its own TPM budget
    tasks = [
        run_lens(
            lens,
            SYSTEM_PROMPTS[lens["lens_id"]],
            balanced       # pass full article list — each lens trims itself
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
        quality = score_lens_quality(analysis, lens["lens_name"])
        save_lens_report(
            supabase, lens, summary, fft,
            article_ids, counts, cycle
        )
        print(f"  Quality score: {quality['total']}/10 "
              f"(spec={quality['details'].get('specificity',0):.1f} "
              f"depth={quality['details'].get('signal_depth',0):.1f} "
              f"phi={quality['details'].get('phi002_align',0):.1f})")

        print(f"\n{'='*60}")
        print(f"LENS {lens_id} — {lens['lens_name'].upper()} ({lens['model']})")
        print(f"Perspective: {lens['perspective']}")
        print(f"{'='*60}")
        # Strip think tags before console display (LENS-006)
        # Supabase data already clean via split_output() — console only fix
        clean_analysis = strip_think_tags(analysis)
        summary_print, fft_print = split_output(analysis)
        print(summary_print)
        print()
        print(fft_print)

    # Cross-lens agreement (LENS-005 FIX-031)
    cross_signals = find_cross_lens_signals(results, LENSES)
    if cross_signals:
        print(f"\n[cross-lens] HIGH CONFIDENCE signals ({len(cross_signals)} found):")
        for sig in cross_signals[:5]:
            print(f"  {sig['confidence']} ({sig['lens_count']}/4 lenses): {sig['signal']}")
    else:
        print("\n[cross-lens] No cross-lens agreement signals detected")

    print("\n" + "=" * 60)
    print(f"[lens] Complete — {total} articles — 4 reports saved")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
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
    """
    Cycle labels matching real cron schedule.
    08 UTC=morning | 12 UTC=midday | 16 UTC=afternoon | 20 UTC=evening
    Fix: LENS-005 FIX-021 — wrong hours from earlier session.
    """
    hour = datetime.now(timezone.utc).hour
    if   hour == 8:  return "morning"
    elif hour == 12: return "midday"
    elif hour == 16: return "afternoon"
    elif hour == 20: return "evening"
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

    print(f"[lens] {total} articles -> 4 lenses firing in parallel...")
    print("=" * 60)

    # Fire all 4 lenses in parallel
    # Each lens trims articles to its own TPM budget
    tasks = [
        run_lens(
            lens,
            SYSTEM_PROMPTS[lens["lens_id"]],
            balanced       # pass full article list — each lens trims itself
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
        quality = score_lens_quality(analysis, lens["lens_name"])
        save_lens_report(
            supabase, lens, summary, fft,
            article_ids, counts, cycle
        )
        print(f"  Quality score: {quality['total']}/10 "
              f"(spec={quality['details'].get('specificity',0):.1f} "
              f"depth={quality['details'].get('signal_depth',0):.1f} "
              f"phi={quality['details'].get('phi002_align',0):.1f})")

        print(f"\n{'='*60}")
        print(f"LENS {lens_id} — {lens['lens_name'].upper()} ({lens['model']})")
        print(f"Perspective: {lens['perspective']}")
        print(f"{'='*60}")
        # Strip think tags before console display (LENS-006)
        # Supabase data already clean via split_output() — console only fix
        clean_analysis = strip_think_tags(analysis)
        summary_print, fft_print = split_output(analysis)
        print(summary_print)
        print()
        print(fft_print)

    # Cross-lens agreement (LENS-005 FIX-031)
    cross_signals = find_cross_lens_signals(results, LENSES)
    if cross_signals:
        print(f"\n[cross-lens] HIGH CONFIDENCE signals ({len(cross_signals)} found):")
        for sig in cross_signals[:5]:
            print(f"  {sig['confidence']} ({sig['lens_count']}/4 lenses): {sig['signal']}")
    else:
        print("\n[cross-lens] No cross-lens agreement signals detected")

    print("\n" + "=" * 60)
    print(f"[lens] Complete — {total} articles — 4 reports saved")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
