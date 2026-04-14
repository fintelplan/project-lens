"""
lens_s2d_adversary.py — System 2 Position D: Adversary Narrative
Project Lens | LENS-009
Model: qwen/qwen3-32b (Groq)
Input: lens_raw_articles — STATE tier adversarial sources directly
       (CGTN, TASS, Global Times, Press TV, TRT, Xinhua, Kremlin)
Output: injection_reports (analyst='S2-D')
Key: reads raw articles, NOT processed lens_reports
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from groq import Groq
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-D] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2d")

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL            = "qwen/qwen3-32b"
MAX_TOKENS       = 2000
TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_ARTICLES     = 30        # cap articles sent to model
MAX_ARTICLE_CHARS = 800      # per article snippet
MAX_TOTAL_CHARS  = 20000     # total prompt cap

# Adversarial STATE source IDs — updated with LENS-009 additions
ADVERSARIAL_SOURCE_IDS = [
    "SRC-003",   # Xinhua
    "SRC-004",   # Kremlin
    "SRC-044",   # CGTN
    "SRC-045",   # TASS
    "SRC-046",   # Global Times
    "SRC-047",   # Press TV
    "SRC-048",   # TRT World
]

SYSTEM_PROMPT = """You are S2-D Adversary Narrative Analyst for Project Lens, an OSINT intelligence system.

Your job: analyze a batch of articles from adversarial state media sources and extract the 
dominant narrative they are collectively pushing today.

You answer these questions:
1. What is the PRIMARY NARRATIVE these sources are promoting? (1-2 sentences)
2. What are the KEY CLAIMS being made? (list specific factual or quasi-factual assertions)
3. Who are the NAMED ACTORS — heroes and villains in their framing?
4. What EMOTIONAL TONE do they use? (defiant, victimized, threatening, reassuring, etc.)
5. What COUNTER-NARRATIVE are they building against? (what Western/opposing narrative are they responding to?)
6. What CALL TO ACTION or desired response are they signaling?
7. CONSISTENCY SCORE: how consistent is the narrative across these different sources? (0.0-1.0)

Sources included will be from: Xinhua, Kremlin, CGTN, TASS, Global Times, Press TV, TRT World.
These are official state media or state-adjacent outlets. Their narratives represent official 
or semi-official government messaging.

Rules:
- Be analytically neutral — describe what they are saying, not whether it is true
- Quote specific phrases from the articles as evidence
- Note if different adversarial sources disagree with each other (narrative fractures)
- Compare to what you would expect from Western sources covering the same events

Respond ONLY with valid JSON. No preamble. No markdown fences.

Format:
{
  "analyst": "S2-D",
  "sources_analyzed": ["<source_id list>"],
  "articles_analyzed": <count>,
  "primary_narrative": "<1-2 sentence summary of dominant adversarial narrative>",
  "key_claims": [
    {"claim": "<specific claim>", "source": "<source_id>", "quote": "<supporting quote>"}
  ],
  "named_actors": {
    "heroes": ["<actor names framed positively>"],
    "villains": ["<actor names framed negatively>"],
    "victims": ["<actor names framed as victims>"]
  },
  "emotional_tone": "<primary tone>",
  "counter_narrative_target": "<what opposing narrative are they responding to>",
  "call_to_action": "<what response or belief are they signaling, or 'implicit'>",
  "narrative_consistency_score": <0.0-1.0>,
  "narrative_fractures": "<any disagreements between adversarial sources, or 'none'>",
  "analyst_note": "<1 sentence summary or empty string>"
}"""



# ── TPMGuard ──────────────────────────────────────────────────────────────────
class TPMGuard:
    """
    Rolling 60-second token window guard. Prevents 429 cascades.
    Waits intelligently before each API call. Never crashes — just waits.
    Adapted from GNI MAD pipeline pattern for Project Lens S2.
    """
    def __init__(self, tpm_limit: int = 6000):
        self.tpm_limit = tpm_limit
        self.usage_log = []  # list of (timestamp, tokens)

    def estimate_tokens(self, text: str) -> int:
        return max(1, len(text) // 4)

    def tokens_in_last_60s(self) -> int:
        now = time.time()
        self.usage_log = [(t, tok) for t, tok in self.usage_log if t > now - 60.0]
        return sum(tok for _, tok in self.usage_log)

    def log_usage(self, tokens: int):
        self.usage_log.append((time.time(), tokens))

    def wait_if_needed(self, tokens_needed: int, label: str = ""):
        """Wait until window has headroom. Logs every wait. Returns when safe."""
        while True:
            used = self.tokens_in_last_60s()
            if used + tokens_needed <= self.tpm_limit:
                return
            wait = 10
            log.info(f"[TPMGuard{' '+label if label else ''}] "
                     f"{used}/{self.tpm_limit} TPM used — waiting {wait}s...")
            time.sleep(wait)


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_groq() -> Groq:
    return Groq(api_key=os.environ["GROQ_API_KEY"])


def fetch_adversarial_articles(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    """Fetch recent articles from adversarial STATE sources."""
    try:
        query = sb.table("lens_raw_articles") \
            .select("id, source_id, title, content, url, collected_at") \
            .in_("source_id", ADVERSARIAL_SOURCE_IDS) \
            .order("collected_at", desc=True) \
            .limit(MAX_ARTICLES)

        result = query.execute()
        articles = result.data or []
        log.info(f"Fetched {len(articles)} adversarial articles")

        # Show source breakdown
        by_source = {}
        for a in articles:
            sid = a.get("source_id", "unknown")
            by_source[sid] = by_source.get(sid, 0) + 1
        log.info(f"Source breakdown: {by_source}")

        return articles
    except Exception as e:
        log.error(f"Failed to fetch adversarial articles: {e}")
        return []


def build_articles_prompt(articles: list[dict]) -> str:
    """Build prompt from adversarial articles."""
    sections = []
    total_chars = 0
    included = 0

    for article in articles:
        source_id = article.get("source_id", "unknown")
        title     = article.get("title", "No title")
        content   = article.get("content", "") or ""
        snippet   = content[:MAX_ARTICLE_CHARS] if content else ""

        entry = f"[{source_id}] {title}\n{snippet}\n"

        if total_chars + len(entry) > MAX_TOTAL_CHARS:
            log.info(f"Prompt cap reached at {included} articles")
            break

        sections.append(entry)
        total_chars += len(entry)
        included += 1

    log.info(f"Built prompt with {included} articles ({total_chars} chars)")
    return "\n---\n".join(sections)


def call_adversary_analyst(client: Groq, articles: list[dict], guard: "TPMGuard") -> Optional[dict]:
    """Call qwen3-32b to analyze adversarial narrative."""
    if not articles:
        log.warning("No articles to analyze")
        return None

    articles_text = build_articles_prompt(articles)
    source_ids = list({a.get("source_id") for a in articles})

    user_message = (
        f"Analyze the adversarial narrative from these {len(articles)} "
        f"state media articles.\n\n"
        f"Sources present: {', '.join(source_ids)}\n\n"
        f"--- ARTICLES START ---\n{articles_text}\n--- ARTICLES END ---\n\n"
        f"Return JSON only."
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-D calling qwen3-32b (attempt {attempt})")
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_message},
                ],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )

            raw = response.choices[0].message.content.strip()

            # Strip thinking tags if qwen outputs them
            if "<think>" in raw:
                if "</think>" in raw:
                    raw = raw[raw.index("</think>") + 8:].strip()
                else:
                    raw = raw[raw.index("<think>"):].strip()

            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            log.info(
                f"S2-D result: narrative='{parsed.get('primary_narrative', '')[:80]}...', "
                f"consistency={parsed.get('narrative_consistency_score', 0)}"
            )
            return parsed

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                log.warning(f"Rate limit (429) attempt {attempt} — sleeping 20s")
                time.sleep(20)
            elif "503" in err:
                log.warning(f"503 attempt {attempt} — sleeping 15s")
                time.sleep(15)
            else:
                log.error(f"Unexpected error attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)

    log.error(f"S2-D failed after {MAX_RETRIES} attempts")
    return None


def save_adversary_report(
    sb: Client,
    articles: list[dict],
    analysis: dict,
    run_id: str,
    cycle: Optional[str]
) -> bool:
    """Save S2-D adversary narrative result to injection_reports table."""
    key_claims    = analysis.get("key_claims", [])
    named_actors  = analysis.get("named_actors", {})
    source_ids    = list({a.get("source_id") for a in articles})

    # Flatten key claims into flagged_phrases
    flagged = [c.get("quote", "") for c in key_claims if c.get("quote")]

    row = {
        "run_id":         run_id,
        "cycle":          cycle,
        "lens_report_id": None,
        "analyst":        "S2-D",
        "source_id":      ",".join(source_ids),
        "injection_type": "ADVERSARY_NARRATIVE",
        "evidence": {
            "primary_narrative":          analysis.get("primary_narrative", ""),
            "key_claims":                 key_claims,
            "named_actors":               named_actors,
            "emotional_tone":             analysis.get("emotional_tone", ""),
            "counter_narrative_target":   analysis.get("counter_narrative_target", ""),
            "call_to_action":             analysis.get("call_to_action", "implicit"),
            "narrative_fractures":        analysis.get("narrative_fractures", "none"),
            "narrative_consistency_score": analysis.get("narrative_consistency_score", 0),
            "articles_analyzed":          len(articles),
            "sources_analyzed":           source_ids,
            "analyst_note":               analysis.get("analyst_note", ""),
        },
        "confidence_score": float(analysis.get("narrative_consistency_score", 0.0)),
        "flagged_phrases":  flagged[:10],   # cap at 10
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = sb.table("injection_reports").insert(row).execute()
        saved = len(result.data) if result.data else 0
        log.info(f"Saved {saved} S2-D adversary narrative row")
        return True
    except Exception as e:
        log.error(f"Failed to save S2-D result: {e}")
        return False


def run_s2d(cycle: Optional[str] = None, run_id: Optional[str] = None) -> dict:
    """Main entry point for S2-D Adversary Narrative."""
    start = time.time()
    if not run_id:
        run_id = f"s2d_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    log.info(f"=== S2-D Adversary Narrative START | run_id={run_id} | cycle={cycle} ===")

    try:
        sb     = get_supabase()
        client = get_groq()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    articles = fetch_adversarial_articles(sb, cycle)
    if not articles:
        log.warning("No adversarial articles found — S2-D cannot run")
        return {"status": "NO_ARTICLES", "articles_analyzed": 0}

    guard = TPMGuard(tpm_limit=6000)  # GROQ_S2_API_KEY
    analysis = call_adversary_analyst(client, articles, guard)
    if analysis is None:
        return {"status": "ANALYSIS_FAILED", "articles_analyzed": len(articles)}

    saved = save_adversary_report(sb, articles, analysis, run_id, cycle)

    elapsed = round(time.time() - start, 1)

    summary = {
        "status":                    "COMPLETE" if saved else "SAVE_FAILED",
        "run_id":                    run_id,
        "cycle":                     cycle,
        "articles_analyzed":         len(articles),
        "primary_narrative":         analysis.get("primary_narrative", "")[:120],
        "narrative_consistency":     analysis.get("narrative_consistency_score", 0),
        "key_claims_count":          len(analysis.get("key_claims", [])),
        "elapsed_seconds":           elapsed,
    }

    log.info(f"=== S2-D COMPLETE | {len(articles)} articles | {elapsed}s ===")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    cycle_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_s2d(cycle=cycle_arg)
