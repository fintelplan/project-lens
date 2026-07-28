"""
lens_s2d_adversary.py — System 2 Position D: Adversary Narrative
Project Lens | LENS-009
Model: from the registry — wire("s2d_adversary") in code/lens_models.py.
       Never hardcoded here. History: qwen/qwen3-32b 404'd from 2026-07-17 and
       ran dead 10 days under green checks; Groq gpt-oss-120b resurrected it on
       2026-07-28 but truncated mid-JSON and lost 42 of 60 articles against the
       8,000 TPM ceiling; now Cerebras gpt-oss-120b (D-016), probed 3/3
       stop/valid-JSON at 26-36% of budget on the exact prompt that broke Groq.
Input: lens_raw_articles — STATE tier adversarial sources directly
       (Chinese state-apparatus: Xinhua, CGTN, Global Times, SCMP, CASS;
        Russian state-apparatus: TASS, RT, Valdai, Kremlin;
        Iranian state-apparatus: Press TV)
Output: injection_reports (analyst='S2-D')
Key: reads raw articles, NOT processed lens_reports
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from cerebras.cloud.sdk import Cerebras
from lens_models import (
    LensModelRegistryError,
    assert_model_known,
    fit_max_tokens,
    limits_for,
    wire,
)
from lens_quota_guard import guard_check_with_fallback
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-D] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("s2d")

# ── Constants ─────────────────────────────────────────────────────────────────
# Wire values come from the registry (LR-105) — this file hardcodes no model
# string, no key env, and no output budget. A migration is a registry edit.
PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2d_adversary")

TEMPERATURE      = 0.2
MAX_RETRIES      = 2
RETRY_SLEEP      = 10
MAX_ARTICLES     = 60        # cap articles sent to model (B-2: bumped from 30 for source diversity)
MAX_ARTICLE_CHARS = 800      # per article snippet
MAX_TOTAL_CHARS  = 9000      # per batch — see BUG-001 at the truncation site

# Adversarial STATE-APPARATUS source IDs — LENS-017 B-2 widened
# Per PHI-003: these are state-apparatus organs, NOT the peoples of those countries.
# SRC-IDs verified live in Supabase lens_raw_articles (LENS-017 SQL audit).
ADVERSARIAL_SOURCE_IDS = [
    # Chinese state-apparatus (Xi Office / CCP Politburo)
    "SRC-003",   # Xinhua World News
    "SRC-046",   # Global Times
    "SRC-056",   # South China Morning Post (Hong Kong, CCP-influenced since 2016)
    "SRC-071",   # CGTN (China Global Television Network)
    "SRC-077",   # CASS Institute of World Economics and Politics
    # Russian state-apparatus (Putin Office / Kremlin apparatus)
    "SRC-004",   # Kremlin News
    "SRC-045",   # TASS — Russian News Agency
    "SRC-061",   # Valdai Discussion Club (Kremlin-aligned think tank)
    "SRC-074",   # RT (Russia Today) English
    # Iranian state-apparatus (Khamenei Office / IRGC)
    "SRC-073",   # Press TV
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
    def __init__(self, tpm_limit: int = None, provider: str = None,
                 model: str = None):
        """TPM limit comes from the registry with margin (CC-1c).

        Passing an explicit tpm_limit still works for tests; otherwise the
        (provider, model) pair resolves its real TPM from LIMITS. A hardcoded
        6000 was correct only for the Groq era and silently wrong the moment a
        role moved provider.
        """
        if tpm_limit is None:
            lim = limits_for(provider or PROVIDER, model or MODEL) or {}
            tpm = lim.get("TPM")
            tpm_limit = int(tpm * 0.85) if tpm else 6000
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
        """Wait until window has headroom. Logs every wait. Returns when safe.

        CC-1c over-limit guard: if a single request needs more than the whole
        limit, no amount of waiting can ever satisfy it -- `used + needed <=
        limit` is unsatisfiable even at used=0. The original loop had no
        timeout and no escape, so it would spin forever and burn the job.
        Log it and proceed: the provider will answer with a real 429/413 that
        the retry path can read, which beats an invisible hang.
        """
        if tokens_needed > self.tpm_limit:
            log.error(
                f"[TPMGuard{' '+label if label else ''}] request needs "
                f"{tokens_needed} tokens but the limit is {self.tpm_limit} — "
                f"no wait can satisfy this. Proceeding and letting the provider "
                f"answer rather than looping forever."
            )
            return
        while True:
            used = self.tokens_in_last_60s()
            if used + tokens_needed <= self.tpm_limit:
                return
            wait = 10
            log.info(f"[TPMGuard{' '+label if label else ''}] "
                     f"{used}/{self.tpm_limit} TPM used — waiting {wait}s...")
            time.sleep(wait)


def _retry_after_seconds(exc) -> Optional[float]:
    """Pull the provider's retry-after from a 429, or None if absent (CC-1c).

    Checks the response headers first (authoritative), then falls back to the
    'try again in 7.2s' phrasing Groq puts in the message body. Returns None
    rather than guessing so the caller's default is explicit and visible.
    """
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None)
    if headers:
        for name in ("retry-after", "Retry-After", "x-ratelimit-reset-tokens"):
            raw = headers.get(name)
            if not raw:
                continue
            try:
                return min(float(str(raw).rstrip("s")), 60.0)
            except (TypeError, ValueError):
                continue
    m = re.search(r"try again in ([0-9.]+)s", str(exc))
    if m:
        try:
            return min(float(m.group(1)), 60.0)
        except ValueError:
            pass
    return None


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def get_client() -> Cerebras:
    """Client on THIS position's registry key (LR-094) — no borrowing.

    Mirrors S3-D's three-line Cerebras factory; there is no shared helper and
    inventing one is a refactor for another day. The old get_groq() fell back
    to GROQ_API_KEY when its own key was missing, which broke quota isolation
    and hid the misconfiguration. Missing key now fails loudly.
    """
    key = os.environ.get(KEY_ENV)
    if not key:
        raise RuntimeError(
            f"{KEY_ENV} is not set. S2-D runs on its own key only (LR-094) — "
            f"refusing to borrow another position's quota."
        )
    return Cerebras(api_key=key)


def fetch_adversarial_articles(sb: Client, cycle: Optional[str] = None) -> list[dict]:
    """Fetch recent articles from adversarial STATE-APPARATUS sources.

    LENS-017 B-2: per-source round-robin. Previous behavior sorted all rows by
    collected_at desc and LIMIT 30, which caused TASS to monopolize output
    (20/30 rows = 67% of analysis input). Round-robin gives each live source
    up to per_source_cap slots, so diverse state-apparatus narratives surface.
    """
    per_source_cap = max(6, MAX_ARTICLES // max(1, len(ADVERSARIAL_SOURCE_IDS)))
    collected: list[dict] = []
    by_source: dict[str, int] = {}

    try:
        for src_id in ADVERSARIAL_SOURCE_IDS:
            try:
                result = sb.table("lens_raw_articles") \
                    .select("id, source_id, title, content, url, collected_at") \
                    .eq("source_id", src_id) \
                    .order("collected_at", desc=True) \
                    .limit(per_source_cap) \
                    .execute()
                rows = result.data or []
                collected.extend(rows)
                by_source[src_id] = len(rows)
            except Exception as inner:
                log.warning(f"Source {src_id} fetch failed: {inner}")
                by_source[src_id] = 0

        # Re-sort combined pool by recency for prompt-building determinism
        collected.sort(key=lambda r: r.get("collected_at") or "", reverse=True)

        # Overall cap (prompt-size safety)
        if len(collected) > MAX_ARTICLES:
            collected = collected[:MAX_ARTICLES]

        log.info(f"Fetched {len(collected)} adversarial articles (round-robin, per_source_cap={per_source_cap})")
        log.info(f"Source breakdown: {by_source}")

        # Flag silent sources for operator visibility (PHI-003 absence-is-evidence)
        silent = [s for s, n in by_source.items() if n == 0]
        if silent:
            log.info(f"Sources with zero articles this cycle: {silent} (forensically significant absence)")

        return collected
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
            # BUG-001 (docs/LENS_KNOWN_BUGS.md): this 9000-char cap fires INSIDE a batch the
            # caller already sized to 4500 tokens, so ~1/3 of each batch is dropped and the
            # user message still states the pre-truncation count. Post-cert fix, own probe.
            log.info(f"Prompt cap reached at {included} articles")
            break

        sections.append(entry)
        total_chars += len(entry)
        included += 1

    log.info(f"Built prompt with {included} articles ({total_chars} chars)")
    return "\n---\n".join(sections)


def _art_cost(art):
    """Estimated token cost of one article as it will appear in the prompt.

    Module level so measuring instruments (probe_lens_models.py) import the
    REAL function instead of mirroring a copy. A duplicated copy of production
    logic inside the tool that certifies production is the same dual-source
    disease the LENS-028 registry cured -- when the copy drifts, certs lie.
    """
    sid  = art.get('source_id', '')
    ttl  = art.get('title', '') or ''
    body = (art.get('content', '') or '')[:MAX_ARTICLE_CHARS]
    return max(1, len('[' + sid + '] ' + ttl + '\n' + body + '\n---\n') // 4)


def _split_batches(arts, budget):
    """Greedy token-aware batching. See _art_cost for why this is module level."""
    batches, cur, cur_tok = [], [], 0
    for art in arts:
        cost = _art_cost(art)
        if cur and cur_tok + cost > budget:
            batches.append(cur)
            cur, cur_tok = [], 0
        cur.append(art)
        cur_tok += cost
    if cur:
        batches.append(cur)
    return batches


def call_adversary_analyst(client, articles: list[dict], guard: "TPMGuard") -> Optional[dict]:
    """Call the registry-wired model to analyze adversarial narrative."""
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

    # Budget from the registry, fitted to this prompt (D-007). gpt-oss is a
    # reasoning model that spends ~1500-1900 tokens thinking before it writes,
    # so the old flat MAX_TOKENS=2000 sat close to the starvation line.
    prompt_chars = len(SYSTEM_PROMPT) + len(user_message)
    max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, PROVIDER, MODEL)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"S2-D calling {PROVIDER}/{MODEL} "
                     f"(attempt {attempt}, prompt {prompt_chars} chars, "
                     f"max_tokens {max_tokens})")
            # The vaccine, with teeth: an unregistered pair raises BEFORE the
            # HTTP call. Blast radius is this position only — S2-D dying loudly
            # beats S2-D 404ing silently for 10 days under green checks.
            assert_model_known(PROVIDER, MODEL)
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_message},
                ],
                max_tokens=max_tokens,
                temperature=TEMPERATURE,
            )

            # CC-1c: record REAL usage, never a guess. The old caller logged
            # b_tok + 800 and never counted the completion at all, so the
            # rolling window understated every call by the whole response --
            # which is how the 2026-07-28 cert earned four 429s.
            usage = getattr(response, "usage", None)
            if usage and getattr(usage, "total_tokens", None):
                guard.log_usage(usage.total_tokens)
                details = getattr(usage, "completion_tokens_details", None)
                # Cerebras reports reasoning_tokens; Groq offers no such field.
                # Absent = None, never a guess (CC-1c).
                reasoning = getattr(details, "reasoning_tokens", None) if details else None
                log.info(
                    f"S2-D usage: prompt={usage.prompt_tokens} "
                    f"completion={usage.completion_tokens} "
                    f"total={usage.total_tokens} "
                    f"reasoning={reasoning if reasoning is not None else 'n/a'} "
                    f"budget_used={usage.completion_tokens / max_tokens:.0%}"
                )

            raw = response.choices[0].message.content.strip()

            # Strip reasoning blocks. Kept for gpt-oss-120b: the CC-5 probe
            # returned clean JSON with no <think> wrapper, but reasoning models
            # can emit one and the cost of keeping this is zero.
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

        except LensModelRegistryError:
            # Never retried, never swallowed. An unregistered model is a build
            # bug, not a transient fault — the generic handler below would turn
            # it into two pointless retries and a quiet None, which is exactly
            # the silent-corpse shape the vaccine exists to prevent.
            raise
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as e:
            err = str(e)
            if "429" in err:
                # CC-1c: honour the provider's own retry-after. A flat 20s is a
                # guess that is either wasteful or too short; the provider knows
                # exactly when the window reopens.
                wait = _retry_after_seconds(e)
                if wait is None:
                    wait = 20
                    log.warning(f"Rate limit (429) attempt {attempt} — no "
                                f"retry-after given, falling back to {wait}s")
                else:
                    log.warning(f"Rate limit (429) attempt {attempt} — "
                                f"provider retry-after {wait}s")
                time.sleep(wait)
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

    # ── Pre-flight quota guard (LENS-022 T4) ─────────────────────────────
    try:
        _sb_guard = get_supabase()
        quota_guard = guard_check_with_fallback(positions=["S2-D"], run_id=run_id, sb=_sb_guard)
        skip_result = next((r for r in quota_guard if r.decision == "SKIP" and "S2-D" in r.positions), None)
        if skip_result:
            log.warning(f"S2-D SKIPPED by quota guard: {skip_result.reason}")
            return {"status": "SKIP", "reason": skip_result.reason}
    except Exception as _guard_err:
        log.warning(f"S2-D guard check failed (non-fatal): {_guard_err}")
    # ─────────────────────────────────────────────────────────────────────────
    try:
        sb     = get_supabase()
        client = get_client()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    articles = fetch_adversarial_articles(sb, cycle)
    if not articles:
        log.warning("No adversarial articles found — S2-D cannot run")
        return {"status": "NO_ARTICLES", "articles_analyzed": 0}

    # TPM limit resolves from the registry (CC-1c): Cerebras 30,000 * 0.85
    # = 25,500. No hardcoded number survives a provider move.
    guard = TPMGuard(provider=PROVIDER, model=MODEL)

    # -- Token-aware batch processing (LENS-022) --------------------------
    # TOKEN_BUDGET stays 4500 deliberately. It sizes the PROMPT per batch and
    # is unrelated to the TPM ceiling; changing it changes what S2-D analyses
    # per call, which is BUG-001 territory and gets its own probe + cert.
    # Measure token cost per article, fill batches greedily,
    # use TPMGuard.wait_if_needed() before each call -- no fixed sleep.
    TOKEN_BUDGET = 4500  # safe under 6000 (system prompt ~800 + response ~700)

    batches = _split_batches(articles, TOKEN_BUDGET)
    log.info('S2-D: %d articles -> %d token-aware batches', len(articles), len(batches))

    analyses = []
    for batch_num, batch in enumerate(batches, 1):
        b_tok = sum(_art_cost(a) for a in batch)
        log.info('S2-D batch %d/%d: %d articles (~%d tokens)', batch_num, len(batches), len(batch), b_tok)
        guard.wait_if_needed(b_tok, label='S2-D batch ' + str(batch_num))
        result = call_adversary_analyst(client, batch, guard)
        if result is not None:
            # usage is logged inside call_adversary_analyst from the real
            # response.usage (CC-1c) -- no guessed b_tok + 800 here any more.
            analyses.append(result)
        else:
            log.warning('S2-D batch %d failed -- continuing', batch_num)

    if not analyses:
        return {'status': 'ANALYSIS_FAILED', 'articles_analyzed': len(articles)}

    # Merge: highest consistency as primary, pool all key_claims
    analysis = max(analyses, key=lambda a: a.get('narrative_consistency_score', 0))
    if len(analyses) > 1:
        all_claims = []
        for a in analyses:
            all_claims.extend(a.get('key_claims', []))
        analysis['key_claims'] = all_claims
        log.info('S2-D %d batches merged: %d total claims', len(analyses), len(all_claims))

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
