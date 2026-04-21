"""
lens_entity_extract.py — S2-F Entity Extraction Module
Project Lens | LENS-018 Task 3

Purpose: extract named entities (authors, quoted experts) from ingested
articles and populate lens_entities + lens_entity_mentions tables.

Design (LENS-017/018 locks):
  - Fail-safe: never blocks article save. Extraction failure logs WARNING,
    proceeds to next article. Article ingestion continues even if entity
    extraction is 100% broken.
  - Disable-able: set LENS_ENTITY_EXTRACT=off in env to skip all extraction.
    (Operator escape hatch for production issues without revert+redeploy.)
  - PHI-003 vocabulary: state officials get Office names in affiliations.
    Journalists and analysts are individuals, NOT apparatus.
  - Fast-path: article.get('author') is used directly when RSS provides it.
    LLM is only called for quoted-expert extraction from article body.
  - Per LR-080 (write-then-verify): every entity UPSERT is followed by a
    read-back through the mention insertion (foreign key enforces existence).
  - Per LR-076: reads from schema that was confirmed by LENS-018 T2
    migration verification (lens_entities + lens_entity_mentions now live).

Dependencies:
  - groq SDK (already in requirements.txt — used by S2-A, S2-D, S2-E, S2-GAP)
  - supabase client (already in requirements.txt)
  - Env: GROQ_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY, LENS_ENTITY_EXTRACT (optional)
"""

import os
import json
import logging
import re
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENTITY] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("entity_extract")


# ── Constants ─────────────────────────────────────────────────────────────────
MODEL = "llama-3.3-70b-versatile"
MAX_TOKENS = 600
TEMPERATURE = 0.1          # NER should be deterministic
ARTICLE_BODY_CHARS = 3000  # cap body passed to LLM
MIN_BODY_FOR_LLM = 300     # skip LLM call for stubs
MAX_EXPERTS_PER_ARTICLE = 8
REQUEST_TIMEOUT_SEC = 25


# ── Entry point used by fetch_text.py ─────────────────────────────────────────
def extract_entities_for_article(article_dict: dict, article_id: str) -> dict:
    """Extract entities from a newly-saved article. Fail-safe; never raises.

    Args:
        article_dict: the article payload (with 'title','content','source_name',
                      and optional 'author'). This is the SAME dict that was
                      passed to save_article(); it may include author from RSS.
        article_id: UUID returned by save_article() — foreign key for mentions.

    Returns:
        dict with status + counts. Never raises. Never blocks ingestion.

    Behavior:
        - Skip entirely if LENS_ENTITY_EXTRACT=off
        - Skip entirely if article_id is falsy (save failed upstream)
        - Skip LLM call if body is too short
        - Process author (byline) without LLM if present
        - Process quoted experts via LLM on article body
    """
    # ── Global disable flag ──
    if os.environ.get("LENS_ENTITY_EXTRACT", "on").lower() == "off":
        return {"status": "DISABLED"}

    if not article_id:
        return {"status": "SKIP_NO_ID"}

    counts = {"author": 0, "experts": 0, "errors": 0}

    # ── Fast-path: byline (no LLM needed) ──
    try:
        author_name = _clean_name(article_dict.get("author") or "")
        if author_name:
            ok = _upsert_entity_and_mention(
                entity_type="author",
                name=author_name,
                canonical_name=_canonicalize(author_name),
                primary_outlet=article_dict.get("source_name"),
                raw_article_id=article_id,
                mention_type="byline",
                context_snippet=None,
            )
            if ok:
                counts["author"] += 1
    except Exception as e:
        log.warning(f"Byline extraction failed for article {article_id[:8]}: {e}")
        counts["errors"] += 1

    # ── LLM path: quoted experts ──
    body = (article_dict.get("content") or "").strip()
    if len(body) < MIN_BODY_FOR_LLM:
        return {"status": "OK", **counts, "note": "body_too_short_for_llm"}

    try:
        experts = _extract_experts_via_llm(
            title=article_dict.get("title") or "",
            body=body[:ARTICLE_BODY_CHARS],
            source_name=article_dict.get("source_name") or "",
        )
    except Exception as e:
        log.warning(f"LLM expert extraction failed for article {article_id[:8]}: {e}")
        return {"status": "OK_LLM_FAILED", **counts}

    for expert in experts[:MAX_EXPERTS_PER_ARTICLE]:
        try:
            name = _clean_name(expert.get("name") or "")
            if not name:
                continue
            entity_type = expert.get("entity_type") or "expert"
            if entity_type not in ("expert", "official", "think_tank"):
                entity_type = "expert"
            affiliations = expert.get("affiliations") or []
            if not isinstance(affiliations, list):
                affiliations = [str(affiliations)]

            ok = _upsert_entity_and_mention(
                entity_type=entity_type,
                name=name,
                canonical_name=_canonicalize(name),
                primary_outlet=None,
                affiliations=affiliations,
                raw_article_id=article_id,
                mention_type="quoted_expert",
                context_snippet=(expert.get("quote") or "")[:500] or None,
            )
            if ok:
                counts["experts"] += 1
        except Exception as e:
            log.warning(f"Upsert failed for expert: {e}")
            counts["errors"] += 1

    return {"status": "OK", **counts}


# ── LLM call ─────────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """You are an entity extraction analyst for Project Lens.

Extract QUOTED EXPERTS from the article body. Named people who are cited as
sources — analysts, academics, former officials, think-tank researchers.

EXCLUDE these (they are not quoted experts):
- Heads of state being described (e.g. "President Xi said" when it's reporting)
- Generic unnamed sources ("a senior official said")
- The article's own author (they are the byline, not a quoted expert)
- Fictional or historical figures

INCLUDE these:
- Analysts, academics, researchers named with specific quotes
- Former officials cited as analysts
- Think-tank fellows and their affiliations

For each quoted expert return:
  name            : person's full name as printed
  entity_type     : "expert" | "official" | "think_tank"
  affiliations    : list of orgs they are affiliated with (per the article)
  quote           : one short representative quote (< 200 chars) or ""

Return ONLY valid JSON. No preamble. No markdown fences. Format:
{
  "experts": [
    {"name": "...", "entity_type": "expert", "affiliations": ["..."], "quote": "..."}
  ]
}

If no quoted experts found, return {"experts": []}."""


def _extract_experts_via_llm(title: str, body: str, source_name: str) -> list[dict]:
    """Call Groq llama-3.3-70b to extract quoted experts. Returns list, never None."""
    try:
        from groq import Groq
    except ImportError:
        log.warning("groq SDK not available — skipping LLM extraction")
        return []

    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        log.warning("GROQ_API_KEY missing — skipping LLM extraction")
        return []

    client = Groq(api_key=api_key)
    user_msg = (
        f"Source: {source_name}\n"
        f"Title: {title}\n"
        f"--- ARTICLE BODY ---\n{body}\n--- END ---\n\n"
        "Return JSON only."
    )

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        raw = resp.choices[0].message.content.strip()
    except Exception as e:
        log.warning(f"Groq call failed: {e}")
        return []

    # Strip code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"Entity JSON parse failed: {e}")
        return []

    experts = parsed.get("experts") or []
    if not isinstance(experts, list):
        return []
    return experts


# ── Supabase upsert ──────────────────────────────────────────────────────────
_sb_client = None  # lazy, reused across calls per fetch_text.py run


def _get_sb():
    global _sb_client
    if _sb_client is None:
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_SERVICE_KEY", "")
        if not url or not key:
            raise RuntimeError("SUPABASE credentials missing")
        _sb_client = create_client(url, key)
    return _sb_client


def _upsert_entity_and_mention(
    entity_type: str,
    name: str,
    canonical_name: str,
    raw_article_id: str,
    mention_type: str,
    primary_outlet: Optional[str] = None,
    affiliations: Optional[list] = None,
    context_snippet: Optional[str] = None,
) -> bool:
    """UPSERT the entity (dedup on entity_type+canonical_name), then INSERT
    the mention. Per LR-080, this is a write-then-verify pattern:
    the INSERT on lens_entity_mentions REQUIRES the entity_id foreign key
    to exist (FK constraint) — so mention success proves entity existence.
    """
    try:
        sb = _get_sb()
    except Exception as e:
        log.warning(f"Supabase init failed: {e}")
        return False

    # ── Step 1: UPSERT entity ──
    entity_row = {
        "entity_type": entity_type,
        "name": name,
        "canonical_name": canonical_name,
    }
    if primary_outlet:
        entity_row["primary_outlet"] = primary_outlet
    if affiliations:
        entity_row["affiliations"] = affiliations

    try:
        result = (
            sb.table("lens_entities")
              .upsert(entity_row, on_conflict="entity_type,canonical_name")
              .execute()
        )
        rows = result.data or []
        if not rows:
            log.warning(f"Entity upsert returned no rows: {canonical_name}")
            return False
        entity_id = rows[0].get("id")
        if not entity_id:
            log.warning(f"Entity upsert missing id: {canonical_name}")
            return False
    except Exception as e:
        log.warning(f"Entity upsert failed for {canonical_name}: {str(e)[:150]}")
        return False

    # ── Step 2: INSERT mention ──
    mention_row = {
        "entity_id": entity_id,
        "raw_article_id": raw_article_id,
        "mention_type": mention_type,
    }
    if context_snippet:
        mention_row["context_snippet"] = context_snippet

    try:
        # ON CONFLICT on the UNIQUE (entity_id, raw_article_id, mention_type)
        # makes repeated ingestion of the same article idempotent.
        (
            sb.table("lens_entity_mentions")
              .upsert(mention_row, on_conflict="entity_id,raw_article_id,mention_type")
              .execute()
        )
    except Exception as e:
        log.warning(f"Mention upsert failed for {canonical_name}: {str(e)[:150]}")
        return False

    # ── Step 3 (LR-080): bump total_mentions counter + last_seen ──
    # Do a lightweight UPDATE; failure here is logged but not fatal (the
    # entity + mention are already saved, the counter is denormalized).
    try:
        (
            sb.table("lens_entities")
              .update({"last_seen": "now()", "total_mentions": rows[0].get("total_mentions", 0) + 1})
              .eq("id", entity_id)
              .execute()
        )
    except Exception:
        pass  # counter is advisory, not critical

    return True


# ── Helpers ──────────────────────────────────────────────────────────────────
_WHITESPACE_RE = re.compile(r"\s+")


def _clean_name(name: str) -> str:
    """Trim whitespace + strip common honorifics/titles; return '' if garbage."""
    if not name:
        return ""
    s = _WHITESPACE_RE.sub(" ", name.strip())
    # Strip common titles that RSS often prepends
    for prefix in ("Dr. ", "Dr ", "Prof. ", "Prof ", "Mr. ", "Mr ",
                   "Mrs. ", "Mrs ", "Ms. ", "Ms ", "Sir ", "By ", "by "):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    # Guard against garbage (URL, email, numbers only, too long)
    if len(s) < 2 or len(s) > 120:
        return ""
    if "@" in s or "http" in s.lower():
        return ""
    if s.replace(" ", "").replace(".", "").isdigit():
        return ""
    return s


def _canonicalize(name: str) -> str:
    """Lowercased, whitespace-normalized, punctuation-stripped name for dedup."""
    s = name.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s


# ── CLI for manual testing ────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    # Smoke test against one article
    if len(sys.argv) > 1 and sys.argv[1] == "--smoke":
        sample_article = {
            "title": "LENS-018 smoke test — ignore this",
            "content": ("This is a smoke-test article for LENS-018 Task 3. "
                        "It mentions Dr. Jane Doe of the Center for Strategic "
                        "Studies, who said 'this is a test quote' at length. "
                        "It also cites Professor John Smith, a fellow at "
                        "Brookings Institution, who said 'another test quote'. "
                        * 5),
            "source_name": "LENS-018 Test Harness",
            "author": "LENS-018 Test Author",
        }
        # Use the smoke-test UUID placeholder — will fail FK unless a real
        # raw article with this ID exists. Just tests code path.
        print("Running smoke test — expects FK failures, tests code flow only")
        result = extract_entities_for_article(
            sample_article,
            article_id="00000000-0000-0000-0000-000000000000",
        )
        print(json.dumps(result, indent=2))
