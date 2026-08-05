"""
lens_framing_rubrics.py v2 — S2-F Operations-Based Pretense Detection
Project Lens | LENS-019.5

CHANGE FROM v1: Operations-based detection replaces 5-axis scoring as primary
unit. Catalog of 29 operations loaded from data/lens-OPS-001_catalog_v3_1.json
(versioned filename per LR-088 discipline).

Each detection: operation present in article (true/false), evidence phrase
quoted from article, brief reasoning, alternative hypotheses listed.

Detection stage discipline (per operator architectural decision):
  - early_warning operations (26): can fire Watch Alert as primary trigger
  - post_suspect operations (3): only meaningful AFTER voice is flagged by
    early_warning operations. NEVER counted toward Watch Alert thresholds.
    OP-024 Country-as-apparatus collapse, OP-025 On-behalf-of-people pretense,
    OP-026 Apparatus-criticism weaponized into people-targeting are post_suspect
    because country-name-as-apparatus convention is journalistic industry baseline.

Two API modes:
  - detect_operations_in_article(stage='early_warning')
      Used by Watch aggregator. Scores only the 26 early_warning operations.
  - detect_operations_in_article(stage='all')
      Used by Clarity and Verification aggregators on already-suspected voices.
      Scores all 29 operations including the 3 post_suspect operations.

Window-agnostic: this module produces ONE detection result for ONE article.
Aggregation into Watch/Clarity/Verification alerts is LENS-020's job.

Rules honored (from session record):
  LR-076: read before write (this module reads the catalog; LENS-020 writes detections)
  LR-080: write-then-verify (N/A here; LENS-020's concern)
  LR-083: investigation not research-paper (operations name candidates, not conclusions)
  LR-088: versioned content needs versioned filenames
  LENS-014: non-touching philosophy (rubric scores; does not modify scored voices)
  LENS-004 SHARED_RULES: no predictions, food-for-thought questions only
  PHI-002: pro-people and anti-pretense as root question
  PHI-003: Office vocabulary (Xi Office, Putin Office, Trump Office, etc.)
  PHI-004: cognitive sovereignty cadence (alert is the medicine, timing is the dose)

Dependencies:
  - groq SDK (already in requirements.txt)
  - no supabase client here (this is a pure detector)
  - Env: GROQ_S2F_API_KEY (preferred) or GROQ_API_KEY (fallback)
  - Data: data/lens-OPS-001_catalog_v3_1.json must be present at module init
"""

import os
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2F-RUBRIC] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("s2f_rubric")


# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════
MAX_TOKENS = 6000              # Larger than v1 — operations output is more verbose
TEMPERATURE = 0.1              # Deterministic detection
ARTICLE_BODY_CHARS = 4500      # Slightly larger than v1 — operations need more context
MIN_BODY_CHARS = 400
REQUEST_TIMEOUT_SEC = 600       # Larger than v1 — bigger prompt + bigger output

CATALOG_PATH = "data/lens-OPS-001_catalog_v4_0.json"


# ══════════════════════════════════════════════════════════════════════════════
# Catalog loading
# ══════════════════════════════════════════════════════════════════════════════
def _resolve_catalog_path() -> Path:
    """Locate catalog file; tolerates being called from repo root or code/."""
    candidates = [
        Path(CATALOG_PATH),
        Path("..") / CATALOG_PATH,
        Path(__file__).parent.parent / CATALOG_PATH,
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"Catalog not found. Tried: {[str(p) for p in candidates]}"
    )


_catalog_cache = None

def _load_catalog() -> dict:
    """Load operations catalog. Cached after first load."""
    global _catalog_cache
    if _catalog_cache is None:
        path = _resolve_catalog_path()
        with open(path, encoding="utf-8") as f:
            _catalog_cache = json.load(f)
        log.info(
            f"Catalog loaded: {_catalog_cache['catalog_id']} "
            f"{_catalog_cache['catalog_version']} — "
            f"{len(_catalog_cache['operations'])} operations"
        )
    return _catalog_cache


# ══════════════════════════════════════════════════════════════════════════════
# PHI-003 legitimacy categories (operator-locked, LENS-017)
# ══════════════════════════════════════════════════════════════════════════════
LEGITIMACY_CATEGORY = {
    "elected_bounded":        "Elected, term-bounded, constitutionally constrained",
    "hybrid_contested":       "Elected with eroded constraints (still bounded but degraded)",
    "unelected_indefinite":   "Unelected, indefinite tenure, unconstrained by popular mandate",
    "unknown":                "Unknown or not applicable",
}

OFFICE_LEGITIMACY = {
    # Unelected-indefinite
    "xi_office":       "unelected_indefinite",
    "putin_office":    "unelected_indefinite",
    "khamenei_office": "unelected_indefinite",
    "kim_office":      "unelected_indefinite",
    "mbs_office":      "unelected_indefinite",
    "min_aung_hlaing_junta": "unelected_indefinite",
    # Hybrid contested
    "erdogan_office":  "hybrid_contested",
    "orban_office":    "hybrid_contested",
    "modi_office":     "hybrid_contested",
    # Elected bounded
    "trump_office":    "elected_bounded",
    "biden_office":    "elected_bounded",
    "netanyahu_office":"elected_bounded",
    "macron_office":   "elected_bounded",
    "starmer_office":  "elected_bounded",
    "eu_commission":   "elected_bounded",
}


# ══════════════════════════════════════════════════════════════════════════════
# Prompt construction
# ══════════════════════════════════════════════════════════════════════════════
_PROMPT_HEAD = """You are S2-F Operations-Based Pretense Detector for Project Lens.

Project Lens is a pretense detector built on PHI-002: pro-people and anti-pretense.
Your job: read ONE article and detect which catalog operations are present in
the article's framing of ONE specific state_actor_lens, written by ONE specific
voice (byline author or quoted expert).

You DO NOT detect bias. Everyone has opinions. You detect SPECIFIC PRETENSE
OPERATIONS from a curated catalog. Each operation is a concrete move that fake
gentlemen perform in their writing — moves that are individually plausible but
cumulatively harmful to readers' cognitive sovereignty.

VOCABULARY DISCIPLINE (PHI-003, non-negotiable):
  - Refer to apparatus by Office name in your reasoning: "Xi Office", "Putin Office",
    "Trump Office", "Khamenei Office", "Netanyahu Office", "Modi Office", etc.
  - The peoples (Chinese, Russian, American, Iranian, Israeli, Indian) are
    intended beneficiaries of Project Lens, never targets.
  - When you quote evidence_phrase from the article, quote the article's actual
    words even if they violate this discipline. Your reasoning uses Office names;
    quoted evidence preserves the article's vocabulary.

WHAT YOU MUST NOT DO:
  - Never predict future behavior ("will happen", "is going to do X").
  - Never conclude voice intent ("Voice X is state asset of Y").
  - Never evaluate writing quality (the best pretense is well-written).
  - Never mark an operation present without an evidence phrase quoted from the article.
  - Never invent text — every evidence_phrase must be verbatim from the article.

DETECTION DISCIPLINE:
  - For each operation in the catalog, decide: PRESENT, NOT PRESENT, or NOT APPLICABLE.
  - PRESENT requires evidence_phrase (verbatim from article) AND brief reasoning.
  - NOT PRESENT means you looked and the operation is not in this article.
  - NOT APPLICABLE means the operation could not apply (e.g., article does not
    cover this state_actor_lens at all).
  - Do not force detection. False positives harm signal-to-noise.
  - Do not avoid detection. False negatives miss real pretense.

THE CATALOG (operations to detect):
"""


def _build_catalog_block(catalog: dict, stage_filter: str, article_chars: int = 99999) -> str:
    """Build the part of the prompt that describes each operation.

    stage_filter:
      'early_warning' -> only score the 26 early_warning operations (Watch mode)
      'all'           -> score all 29 operations (Clarity/Verification mode)
    article_chars: length of article body — used to filter genre-specific ops (v4)
    """
    lines = []
    for op in catalog["operations"]:
        if stage_filter == "early_warning" and op["detection_stage"] != "early_warning":
            continue
        # v4 genre filter: steno ops only for short articles
        if op.get("genre_context") == "stenographic" and article_chars > 3000:
            continue
        max_chars = op.get("max_article_chars")
        if max_chars and article_chars > max_chars:
            continue
        lines.append(f"\n--- {op['id']}: {op['name']} ---")
        lines.append(f"Detection stage: {op['detection_stage']}")
        lines.append(f"Primary lens: {op['primary_lens']} ({catalog['lenses'][str(op['primary_lens'])]})")
        if op.get("secondary_lens"):
            lines.append(f"Secondary lens: {op['secondary_lens']} ({catalog['lenses'][str(op['secondary_lens'])]})")
        lines.append(f"Description: {op['description']}")
        lines.append(f"Evidence pattern (what to look for): {op['evidence_pattern']}")
        if op.get("watch_protocol"):
            lines.append(f"Watch protocol: {op['watch_protocol']}")
    return "\n".join(lines)


_PROMPT_TAIL_TEMPLATE = """

OUTPUT FORMAT — return ONLY valid JSON. No preamble. No markdown fences.

{{
  "analyst": "S2-F",
  "rubric_version": "v2-operations",
  "catalog_version": "{catalog_version}",
  "state_actor_lens": "<the office name you were asked to score against>",
  "stage_filter": "{stage_filter}",
  "operations_detected": [
    {{
      "id": "<OP-NNN from catalog>",
      "name": "<operation name from catalog>",
      "present": true,
      "evidence_phrase": "<exact phrase from article, max 250 chars, verbatim>",
      "reasoning": "<1-3 sentences why this operation is present, max 400 chars>",
      "primary_lens": <int>,
      "detection_stage": "<early_warning or post_suspect>",
      "alternative_hypotheses_considered": [
        "<one alternative explanation that could make this detection wrong>"
      ]
    }}
  ],
  "operations_not_present": [
    {{
      "id": "<OP-NNN>",
      "reason": "<one sentence why operation is absent or not applicable, max 200 chars>"
    }}
  ],
  "confidence": <0.0-1.0, how confident you are this article is scoreable against this lens>,
  "not_applicable": <true|false, true if article does not substantially address this state_actor_lens>,
  "food_for_thought": "<1 open question for the reader to hold while watching this voice, max 200 chars>"
}}

If not_applicable is true, operations_detected may be empty and confidence should be < 0.3.
food_for_thought is always required (per LENS-004 SHARED_RULES)."""


def _build_system_prompt(stage_filter: str, article_chars: int = 99999) -> str:
    """Construct the full system prompt for the given stage filter."""
    catalog = _load_catalog()
    head = _PROMPT_HEAD
    catalog_block = _build_catalog_block(catalog, stage_filter, article_chars)
    tail = _PROMPT_TAIL_TEMPLATE.format(
        catalog_version=catalog["catalog_version"],
        stage_filter=stage_filter,
    )
    return head + catalog_block + tail


# ══════════════════════════════════════════════════════════════════════════════
# TPMGuard (adapted from lens_s2e_legitimacy.py)
# ══════════════════════════════════════════════════════════════════════════════
class TPMGuard:
    def __init__(self, tpm_limit: int = 6000):
        self.tpm_limit = tpm_limit
        self.usage_log = []

    def tokens_in_last_60s(self) -> int:
        now = time.time()
        self.usage_log = [(t, tok) for t, tok in self.usage_log if t > now - 60.0]
        return sum(tok for _, tok in self.usage_log)

    def log_usage(self, tokens: int):
        self.usage_log.append((time.time(), tokens))

    def wait_if_needed(self, tokens_needed: int, label: str = ""):
        while True:
            used = self.tokens_in_last_60s()
            if used + tokens_needed <= self.tpm_limit:
                return
            wait = 10
            log.info(f"[TPMGuard {label}] {used}/{self.tpm_limit} TPM — waiting {wait}s")
            time.sleep(wait)


_tpm_guard = TPMGuard(tpm_limit=6000)


# ══════════════════════════════════════════════════════════════════════════════
# LLM client -- provider-agnostic, one of five explicit providers
# ══════════════════════════════════════════════════════════════════════════════
# Provider selection via the S2F_PROVIDER env var.
# THERE IS NO DEFAULT: an unset or unrecognised value logs an error and
# returns (None, None, None). The five explicit providers are:
#   "cerebras"   -- CEREBRAS_API_KEY  (+ CEREBRAS_MODEL, default gpt-oss-120b)
#   "openrouter" -- OPENROUTER_API_KEY (model pinned openai/gpt-oss-120b:free)
#   "ollama"     -- OLLAMA_MODEL required (+ OLLAMA_HOST, default localhost:11434)
#   "cloudflare" -- CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID
#                   (+ CLOUDFLARE_MODEL, default @cf/openai/gpt-oss-120b)
#   "mistral"    -- MISTRAL_API_KEY   (+ MISTRAL_MODEL, default mistral-medium-latest)
# All five expose an OpenAI-compatible chat.completions.create() interface.
# Returned tuple: (client_object, model_name, provider_name) or (None, None, None)


def _get_llm_client():
    """Provider-agnostic LLM client factory.

    Returns (client, model_name, provider) on success or (None, None, None).
    """
    provider = os.environ.get("S2F_PROVIDER", "").lower().strip()

    if provider == "cerebras":
        try:
            from cerebras.cloud.sdk import Cerebras
        except ImportError:
            log.error("cerebras-cloud-sdk not installed — pip install cerebras-cloud-sdk")
            return None, None, None
        key = os.environ.get("CEREBRAS_API_KEY", "")
        if not key:
            log.error("S2F_PROVIDER=cerebras but CEREBRAS_API_KEY not set")
            return None, None, None
        cerebras_model = os.environ.get("CEREBRAS_MODEL", "gpt-oss-120b")
        log.info(f"Using Cerebras provider (model: {cerebras_model})")
        return Cerebras(api_key=key), cerebras_model, "cerebras"

    if provider == "openrouter":
        try:
            from openai import OpenAI
        except ImportError:
            log.error("openai SDK not installed — pip install openai")
            return None, None, None
        key = os.environ.get("OPENROUTER_API_KEY", "")
        if not key:
            log.error("S2F_PROVIDER=openrouter but OPENROUTER_API_KEY not set")
            return None, None, None
        # OpenRouter uses OpenAI-compatible API at custom base URL.
        # Llama 4 Maverick free variant: meta-llama/llama-4-maverick:free
        # Free tier limits: 20 RPM, 50 req/day (1000/day with $10 balance)
        client = OpenAI(
            api_key=key,
            base_url="https://openrouter.ai/api/v1",
        )
        model = "openai/gpt-oss-120b:free"
        log.info(f"Using OpenRouter provider (model: {model})")
        return client, model, "openrouter"

    if provider == "ollama":
        try:
            from openai import OpenAI
        except ImportError:
            log.error("openai SDK not installed — pip install openai")
            return None, None, None
        model = os.environ.get("OLLAMA_MODEL", "")
        if not model:
            log.error("S2F_PROVIDER=ollama but OLLAMA_MODEL not set")
            log.error("Set OLLAMA_MODEL to a tag ollama has pulled (e.g. 'llama3:8b', 'qwen3.5:9b')")
            return None, None, None
        host = os.environ.get("OLLAMA_HOST", "localhost:11434")
        # Ollama OpenAI-compatible endpoint. API key is not validated but must be non-empty.
        client = OpenAI(
            api_key="ollama",  # Ollama ignores this but openai SDK requires non-empty
            base_url=f"http://{host}/v1",
        )
        log.info(f"Using Ollama provider (model: {model}, host: {host})")
        return client, model, "ollama"

    if provider == "cloudflare":
        try:
            from openai import OpenAI
        except ImportError:
            log.error("openai SDK not installed — pip install openai")
            return None, None, None
        token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
        if not token or not account_id:
            log.error("S2F_PROVIDER=cloudflare but CLOUDFLARE_API_TOKEN or CLOUDFLARE_ACCOUNT_ID not set")
            return None, None, None
        cf_model = os.environ.get("CLOUDFLARE_MODEL", "@cf/openai/gpt-oss-120b")
        client = OpenAI(
            api_key=token,
            base_url=f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1",
        )
        log.info(f"Using Cloudflare Workers AI provider (model: {cf_model})")
        return client, cf_model, "cloudflare"
    if provider == "mistral":
        try:
            from openai import OpenAI
        except ImportError:
            log.error("openai SDK not installed — pip install openai")
            return None, None, None
        key = os.environ.get("MISTRAL_API_KEY", "")
        if not key:
            log.error("S2F_PROVIDER=mistral but MISTRAL_API_KEY not set")
            return None, None, None
        mistral_model = os.environ.get("MISTRAL_MODEL", "mistral-medium-latest")
        client = OpenAI(
            api_key=key,
            base_url="https://api.mistral.ai/v1",
        )
        log.info(f"Using Mistral provider (model: {mistral_model})")
        return client, mistral_model, "mistral"
    if not provider:
        log.error("S2F_PROVIDER is not set. Set one of: "
                  "cerebras, openrouter, ollama, cloudflare, mistral.")
    else:
        log.error(f"S2F_PROVIDER={provider!r} is not a recognised provider. "
                  "Set one of: cerebras, openrouter, ollama, cloudflare, mistral.")
    return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
# Public API — DetectionResult and detect_operations_in_article
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class DetectionResult:
    """Return contract for detect_operations_in_article."""
    status: str                              # "OK" | "LLM_FAILED" | "PARSE_FAILED" | "SKIP_TOO_SHORT" | "SKIP_NO_KEY" | "SKIP_NO_CATALOG"
    state_actor_lens: str
    stage_filter: str                        # "early_warning" | "all"
    catalog_version: str = ""
    rubric_version: str = "v2-operations"
    operations_detected: Optional[list] = None       # list of dicts
    operations_not_present: Optional[list] = None    # list of dicts
    confidence: float = 0.0
    not_applicable: bool = False
    food_for_thought: str = ""
    error: Optional[str] = None

    def operation_count(self) -> int:
        if not self.operations_detected:
            return 0
        return len(self.operations_detected)

    def early_warning_operations(self) -> list:
        if not self.operations_detected:
            return []
        return [op for op in self.operations_detected
                if op.get("detection_stage") == "early_warning"]

    def post_suspect_operations(self) -> list:
        if not self.operations_detected:
            return []
        return [op for op in self.operations_detected
                if op.get("detection_stage") == "post_suspect"]


def detect_operations_in_article(
    article_title: str,
    article_body: str,
    article_source: str,
    voice_name: str,
    voice_type: str,                # "author" | "expert" | "official" | "think_tank"
    state_actor_lens: str,          # e.g. "xi_office", "trump_office"
    stage_filter: str = "early_warning",
) -> DetectionResult:
    """Detect catalog operations in ONE article × ONE voice × ONE state_actor_lens.

    Window-agnostic. Returns a DetectionResult. LENS-020 handles cron + DB persistence.

    Args:
        stage_filter:
            "early_warning" — score only the 26 early_warning operations
                              (used by Watch aggregator)
            "all"           — score all 29 operations including the 3 post_suspect
                              (used by Clarity/Verification aggregators on
                              already-suspected voices)

    Returns DetectionResult(status="OK") on success. Never raises.
    """
    # ── Catalog availability ──
    try:
        catalog = _load_catalog()
    except FileNotFoundError as e:
        return DetectionResult(
            status="SKIP_NO_CATALOG",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            error=str(e),
        )

    # ── Validate stage_filter ──
    if stage_filter not in ("early_warning", "all"):
        return DetectionResult(
            status="SKIP_BAD_STAGE",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            catalog_version=catalog["catalog_version"],
            error=f"stage_filter must be 'early_warning' or 'all', got {stage_filter!r}",
        )

    # ── Body too short ──
    body = (article_body or "").strip()
    if len(body) < MIN_BODY_CHARS:
        return DetectionResult(
            status="SKIP_TOO_SHORT",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            catalog_version=catalog["catalog_version"],
            error=f"Body {len(body)} chars < {MIN_BODY_CHARS} minimum",
        )

    # ── LLM client (provider-agnostic) ──
    client, model_name, provider = _get_llm_client()
    if client is None:
        return DetectionResult(
            status="SKIP_NO_KEY",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            catalog_version=catalog["catalog_version"],
            error="No LLM API key available",
        )

    # ── Legitimacy context for prompt ──
    legitimacy = OFFICE_LEGITIMACY.get(state_actor_lens, "unknown")

    # ── Build prompt ──
    system_prompt = _build_system_prompt(stage_filter, article_chars=len(body))
    user_msg = (
        f"VOICE TO ANALYZE:\n"
        f"  Name: {voice_name}\n"
        f"  Type: {voice_type}\n\n"
        f"STATE ACTOR LENS: {state_actor_lens}\n"
        f"LEGITIMACY CATEGORY (PHI-003): {legitimacy}\n"
        f"STAGE FILTER: {stage_filter}\n\n"
        f"ARTICLE:\n"
        f"  Source: {article_source}\n"
        f"  Title: {article_title}\n"
        f"  Body:\n{body[:ARTICLE_BODY_CHARS]}\n\n"
        f"Detect which catalog operations are present in this article's framing "
        f"of {state_actor_lens}. Return JSON only."
    )

    # ── Call LLM ──
    estimated_tokens = 3000  # bigger prompt for ops detection
    _tpm_guard.wait_if_needed(estimated_tokens, label=f"S2F-{state_actor_lens}-{provider}")

    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_msg},
            ],
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        raw = resp.choices[0].message.content.strip()
        _tpm_guard.log_usage(estimated_tokens)
    except Exception as e:
        return DetectionResult(
            status="LLM_FAILED",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            catalog_version=catalog["catalog_version"],
            error=f"{provider} call: {str(e)[:200]}",
        )

    # ── Strip code fences ──
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return DetectionResult(
            status="PARSE_FAILED",
            state_actor_lens=state_actor_lens,
            stage_filter=stage_filter,
            catalog_version=catalog["catalog_version"],
            error=f"JSON parse: {str(e)[:200]}",
        )

    # ── Empty-field validator (LENS-019.5 calibration round 3 finding) ──
    # LLM sometimes returns operations with empty evidence_phrase or reasoning.
    # Per prompt contract, every "present" operation MUST have evidence_phrase
    # (verbatim from article) AND reasoning. Filter out malformed entries
    # rather than passing them to downstream consumers.
    raw_ops = parsed.get("operations_detected", []) or []
    valid_ops = []
    rejected_ops = []
    for op in raw_ops:
        evidence = (op.get("evidence_phrase") or "").strip()
        reasoning = (op.get("reasoning") or "").strip()
        if not evidence or not reasoning:
            rejected_ops.append({
                "id": op.get("id"),
                "reason": "empty evidence_phrase or reasoning",
            })
            continue
        valid_ops.append(op)
    if rejected_ops:
        log.warning(
            f"Empty-field validator rejected {len(rejected_ops)} of {len(raw_ops)} "
            f"operations: {[r['id'] for r in rejected_ops]}"
        )

    # ── Build DetectionResult ──
    return DetectionResult(
        status="OK",
        state_actor_lens=parsed.get("state_actor_lens", state_actor_lens),
        stage_filter=parsed.get("stage_filter", stage_filter),
        catalog_version=parsed.get("catalog_version", catalog["catalog_version"]),
        rubric_version=parsed.get("rubric_version", "v2-operations"),
        operations_detected=valid_ops,
        operations_not_present=parsed.get("operations_not_present", []),
        confidence=float(parsed.get("confidence", 0.0) or 0.0),
        not_applicable=bool(parsed.get("not_applicable", False)),
        food_for_thought=str(parsed.get("food_for_thought", ""))[:200],
    )


# ══════════════════════════════════════════════════════════════════════════════
# CLI smoke test
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# Ensemble API — dual-provider detection (LENS-020 production architecture)
# ══════════════════════════════════════════════════════════════════════════════
# Architecture decision DECISION-009 (Apr 29 2026):
#   Primary:   qwen-3-235b on Cerebras  (OP-024-029 structural/apparatus)
#   Secondary: gpt-oss-120b on Cloudflare (OP-002/003/005/008/010/011/015/016/022)
#   Sequential calls with 2s sleep to avoid TPM quota collision.
#   Union of detected operations from both models.

import copy

def detect_operations_ensemble(
    article_title: str,
    article_body: str,
    article_source: str,
    voice_name: str,
    voice_type: str,
    state_actor_lens: str,
    stage_filter: str = "early_warning",
    inter_model_sleep: float = 2.0,
) -> DetectionResult:
    """Run dual-provider ensemble detection and return union of operations.

    Calls qwen-3-235b (Cerebras) then gpt-oss-120b (Cloudflare) sequentially.
    Returns a merged DetectionResult with union of detected operations.
    If one provider fails, returns the other's result (graceful degradation).
    If both fail, returns the first failure result.

    Args:
        inter_model_sleep: seconds to sleep between model calls (default 2s)
                           prevents TPM quota collision on shared Cerebras guard.
    """
    args = dict(
        article_title=article_title,
        article_body=article_body,
        article_source=article_source,
        voice_name=voice_name,
        voice_type=voice_type,
        state_actor_lens=state_actor_lens,
        stage_filter=stage_filter,
    )

    # ── Primary: qwen-3-235b on Cerebras ──
    original_provider = os.environ.get("S2F_PROVIDER", "")
    original_model = os.environ.get("CEREBRAS_MODEL", "")

    os.environ["S2F_PROVIDER"] = "cerebras"
    os.environ["CEREBRAS_MODEL"] = "gpt-oss-120b"
    log.info("[ENSEMBLE] Running primary: qwen-3-235b on Cerebras")
    result_primary = detect_operations_in_article(**args)
    log.info(f"[ENSEMBLE] Primary result: {result_primary.status} "
             f"({result_primary.operation_count()} ops)")

    # ── Sleep between models ──
    log.info(f"[ENSEMBLE] Sleeping {inter_model_sleep}s between models")
    time.sleep(inter_model_sleep)

    # ── Secondary: gpt-oss-120b on Cloudflare ──
    os.environ["S2F_PROVIDER"] = "cloudflare"
    os.environ["CLOUDFLARE_MODEL"] = "@cf/openai/gpt-oss-120b"
    log.info("[ENSEMBLE] Running secondary: gpt-oss-120b on Cloudflare")
    result_secondary = detect_operations_in_article(**args)
    log.info(f"[ENSEMBLE] Secondary result: {result_secondary.status} "
             f"({result_secondary.operation_count()} ops)")

    # ── Restore original env ──
    if original_provider:
        os.environ["S2F_PROVIDER"] = original_provider
    else:
        os.environ.pop("S2F_PROVIDER", None)
    if original_model:
        os.environ["CEREBRAS_MODEL"] = original_model
    else:
        os.environ.pop("CEREBRAS_MODEL", None)

    # ── Graceful degradation ──
    primary_ok = result_primary.status == "OK"
    secondary_ok = result_secondary.status == "OK"

    if not primary_ok and not secondary_ok:
        log.warning("[ENSEMBLE] Both providers failed — returning primary failure")
        return result_primary

    if not primary_ok:
        log.warning("[ENSEMBLE] Primary failed — returning secondary only")
        return result_secondary

    if not secondary_ok:
        log.warning("[ENSEMBLE] Secondary failed — returning primary only")
        return result_primary

    # ── Merge: union of operations ──
    # Use primary as base. Add secondary ops not already detected by primary.
    primary_op_ids = {op["id"] for op in (result_primary.operations_detected or [])}
    secondary_unique = [
        op for op in (result_secondary.operations_detected or [])
        if op["id"] not in primary_op_ids
    ]

    merged_ops = list(result_primary.operations_detected or []) + secondary_unique
    merged_confidence = max(result_primary.confidence, result_secondary.confidence)

    log.info(
        f"[ENSEMBLE] Merged: {len(result_primary.operations_detected or [])} primary "
        f"+ {len(secondary_unique)} secondary-unique = {len(merged_ops)} total ops"
    )

    merged = copy.copy(result_primary)
    merged.operations_detected = merged_ops
    merged.confidence = merged_confidence
    merged.rubric_version = "v2-operations-ensemble"
    # food_for_thought: prefer primary's question (qwen-3 tends to be sharper here)
    if not merged.food_for_thought and result_secondary.food_for_thought:
        merged.food_for_thought = result_secondary.food_for_thought

    return merged

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    if len(sys.argv) > 1 and sys.argv[1] == "--smoke":
        # Quick smoke test against synthetic article
        sample = {
            "title": "Test article — Trump, Xi, and the meaning of nothing",
            "body": (
                "WASHINGTON, April 26 (Test) - When President Donald Trump returned "
                "to office, he vowed to use tariffs to reset relations with China, "
                "which he said was 'killing' the United States. China did not back "
                "down. Beijing retaliated. Now policy appears adrift. Critics argue "
                "such inconsistencies have undermined the U.S. Dr. Jane Doe of CSIS "
                "said 'their entire strategy ran aground'. Meanwhile Wang Dong of "
                "Peking University said 'inconsistency erodes US credibility'. " * 3
            ),
            "source": "LENS-019.5 Smoke Harness",
            "voice_name": "Test Author",
            "voice_type": "author",
        }

        print("=" * 76)
        print("LENS-019.5 Operations-Based Rubric Smoke Test")
        print("=" * 76)
        print()

        for lens in ["xi_office", "trump_office"]:
            for stage in ["early_warning", "all"]:
                print(f"\n── Detecting on {lens} (stage={stage}) ──")
                result = detect_operations_in_article(
                    article_title=sample["title"],
                    article_body=sample["body"],
                    article_source=sample["source"],
                    voice_name=sample["voice_name"],
                    voice_type=sample["voice_type"],
                    state_actor_lens=lens,
                    stage_filter=stage,
                )
                print(f"Status: {result.status}")
                print(f"Operations detected: {result.operation_count()}")
                if result.operations_detected:
                    for op in result.operations_detected:
                        print(f"  - {op.get('id')}: {op.get('name')} "
                              f"[{op.get('detection_stage')}, lens {op.get('primary_lens')}]")
                print(f"Confidence: {result.confidence}")
                print(f"FFT: {result.food_for_thought}")
                if result.error:
                    print(f"Error: {result.error}")

    else:
        print("Usage: python code/lens_framing_rubrics.py --smoke")
        print()
        print("This module is imported by LENS-020 scoring cron; it is not run standalone")
        print("in production. The --smoke flag runs one synthetic article through")
        print("xi_office and trump_office lenses at both early_warning and all stages.")
