"""
probe_lens_models.py -- LENS-028 CC-5 probe pack.

Measures whether a candidate model can actually do a Lens position's real job,
BEFORE that model is wired into a live cron. Built because the April GNI
evidence against gpt-oss-120b (3/3 failures on Iran/US content) is confounded:
those tests ran at max_tokens=200, and gpt-oss burns ~1500-1600 tokens
reasoning before it writes a word. Silent-empty is ALSO the starvation
symptom. This probe re-tests at proper budgets so the content-filter question
gets a clean answer (D-002, D-008).

READ-ONLY against production data. The only file it writes is
probe_results.jsonl (append-only, committed -- permanent record).
It never writes to Supabase and never mutates a position's state.

Design rules encoded:
  R-S80-2  The fixture is the position's REAL prompt, rebuilt from the same
           live Supabase reads the position performs. Never a toy prompt --
           a toy prompt measures nothing about the real call shape.
  LR-094   Each role probes on its OWN key. Never borrow another position's
           key: a shared-quota probe result is not evidence about the
           position we care about. Key VALUES are never printed or logged.
  LR-095   On any HTTP error, capture r.text[:200] before diagnosing.
  D-007    Budget = fit_max_tokens(prompt_chars, role max_out) -- the same
           formula the production call site will use.

Deliberately does NOT call assert_model_known(): the baseline candidate
(llama-3.3-70b-versatile) is a dying model that is intentionally absent from
the registry. This is a measurement tool, not a production call site -- the
vaccine belongs at call sites, not here.

Pacing: Groq free tier is 8K TPM. One heavy call is ~3K prompt + 2.4K output
= ~5.4K tokens, so two calls inside one minute breach TPM and earn a 429.
The probe sleeps 65s between calls on the same key and prints a running
per-key token total.

Usage:
    python probe_lens_models.py --role s2d_adversary --candidate baseline
    python probe_lens_models.py --role s2d_adversary --candidate primary --trials 3
    python probe_lens_models.py --role s2d_adversary --candidate baseline --dry-run

One role per invocation, on purpose. There is no "run everything" mode:
probes cost real quota on production keys and must be read between waves.

Author: Claude Opus 5 for James Maverick (Bro Alpha) | LENS-028 CC-5
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.join(REPO_ROOT, "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

from dotenv import load_dotenv
load_dotenv()

from lens_models import (          # noqa: E402  (path setup must precede import)
    ROLES,
    fallback,
    fit_max_tokens,
    get_role,
    wire,
)

RESULTS_PATH = os.path.join(REPO_ROOT, "probe_results.jsonl")
FIXTURE_DIR = os.path.join(REPO_ROOT, "probe_fixtures")

# The baseline is the model everything currently runs on. It dies 2026-08-16,
# so this measurement is unrepeatable after that date -- always probe it first.
BASELINE_PROVIDER = "groq"
BASELINE_MODEL = "llama-3.3-70b-versatile"

# Seconds between calls sharing one key. 60s window + 5s safety.
PACING_SLEEP_S = 65

PROVIDER_ENDPOINTS = {
    "groq": "https://api.groq.com/openai/v1/chat/completions",
    "sambanova": "https://api.sambanova.ai/v1/chat/completions",
    # Cerebras exposes an OpenAI-compatible surface at /v1. Production uses the
    # cerebras-cloud-sdk instead; the raw endpoint is used HERE so LR-095 can
    # read r.text on errors and so the usage object arrives unwrapped.
    "cerebras": "https://api.cerebras.ai/v1/chat/completions",
}

REFUSAL_PATTERN = re.compile(
    r"I can't|I cannot|I'm unable|as an AI|I won't|cannot assist|not able to provide",
    re.IGNORECASE,
)

MIN_USEFUL_CONTENT = 200

# D-017: for JSON-output roles, completion consumption above this fraction of
# the budget is MARGINAL, not passing. Evidence: S2-D probed 3/3 clean at 79%
# and truncated in production on a prompt only 312 chars larger, losing 42 of
# 60 articles. Truncated JSON has no partial value.
D017_MARGINAL_RATIO = 0.60


class ProbeError(RuntimeError):
    """Fatal probe misconfiguration -- abort loudly, never guess."""


# ---------------------------------------------------------------------------
# Fixture -- the role's REAL prompt
# ---------------------------------------------------------------------------
@dataclass
class Fixture:
    """One position's real call shape, ready to send."""
    system: str
    user: str
    requires_json: bool
    temperature: float
    origin: str                      # how this fixture was obtained
    detail: dict = field(default_factory=dict)

    @property
    def prompt_chars(self) -> int:
        """Total characters on the wire -- system AND user both count."""
        return len(self.system) + len(self.user)


def _supabase():
    """Production Supabase client, read-only use only."""
    from supabase import create_client
    try:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_KEY"]
    except KeyError as e:
        raise ProbeError(f"Missing {e} -- cannot rebuild a real fixture.")
    return create_client(url, key)


def fixture_s2d_adversary() -> Fixture:
    """Rebuild S2-D's real prompt from live adversarial-source articles.

    Mirrors lens_s2d_adversary.run_s2d() exactly: fetch round-robin articles,
    split into token-aware batches, and take BATCH 1 -- because the real wire
    call is per batch, not per full article set. Probing the whole set would
    measure a prompt the position never actually sends.

    Imports S2-D's REAL _art_cost/_split_batches rather than mirroring them:
    a copy of production logic inside the instrument that certifies production
    drifts silently, and then the certs lie.
    """
    import lens_s2d_adversary as s2d

    sb = _supabase()
    articles = s2d.fetch_adversarial_articles(sb)
    if not articles:
        raise ProbeError(
            "S2-D fixture: zero adversarial articles in lens_raw_articles. "
            "Cannot build the real prompt -- refusing to invent one."
        )

    # Must match run_s2d()'s TOKEN_BUDGET. Not importable (it is a local), so
    # it is asserted against the real batching behaviour below rather than
    # trusted blind.
    token_budget = 4500

    batches = s2d._split_batches(articles, token_budget)
    batch = batches[0]

    # Mirror of call_adversary_analyst()'s user_message construction.
    articles_text = s2d.build_articles_prompt(batch)
    source_ids = list({a.get("source_id") for a in batch})
    user_message = (
        f"Analyze the adversarial narrative from these {len(batch)} "
        f"state media articles.\n\n"
        f"Sources present: {', '.join(source_ids)}\n\n"
        f"--- ARTICLES START ---\n{articles_text}\n--- ARTICLES END ---\n\n"
        f"Return JSON only."
    )

    return Fixture(
        system=s2d.SYSTEM_PROMPT,
        user=user_message,
        requires_json=True,
        temperature=s2d.TEMPERATURE,
        origin="live-rebuild:lens_s2d_adversary",
        detail={
            "articles_fetched": len(articles),
            "batches": len(batches),
            "batch_1_articles": len(batch),
            "batch_1_est_tokens": sum(s2d._art_cost(a) for a in batch),
            "source_ids_in_batch": sorted(s for s in source_ids if s),
        },
    )


def fixture_lens1() -> Fixture:
    """Rebuild Lens-1's real prompt from the live balanced article pool.

    Mirrors run_lens_with_tpm_guard()'s sequence: fetch the balanced pool, trim
    it to THIS provider's budget, rebuild domain blocks, then format
    ANALYSIS_PROMPT. The trim is provider-specific, so the provider is taken
    from the registry -- probing with the wrong provider's budget would measure
    a prompt Lens-1 never sends.

    Output is prose (split on a '## FOOD FOR THOUGHT' marker), not JSON.
    """
    from collections import Counter
    import analyze_lens_multi as alm

    provider, _model, _key_env, _max_out = wire("lens1")

    sb = alm.get_supabase()
    # Returns (balanced, all_articles, counts). Production passes `balanced`
    # to each lens, which then trims it to its own provider budget.
    articles_full, _all_articles, _counts = alm.fetch_balanced_articles(sb)
    if not articles_full:
        raise ProbeError(
            "Lens-1 fixture: fetch_balanced_articles returned nothing. "
            "Cannot build the real prompt -- refusing to invent one."
        )

    articles_trimmed = alm.trim_articles_to_budget(articles_full, provider)
    domain_blocks, total = alm.build_domain_blocks(articles_trimmed, {})
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    counts = Counter((a.get("domain") or "POWER").upper() for a in articles_trimmed)
    user_prompt = alm.build_prompt(domain_blocks, total, date_str, counts)

    return Fixture(
        system=alm.SYSTEM_PROMPTS[1],
        user=user_prompt,
        requires_json=False,          # prose + '## FOOD FOR THOUGHT' section
        temperature=0.3,
        origin="live-rebuild:analyze_lens_multi",
        detail={
            "articles_fetched": len(articles_full),
            "articles_after_trim": len(articles_trimmed),
            "articles_in_blocks": total,
            "trim_provider": provider,
            "domains": dict(counts),
        },
    )


def fixture_s2a_injection() -> Fixture:
    """Rebuild S2-A's real prompt, including the pre-call sanitisation step.

    run_s2a() sanitises the summary BEFORE the API call and discloses the
    replaced vocabulary inside the prompt, so a fixture that skipped
    sanitize_text() would be measuring a different prompt than production
    sends. Report 1 of the fetched batch is used (each report is its own call).
    """
    import lens_s2a_injection as s2a

    sb = s2a.get_supabase()
    reports = s2a.fetch_latest_reports(sb, None)
    if not reports:
        raise ProbeError(
            "S2-A fixture: no rows in lens_reports. Cannot build the real "
            "prompt -- refusing to invent one."
        )

    report = reports[0]
    raw_text = report.get("summary", "")
    san = s2a.sanitize_text(raw_text[:s2a.MAX_REPORT_CHARS] if raw_text else "")
    replaced_phrases = san["replaced_phrases"]
    report["_sanitized_summary"] = san["sanitized_text"]

    # Mirror of call_injection_tracer()'s user_message construction.
    report_id = report.get("id", "unknown")
    lens_name = report.get("domain_focus", "unknown")
    san_text = s2a.truncate_report(
        report.get("_sanitized_summary", report.get("summary", "")))
    if not san_text.strip():
        raise ProbeError("S2-A fixture: sanitized summary is empty.")

    replaced_note = ""
    if replaced_phrases:
        replaced_note = (
            f"\n\nPRE-DETECTED INJECTION VOCABULARY (already replaced in text above):\n"
            + ", ".join(f'"{p}"' for p in replaced_phrases)
            + "\nThese qualify as EMOTIONAL_PRIME injections. Include them in your findings."
        )

    user_message = (
        f"Analyze this intelligence report for narrative injection patterns.\n\n"
        f"Lens: {lens_name}\nReport ID: {report_id}\n\n"
        f"--- SANITIZED REPORT START ---\n{san_text}\n--- SANITIZED REPORT END ---"
        f"{replaced_note}\n\nReturn JSON only."
    )

    return Fixture(
        system=s2a.SYSTEM_PROMPT,
        user=user_message,
        requires_json=True,
        temperature=s2a.TEMPERATURE,
        origin="live-rebuild:lens_s2a_injection",
        detail={
            "reports_fetched": len(reports),
            "report_lens": lens_name,
            "replaced_phrases": len(replaced_phrases),
            "sanitized_chars": len(san_text),
        },
    )


def fixture_mission_analyst() -> Fixture:
    """Rebuild MA's real synthesis prompt from S1 + S2 + corrections + S3.

    Uses the module's own build_synthesis_prompt(), so the ordering rules
    (mandatory corrections first, the 0.6 S1 cap, the S2 cap) come from
    production rather than from a copy.
    """
    import lens_mission_analyst as ma

    sb = ma.get_supabase()
    s1_reports = ma.fetch_s1_reports(sb, None)
    s2_reports = ma.fetch_s2_reports(sb, None)
    if not s1_reports and not s2_reports:
        raise ProbeError(
            "MA fixture: no S1 or S2 reports available. Cannot build the real "
            "prompt -- refusing to invent one."
        )

    corrections, contamination_depth = ma.apply_s2_corrections(s2_reports)
    s3_context = ma.fetch_s3_context(sb)
    prompt = ma.build_synthesis_prompt(s1_reports, s2_reports, corrections,
                                       None, s3_context)

    # Mirror of call_mission_analyst()'s user_message wrapper.
    user_message = (
        f"Synthesize the following intelligence into a macro report.\n"
        f"Cycle: {'latest'}\n\n"
        f"{prompt}\n\n"
        f"Return JSON only."
    )

    return Fixture(
        system=ma.SYSTEM_PROMPT,
        user=user_message,
        requires_json=True,
        temperature=ma.TEMPERATURE,
        origin="live-rebuild:lens_mission_analyst",
        detail={
            "s1_reports": len(s1_reports),
            "s2_reports": len(s2_reports),
            "corrections": len(corrections),
            "contamination_depth": contamination_depth,
            "s3_context_keys": sorted(s3_context.keys()) if s3_context else [],
        },
    )


def fixture_s2e_legitimacy() -> Fixture:
    """Rebuild S2-E's real prompt for report 1 of the fetched batch.

    NOTE: production reads report['lens_name'], but fetch_latest_reports()
    selects 'domain_focus' -- so the live prompt always says 'Lens: unknown'.
    This fixture reproduces that faithfully rather than silently correcting it;
    a probe must measure what production actually sends. Logged as BUG-002.
    """
    import lens_s2e_legitimacy as s2e

    sb = s2e.get_supabase()
    reports = s2e.fetch_latest_reports(sb, None)
    if not reports:
        raise ProbeError(
            "S2-E fixture: no rows in lens_reports. Cannot build the real "
            "prompt -- refusing to invent one."
        )

    report = reports[0]
    report_id = report.get("id", "unknown")
    lens_name = report.get("lens_name", "unknown")     # BUG-002: always 'unknown'
    report_text = s2e.truncate_report(report.get("summary", ""))
    if not report_text.strip():
        raise ProbeError("S2-E fixture: report summary is empty.")

    user_message = (
        f"Extract state actors and apply the 6-point legitimacy assessment.\n\n"
        f"Lens: {lens_name}\n"
        f"Report ID: {report_id}\n\n"
        f"--- REPORT START ---\n{report_text}\n--- REPORT END ---\n\n"
        f"Return JSON only."
    )

    return Fixture(
        system=s2e.SYSTEM_PROMPT,
        user=user_message,
        requires_json=True,
        temperature=s2e.TEMPERATURE,
        origin="live-rebuild:lens_s2e_legitimacy",
        detail={
            "reports_fetched": len(reports),
            "report_domain_focus": report.get("domain_focus", "?"),
            "lens_name_sent": lens_name,
            "report_chars": len(report_text),
        },
    )


# role_key -> builder. A role absent here has NO faithful builder yet and must
# be given one, or a captured prompt at probe_fixtures/<role>.txt. There is
# deliberately no generic fallback that would fabricate a prompt.
FIXTURE_BUILDERS: dict[str, Callable[[], Fixture]] = {
    "s2d_adversary": fixture_s2d_adversary,
    "lens1": fixture_lens1,
    "s2a_injection": fixture_s2a_injection,
    "mission_analyst": fixture_mission_analyst,
    "s2e_legitimacy": fixture_s2e_legitimacy,
}


def load_fixture(role_key: str) -> Fixture:
    """Real prompt for a role: live rebuild first, captured file second.

    Never synthesises a prompt. If neither path is available the probe aborts
    with instructions, because a toy prompt would produce a confident number
    that means nothing (R-S80-2).
    """
    builder = FIXTURE_BUILDERS.get(role_key)
    if builder is not None:
        return builder()

    path = os.path.join(FIXTURE_DIR, f"{role_key}.txt")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        spec = get_role(role_key)
        if "===USER===" in raw:
            system, user = raw.split("===USER===", 1)
        else:
            system, user = "", raw
        if not user.strip():
            raise ProbeError(f"Captured fixture {path} has an empty user prompt.")
        return Fixture(
            system=system.strip(),
            user=user.strip(),
            requires_json=True,
            temperature=0.2,
            origin=f"captured-file:{os.path.relpath(path, REPO_ROOT)}",
            detail={"note": "extracted from a production run log",
                    "role_note": spec.get("note", "")},
        )

    raise ProbeError(
        f"No fixture for role '{role_key}'.\n"
        f"  Either add a faithful builder to FIXTURE_BUILDERS (preferred: it\n"
        f"  rebuilds the prompt from the same live reads the position does),\n"
        f"  or capture the prompt from that position's last production run\n"
        f"  into {os.path.join('probe_fixtures', role_key + '.txt')}.\n"
        f"  A hand-written prompt is NOT acceptable -- it measures nothing."
    )


# ---------------------------------------------------------------------------
# Candidate resolution
# ---------------------------------------------------------------------------
@dataclass
class Candidate:
    provider: str
    model: str
    key_env: str
    label: str


def resolve_override(provider: str, model: str, key_env: str) -> Candidate:
    """An explicitly specified, possibly UNREGISTERED pairing.

    Exists to test a migration target before it is written into the registry
    (e.g. MA on Cerebras). All three parts must be given together -- guessing
    any one of them is how a probe ends up measuring the wrong key's quota.
    """
    missing = [n for n, v in (("--provider", provider), ("--model", model),
                              ("--key-env", key_env)) if not v]
    if missing:
        raise ProbeError(
            f"Override needs all three of --provider/--model/--key-env; "
            f"missing {', '.join(missing)}. Refusing to guess."
        )
    if provider not in PROVIDER_ENDPOINTS and provider != "gemini":
        raise ProbeError(
            f"No probe transport for provider '{provider}'. "
            f"Known: {', '.join(sorted(list(PROVIDER_ENDPOINTS) + ['gemini']))}"
        )
    return Candidate(provider, model, key_env, "override")


def resolve_candidate(role_key: str, which: str) -> Candidate:
    """Pick the (provider, model, key_env) triple for this probe.

    LR-094: always the role's OWN key. The baseline runs on the role's key too,
    so its numbers are comparable with the primary's on the same quota bucket.
    """
    spec = get_role(role_key)

    if which == "primary":
        provider, model, key_env, _max_out = wire(role_key)
        return Candidate(provider, model, key_env, "primary")

    if which == "fallback":
        fb = fallback(role_key)
        if fb is None:
            raise ProbeError(
                f"Role '{role_key}' has no fallback leg in the registry -- "
                f"nothing to probe. Registry note: {spec.get('note','') or 'none'}"
            )
        provider, model, key_env = fb
        return Candidate(provider, model, key_env, "fallback")

    if which == "baseline":
        key_env = spec["key_env"]
        if spec["provider"] != "groq":
            raise ProbeError(
                f"Baseline is Groq {BASELINE_MODEL}, but role '{role_key}' is a "
                f"{spec['provider']} position using {key_env}. A Groq baseline on a "
                f"non-Groq key would violate LR-094 and measure the wrong bucket."
            )
        return Candidate(BASELINE_PROVIDER, BASELINE_MODEL, key_env, "baseline")

    raise ProbeError(f"Unknown candidate '{which}'.")


def require_key(key_env: str) -> str:
    """Fetch the key. Never returns it to a log, never prints it."""
    value = os.environ.get(key_env)
    if not value:
        raise ProbeError(
            f"Env var {key_env} is not set. This role probes on its own key "
            f"only (LR-094) -- refusing to borrow another position's key. "
            f"Add {key_env} to .env and retry."
        )
    return value


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def strip_reasoning(raw: str) -> str:
    """Remove <think> blocks and code fences, as production call sites do."""
    text = raw
    if "<think>" in text:
        if "</think>" in text:
            text = text[text.index("</think>") + len("</think>"):]
        else:
            text = text[text.index("<think>"):]
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) > 1:
            text = parts[1]
            if text.startswith("json"):
                text = text[4:]
    return text.strip()


def json_parses(content: str) -> bool:
    try:
        json.loads(strip_reasoning(content))
        return True
    except Exception:
        return False


def refusal_flag(content: str) -> bool:
    """FLAG only -- never a verdict. Chat reads the flagged lines and judges.

    Three signals, any one trips it: empty output, explicit refusal language,
    or output too short to be a real analysis (the silent-starvation shape).
    """
    if not content or not content.strip():
        return True
    if REFUSAL_PATTERN.search(content):
        return True
    return len(content) < MIN_USEFUL_CONTENT


# ---------------------------------------------------------------------------
# Wire calls
# ---------------------------------------------------------------------------
def call_openai_compatible(endpoint: str, api_key: str, model: str,
                           fixture: Fixture, max_tokens: int) -> dict:
    """Groq / SambaNova. Raw requests so LR-095 can read r.text on errors."""
    messages = []
    if fixture.system:
        messages.append({"role": "system", "content": fixture.system})
    messages.append({"role": "user", "content": fixture.user})

    r = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
        json={"model": model, "messages": messages,
              "max_tokens": max_tokens, "temperature": fixture.temperature},
        timeout=180,
    )
    out = {"http_status": r.status_code, "content": "", "finish_reason": None,
           "usage": {}, "error_text": None}

    if r.status_code != 200:
        out["error_text"] = r.text[:200]       # LR-095
        print(f"  HTTP {r.status_code}: {out['error_text']}")
        return out

    body = r.json()
    choice = (body.get("choices") or [{}])[0]
    out["content"] = (choice.get("message") or {}).get("content") or ""
    out["finish_reason"] = choice.get("finish_reason")
    usage = body.get("usage") or {}
    out["usage"] = {"prompt": usage.get("prompt_tokens"),
                    "completion": usage.get("completion_tokens"),
                    "total": usage.get("total_tokens")}
    # Whole usage object, verbatim. CC-1c depends on knowing whether every
    # provider really returns total_tokens in the Groq shape -- assuming that
    # is how a guard ends up logging None into its rolling window.
    out["raw_usage"] = usage
    return out


def call_gemini(api_key: str, model: str, fixture: Fixture,
                max_tokens: int) -> dict:
    endpoint = (f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent")
    payload = {
        "contents": [{"role": "user", "parts": [{"text": fixture.user}]}],
        "generationConfig": {"maxOutputTokens": max_tokens,
                             "temperature": fixture.temperature},
    }
    if fixture.system:
        payload["system_instruction"] = {"parts": [{"text": fixture.system}]}

    r = requests.post(endpoint, headers={"Content-Type": "application/json"},
                      params={"key": api_key}, json=payload, timeout=180)
    out = {"http_status": r.status_code, "content": "", "finish_reason": None,
           "usage": {}, "error_text": None}

    if r.status_code != 200:
        out["error_text"] = r.text[:200]       # LR-095
        print(f"  HTTP {r.status_code}: {out['error_text']}")
        return out

    body = r.json()
    cand = (body.get("candidates") or [{}])[0]
    parts = (cand.get("content") or {}).get("parts") or []
    out["content"] = "".join(p.get("text", "") for p in parts)
    out["finish_reason"] = cand.get("finishReason")
    usage = body.get("usageMetadata") or {}
    out["usage"] = {"prompt": usage.get("promptTokenCount"),
                    "completion": usage.get("candidatesTokenCount"),
                    "total": usage.get("totalTokenCount")}
    return out


def execute_trial(cand: Candidate, api_key: str, fixture: Fixture,
                  max_tokens: int) -> dict:
    if cand.provider in PROVIDER_ENDPOINTS:
        return call_openai_compatible(PROVIDER_ENDPOINTS[cand.provider],
                                      api_key, cand.model, fixture, max_tokens)
    if cand.provider == "gemini":
        return call_gemini(api_key, cand.model, fixture, max_tokens)
    raise ProbeError(
        f"No probe transport for provider '{cand.provider}'. CC-5 covers "
        f"Groq / Gemini / SambaNova; add a transport before probing it."
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def run(role_key: str, which: str, trials: int, dry_run: bool,
        max_out_override: Optional[int] = None,
        raw_max_tokens: Optional[int] = None,
        override_provider: Optional[str] = None,
        override_model: Optional[str] = None,
        override_key_env: Optional[str] = None) -> int:
    if role_key not in ROLES:
        raise ProbeError(f"Unknown role '{role_key}'. Known roles: "
                         f"{', '.join(sorted(ROLES))}")

    spec = get_role(role_key)
    if override_provider or override_model or override_key_env:
        cand = resolve_override(override_provider, override_model,
                                override_key_env)
    else:
        cand = resolve_candidate(role_key, which)

    print("=" * 72)
    print(f"PROBE  role={role_key}  candidate={which}")
    print("=" * 72)
    print(f"  provider     : {cand.provider}")
    print(f"  model        : {cand.model}")
    print(f"  key_env      : {cand.key_env} (value never printed)")
    print(f"  registry note: {spec.get('note','') or 'none'}")
    if which == "baseline":
        print(f"  NOTE         : {BASELINE_MODEL} dies 2026-08-16 -- this "
              f"measurement is unrepeatable after that date.")

    api_key = None if dry_run else require_key(cand.key_env)
    if dry_run and not os.environ.get(cand.key_env):
        print(f"  WARNING      : {cand.key_env} is NOT set -- a live run would abort here.")

    print("\n-- building fixture (real prompt, live reads) --")
    fixture = load_fixture(role_key)
    # The starvation diagnostic: when a primary returns finish_reason=length,
    # that is BUDGET, not refusal. Re-running at a higher max_out inside the
    # fit_max_tokens headroom distinguishes the two. Tagged in the jsonl so a
    # boosted trial can never be mistaken for a standard one.
    effective_max_out = max_out_override or spec["max_out"]
    if raw_max_tokens:
        # Bypass fit_max_tokens entirely. Legitimate ONLY where the provider's
        # real ceiling has been verified to allow it -- fit_max_tokens hardcodes
        # Groq's 7500 and would strangle a 65k-context Cerebras call.
        max_tokens = raw_max_tokens
    else:
        max_tokens = fit_max_tokens(fixture.prompt_chars, effective_max_out,
                                    cand.provider, cand.model)

    print(f"  origin           : {fixture.origin}")
    for k, v in fixture.detail.items():
        print(f"  {k:17}: {v}")
    print(f"  system chars     : {len(fixture.system)}")
    print(f"  user chars       : {len(fixture.user)}")
    print(f"  prompt chars     : {fixture.prompt_chars}")
    print(f"  requires_json    : {fixture.requires_json}")
    print(f"  temperature      : {fixture.temperature}")

    est_prompt_tokens = fixture.prompt_chars // 3
    print("\n-- budget (D-007) --")
    print(f"  role max_out     : {spec['max_out']}"
          + (f"  -> OVERRIDE {effective_max_out}" if max_out_override else ""))
    if raw_max_tokens:
        print(f"  RAW max_tokens   : {max_tokens}   "
              f"[fit_max_tokens BYPASSED -- provider ceiling verified]")
    else:
        from lens_models import request_ceiling
        ceil = request_ceiling(cand.provider, cand.model)
        print(f"  fit_max_tokens   : {max_tokens}   "
              f"[ceiling {ceil if ceil is not None else 'UNRESOLVED -> cap'}"
              f", cap {effective_max_out}, prompt {fixture.prompt_chars} chars]")
    print(f"  est prompt tokens: ~{est_prompt_tokens}")
    print(f"  est call total   : ~{est_prompt_tokens + max_tokens} tokens")

    print("\n-- pacing plan --")
    print(f"  trials           : {trials}")
    print(f"  sleep between    : {PACING_SLEEP_S}s (same key: {cand.key_env})")
    est_each = est_prompt_tokens + max_tokens
    print(f"  est total tokens : ~{est_each * trials} on {cand.key_env}")
    print(f"  est wall clock   : ~{PACING_SLEEP_S * (trials - 1)}s of sleep + call time")
    if cand.provider == "groq" and est_each > 8000:
        print(f"  !! WARNING: ~{est_each} tokens exceeds the 8K Groq TPM ceiling "
              f"-- expect 429/413. Shrink the prompt before probing.")

    if dry_run:
        print("\nDRY RUN -- no wire calls made, nothing written.")
        print("=" * 72)
        return 0

    print(f"\n-- running {trials} trial(s) --")
    key_token_total = 0
    written = 0

    for trial in range(1, trials + 1):
        if trial > 1:
            print(f"  pacing: sleeping {PACING_SLEEP_S}s "
                  f"(running total {key_token_total} tokens on {cand.key_env})")
            time.sleep(PACING_SLEEP_S)

        print(f"  trial {trial}/{trials} ...")
        started = time.time()
        try:
            res = execute_trial(cand, api_key, fixture, max_tokens)
        except requests.RequestException as e:
            res = {"http_status": None, "content": "", "finish_reason": None,
                   "usage": {}, "error_text": f"{type(e).__name__}: {e}"[:200]}
            print(f"  transport error: {res['error_text']}")
        latency = round(time.time() - started, 2)

        content = res["content"] or ""
        usage = res.get("usage") or {}
        if usage.get("total"):
            key_token_total += usage["total"]

        # D-017: a probe certifies the prompt SIZE it held, not just its shape
        # (R-S80-2 extended). S2-D passed 3/3 at 79% of its completion budget
        # and then truncated in production on a prompt 312 chars larger. For
        # JSON roles a near-full budget is MARGINAL, never passing -- there is
        # no partial credit for JSON that stops mid-string.
        completion_ratio = None
        d017_marginal = False
        if usage.get("completion") and max_tokens:
            completion_ratio = round(usage["completion"] / max_tokens, 3)
            d017_marginal = bool(fixture.requires_json
                                 and completion_ratio > D017_MARGINAL_RATIO)

        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "role": role_key,
            "candidate": which,
            "provider": cand.provider,
            "model": cand.model,
            "trial": trial,
            "http_status": res["http_status"],
            "finish_reason": res["finish_reason"],
            "prompt_chars": fixture.prompt_chars,
            "max_tokens": max_tokens,
            "max_out_override": max_out_override,
            "starvation_retest": bool(max_out_override),
            "raw_budget": bool(raw_max_tokens),
            "raw_usage": res.get("raw_usage"),
            "completion_ratio": completion_ratio,
            "d017_marginal": d017_marginal,
            "usage_prompt_tokens": usage.get("prompt"),
            "usage_completion_tokens": usage.get("completion"),
            "usage_total_tokens": usage.get("total"),
            "latency_s": latency,
            "content_len": len(content),
            "json_parse_ok": (json_parses(content)
                              if fixture.requires_json else None),
            "refusal_flag": refusal_flag(content),
            "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "content_head": content[:400],
            "error_text": res.get("error_text"),
            "fixture_origin": fixture.origin,
        }
        with open(RESULTS_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        written += 1

        ratio_str = (f"{completion_ratio:.0%}" if completion_ratio is not None
                     else "n/a")
        print(f"    http={record['http_status']} "
              f"finish={record['finish_reason']} "
              f"len={record['content_len']} "
              f"json_ok={record['json_parse_ok']} "
              f"refusal_flag={record['refusal_flag']} "
              f"tokens={record['usage_total_tokens']} "
              f"budget_used={ratio_str}"
              f"{'  <<< D-017 MARGINAL' if d017_marginal else ''} "
              f"{latency}s")

    print(f"\n  {written} line(s) appended to "
          f"{os.path.relpath(RESULTS_PATH, REPO_ROOT)}")
    print(f"  total tokens this run on {cand.key_env}: {key_token_total}")
    print("=" * 72)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="CC-5 probe pack -- measure a candidate model on a "
                    "position's REAL prompt. One role per invocation.")
    p.add_argument("--role", required=True,
                   help="registry role key, e.g. s2d_adversary")
    p.add_argument("--candidate", required=True,
                   choices=["baseline", "primary", "fallback"],
                   help="baseline = llama-3.3-70b-versatile (probe FIRST, "
                        "dies 2026-08-16); primary = wire(role); "
                        "fallback = fallback(role)")
    p.add_argument("--trials", type=int, default=3,
                   help="trials on the same key (default 3)")
    p.add_argument("--max-out", type=int, default=None,
                   help="override the role's registry max_out for a tagged "
                        "starvation re-test (finish_reason=length means budget, "
                        "not refusal). Still passes through fit_max_tokens.")
    p.add_argument("--raw-max-tokens", type=int, default=None,
                   help="bypass fit_max_tokens entirely (tagged raw_budget in "
                        "the jsonl). ONLY where the provider's real ceiling is "
                        "verified -- fit_max_tokens hardcodes Groq's 7500.")
    p.add_argument("--provider", default=None,
                   help="override provider (needs --model and --key-env too)")
    p.add_argument("--model", default=None, help="override wire model ID")
    p.add_argument("--key-env", default=None,
                   help="override key env var name (value never printed)")
    p.add_argument("--dry-run", action="store_true",
                   help="build fixture + show budget and pacing, no wire calls")
    args = p.parse_args()

    try:
        return run(args.role, args.candidate, args.trials, args.dry_run,
                   max_out_override=args.max_out,
                   raw_max_tokens=args.raw_max_tokens,
                   override_provider=args.provider,
                   override_model=args.model,
                   override_key_env=args.key_env)
    except ProbeError as e:
        print(f"\nPROBE ABORTED: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
