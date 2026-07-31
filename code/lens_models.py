"""
code/lens_models.py -- LENS-028 L-CLIFF model registry (single source of truth).

Every provider name, wire model ID, key env, output budget, and rate limit
lives HERE and only here. The quota guard and every call site import from
this file. A model string that is not in this registry must fail LOUDLY at
runtime (assert_model_known) -- that assertion is the vaccine against the
MODEL-404 shape (S2-D ran dead for 10 days under green checks, Jul 2026).

Rules encoded:
  - Per-request ceilings are DERIVED, not hardcoded: min(CTX, TPM) per
    (provider, model), from the LIMITS table below (D-015). The old
    "Groq 8192" constant was never a context limit -- Groq gpt-oss-120b
    has CTX 131,072 and is bound at 8,000 by TPM. Do not restore 8192.
  - gpt-oss models are REASONING models: they spend ~1500-1600 tokens
    thinking before writing. Output budgets below 2200 risk silent empty
    responses (starvation). Heavy analytical roles get 2400+.
  - NO China-lineage models as primary or fallback (James ruling, Jul 27
    2026). See docs/LENS_LCLIFF_DECISIONS.md D-004.
  - Limits marked VERIFIED were read from console.groq.com/settings/limits
    on 2026-07-27. Limits marked None are LIMITS_UNKNOWN -- the guard's
    conservative PROCEED path handles them; never invent a number.

Sources: LENS-028 byte census at HEAD 9b2836d + runtime logs of Jul 27
(manage-analyze 81938609577, s2f 81938481905) + Groq console receipts.
"""

import logging

log = logging.getLogger("lens_models")

# --------------------------------------------------------------------------
# Wire model IDs (exact strings sent on the wire -- prefixes matter)
# --------------------------------------------------------------------------
GROQ_GPT_OSS_120B = "openai/gpt-oss-120b"
GROQ_GPT_OSS_20B = "openai/gpt-oss-20b"
CEREBRAS_GPT_OSS_120B = "gpt-oss-120b"
CLOUDFLARE_GPT_OSS_120B = "@cf/openai/gpt-oss-120b"
SAMBANOVA_LLAMA_33_70B = "Meta-Llama-3.3-70B-Instruct"  # SambaNova format, LR-005(A)
GEMINI_25_FLASH = "gemini-2.5-flash"            # dies 2026-10-16 (Google page)
GEMINI_25_FLASH_LITE = "gemini-2.5-flash-lite"
# Dated id, never an alias (D-015). No `mistral-small-latest` model card exists,
# and -latest aliases carry far lower limits than dated ids (medium-latest
# 25,000 TPM vs medium-2508 356,250). An alias is an unpinned wire id.
# Higher-throughput option if a fallback ever needs it: mistral-small-2506
# (TPM 2,250,000, RPS 5.00). Not wired -- recorded only.
MISTRAL_SMALL = "mistral-small-2603"
COHERE_CMD_R_PLUS = "command-r-plus-08-2024"

# Budget-fitting constants (D-015). The old GROQ_REQUEST_CEILING = 8192 is gone:
# it was never a context limit, it was TPM, and it was Groq-only. Ceilings now
# derive from LIMITS per (provider, model) -- see request_ceiling().
MARGIN_FRACTION = 0.08   # proportional: estimate error scales with request size
MARGIN_FLOOR = 200       # never trust a ceiling to the last token
CHARS_PER_TOKEN = 3      # deliberately conservative; measured mean is 4.42 and
                         # loosening it to 4 is CC-1d, its own decision + probe


class LensModelRegistryError(RuntimeError):
    """Raised when a call site tries to use a model the registry does not know."""


# --------------------------------------------------------------------------
# ROLES: role_key -> spec
#   provider / model:  exact wire values
#   key_env:           env var holding the API key for THIS position (LR-094)
#   max_out:           default output budget (reasoning burn included)
#   fb_provider/fb_model/fb_key_env: fallback leg (None = no fallback)
#   note:              VERIFY tags are open items for the sweep/probe
# --------------------------------------------------------------------------
ROLES = {
    # ---- 4-lens engine (analyze_lens_multi.py via lens_orchestrator.py) ----
    "lens1": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_API_KEY", "max_out": 2400,
        "fb_provider": "sambanova", "fb_model": SAMBANOVA_LLAMA_33_70B,
        "fb_key_env": "SAMBANOVA_API_KEY",
        "note": "was qwen/qwen3-32b (dead 2026-07-17); ran on dying 70b fallback",
    },
    "lens2": {
        "provider": "gemini", "model": GEMINI_25_FLASH,
        "key_env": "GEMINI_API_KEY", "max_out": 2400,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "gemini-2.5-flash dies 2026-10-16 -> one-line edit here in Oct",
    },
    "lens3": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 2400,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_API_KEY",
        "note": "old Groq fallback was 70b-versatile (dies Aug 16)",
    },
    "lens4": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 2400,
        "fb_provider": "sambanova", "fb_model": SAMBANOVA_LLAMA_33_70B,
        "fb_key_env": "SAMBANOVA_API_KEY",
        "note": "existing LR-005(A) pattern, kept",
    },
    # ---- orchestrator watchdog ----
    "ai5_watchdog": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_MANAGER_API_KEY", "max_out": 1600,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_MANAGER_API_KEY",
        "note": "was max_tokens=300 (starvation bomb); key was GROQ_MA_API_KEY"
                " -- CC-6 wires the provisioned-but-unused GROQ_MANAGER_API_KEY",
    },
    # ---- S2 family ----
    "s2a_injection": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2A_API_KEY", "max_out": 2400,
        "fb_provider": "sambanova", "fb_model": SAMBANOVA_LLAMA_33_70B,
        "fb_key_env": "SAMBANOVA_API_KEY",
        "note": "was MAX_TOKENS=1800",
    },
    "s2b_coordination": {
        "provider": "gemini", "model": GEMINI_25_FLASH_LITE,
        "key_env": "GEMINI_S2B_API_KEY", "max_out": 2400,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "was gemini-2.0-flash (shut down 2026-06-01); needs long"
                " context -- probe the 200-article prompt on flash-lite",
    },
    "s2c_emotion": {
        "provider": "mistral", "model": MISTRAL_SMALL,
        "key_env": "MISTRAL_API_KEY", "max_out": 1600,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "",
    },
    "s2d_adversary": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 8000,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "D-016: moved to Cerebras 2026-07-28. On Groq's 8,000 ceiling"
                " it could never analyse more than ~23 articles per call and"
                " lost 42 of 60 to mid-JSON truncation in the cert run; the"
                " BUG-001 fix makes that worse, not better. Probed on Cerebras"
                " 3/3 stop/valid-JSON at 26-36% of budget (D-017 PASS) on the"
                " exact 11,589-char prompt that truncated on Groq",
    },
    "s2e_legitimacy": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 16_000,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "D-016: moved to Cerebras 2026-07-28. On Groq it returned ZERO"
                " characters 3/3 at a 2400 budget -- ~2,000 tokens of reasoning"
                " against an 8,000 TPM ceiling left nothing for output, which"
                " reads exactly like a refusal and is how April's evidence was"
                " misread. max_out 16,000 (CC-7a, LENS-029): budget_used rose 54->62->68%"
                " on flat prompts as reasoning grew 2,437->2,883; probed 3/3 stop at 26-43%",
    },
    "s2gap": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2_API_KEY", "max_out": 4000,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S2_API_KEY",
        "note": "was MAX_TOKENS=1500",
    },
    "mission_analyst": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 5_000,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "D-016: moved to Cerebras 2026-07-28. Its ~30,000-char synthesis"
                " prompt (~6,900 tokens) collapsed fit_max_tokens to the 768"
                " floor on Groq -- guaranteed silent-empty for a reasoning model"
                " -- and ~10,800 tokens/call exceeded the 8,000 ceiling outright."
                " max_out 5,000 per D-017 (2,356 observed); the approved 4,000"
                " was 59%, clearing the threshold by a single point",
    },
    "entity_extract": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_API_KEY", "max_out": 1600,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_API_KEY",
        "note": "was MAX_TOKENS=600 (starvation bomb). VERIFY key_env at sweep",
    },
    # ---- report generators ----
    "s1_report": {
        "provider": "mistral", "model": MISTRAL_SMALL,
        "key_env": "MISTRAL_API_KEY", "max_out": 2400,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "",
    },
    "s2_report": {
        "provider": "mistral", "model": MISTRAL_SMALL,
        "key_env": "MISTRAL_API_KEY", "max_out": 2400,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "",
    },
    # ---- S3 family ----
    "s3a_patterns": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S3_API_KEY", "max_out": 2500,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S3_API_KEY",
        "note": "was hardcoded llama-3.3-70b-versatile at L61",
    },
    "s3b_history": {
        "provider": "gemini", "model": GEMINI_25_FLASH_LITE,
        "key_env": "GEMINI_S3B_API_KEY", "max_out": 2400,
        "fb_provider": "mistral", "fb_model": MISTRAL_SMALL,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "was gemini-2.0-flash (shut down 2026-06-01)",
    },
    "s3c_drift": {
        "provider": "cohere", "model": COHERE_CMD_R_PLUS,
        "key_env": "COHERE_API_KEY", "max_out": 2400,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "still a Cohere-recommended current model (Jul 27 check);"
                " command-a-03-2025 is the eventual upgrade",
    },
    "s3d_longterm": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 2400,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_API_KEY",
        "note": "",
    },
    "s3f_countercheck": {
        "provider": "mistral", "model": MISTRAL_SMALL,
        "key_env": "MISTRAL_API_KEY", "max_out": 1600,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "",
    },
    # ---- S2-F ensemble ----
    "s2f_primary": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 2000,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "runtime truth Jul 27: wire = gpt-oss-120b; the qwen-3-235b"
                " ENSEMBLE banner is a stale label -- fix banner in CC-6",
    },
    "s2f_secondary": {
        "provider": "cloudflare", "model": CLOUDFLARE_GPT_OSS_120B,
        "key_env": "CLOUDFLARE_API_TOKEN", "max_out": 2000,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "Jul 27 run: 10x200 vs 42x429, retries ~1s apart, no backoff"
                " -- IMPORTANT-tier fix, not cliff work",
    },
}

# --------------------------------------------------------------------------
# LIMITS: (provider, wire_model) -> known limits. None = LIMITS_UNKNOWN.
# VERIFIED = read from provider console/docs on 2026-07-27 by James.
# --------------------------------------------------------------------------
#
# METER records HOW a provider rate-limits (D-015):
#   "tokens"   -> TPM exists and binds a single request
#   "requests" -> the provider limits calls, NOT tokens. Cohere (20 req/min)
#                 and Cloudflare (300 req/min + neurons/day) have NO TPM by
#                 design. "No TPM exists" and "nobody has checked" must never
#                 collapse into the same silence.
# CTX is the context window. It is rarely the binding constraint: Groq
# gpt-oss-120b has CTX 131,072 and still resolves to 8,000 because TPM binds.
LIMITS = {
    ("groq", GROQ_GPT_OSS_120B): {"METER": "tokens", "RPM": 30, "RPD": 1_000,
                                  "TPM": 8_000, "TPD": 200_000,
                                  "CTX": 131_072},   # VERIFIED console Jul 27
    ("groq", GROQ_GPT_OSS_20B): {"METER": "tokens", "RPM": 30, "RPD": 1_000,
                                 "TPM": 8_000, "TPD": 200_000,
                                 "CTX": 131_072},    # VERIFIED console Jul 27
    ("cerebras", CEREBRAS_GPT_OSS_120B): {"METER": "tokens", "RPM": 5,
                                          "RPD": 2_400, "TPM": 30_000,
                                          "TPD": 1_000_000, "CTX": 131_000,
                                          "MAX_COMPLETION": 40_000},
    # ^ VERIFIED-console 2026-07-28. Overrides the public docs (which said
    #   CTX 65,536 / max output 32,768). Console also warns short-interval
    #   enforcement ("60 RPM may be enforced as 1 req/sec") -- at RPM 5 that
    #   is why Cerebras roles need ~65s spacing between sequential calls.
    ("cloudflare", CLOUDFLARE_GPT_OSS_120B): {"METER": "requests", "RPM": 300,
                                              "CTX": 128_000},
    # ^ VERIFIED-docs 2026-07-28. Metered in Neurons (10,000/day free), a GPU
    #   compute unit with no honest token conversion -- so no TPM, by design.
    ("gemini", GEMINI_25_FLASH): None,               # LIMITS_UNKNOWN -> AI Studio
    ("gemini", GEMINI_25_FLASH_LITE): None,          # LIMITS_UNKNOWN -> AI Studio
    ("mistral", MISTRAL_SMALL): {"METER": "tokens", "TPM": 50_000, "RPD": 2_000,
                                 "CTX": 128_000},
    # ^ TPM/RPS VERIFIED-console 2026-07-28 (RPS 0.83). CTX is VERIFY, not
    #   VERIFIED: 128k comes from the Mistral Small 3.1/3.2 cards, and this is
    #   a dated id -- re-read the card before upgrading the tag.
    ("cohere", COHERE_CMD_R_PLUS): {"METER": "requests", "RPM": 20,
                                    "RPD": 1_000, "CTX": 128_000},
    # ^ 20 req/min VERIFIED-docs 2026-07-28; no TPM exists on trial keys.
    #   CTX 128k is VERIFY (docs summary, not a fetched model card).
}

# Back-compat single-axis view for lens_quota_guard.PROVIDER_LIMITS
_GOVERNING_AXIS = {"groq": "TPD", "cerebras": "TPD", "sambanova": "TPD",
                   "gemini": "RPD", "mistral": "RPD", "cohere": "RPD",
                   "cloudflare": "RPD"}

PROVIDER_LIMITS = {}
for (_prov, _model), _lim in LIMITS.items():
    if _lim:
        _axis = _GOVERNING_AXIS.get(_prov)
        if _axis and _axis in _lim:
            PROVIDER_LIMITS[(_prov, _model)] = {_axis: _lim[_axis]}

_KNOWN_WIRE = {(spec["provider"], spec["model"]) for spec in ROLES.values()}
_KNOWN_WIRE |= {(spec["fb_provider"], spec["fb_model"])
                for spec in ROLES.values() if spec["fb_provider"]}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def get_role(role_key):
    """Return the spec dict for a role, or raise loudly."""
    try:
        return ROLES[role_key]
    except KeyError:
        raise LensModelRegistryError(
            f"[LENS-MODELS] Unknown role '{role_key}'. Add it to "
            f"code/lens_models.py -- never hardcode a model at a call site."
        )


def wire(role_key):
    """(provider, model, key_env, max_out) for a role's primary leg."""
    s = get_role(role_key)
    return s["provider"], s["model"], s["key_env"], s["max_out"]


def fallback(role_key):
    """(provider, model, key_env) for a role's fallback leg, or None."""
    s = get_role(role_key)
    if not s["fb_provider"]:
        return None
    return s["fb_provider"], s["fb_model"], s["fb_key_env"]


def assert_model_known(provider, model):
    """The vaccine. Call before every LLM request. Unknown pair = loud fail."""
    if (provider, model) not in _KNOWN_WIRE:
        raise LensModelRegistryError(
            f"[LENS-MODELS] ({provider!r}, {model!r}) is not in the registry. "
            f"This is exactly how S2-D died silently for 10 days -- refusing "
            f"to call. Register the model in code/lens_models.py first."
        )


def limits_for(provider, model):
    """Full limits dict for a wire pair, or None (LIMITS_UNKNOWN)."""
    return LIMITS.get((provider, model))


def request_ceiling(provider, model):
    """Largest single request this pair can ever satisfy, or None (D-015).

        CTX + TPM                -> min(CTX, TPM)
        CTX only, METER=requests -> CTX
        otherwise                -> None

    The context window is rarely the cap. A request larger than the per-minute
    allowance can never be served, whatever CTX says -- Groq gpt-oss-120b has
    CTX 131,072 and resolves to 8,000. The old hardcoded 8,192 looked right for
    the wrong reason: it was TPM all along. DO NOT restore 8,192.

    Returns None rather than raising. Raising would break S2-C/S3-B/S3-C/S2-F,
    which work today on flat budgets -- a documentation gap must not become an
    outage. Loudness belongs in verify_registry_alignment(), which reports
    unresolved ceilings to the pre-flight line. Guard reports, call sites enforce.
    """
    lim = LIMITS.get((provider, model))
    if not lim:
        return None
    ctx = lim.get("CTX")
    tpm = lim.get("TPM")
    if ctx and tpm:
        return min(ctx, tpm)
    if ctx and lim.get("METER") == "requests":
        return ctx
    if tpm:
        return tpm
    return None


def fit_max_tokens(prompt_chars, cap, provider, model):
    """Output budget that fits this pair's real ceiling (D-015).

    provider AND model are required: two Groq models on one key have different
    TPM (llama-3.3-70b 12,000 vs gpt-oss-120b 8,000), so a provider-only lookup
    would have refused the Mission Analyst baseline (6,893 + 2,500 = 9,393) that
    in fact ran clean 3/3.

    Unresolved ceiling -> return cap unchanged and log an ERROR naming the pair.
    That is exactly today's behaviour for those roles, so it is status-quo
    preserving rather than a new failure mode.

    Floor 768 so reasoning models are never fully starved. Margin is
    proportional because token-estimate error scales with request size.
    """
    ceiling = request_ceiling(provider, model)
    if ceiling is None:
        log.error(
            "[LENS-MODELS] no resolvable ceiling for (%r, %r) -- returning cap "
            "%s unchanged. Add CTX/TPM/METER to LIMITS to enable fitting.",
            provider, model, cap)
        return cap
    usable = ceiling - max(MARGIN_FLOOR, int(ceiling * MARGIN_FRACTION))
    return max(768, min(cap, usable - prompt_chars // CHARS_PER_TOKEN))


if __name__ == "__main__":
    # Self-test: every fallback pair is registered, no China-lineage models.
    banned = ("qwen", "deepseek", "kimi", "minimax", "moonshot", "glm", "yi-")
    for rk, s in ROLES.items():
        for prov, mod in ((s["provider"], s["model"]),
                          (s["fb_provider"], s["fb_model"])):
            if mod is None:
                continue
            assert (prov, mod) in _KNOWN_WIRE, (rk, prov, mod)
            low = mod.lower()
            assert not any(b in low for b in banned), (
                f"China-lineage model in registry: {rk} -> {mod}")
        assert s["max_out"] >= 768, (rk, s["max_out"])
    print(f"lens_models self-test OK: {len(ROLES)} roles, "
          f"{len(_KNOWN_WIRE)} wire pairs, {len(PROVIDER_LIMITS)} limit rows")
