"""
code/lens_models.py -- LENS-028 L-CLIFF model registry (single source of truth).

Every provider name, wire model ID, key env, output budget, and rate limit
lives HERE and only here. The quota guard and every call site import from
this file. A model string that is not in this registry must fail LOUDLY at
runtime (assert_model_known) -- that assertion is the vaccine against the
MODEL-404 shape (S2-D ran dead for 10 days under green checks, Jul 2026).

Rules encoded:
  - Groq 8K per-request ceiling: prompt_tokens + max_tokens <= 8192, else
    HTTP 413 (unretryable). fit_max_tokens() applies GNI's proven formula
    (GNI commit 7097460): max(768, min(cap, 7500 - prompt_chars // 3)).
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
MISTRAL_SMALL = "mistral-small-latest"
COHERE_CMD_R_PLUS = "command-r-plus-08-2024"

GROQ_REQUEST_CEILING = 8192  # prompt + max_tokens hard cap per request


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
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2DGCOM_API_KEY", "max_out": 2400,
        "fb_provider": "sambanova", "fb_model": SAMBANOVA_LLAMA_33_70B,
        "fb_key_env": "SAMBANOVA_API_KEY",
        "note": "DEAD since 2026-07-17 (qwen/qwen3-32b 404). Content-heaviest"
                " role: content-fitness probe is mandatory before cert",
    },
    "s2e_legitimacy": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2E_API_KEY", "max_out": 2400,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S2E_API_KEY",
        "note": "was MAX_TOKENS=2000; log label said bare llama-3.3-70b",
    },
    "s2gap": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2_API_KEY", "max_out": 2400,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S2_API_KEY",
        "note": "was MAX_TOKENS=1500",
    },
    "mission_analyst": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_MA_API_KEY", "max_out": 2500,
        "fb_provider": "sambanova", "fb_model": SAMBANOVA_LLAMA_33_70B,
        "fb_key_env": "SAMBANOVA_API_KEY",
        "note": "",
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
LIMITS = {
    ("groq", GROQ_GPT_OSS_120B): {"RPM": 30, "RPD": 1_000, "TPM": 8_000,
                                  "TPD": 200_000},   # VERIFIED console Jul 27
    ("groq", GROQ_GPT_OSS_20B): {"RPM": 30, "RPD": 1_000, "TPM": 8_000,
                                 "TPD": 200_000},    # VERIFIED console Jul 27
    ("cerebras", CEREBRAS_GPT_OSS_120B): {"TPD": 1_000_000},  # banked LENS-026; VERIFY
    ("cloudflare", CLOUDFLARE_GPT_OSS_120B): None,   # neurons/day model; VERIFY
    ("sambanova", SAMBANOVA_LLAMA_33_70B): None,     # VERIFY (probe also checks ctx)
    ("gemini", GEMINI_25_FLASH): None,               # VERIFY free-tier RPD
    ("gemini", GEMINI_25_FLASH_LITE): None,          # VERIFY free-tier RPD
    ("mistral", MISTRAL_SMALL): {"RPD": 2_000},      # banked; VERIFY
    ("cohere", COHERE_CMD_R_PLUS): {"RPD": 1_000},   # banked; VERIFY
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


def fit_max_tokens(prompt_chars, cap):
    """Groq 8K ceiling guard (GNI formula, commit 7097460).

    prompt_chars // 3 approximates prompt tokens; keep prompt + output
    inside ~7500 to leave headroom under the 8192 hard cap. Floor 768 so
    reasoning models are never fully starved -- if even 768 does not fit,
    the PROMPT must shrink (that is a caller bug, and 413 will say so).
    """
    return max(768, min(cap, 7500 - prompt_chars // 3))


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
