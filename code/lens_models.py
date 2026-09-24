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
  - NO China-lineage models as primary or fallback (Bro Alpha ruling, Jul 27
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
# TOMBSTONE (CC-31): provider dead 2026-07-28 (HTTP 402, balance_units 0).
# No ROLES row references it. Kept as a corpse marker so the id cannot be
# silently re-wired; deleting it is provider retirement, a separate purpose.
GEMINI_25_FLASH = "gemini-2.5-flash"            # dies 2026-10-16 (Google page)
GEMINI_25_FLASH_LITE = "gemini-2.5-flash-lite"
# Dated id, never an alias (D-015). No `mistral-small-latest` model card exists,
# and -latest aliases carry far lower limits than dated ids (medium-latest
# 25,000 TPM vs medium-2508 356,250). An alias is an unpinned wire id.
# mistral-small-2506 was recorded here as a higher-throughput option.
# It is NOT one -- it returned 429 on a cold call 2026-09-13, like every
# other mistral-small* id. Do not wire it.
MISTRAL_SMALL = "mistral-small-2603"
MISTRAL_SMALL_LATEST = "mistral-small-latest"  # FLOATING alias -- what
# production actually sets (lens-s2f-scoring.yml, lens_regular_report.py).
# Can change model without a commit. Pin it when there is time.
MINISTRAL_8B = "ministral-8b-2512"
# D-026: probed on S2-E real 9,457-char prompt, 5/5 with
# response_format, actors 7/6/8/7/8 against the banked band. CTX
# 262,144, deprecation None (model card 2026-09-14). The whole
# mistral-small* class has returned 429 on a healthy key since
# 2026-09-04 and the reason is UNKNOWN.
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
        "key_env": "GROQ_API_KEY", "max_out": 2500,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "CC-76 (LENS-042): canary lenses take NO cross-lens fallback --"
                " a fallback onto another lens's model collapses epistemic"
                " diversity (S1-001: gpt-oss counted twice); a dead provider must"
                " show FAILED (CC-75). analyze_lens_multi.LENSES is the wire and"
                " does not read this row; call_groq sends max_tokens=2500.",
    },
    "lens2": {
        "provider": "gemini", "model": GEMINI_25_FLASH,
        "key_env": "GEMINI_API_KEY", "max_out": 2400,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "CC-76: no fallback (see lens1). Shutdown date: 2026-10-16 was"
                " announced, then removed from the Gemini API deprecations page"
                " on 2026-08-03; Vertex lists 2026-10-20 (read 2026-09-17)."
                " max_out not re-checked against call_gemini.",
    },
    "lens3": {
        "provider": "cohere", "model": COHERE_CMD_R_PLUS,
        "key_env": "COHERE_API_KEY", "max_out": 2500,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "CC-76: was cerebras gpt-oss-120b, dead ~2026-08-17 (402);"
                " no Causal Chain row after 2026-08-17 14:07. Probed 2/2 on the"
                " production lens prompt: COMPLETE, in 7501, out 1300-1499 at"
                " 2500, 105-147s. Cohere trial key: 1,000 calls/month.",
    },
    "lens4": {
        "provider": "mistral", "model": MINISTRAL_8B,
        "key_env": "MISTRAL_API_KEY", "max_out": 4000,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "CC-76: was cerebras gpt-oss-120b -- the SAME model as lens3"
                " (the S1-001 structure) -- with a sambanova fallback dead since"
                " 2026-07-28. Probed 6/6 finish=stop: out 1863-2469 (98.8% of the"
                " old 2500 at the top), so the cap is 4000.",
    },
    # ---- orchestrator watchdog ----
    "ai5_watchdog": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_MA_API_KEY", "max_out": 1600,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_MA_API_KEY",
        "note": "was max_tokens=300 (starvation bomb). key_env CORRECTED to"
                " GROQ_MA_API_KEY at CC-28a -- that is what lens_orchestrator.py"
                " reads (:25) and what lens-manage-analyze.yml supplies (:45,:67)."
                " GROQ_MANAGER_API_KEY is supplied by NO workflow and read by NO"
                " code, so the CC-6 intention never shipped; wiring to it would"
                " have 401d exactly as S2-GAP did (LR-116; CC-15 precedent:"
                " production wins, the row moves). Sole consumer since MA left"
                " for Cerebras at 869f368, so LR-094 isolation already holds.",
    },
    # ---- S2 family ----
    "s2a_injection": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2A_API_KEY", "max_out": 4600,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "was MAX_TOKENS=1800; fallback was sambanova/Meta-Llama-3.3-70B-Instruct, dead since 2026-07-28 (HTTP 402, balance_units 0); moved to mistral-small-2603, completing the all-fallbacks-to-mistral direction recorded as D-015. Row now matches the Mistral fallback CC-14 already gave the call site.",
    },
    "s2b_coordination": {
        "provider": "gemini", "model": GEMINI_25_FLASH_LITE,
        "key_env": "GEMINI_S2B_API_KEY", "max_out": 2400,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "was gemini-2.0-flash (shut down 2026-06-01); needs long"
                " context -- probe the 200-article prompt on flash-lite",
    },
    "s2c_emotion": {
        "provider": "mistral", "model": MINISTRAL_8B,
        "key_env": "MISTRAL_API_KEY", "max_out": 1600,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "",
    },
    "s2d_adversary": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 8000,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
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
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
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
        "key_env": "GROQ_S2DGCOM_API_KEY", "max_out": 4000,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S2_API_KEY",
        "note": "was MAX_TOKENS=1500",
    },
    "mission_analyst": {
        "provider": "cerebras", "model": CEREBRAS_GPT_OSS_120B,
        "key_env": "CEREBRAS_API_KEY", "max_out": 5_000,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
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
        "key_env": "GROQ_S3_API_KEY", "max_out": 1600,
        "fb_provider": "groq", "fb_model": GROQ_GPT_OSS_20B,
        "fb_key_env": "GROQ_S3_API_KEY",
        "note": ("was MAX_TOKENS=600 (starvation bomb). CC-113 (LENS-045): moved off"
                 " GROQ_API_KEY, Lens 1's org, after its 160 TPD refusals on 2026-09-22"
                 " evening starved Lens 1 (429_tpd). GROQ_S3_API_KEY was idle."),
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
    "compendium_intro": {
        "provider": "groq", "model": GROQ_GPT_OSS_120B,
        "key_env": "GROQ_S2DGCOM_API_KEY", "max_out": 1200,
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "3-sentence exec intro only. Call site shipped max_tokens=200,"
                " which gpt-oss reasoning ALONE exceeds (236-484 measured on"
                " lens1) -- the budget must move with the model. Shares"
                " S2-GAP's dedicated key (LENS-022 quota isolation).",
    },
    "regular_report": {
        "provider": "mistral", "model": MISTRAL_SMALL_LATEST,
        "key_env": "MISTRAL_API_KEY", "max_out": 4096,
        "fb_provider": "cerebras", "fb_model": CEREBRAS_GPT_OSS_120B,
        "fb_key_env": "CEREBRAS_API_KEY",
        "note": "PROVIDERS is a 2-LEG chain mistral -> cerebras (CC-43)."
                " Leg 3 was groq/llama-3.3-70b-versatile and was REMOVED:"
                " this position's real prompt exceeds Groq TPM several"
                " times over, so no Groq model can serve it -- measure it"
                " with probe_lens_models.py --role regular_report --dry-run"
                " before re-proposing one. The cerebras leg is NOT reachable"
                " on an API failure: _FORCE_PROVIDER is written, never read"
                " (CC-44).",
    },
    # ---- S3 family ----
    "s3a_patterns": {
        "provider": "cohere", "model": COHERE_CMD_R_PLUS,
        "key_env": "COHERE_API_KEY", "max_out": 4000,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
        "fb_key_env": "MISTRAL_API_KEY",
        "note": "CC-74 (LENS-042): Cerebras free tier withdrawn ~2026-08-17"
                " (402 payment_required, x-should-retry false); S3-A saved"
                " nothing after 2026-08-17. Cohere probed 3/3 on the production"
                " prompt (19,256 chars): 200, finish COMPLETE, in=4662,"
                " out 634-932, valid schema, 27-52s. Fallback ministral-8b"
                " first probed on this prompt at the CC-74 gate.",
    },
    "s3b_history": {
        "provider": "gemini", "model": GEMINI_25_FLASH_LITE,
        "key_env": "GEMINI_S3B_API_KEY", "max_out": 2400,
        "fb_provider": "mistral", "fb_model": MINISTRAL_8B,
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
        "provider": "mistral", "model": MINISTRAL_8B,
        "key_env": "MISTRAL_API_KEY", "max_out": 16000,   # CC-93: 8000 cut 2026-09-21; probed 6,571 stop / >8,000 length
        "fb_provider": None, "fb_model": None, "fb_key_env": None,
        "note": "CC-82 (LENS-042): was cerebras gpt-oss-120b, dead ~2026-08-17;"
                " the ministral fallback then truncated at 2500 (cut mid-string at"
                " 9.2-9.9K chars, 2026-09-14 and 09-17) so S3-D has saved nothing"
                " since 2026-09-03. No fallback: a dead leg must show FAILED."
                " Cohere would fit the charter's reasoning depth but S3-C already"
                " takes 394s on Mon/Thu and the wave has a wall clock -- revisit"
                " once timeout-minutes 50 is certified.",
    },
    "s3f_countercheck": {
        "provider": "mistral", "model": MINISTRAL_8B,
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
# VERIFIED = read from provider console/docs on 2026-07-27 by Bro Alpha.
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
    ("mistral", MINISTRAL_8B): {"METER": "tokens", "TPM": 625_000,
                                "RPS": 3.13, "CTX": 262_144},
    # ^ VERIFIED-console 2026-09-14. Mistral has no RPD axis at all;
    #   RPS is the request limit. CTX from the model card.
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


# -- CC-81 (LENS-042): the canary's air --------------------------------------
# Gas-mask test ARM 4. The four S1 lenses breathe these keys; a probe or test
# that spends them competes with the canary (GROQ_API_KEY is also Collection's).
CANARY_LENS_ROLES = ("lens1", "lens2", "lens3", "lens4")

# CC-115 (LENS-045): providers that refuse every call. A position whose primary is
# listed goes straight to its fallback leg instead of trying, waiting and failing.
# Remove an entry to restore the primary. Evidence: PROVIDER HEALTH said DOWN.
RETIRED_PROVIDERS = {
    "cerebras": "402 payment_required on every call since ~2026-08-17; "
                "DOWN in provider health 2026-09-23 (payment x14 in 2 runs)",
}
CANARY_AIR_MIN_REASON = 12


def canary_air_keys():
    """Key env names the S1 lenses use, derived from ROLES (never a copy)."""
    return sorted({ROLES[r]["key_env"] for r in CANARY_LENS_ROLES})


def canary_air_guard(key_env, purpose):
    """Refuse to use a canary key unless LENS_ALLOW_CANARY_AIR holds a written
    reason (not a bare 1/true). Returns the reason, or None for other keys."""
    import os
    if key_env not in canary_air_keys():
        return None
    reason = os.environ.get("LENS_ALLOW_CANARY_AIR", "").strip()
    if len(reason) < CANARY_AIR_MIN_REASON:
        raise RuntimeError(
            f"{purpose} wants {key_env}, which a System 1 lens breathes "
            f"(CANARY GAS-MASK TEST, ARM 4). Set LENS_ALLOW_CANARY_AIR to a "
            f"written reason of at least {CANARY_AIR_MIN_REASON} characters "
            f"to proceed, e.g. LENS_ALLOW_CANARY_AIR=\"LENS-043 lens4 cap probe x3\".")
    log.warning("[CANARY AIR] %s is spending %s -- reason: %s", purpose, key_env, reason)
    return reason


if __name__ == "__main__":
    # Self-test: every fallback pair is registered, no China-lineage models.
    # CC-83 (LENS-042): Bro Alpha ruled China-related engines and models are never
    # used, no exception (extends D-004). Families are listed by name because
    # a registry row carries a model id, not a country. Anything outside a
    # Freedom-from-Fear origin that is not on this list is Bro Alpha's call.
    banned = ("qwen", "deepseek", "kimi", "minimax", "moonshot", "glm", "yi-",
              "zhipu", "zai", "chatglm", "ernie", "baidu", "doubao", "hunyuan",
              "baichuan", "internlm", "step-", "skywork", "tele-", "sensetime")
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
    assert canary_air_keys() and all(isinstance(k, str) and k for k in canary_air_keys())
    print(f"lens_models self-test OK: {len(ROLES)} roles, "
          f"{len(_KNOWN_WIRE)} wire pairs, {len(PROVIDER_LIMITS)} limit rows")
