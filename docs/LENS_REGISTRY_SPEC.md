# LENS REGISTRY SPEC v2 — provider-aware ceilings

**Status: APPROVED PROPOSAL, not yet built. Freeze holds until the S2-D cert is read.**
Supersedes the v1 sketch (separate `REQUEST_CEILINGS` dict — rejected: a second source
for a truth `LIMITS` already holds).

---

## 1. Why the old constant was wrong

`fit_max_tokens` hardcoded `7500`, derived from a supposed Groq per-request ceiling of
8,192. Both numbers were wrong in an instructive way.

Groq `openai/gpt-oss-120b` has a **131,072-token context window** and **65,536 max
completion tokens** (console docs, 2026-07-28). Nothing about 8,192 is a context limit.
The real binding constraint was always **TPM**: a single request larger than the
per-minute allowance can never be served, whatever the context window says.

**Do not "restore" 8,192.** It was TPM all along, and Groq's verified TPM is 8,000.

The cost of the hardcoded constant was measured, not theorised: Mission Analyst and S2-E
were both strangled by a Groq ceiling while sitting on Cerebras-capable prompts, and S2-E
returned literally zero characters 3/3 until the budget was raised.

---

## 2. Ceiling resolution

```python
def request_ceiling(provider, model) -> int | None:
    """Largest single request this pair can ever satisfy, or None if unknowable.

    CTX + TPM               -> min(CTX, TPM)      (token-metered providers)
    CTX only, METER=requests -> CTX               (no minute allowance can bind)
    otherwise                -> None              (LIMITS_UNKNOWN)
    """
```

**Returns `None`; never raises.** Raising would break S2-C, S3-B, S3-C and S2-F, which all
work today on flat budgets — a documentation gap must not become an outage. `fit_max_tokens`
with an unresolved ceiling **returns `cap` unchanged and logs an ERROR naming the pair**,
which is exactly today's behaviour, so the change is status-quo-preserving.

Loudness lives in `verify_registry_alignment()`, which reports unresolved ceilings
alongside unregistered pairs and therefore reaches the pre-flight Telegram line.
**Guard reports, call sites enforce.**

### Signature — pair-keyed, provider required positional

```python
def fit_max_tokens(prompt_chars: int, cap: int,
                   provider: str, model: str) -> int:
    ceiling = request_ceiling(provider, model)
    if ceiling is None:
        log.error(f"[LENS-MODELS] no resolvable ceiling for "
                  f"({provider!r}, {model!r}) -- returning cap unchanged")
        return cap
    usable = ceiling - max(MARGIN_FLOOR, int(ceiling * MARGIN_FRACTION))
    return max(768, min(cap, usable - prompt_chars // CHARS_PER_TOKEN))
```

**Provider alone is insufficient** — two Groq models on the same key have different TPM:
`llama-3.3-70b-versatile` is 12,000 and `openai/gpt-oss-120b` is 8,000. A provider-only
lookup would have refused the MA baseline (6,893 + 2,500 = 9,393) that in fact ran clean
3/3. The pair is what `LIMITS` is already keyed by.

`MARGIN_FRACTION = 0.08` (proportional: estimate error scales with request size).

---

## 3. LIMITS rows — CTX and METER inside the existing table

`METER` distinguishes **"no TPM exists"** from **"nobody has checked."** Those two must
never collapse into the same silence.

| pair | METER | TPM | CTX | other | tag |
| --- | --- | --- | --- | --- | --- |
| groq / `openai/gpt-oss-120b` | tokens | 8,000 | 131,072 | RPM 30, RPD 1,000, TPD 200,000 | VERIFIED-console 07-27 |
| groq / `openai/gpt-oss-20b` | tokens | 8,000 | — | RPM 30, RPD 1,000, TPD 200,000 | VERIFIED-console 07-27 |
| cerebras / `gpt-oss-120b` | tokens | 30,000 | **131,000** | RPM 5, **RPD 2,400**, TPD 1,000,000, MAX_COMPLETION 40,000 | **VERIFIED-console 07-28** |
| mistral / `mistral-small-2603` | tokens | 50,000 | 128,000 (VERIFY) | RPS 0.83 | VERIFIED-console 07-28 |
| cohere / `command-r-plus-08-2024` | **requests** | — (none exists) | 128,000 (VERIFY) | 20 req/min | VERIFIED-docs 07-28 |
| cloudflare / `@cf/openai/gpt-oss-120b` | **requests** | — (none exists) | **128,000** | 300 req/min, 10,000 Neurons/day | VERIFIED-docs 07-28 |
| gemini / `gemini-2.5-flash` | ? | **UNKNOWN** | 1,048,576 in / 65,536 out | — | LIMITS_UNKNOWN |
| gemini / `gemini-2.5-flash-lite` | ? | **UNKNOWN** | 1,048,576 in / 65,536 out | — | LIMITS_UNKNOWN |

### Resolved ceilings

| pair | ceiling | binding constraint |
| --- | --- | --- |
| groq gpt-oss-120b | **8,000** | TPM (context is 131,072 and irrelevant) |
| groq llama-3.3-70b | **12,000** | TPM |
| cerebras gpt-oss-120b | **30,000** | TPM — CTX 131,000 unreachable |
| mistral small-2603 | **50,000** | TPM |
| cohere command-r-plus | **128,000** | CTX (request-metered) |
| cloudflare gpt-oss-120b | **128,000** | CTX (request-metered) |
| gemini (both) | **None** | unresolved -> cap returned, ERROR logged |

**Cerebras console overrides the public docs.** Docs said CTX 65,536 / max output 32,768;
the org console says **CTX 131,000 / MAX_COMPLETION 40,000 / RPD 2,400**. The ceiling still
resolves to 30,000 because TPM binds either way — but the recorded numbers must be the
console's, and the tag is VERIFIED-console.

The console also warns that **short intervals are enforced** ("60 RPM may be enforced as
1 req/sec"). At RPM 5 that independently supports the ~65s S2-E spacing: 4 sequential
calls cannot share a minute at any margin.

---

## 4. Mistral — pin a dated ID, never an alias

**RULING (2026-07-28): the registry uses `mistral-small-2603`.**

Grounds: no `mistral-small-latest` model card exists at all, and `-latest` aliases carry
far lower limits than dated IDs — `mistral-medium-latest` is 25,000 TPM against
`mistral-medium-2508` at 356,250. An alias is an unpinned wire ID, which is the exact
class of drift the registry exists to kill.

- Registry: **`mistral-small-2603`** — TPM 50,000, RPS 0.83 (VERIFIED-console 07-28)
- Higher-throughput option if a fallback ever needs it: **`mistral-small-2506`** — TPM
  2,250,000, RPS 5.00. Recorded as a comment only; not wired.

### Byte-check result (grep, not assumption)

**Production calls `mistral-small-latest` everywhere. The `mistral-medium-latest` default
is dead in production — but it is a live trap.**

- `lens_framing_rubrics.py:399` — `os.environ.get("MISTRAL_MODEL", "mistral-medium-latest")`
- `.github/workflows/lens-s2f-scoring.yml:77` — sets `MISTRAL_MODEL: "mistral-small-latest"`
- s2f-scoring is the **only** workflow that runs framing_rubrics (sole entrypoint
  `lens_s2f_scoring_cron.py:89`), so the medium default is never reached on the cron path.

The trap: anyone running framing_rubrics locally, or adding a workflow that forgets
`MISTRAL_MODEL`, silently gets a **different model with different limits**. The registry
should own this value so the env-var default disappears entirely.

**Sweep scope when the alias is dropped — larger than it looks: 15 literals across 10 files**
(`lens_regular_report`, `lens_s1_report`, `lens_s2_step_report`, `lens_s2a_injection`,
`lens_s2b_coordination`, `lens_s2c_emotion`, `lens_s3_step_report`, `lens_s3b_truehistory`,
`lens_s3d_longterm`, `lens_s3f_countercheck`), plus `lens_models.py` and the s2f workflow.

---

## 5. Gemini — deferred, and the target has moved

Still **LIMITS_UNKNOWN**; AI Studio not yet read. Context windows are published
(1,048,576 in / 65,536 out for both 2.5 Flash and Flash-Lite) but no free-tier TPM/RPM/RPD.

**Gemini 3.x now exists** (3.1 / 3.5 / 3.6 Flash and Flash-Lite). Since `gemini-2.5-flash`
dies **2026-10-16**, the deferred Gemini decision should evaluate a **3.x Flash-Lite**
rather than migrating onto 2.5 — moving to a model with a known death date buys nothing.

---

## 6. Prerequisite ordering

The VERIFY sweep is a **prerequisite for CC-3/CC-4, not a follow-up**: those blocks make
every call site adopt `fit_max_tokens`. With `None`-instead-of-raise this is no longer an
outage risk, but any pair still tagged LIMITS_UNKNOWN silently keeps its flat `cap`, so the
migration would be cosmetic for those positions.

Outstanding console reads: Gemini free-tier RPM/TPM/RPD (AI Studio).

---

*LENS_REGISTRY_SPEC.md v2 | 2026-07-28 | LENS-028 | proposal — no code written*
