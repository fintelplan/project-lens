"""patch_add_openrouter_provider.py — extend _get_llm_client() with OpenRouter provider.

Post-LENS-019.5 bias-test work: adds Llama 4 Maverick free variant via OpenRouter
as a third provider option for comparing against Cerebras+qwen-3-235b on
China-sensitive content (Article 6 bias test).

OpenRouter uses OpenAI-compatible API. Requires:
  - openai Python package (already in requirements.txt for most projects)
  - OPENROUTER_API_KEY env var
  - Model: meta-llama/llama-4-maverick:free (community-funded free tier)
  - Base URL: https://openrouter.ai/api/v1

Usage after patch:
  S2F_PROVIDER=openrouter python calibrate_rubric_article6_chosunbiz.py

Idempotent. Safe to run multiple times. To revert: --revert flag.
"""
import sys
import pathlib

p = pathlib.Path("code/lens_framing_rubrics.py")
if not p.exists():
    print("FAIL: code/lens_framing_rubrics.py not found")
    sys.exit(1)

src = p.read_text(encoding="utf-8")

REVERT = "--revert" in sys.argv

# Anchor: end of cerebras branch, before "# Default: groq"
ANCHOR = '''        log.info("Using Cerebras provider (model: qwen-3-235b-a22b-instruct-2507)")
        return Cerebras(api_key=key), "qwen-3-235b-a22b-instruct-2507", "cerebras"

    # Default: groq'''

# New OpenRouter branch to inject (between cerebras and groq default)
OPENROUTER_BLOCK = '''        log.info("Using Cerebras provider (model: qwen-3-235b-a22b-instruct-2507)")
        return Cerebras(api_key=key), "qwen-3-235b-a22b-instruct-2507", "cerebras"

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
        model = "meta-llama/llama-4-maverick:free"
        log.info(f"Using OpenRouter provider (model: {model})")
        return client, model, "openrouter"

    # Default: groq'''

if REVERT:
    if OPENROUTER_BLOCK not in src:
        print("Already reverted (no openrouter block present).")
        sys.exit(0)
    src = src.replace(OPENROUTER_BLOCK, ANCHOR, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: openrouter provider removed.")
    import py_compile
    py_compile.compile(str(p), doraise=True)
    print("Module compiles cleanly.")
    sys.exit(0)

# Idempotency: check if already patched
if 'provider == "openrouter"' in src:
    print("Already patched (openrouter provider present).")
    sys.exit(0)

# Anchor check
if ANCHOR not in src:
    print("FAIL: anchor not found. Check that the cerebras branch matches:")
    print(f'  Expected: ...qwen-3-235b-a22b-instruct-2507"), "cerebras"\\n\\n    # Default: groq')
    print()
    print("Inspect with: grep -n 'qwen-3-235b' code/lens_framing_rubrics.py")
    sys.exit(1)

# Apply patch
src = src.replace(ANCHOR, OPENROUTER_BLOCK, 1)
p.write_text(src, encoding="utf-8")

# Verify compile
import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: openrouter provider added, module compiles cleanly.")
print()
print("Test the new branch:")
print('  S2F_PROVIDER=openrouter python -c "import sys; sys.path.insert(0,\\"code\\"); from lens_framing_rubrics import _get_llm_client; c,m,p=_get_llm_client(); print(f\\"client={type(c).__name__}, model={m}, provider={p}\\")"')
