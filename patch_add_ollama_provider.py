"""patch_add_ollama_provider.py — extend _get_llm_client() with Ollama local provider.

Post-LENS-019.5 cross-lab bias testing pivot: adds Ollama as a fourth provider
option to enable testing local open-source models (DeepSeek, Llama 3, multiple
Qwen variants, Gemma 4, gpt-oss).

Ollama exposes an OpenAI-compatible endpoint at localhost:11434/v1, so we reuse
the openai SDK already installed for OpenRouter. No new dependencies.

Configuration via env vars:
  S2F_PROVIDER=ollama          (required to activate)
  OLLAMA_MODEL=gpt-oss:20b     (required, must be a model ollama has pulled)
  OLLAMA_HOST=localhost:11434  (optional, defaults to localhost:11434)

Usage examples:
  S2F_PROVIDER=ollama OLLAMA_MODEL=llama3:8b python calibrate_rubric_article1_v3.py
  S2F_PROVIDER=ollama OLLAMA_MODEL=qwen3.5:9b python calibrate_rubric_article3.py

Idempotent. To revert: --revert flag.
"""
import sys
import pathlib

p = pathlib.Path("code/lens_framing_rubrics.py")
if not p.exists():
    print("FAIL: code/lens_framing_rubrics.py not found")
    sys.exit(1)

src = p.read_text(encoding="utf-8")
REVERT = "--revert" in sys.argv

# Anchor: end of openrouter branch, before "# Default: groq"
# Match the exact end of the openrouter block we added earlier
ANCHOR = '''        log.info(f"Using OpenRouter provider (model: {model})")
        return client, model, "openrouter"

    # Default: groq'''

# New Ollama branch to inject (between openrouter and groq default)
OLLAMA_BLOCK = '''        log.info(f"Using OpenRouter provider (model: {model})")
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

    # Default: groq'''

if REVERT:
    if OLLAMA_BLOCK not in src:
        print("Already reverted (no ollama block present).")
        sys.exit(0)
    src = src.replace(OLLAMA_BLOCK, ANCHOR, 1)
    p.write_text(src, encoding="utf-8")
    print("OK: ollama provider removed.")
    import py_compile
    py_compile.compile(str(p), doraise=True)
    print("Module compiles cleanly.")
    sys.exit(0)

# Idempotency: check if already patched
if 'provider == "ollama"' in src:
    print("Already patched (ollama provider present).")
    sys.exit(0)

# Anchor check
if ANCHOR not in src:
    print("FAIL: anchor not found. Check that the openrouter branch matches the expected pattern.")
    print("Inspect with: grep -n 'Using OpenRouter' code/lens_framing_rubrics.py")
    sys.exit(1)

# Apply patch
src = src.replace(ANCHOR, OLLAMA_BLOCK, 1)
p.write_text(src, encoding="utf-8")

# Verify compile
import py_compile
py_compile.compile(str(p), doraise=True)
print("OK: ollama provider added, module compiles cleanly.")
print()
print("Smoke test:")
print('  S2F_PROVIDER=ollama OLLAMA_MODEL=llama3:8b python -c "import sys; sys.path.insert(0,\\"code\\"); from lens_framing_rubrics import _get_llm_client; c,m,p=_get_llm_client(); print(f\\"client={type(c).__name__}, model={m}, provider={p}\\")"')
