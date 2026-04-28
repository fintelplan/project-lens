"""smoke_test_ollama.py — single-call connectivity + JSON test for Ollama provider.

Before running full calibration sweeps on Ollama, verify:
  1. Ollama daemon is reachable at localhost:11434
  2. The configured model exists and responds
  3. The model honors response_format={"type": "json_object"} OR can produce valid JSON
  4. Latency is reasonable for our prompt size

Usage:
  python smoke_test_ollama.py llama3:8b
  python smoke_test_ollama.py qwen3.5:9b
  python smoke_test_ollama.py gpt-oss:20b

Default model if no arg: llama3:8b (smallest, most likely to work first)
"""
import sys
import os
import time
import json

DEFAULT_MODEL = "llama3:8b"
model = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

print(f"=" * 70)
print(f"  Ollama smoke test: {model}")
print(f"=" * 70)
print()

# Check openai SDK
try:
    from openai import OpenAI
    print(f"[ OK ] openai SDK importable")
except ImportError as e:
    print(f"[FAIL] openai SDK not installed — pip install openai")
    sys.exit(1)

# Configure client
host = os.environ.get("OLLAMA_HOST", "localhost:11434")
print(f"[INFO] Ollama host: {host}")
print(f"[INFO] Model: {model}")
print()

client = OpenAI(api_key="ollama", base_url=f"http://{host}/v1")

# Test 1: Plain text completion (verifies daemon + model)
print("--- Test 1: Plain text completion ---")
t0 = time.time()
try:
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": "Reply with exactly the word: PONG"},
        ],
        max_tokens=10,
    )
    dt = time.time() - t0
    text = resp.choices[0].message.content.strip()
    print(f"[ OK ] Plain text response in {dt:.1f}s: {text!r}")
except Exception as e:
    dt = time.time() - t0
    print(f"[FAIL] After {dt:.1f}s: {type(e).__name__}: {e}")
    print()
    print("Likely causes:")
    print(f"  - Ollama daemon not running. Start with: ollama serve")
    print(f"  - Model '{model}' not pulled. Run: ollama pull {model}")
    print(f"  - Wrong host. Check OLLAMA_HOST env var (current: {host})")
    sys.exit(1)

print()

# Test 2: JSON output capability
print("--- Test 2: JSON structured output ---")
t0 = time.time()
try:
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a JSON-only response model. Return valid JSON, no prose."},
            {"role": "user", "content": 'Return a JSON object with three fields: "name" (string), "count" (number), "tags" (array of strings). Use any sensible test values.'},
        ],
        response_format={"type": "json_object"},
        max_tokens=200,
    )
    dt = time.time() - t0
    text = resp.choices[0].message.content.strip()
    print(f"[INFO] Response in {dt:.1f}s, length {len(text)} chars")

    # Try parsing
    try:
        parsed = json.loads(text)
        print(f"[ OK ] Valid JSON: {json.dumps(parsed, indent=2)[:200]}")
        json_supported = True
    except json.JSONDecodeError as je:
        print(f"[WARN] JSON parse failed: {je}")
        print(f"[WARN] Raw output (first 300 chars): {text[:300]}")
        json_supported = False
except Exception as e:
    dt = time.time() - t0
    print(f"[FAIL] After {dt:.1f}s: {type(e).__name__}: {e}")
    print()
    print("response_format may not be supported by this model.")
    print("Falling back to non-JSON-mode...")

    t0 = time.time()
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a JSON-only response model. Return valid JSON, no prose."},
                {"role": "user", "content": 'Return a JSON object with: "name", "count", "tags". Use sensible test values.'},
            ],
            max_tokens=200,
        )
        dt = time.time() - t0
        text = resp.choices[0].message.content.strip()
        print(f"[INFO] Without response_format: response in {dt:.1f}s")

        try:
            # Strip markdown fences if present
            cleaned = text.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                cleaned = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
            parsed = json.loads(cleaned)
            print(f"[ OK ] Valid JSON (without response_format): {json.dumps(parsed, indent=2)[:200]}")
            json_supported = "without_response_format"
        except json.JSONDecodeError as je:
            print(f"[FAIL] Even without response_format, JSON parse failed: {je}")
            print(f"[FAIL] Raw output: {text[:300]}")
            json_supported = False
    except Exception as e2:
        print(f"[FAIL] Fallback also failed: {e2}")
        json_supported = False

print()
print("=" * 70)
print("  VERDICT")
print("=" * 70)
if json_supported is True:
    print(f"[ READY ] {model} works with response_format=json_object")
    print(f"[ READY ] Safe to use for calibration sweep")
elif json_supported == "without_response_format":
    print(f"[PARTIAL] {model} produces JSON but doesn't honor response_format")
    print(f"[PARTIAL] Calibration may need a small modification (strip markdown fences)")
else:
    print(f"[ STOP  ] {model} does NOT produce reliably-parseable JSON")
    print(f"[ STOP  ] Try a different model — recommend qwen3.5:9b or gpt-oss:20b")
