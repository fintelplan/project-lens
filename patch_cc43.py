from pathlib import Path

ARROW = b"\xe2\x86\x92"          # U+2192, kept as escapes per LR-101

def eol_of(raw):
    crlf = raw.count(b"\r\n")
    lf = raw.count(b"\n") - crlf
    assert not (crlf and lf), "MIXED EOL -- STOP (crlf=%d bare_lf=%d)" % (crlf, lf)
    return b"\r\n" if crlf else b"\n"

# ---------------- file 1: code/lens_regular_report.py ----------------
p1 = Path("code/lens_regular_report.py")
b1 = p1.read_bytes()
E = eol_of(b1)
print("lens_regular_report.py EOL:", repr(E))

for probe in (b"CC-43", b"mistral-small -> cerebras"):
    assert probe not in b1, "ALREADY PATCHED (%r present) -- STOP" % probe

# edit 1 -- docstring chain line
old = (b"Model: mistral-small-latest (free) " + ARROW +
       b" Cerebras fallback " + ARROW + b" Groq fallback")
new = (b"Model: mistral-small-latest (free) -> Cerebras fallback" + E +
       b"NOTE (CC-43): Groq leg 3 removed -- this position's real prompt exceeds" + E +
       b"  Groq's TPM ceiling several times over. The Cerebras leg is also NOT" + E +
       b"  reachable on an API failure: _FORCE_PROVIDER is written, never read.")
n = b1.count(old); assert n == 1, "edit1 anchor count %d != 1 -- STOP" % n
b1 = b1.replace(old, new, 1)

# edit 2 -- chain comment
old = b"# Provider chain: mistral-small " + ARROW + b" cerebras " + ARROW + b" groq"
new = b"# Provider chain: mistral-small -> cerebras  (groq leg 3 removed, CC-43)"
n = b1.count(old); assert n == 1, "edit2 anchor count %d != 1 -- STOP" % n
b1 = b1.replace(old, new, 1)

# edit 3 -- the groq dict inside PROVIDERS
old = (b"    {" + E +
       b'        "name": "groq",' + E +
       b'        "model": "llama-3.3-70b-versatile",' + E +
       b'        "key_env": "GROQ_API_KEY",' + E +
       b'        "base_url": None,  # uses groq SDK' + E +
       b"    }," + E)
n = b1.count(old); assert n == 1, "edit3 anchor count %d != 1 -- STOP" % n
b1 = b1.replace(old, b"", 1)

# edit 4 -- the elif branch (carries its own import)
old = (b'            elif prov["name"] == "groq":' + E +
       b"                from groq import Groq" + E +
       b"                client = Groq(api_key=key)" + E +
       b'                log.info(f"LLM: using Groq ({prov[\'model\']})")' + E +
       b'                return client, prov["model"], "groq"' + E)
n = b1.count(old); assert n == 1, "edit4 anchor count %d != 1 -- STOP" % n
b1 = b1.replace(old, b"", 1)

p1.write_bytes(b1)
print("CC-43 file 1: 4 edits applied")

# ---------------- file 2: code/lens_models.py ----------------
p2 = Path("code/lens_models.py")
b2 = p2.read_bytes()
E2 = eol_of(b2)
print("lens_models.py EOL:", repr(E2))
assert b"CC-43" not in b2, "ALREADY PATCHED -- STOP"

old = (b'        "note": "PROVIDERS is a 3-LEG chain mistral -> cerebras -> groq."' + E2 +
       b'                " This 2-leg schema cannot express leg 3: groq /"' + E2 +
       b'                " GROQ_GPT_OSS_120B / GROQ_API_KEY / max_out 4096."' + E2 +
       b'                " Probe leg 3 with --provider/--model/--key-env overrides.",')
new = (b'        "note": "PROVIDERS is a 2-LEG chain mistral -> cerebras (CC-43)."' + E2 +
       b'                " Leg 3 was groq/llama-3.3-70b-versatile and was REMOVED:"' + E2 +
       b'                " this position\'s real prompt exceeds Groq TPM several"' + E2 +
       b'                " times over, so no Groq model can serve it -- measure it"' + E2 +
       b'                " with probe_lens_models.py --role regular_report --dry-run"' + E2 +
       b'                " before re-proposing one. The cerebras leg is NOT reachable"' + E2 +
       b'                " on an API failure: _FORCE_PROVIDER is written, never read"' + E2 +
       b'                " (CC-44).",')
n = b2.count(old); assert n == 1, "file2 anchor count %d != 1 -- STOP" % n
p2.write_bytes(b2.replace(old, new, 1))
print("CC-43 file 2: note corrected")
