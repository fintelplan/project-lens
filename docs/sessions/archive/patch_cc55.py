import pathlib
p = pathlib.Path("code/lens_s2e_legitimacy.py")
b = p.read_bytes()
crlf, lf = b.count(b"\r\n"), b.count(b"\n")
nl = b"\r\n" if crlf > 0 else b"\n"
assert crlf == 0 or crlf == lf, "MIXED line endings in file -- STOP (crlf=%d lf=%d)" % (crlf, lf)
print("line ending: %r  bytes=%d" % (nl, len(b)))
assert b"_call_fallback_leg" not in b, "CC-55 already applied -- STOP (idempotency)"

FUNC = nl.join(x.encode("ascii") for x in [
 'def _call_fallback_leg(user_message: str, prompt_chars: int,',
 '                       lens_name: str) -> Optional[dict]:',
 '    """CC-55: the registry fb leg, via plain requests (the CC-54 pattern).',
 '',
 '    Returns a parsed dict, or None so the caller\'s FAILED path is unchanged',
 '    when both legs are down. LR-142: fallback() returns None for a role with',
 '    no declared leg, so FB_PROVIDER may be None here -- guarded below.',
 '    """',
 '    if FB_PROVIDER != "mistral":',
 '        log.error(f"S2-E fallback leg is {FB_PROVIDER}/{FB_MODEL}; only mistral is wired")',
 '        return None',
 '    key = os.environ.get(FB_KEY_ENV)',
 '    if not key:',
 '        log.error(f"{FB_KEY_ENV} not set -- S2-E fallback leg unavailable")',
 '        return None',
 '    fb_max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, FB_PROVIDER, FB_MODEL)',
 '    try:',
 '        assert_model_known(FB_PROVIDER, FB_MODEL)',
 '        log.warning(',
 '            f"S2-E FALLBACK: primary {PROVIDER}/{MODEL} exhausted -- "',
 '            f"calling {FB_PROVIDER}/{FB_MODEL} for {lens_name}"',
 '        )',
 '        resp = requests.post(',
 '            "https://api.mistral.ai/v1/chat/completions",',
 '            headers={"Authorization": "Bearer " + key,',
 '                     "Content-Type": "application/json"},',
 '            json={"model": FB_MODEL,',
 '                  "messages": [{"role": "system", "content": SYSTEM_PROMPT},',
 '                               {"role": "user", "content": user_message}],',
 '                  "max_tokens": fb_max_tokens,',
 '                  "temperature": TEMPERATURE},',
 '            timeout=180)',
 '        if resp.status_code != 200:',
 '            log.error(f"S2-E fallback HTTP {resp.status_code}: {resp.text[:200]}")',
 '            return None',
 '        body  = resp.json()',
 '        usage = body.get("usage") or {}',
 '        log.info(',
 '            f"S2-E fallback usage: prompt={usage.get(chr(39)+chr(39))}"',
 '            if False else',
 '            f"S2-E fallback usage: prompt={usage.get(\'prompt_tokens\')} "',
 '            f"completion={usage.get(\'completion_tokens\')} "',
 '            f"total={usage.get(\'total_tokens\')}"',
 '        )',
 '        raw = body["choices"][0]["message"]["content"].strip()',
 '        fence = chr(96) * 3   # LR-078: never a literal backtick in a patch body',
 '        if raw.startswith(fence):',
 '            raw = raw.split(fence)[1]',
 '            if raw.startswith("json"):',
 '                raw = raw[4:]',
 '        parsed = json.loads(raw.strip())',
 '        vr = validate_parsed_response(parsed, "S2-E")',
 '        if not vr.valid:',
 '            log.warning(format_validation_for_log(vr))',
 '        actors = parsed.get("actors_assessed", [])',
 '        low_actors = parsed.get("low_legitimacy_actors_pushing_narrative", [])',
 '        # LENS-037: calibration logged every wave, on purpose. Changing this',
 '        # position\'s model moved actors/row 4.00 -> 8.50 at D-016 and nobody',
 '        # saw it for three weeks. Probed on mistral-small-2603 3/3 stop,',
 '        # actors 8/7/8, low 5/3/5 -- inside the Cerebras-era band.',
 '        log.info(',
 '            f"S2-E fallback result for {lens_name}: {len(actors)} actors, "',
 '            f"{len(low_actors)} LOW legitimacy"',
 '        )',
 '        return parsed',
 '    except Exception as e:',
 '        log.error(f"S2-E fallback failed: {e}")',
 '        return None',
 '',
 '',
])

edits = []
edits.append((b"import logging" + nl,
              b"import logging" + nl + b"import requests" + nl))
edits.append((b"    fit_max_tokens," + nl,
              b"    fallback," + nl + b"    fit_max_tokens," + nl))
edits.append((b'PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2e_legitimacy")' + nl,
              b'PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2e_legitimacy")' + nl
              + nl.join(x.encode("ascii") for x in [
                  '# CC-55: the registry declared this leg since the cliff and no call site',
                  '# read it. Cerebras ended its free tier 2026-08-17 and S2-E produced',
                  '# nothing while mistral-small-2603 sat in lens_models.py, unreachable.',
                  '# LR-142: fallback() returns None for a role with no leg -- unpack',
                  '# defensively so a future copy of this block cannot raise at import.',
                  '_FB = fallback("s2e_legitimacy") or (None, None, None)',
                  'FB_PROVIDER, FB_MODEL, FB_KEY_ENV = _FB',
                  ''])))
edits.append((b"def build_correction_to_ma(analysis: dict) -> dict:",
              FUNC + b"def build_correction_to_ma(analysis: dict) -> dict:"))
edits.append((b'    log.error(f"S2-E failed after {MAX_RETRIES} attempts for {lens_name}")'
              + nl + b"    return None",
              b'    log.error(f"S2-E failed after {MAX_RETRIES} attempts for {lens_name}")'
              + nl + b"    return _call_fallback_leg(user_message, prompt_chars, lens_name)"))

for i, (old, new) in enumerate(edits, 1):
    n = b.count(old)
    assert n == 1, "anchor %d count %d != 1 -- STOP, report back" % (i, n)
    b = b.replace(old, new)

p.write_bytes(b)
print("CC-55 applied: %d bytes" % len(b))
