import pathlib
p = pathlib.Path("code/lens_s2d_adversary.py")
b0 = p.read_bytes()
crlf, lf = b0.count(b"\r\n"), b0.count(b"\n")
nl = b"\r\n" if crlf > 0 else b"\n"
assert crlf == 0 or crlf == lf, "MIXED line endings -- STOP (crlf=%d lf=%d)" % (crlf, lf)
print("line ending: %r  bytes=%d" % (nl, len(b0)))
assert b"_call_fallback_leg" not in b0, "CC-56 already applied -- STOP (idempotency)"

FUNC = nl.join(x.encode("ascii") for x in [
 'def _call_fallback_leg(user_message: str, prompt_chars: int) -> Optional[dict]:',
 '    """CC-56: the registry fb leg, via plain requests (the CC-55 pattern).',
 '',
 '    Returns a parsed dict, or None so the caller\'s failure path is unchanged',
 '    when both legs are down. LR-142: fallback() returns None for a role with',
 '    no declared leg, so FB_PROVIDER may be None here -- guarded below.',
 '',
 '    NO response-guard call here, on purpose: S2-D\'s primary path has none,',
 '    and a leg that validates while the primary does not is an asymmetry',
 '    invented at the fallback rather than an improvement.',
 '',
 '    CALIBRATION, measured 2026-08-19 BEFORE wiring, on production\'s own',
 '    fixture: 3/3 stop, all fields present, budget_used 12%, ~7s. Per batch it',
 '    returned 6-7 key_claims against Cerebras\' ~13.3, and consistency',
 '    0.85/0.70/0.70 against the Cerebras band 0.78-0.92. THIS LEG IS A',
 '    DIFFERENT EXTRACTOR, NOT A DROP-IN: claim counts roughly halve and',
 '    confidence_score steps down. Shipped documented because the alternative',
 '    is no adversary-narrative input to MA at all.',
 '    """',
 '    if FB_PROVIDER != "mistral":',
 '        log.error(f"S2-D fallback leg is {FB_PROVIDER}/{FB_MODEL}; only mistral is wired")',
 '        return None',
 '    key = os.environ.get(FB_KEY_ENV)',
 '    if not key:',
 '        log.error(f"{FB_KEY_ENV} not set -- S2-D fallback leg unavailable")',
 '        return None',
 '    fb_max_tokens = fit_max_tokens(prompt_chars, MAX_OUT, FB_PROVIDER, FB_MODEL)',
 '    try:',
 '        assert_model_known(FB_PROVIDER, FB_MODEL)',
 '        log.warning(',
 '            f"S2-D FALLBACK: primary {PROVIDER}/{MODEL} exhausted -- "',
 '            f"calling {FB_PROVIDER}/{FB_MODEL}"',
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
 '            log.error(f"S2-D fallback HTTP {resp.status_code}: {resp.text[:200]}")',
 '            return None',
 '        body  = resp.json()',
 '        usage = body.get("usage") or {}',
 '        log.info(',
 '            f"S2-D fallback usage: prompt={usage.get(\'prompt_tokens\')} "',
 '            f"completion={usage.get(\'completion_tokens\')} "',
 '            f"total={usage.get(\'total_tokens\')}"',
 '        )',
 '        raw = body["choices"][0]["message"]["content"].strip()',
 '        if "<think>" in raw and "</think>" in raw:',
 '            raw = raw[raw.index("</think>") + 8:].strip()',
 '        fence = chr(96) * 3   # LR-078: never a literal backtick in a patch body',
 '        if raw.startswith(fence):',
 '            raw = raw.split(fence)[1]',
 '            if raw.startswith("json"):',
 '                raw = raw[4:]',
 '        parsed = json.loads(raw.strip())',
 '        log.info(',
 '            f"S2-D fallback result: claims={len(parsed.get(\'key_claims\', []))} "',
 '            f"consistency={parsed.get(\'narrative_consistency_score\', 0)}"',
 '        )',
 '        return parsed',
 '    except Exception as e:',
 '        log.error(f"S2-D fallback failed: {e}")',
 '        return None',
 '',
 '',
])

edits = [
 (b"import logging" + nl,
  b"import logging" + nl + b"import requests" + nl),
 (b"    fit_max_tokens," + nl,
  b"    fallback," + nl + b"    fit_max_tokens," + nl),
 (b'PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2d_adversary")' + nl,
  b'PROVIDER, MODEL, KEY_ENV, MAX_OUT = wire("s2d_adversary")' + nl
  + nl.join(x.encode("ascii") for x in [
      '# CC-56: the registry declared this leg since the cliff and no call site',
      '# read it. Cerebras ended its free tier 2026-08-17 and S2-D produced',
      '# nothing while mistral-small-2603 sat in lens_models.py, unreachable.',
      '# LR-142: fallback() returns None for a role with no leg -- unpack',
      '# defensively so a future copy of this block cannot raise at import.',
      '_FB = fallback("s2d_adversary") or (None, None, None)',
      'FB_PROVIDER, FB_MODEL, FB_KEY_ENV = _FB',
      ''])),
 (b"def save_adversary_report(", FUNC + b"def save_adversary_report("),
 (b'    log.error(f"S2-D failed after {MAX_RETRIES} attempts")' + nl + b"    return None",
  b'    log.error(f"S2-D failed after {MAX_RETRIES} attempts")' + nl
  + b"    return _call_fallback_leg(user_message, prompt_chars)"),
]

b = b0
deltas = []
for i, (old, new) in enumerate(edits, 1):
    n = b.count(old)
    assert n == 1, "anchor %d count %d != 1 -- STOP, report back" % (i, n)
    b = b.replace(old, new)
    deltas.append((old, new))

SYMS = ["_call_fallback_leg", "FB_PROVIDER", "FB_MODEL", "FB_KEY_ENV",
        "fit_max_tokens", "requests.post", "assert_model_known", "fallback("]
print("%-20s %5s %5s %5s %5s" % ("symbol", "pre", "delta", "pred", "final"))
bad = []
for s in SYMS:
    e = s.encode()
    pre = b0.count(e)
    add = sum(new.count(e) - old.count(e) for old, new in deltas)
    fin = b.count(e)
    flag = "" if pre + add == fin else "  <-- MISMATCH"
    if flag:
        bad.append(s)
    print("%-20s %5d %5d %5d %5d%s" % (s, pre, add, pre + add, fin, flag))
assert not bad, "count mismatch on %s -- STOP, nothing written" % bad
p.write_bytes(b)
print("CC-56 applied: %d -> %d bytes" % (len(b0), len(b)))
