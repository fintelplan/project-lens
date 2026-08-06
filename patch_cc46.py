from pathlib import Path

p = Path("code/lens_s2b_coordination.py")
b = p.read_bytes()
crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
assert not (crlf and lf), "MIXED EOL -- STOP (crlf=%d bare_lf=%d)" % (crlf, lf)
E = b"\r\n" if crlf else b"\n"
print("EOL:", repr(E), "crlf=%d bare_lf=%d" % (crlf, lf))
assert b"visible_text" not in b, "ALREADY PATCHED -- STOP"

old = b'        body    = truncate_report(r.get("content", "") or "")'
new = (b'        # CC-46: strip markup BEFORE the length clip, so the 8000-char' + E +
       b'        # budget is spent on text a model can read, not on tags and' + E +
       b'        # base64 tracking URLs. No article is dropped -- only markup.' + E +
       b'        body    = truncate_report(visible_text(r.get("content", "") or ""))')
n = b.count(old); assert n == 1, "body anchor count %d != 1 -- STOP" % n
b = b.replace(old, new, 1)

imp_old = b"MISTRAL_FALLBACK_MODEL = "
n = b.count(imp_old); assert n >= 1, "import anchor not found -- STOP"
idx = b.index(imp_old)
b = b[:idx] + b"from lens_text_utils import visible_text, extractor_name" + E + E + b[idx:]

log_old = b'    log.info(f"S2-B prompt: {len(sections)} raw articles, {total_chars} chars "'
log_new = (b'    log.info(f"S2-B prompt: {len(sections)} raw articles, {total_chars} chars "' + E +
           b'             f"[visible-text via {extractor_name()}] "')
n = b.count(log_old); assert n == 1, "log anchor count %d != 1 -- STOP" % n
b = b.replace(log_old, log_new, 1)

p.write_bytes(b)
print("CC-46: visible_text wired into S2-B build_prompt")
