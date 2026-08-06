from pathlib import Path

p = Path("code/lens_mission_analyst.py")
b = p.read_bytes()

crlf = b.count(b"\r\n")
lf = b.count(b"\n") - crlf
assert not (crlf and lf), "MIXED EOL -- STOP (crlf=%d bare_lf=%d)" % (crlf, lf)
E = b"\r\n" if crlf else b"\n"
print("lens_mission_analyst.py EOL:", repr(E), "crlf=%d bare_lf=%d" % (crlf, lf))

assert b"CC-45" not in b, "ALREADY PATCHED -- STOP"
assert b"overflow=" not in b, "ALREADY PATCHED (overflow= present) -- STOP"

old = (b'        else:' + E +
       b'            log.info("S3 context skipped ' + b"\xe2\x80\x94" + b' prompt cap reached")' + E)

new = (b'        else:' + E +
       b'            # CC-45: report WHAT was dropped and by how much, so the cap' + E +
       b'            # can be sized from measurement instead of guessed. Prefix is' + E +
       b'            # unchanged so existing log greps still match (LR-122).' + E +
       b'            _s3_over = total_chars + len(s3_section) - MAX_TOTAL_CHARS' + E +
       b'            log.info("S3 context skipped ' + b"\xe2\x80\x94" + b' prompt cap reached: "' + E +
       b'                     f"s3_chars={len(s3_section)} total_chars={total_chars} "' + E +
       b'                     f"cap={MAX_TOTAL_CHARS} overflow={_s3_over}")' + E)

n = b.count(old)
assert n == 1, "anchor count %d != 1 -- STOP" % n
p.write_bytes(b.replace(old, new, 1))
print("CC-45: skip line instrumented")
