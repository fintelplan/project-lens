from pathlib import Path
p = Path("docs/LENS_TARGET_AND_ORDER.md")
b = p.read_bytes()
print("BEFORE  CRLF", b.count(b"\r\n"), "bare_LF", b.count(b"\n") - b.count(b"\r\n"))
assert b.count(b"\r\n") == 0, "expected pure LF -- STOP"
assert b"S2-E EXCLUDED" not in b, "ALREADY AMENDED -- STOP"

old = b"### IMPORTANT\n"
new = b"""2  S2-E EXCLUDED FROM MISSION ANALYST, EVERY WAVE  [R3]
   "Prompt cap reached at S2 entry for S2-E" fires in BOTH slots:
   total_chars=27664 evening 2026-08-07, 27465 morning 2026-08-08 against
   an s2_budget of 28000. S2 runs at ~98% of budget CHRONICALLY, so the
   break is systematic, not occasional. The loop breaks AT the S2-E entry,
   so S2-E's evidence -- and anything ordered after it -- never reaches MA's
   synthesis, while S2-E still writes 4 findings to the DB every wave.
   Same intelligence-validity class as the dropped S3 circuit: the synthesis
   position is not seeing its inputs. NOT strictly silent -- the log line
   existed before CC-47 without the numbers and was simply never read.
   HEADROOM EXISTS: MA total=10461 is ~35% of the Cerebras 30,000 ceiling;
   a 45,000-char cap would sit near 58%.
   BLOCKED ON 6d: raising MAX_TOTAL_CHARS again also moves the S1 loop's
   * 0.6 fraction again -- the exact assumption that was wrong in CC-47.
   Log S1's share FIRST, then size the cap with all three consumers visible.

### IMPORTANT
"""
n = b.count(old); assert n == 1, "IMPORTANT anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(old, new, 1))
d = p.read_bytes()
print("AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))
print("DONE")
