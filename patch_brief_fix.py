from pathlib import Path
p = Path("docs/NEXT_SESSION_BRIEF_LENS034.md")
b = p.read_bytes()
print("BEFORE  CRLF", b.count(b"\r\n"), "bare_LF", b.count(b"\n") - b.count(b"\r\n"))
assert b.count(b"\r\n") == 0, "expected pure LF -- STOP"
old = b"| d1acc5d | CC-47 S3 context reserved slot -- AWAITING CERT |"
new = b"| d1acc5d | CC-47 S3 context reserved slot (cert status: see the order) |"
n = b.count(old); assert n == 1, "anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(old, new, 1))
print("AFTER   CRLF", p.read_bytes().count(b"\r\n"), "DONE")
