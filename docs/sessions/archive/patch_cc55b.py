import pathlib
p = pathlib.Path("code/lens_s2e_legitimacy.py")
b = p.read_bytes()
nl = b"\r\n" if b.count(b"\r\n") > 0 else b"\n"
old = (b'            f"S2-E fallback usage: prompt={usage.get(chr(39)+chr(39))}"' + nl
       + b"            if False else" + nl)
n = b.count(old)
assert n == 1, "anchor count %d != 1 -- STOP, report back" % n
b = b.replace(old, b"")
assert b"if False else" not in b, "dead branch still present -- STOP"
p.write_bytes(b)
print("CC-55b: dead conditional removed, %d bytes" % len(b))
