from pathlib import Path
p = Path("docs/LENS_TARGET_AND_ORDER.md")
b = p.read_bytes()
old = b"2  S2-E EXCLUDED FROM MISSION ANALYST, EVERY WAVE  [R3]"
new = b"1b S2-E EXCLUDED FROM MISSION ANALYST, EVERY WAVE  [R3]"
n = b.count(old); assert n == 1, "anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(old, new, 1))
print("renumbered to 1b")
