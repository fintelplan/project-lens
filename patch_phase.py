from pathlib import Path

# ---------- 1. contract: PHASE TRANSITION section ----------
p = Path("docs/LENS_CONTRACT.md")
b = p.read_bytes()
crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
print("contract BEFORE  CRLF", crlf, "bare_LF", lf)
assert not crlf, "expected pure LF -- STOP"
assert b"PHASE TRANSITION" not in b, "ALREADY PATCHED -- STOP"

add = """
## PHASE TRANSITION (added LENS-033, 2026-08-07)
A target is FINISHED when its definition of done is met -- not when we get
bored, and not when something more interesting appears. A half-finished phase
abandoned for a new one is how the loop restarts under a different name.

Regenerating the order is NOT changing the target. The phase ends only when
the order has no urgent and no important items left -- only accepted retire
candidates and lifecycle maintenance.

When it does, four steps, in this order:
1. DECLARE THE TARGET ACHIEVED, WITH EVIDENCE -- the specific runs and
   measurements that prove it, not "we think we are done." Skipping this step
   is exactly what happened after the Aug-16 cliff: it was achieved and
   certified, never declared, and the working target drifted to a much larger
   one undeclared. That drift is what grew the item list 13 -> 15 in a single
   session.
2. ARCHIVE THE COMPLETED ORDER -- git mv to
   docs/archive/LENS_TARGET_AND_ORDER_<target-slug>.md. An archived order is
   history and SHOULD be named; only the live one keeps the fixed path, so
   the OPEN prompt can always name it and no agent has to hunt for the
   current version.
3. DECLARE THE NEW TARGET -- JAMES RULES THIS. A target states what Lens is
   FOR at this stage. Claude proposes options with honest leans; James
   decides. Claude does not choose a target.
4. REGENERATE THE ORDER FROM SCRATCH AGAINST THE NEW TARGET. Ranks are
   TARGET-RELATIVE, so every surviving item is RE-CLASSIFIED, never
   inherited. Precedent: RT teasers ranked "able to wait" under the cliff
   target and URGENT under the no-silent-failure target. The same reversal
   will happen again.
"""
p.write_bytes(b + add.encode("utf-8"))
d = p.read_bytes()
print("contract AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))

# ---------- 2. protocol: changelog clause in CLOSE step 4 ----------
p2 = Path("docs/LENS_SESSION_PROTOCOL.md")
b2 = p2.read_bytes()
print("protocol BEFORE  CRLF", b2.count(b"\r\n"), "bare_LF", b2.count(b"\n") - b2.count(b"\r\n"))
assert b"CHANGED THIS REGENERATION" not in b2, "ALREADY PATCHED -- STOP"

old = b"5) Declare NEXT session's mission = new top of the order.\n"
new = (b"   The regenerated order carries a CHANGED THIS REGENERATION section:\n"
       b"   items closed, merged, retired, re-ranked -- one line each. git log -p\n"
       b"   keeps every version; the section says WHY it changed.\n"
       b"   If the target is ACHIEVED, run the PHASE TRANSITION ritual instead\n"
       b"   (LENS_CONTRACT.md) -- James declares the new target, not Claude.\n"
       b"5) Declare NEXT session's mission = new top of the order.\n")
n = b2.count(old); assert n == 1, "protocol anchor %d != 1 -- STOP" % n
p2.write_bytes(b2.replace(old, new, 1))
d2 = p2.read_bytes()
print("protocol AFTER   CRLF", d2.count(b"\r\n"), "bare_LF", d2.count(b"\n") - d2.count(b"\r\n"))
print("DONE")
