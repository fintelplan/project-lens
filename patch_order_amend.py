from pathlib import Path

p = Path("docs/LENS_TARGET_AND_ORDER.md")
b = p.read_bytes()
crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
print("BEFORE  CRLF", crlf, "bare_LF", lf)
assert not crlf, "expected pure LF -- STOP"
assert b"LR-138" not in b, "ALREADY AMENDED -- STOP"

old = b"### OTHERS\n"
new = b"""### IMPORTANT (continued)
6b REGISTER AND ROUTING  [R1/R5]
   6b.1 REGISTER-INTEGRITY GATE (was B7 in the LENS-032 brief). It counts
        rule DEFINITIONS across BOTH formats -- "^## LR-" headings AND the
        compact "^LR-\\d{3}\\s" entries -- excluding prose cross-references,
        then diffs against the expected unbroken sequence. Both gates in use
        today pass on a register that has silently lost a rule; this is the
        only thing that would have caught LR-124..126 vanishing. Lives in
        tests/ so CI enforces it.
        DECLARED: this item was DROPPED from the 2026-08-07 regeneration
        without a retire decision. That was a violation of the RETIRE clause
        in its first application. Restored here.
   6b.2 MINT LR-138 -- an artifact is not verified by its own consistency.
        Before committing a change AGREED in conversation, grep one
        distinguishing phrase per agreed element and report the hits.
        git log --stat proves message-vs-contents; it cannot prove
        contents-vs-agreement. Evidence: 657f5cf described 3 changes and
        shipped 2; fcde3ad shipped 1 of 6 agreed prompt changes and its
        message was ACCURATE. Register is lens-DOC-002_rules.md in the repo
        ROOT, CRLF 402 / bare_LF 248 -- append onto raw bytes, assert the
        bare-LF count unchanged either side.
   6b.3 AMEND LR-078 -- never pipe a heredoc into python (python - << EOF);
        always cat > file << EOF then python file. Never place a literal
        backtick in a heredoc body; build fences with chr(96). Evidence:
        two stalls in one session, one of which produced 657f5cf.
   6b.4 ADD THE ROUTING RULE to LENS_CONTRACT.md: a finding is routed ONCE
        by what it is -- do (order) / learned (LR) / chosen (D) / true now
        (brief). A finding in two homes is a routing error. Then promote the
        durable hazards out of the briefs into the register: the HAZARDS
        section has been acting as a rules register nobody promotes from,
        which is why LENS-032 carried eleven hazards and the register has
        not gained a rule since LR-137.

### OTHERS
"""
n = b.count(old); assert n == 1, "OTHERS anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(old, new, 1))
d = p.read_bytes()
print("AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))
print("DONE")
