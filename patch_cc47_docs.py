from pathlib import Path

F = chr(96) * 3   # code fence, built not pasted -- literal backticks break the heredoc

# ---------- 1. contract ----------
p = Path("docs/LENS_CONTRACT.md")
b = p.read_bytes()
crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
print("contract BEFORE  CRLF", crlf, "bare_LF", lf)
assert not crlf, "expected LF -- STOP"
assert b"- v2 --" not in b, "ALREADY PATCHED -- STOP"

old = b"- Open/close prompts: docs/LENS_SESSION_PROTOCOL.md. Live state: latest docs/NEXT_SESSION_BRIEF_*.md."
new = (b"- TARGET + WORKING ORDER (read FIRST, it names this session's mission): docs/LENS_TARGET_AND_ORDER.md.\n"
       b"- Open/close prompts: docs/LENS_SESSION_PROTOCOL.md. Live state (SESSION STATE ONLY, never the item list): latest docs/NEXT_SESSION_BRIEF_*.md.")
n = b.count(old); assert n == 1, "docmap anchor %d != 1 -- STOP" % n
b = b.replace(old, new, 1)

vold = b"## VERSION LOG\n"
vnew = (b"## VERSION LOG\n"
        b"- v2 -- 2026-08-06, LENS-033. Added MISSION AND SCOPE and DISCOVERY POLICY after James named the loop: each session found a weak point, fixed it, ran out of context, and the next agent found another. Cause: the Aug-16 target was achieved and never formally closed, so the working target drifted undeclared and the item list grew 13 -> 15 inside one session. Rank is now target-relative; freshness confers no priority.\n")
n = b.count(vold); assert n == 1, "version anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(vold, vnew, 1))
d = p.read_bytes()
print("contract AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))

# ---------- 2. protocol ----------
p2 = Path("docs/LENS_SESSION_PROTOCOL.md")
b2 = p2.read_bytes()
print("protocol BEFORE  CRLF", b2.count(b"\r\n"), "bare_LF", b2.count(b"\n") - b2.count(b"\r\n"))
assert b"LENS-0XX OPEN | model:" in b2, "unexpected protocol content -- STOP"
assert b"LENS_TARGET_AND_ORDER" not in b2, "ALREADY REWRITTEN -- STOP"
head, notes = b2.split(b"## Notes", 1)

L = [
"# Lens Session Protocol -- standard open/close prompts",
"**Purpose: token-thrifty session boundaries. Paste verbatim; fill the <>. Adopted LENS-028; rewritten LENS-033 2026-08-06 to anchor every session to a declared target.**",
"",
"## OPEN -- paste as the first message of every Lens session",
F,
"LENS-0XX OPEN | model: <model name>",
"1) Read docs/LENS_CONTRACT.md (law) + docs/LENS_TARGET_AND_ORDER.md",
"   (target + roots + order) + latest docs/NEXT_SESSION_BRIEF_*.md",
"   (session state) + memory.",
"2) Echo LOAD CHECK:",
"   CURRENT TARGET | THIS SESSION'S MISSION = top of the working order",
"   | expected head SHA | first gate | gates ack",
"   (BEV, LR-078/080/092/094/099/101-104, L2 one-question).",
"3) The mission is the TOP OF THE ORDER. It is not chosen freely and not",
"   chosen by whatever looks most broken.",
"4) Then step-0 commands only. All claims = leads, BEV before acting.",
"   If model differs from last session: LR-102 re-audit FIRST.",
F,
"",
"## CLOSE -- paste when context reaches ~80%",
F,
"LENS CLOSE (LR-101..104):",
"1) Did we complete the declared mission? Answer yes/no plainly.",
"2) Record EVERY finding from this session with its evidence. Never suppress.",
"3) Re-analyse: does each new finding join an existing root, or open a new",
"   one? A new root may re-rank items above it.",
"4) REGENERATE docs/LENS_TARGET_AND_ORDER.md -- dated, superseding, root to",
"   sub. Classify each item against the DECLARED TARGET (absolute rule: a",
"   silent live failure is urgent under any target). If the target itself",
"   changed, regenerate the whole order and say why.",
"5) Declare NEXT session's mission = new top of the order.",
"6) Rewrite NEXT_SESSION_BRIEF as SESSION STATE ONLY -- what shipped with",
"   SHAs, what is in flight, hazards found, LIVE vs BANKED. It must NOT",
"   duplicate the order (dual sources of truth are how S2-D died).",
"7) Append LR entries earned. Update memory. One-purpose docs commit,",
"   push command + ls-remote verify.",
F,
"",
"## Notes",
]
p2.write_bytes(("\n".join(L)).encode("utf-8") + notes)
d2 = p2.read_bytes()
print("protocol AFTER   CRLF", d2.count(b"\r\n"), "bare_LF", d2.count(b"\n") - d2.count(b"\r\n"))
print("DONE")
