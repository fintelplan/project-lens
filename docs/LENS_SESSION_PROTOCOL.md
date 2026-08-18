# Lens Session Protocol -- standard open/close prompts
**Purpose: token-thrifty session boundaries. Adopted LENS-028; rewritten LENS-033 2026-08-06 to anchor every session to a declared target; synced to the agreed prompts LENS-033 2026-08-07; v2 LENS-035 2026-08-18.**

**v2 CHANGED HOW THIS FILE IS USED.** Only the OPEN is pasted, because at
open Claude has read nothing yet -- the OPEN is the bootstrap. Step 1 now
makes Claude read THIS FILE, so the CLOSE is invoked by name ("LENS CLOSE")
and its text is read from here.

Why: the close prompt was being pasted from a personal copy, and that copy
had already lost two clauses present in this file (the CHANGED THIS
REGENERATION section and the PHASE TRANSITION branch). A prompt that lives
in two places is a dual source of truth -- the disease that killed S2-D,
inside the document written to prevent it. One home, improvable in the repo.

## OPEN -- paste as the first message of every Lens session
```
LENS-0XX OPEN | model: <model name>
1) Read docs/LENS_CONTRACT.md (law) + docs/LENS_TARGET_AND_ORDER.md
   (target + roots + order) + docs/LENS_SESSION_PROTOCOL.md (this file --
   it holds the CLOSE) + latest docs/NEXT_SESSION_BRIEF_*.md (session
   state) + memory.
2) Echo LOAD CHECK:
   CURRENT TARGET | THIS SESSION'S MISSION = top of the working order
   | HEAD from git ls-remote | first gate | gates ack
   (BEV, LR gate list per LENS_CONTRACT.md, CANARY three-arm gas-mask test
   before any change to S1, Collection or an instrument, L2 one-question).
   HEAD comes from ls-remote, NEVER from the brief: a brief cannot name its
   own commit, so its HEAD line is stale BY CONSTRUCTION. Say so and move on.
3) The mission is the TOP OF THE ORDER. It is not chosen freely and not
   chosen by what looks most broken. If you believe the order is wrong,
   say so and propose a re-order -- do not silently work something else.
4) Then step-0 commands only. All claims = leads, BEV before acting.
   If model differs from last session: LR-102 re-audit FIRST.
   Read date -u and count the waves since the last session's last read
   log. A gap is not lost time -- unread instrumented waves are free
   measurement, and sizing from four samples when sixteen exist is a
   banked-number error.
5) A run's log is not evidence until you have checked headSha AND the
   line count. A 35-line log means nothing ran.
```

## CLOSE -- say "LENS CLOSE"; the steps are below, read at open
```
1) Did we complete the declared mission? Answer yes/no plainly. If no, say
   what blocked it -- a blocked mission is a fine outcome, a vague one is not.
2) Record EVERY finding from this session with its evidence. Never suppress.
3) Re-analyse: does each new finding join an existing root, or open a new
   one? A new root may re-rank items above it.
4) REGENERATE docs/LENS_TARGET_AND_ORDER.md -- dated, superseding, root to
   sub. Classify each item against the DECLARED TARGET (absolute rule: a
   silent live failure is urgent under any target). If the target itself
   changed, regenerate the whole order and say why.
   RETIRE, don't carry: an item that has sat below the line for three
   regenerations unworked is either closed as accepted, or promoted with a
   written reason. The order is a working list, not an archive.
   The regenerated order carries a CHANGED THIS REGENERATION section:
   items closed, merged, retired, re-ranked -- one line each. git log -p
   keeps every version; the section says WHY it changed.
   ASSERT ITEM NUMBERS ARE UNIQUE. A presence-only check passes a duplicate
   (53fe0e3 shipped one), and the brief references items BY NUMBER.
   If the target is ACHIEVED, run the PHASE TRANSITION ritual instead
   (LENS_CONTRACT.md) -- James declares the new target, not Claude.
5) Declare NEXT session's mission = new top of the order.
6) Rewrite NEXT_SESSION_BRIEF as SESSION STATE ONLY -- what shipped with
   SHAs, what is in flight, hazards found, LIVE vs BANKED. Reference order
   items BY NUMBER; never restate them (dual sources of truth killed S2-D).
   HAZARDS PROMOTE OR EXPIRE: a hazard carried forward unchanged into a
   second brief is a durable rule nobody registered -- promote it to the LR
   register or drop it. The HAZARDS section is not an archive either.
7) List every claim this session made that was WRONG, and what the bytes
   said instead. Estimates included.
8) Append LR entries earned -- no number gaps; a hole is indistinguishable
   from a lost rule. Update memory. One-purpose docs commit, push command
   + ls-remote verify.
   BEFORE COMMITTING, run the LR-138 check: grep ONE DISTINGUISHING PHRASE
   PER AGREED ELEMENT and report the hits. git log --stat proves
   message-vs-contents; it CANNOT prove contents-vs-agreement. The phrase
   must be unique BY CONSTRUCTION and the expected count stated in advance
   -- "must be 1" is the wrong test for anything that legitimately recurs.
   Then verify with git log --stat that the message matches the files.
```

## Notes
- Step-0 (unchanged): `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`,
  env load `set -a && source .env && set +a`, truth = `git ls-remote` (LR-104), health = Actions + Telegram.
- The OPEN prompt assumes the brief carries the state; if the brief is stale, say so in the LOAD CHECK and re-anchor from memory + `git log` before any work.
- Claude Code sessions use docs/LENS_LCLIFF_BUILD_PLAN.md task blocks by ID ("execute CC-3") — no free-form instructions.
- ONE load-bearing command block per message. Multiple blocks get partially pasted: a commit block once ran while its patch block did not, committing nothing and printing "working tree clean", which looks exactly like success. Never put a rollback in the same message as an apply (LR-140).
- Never hand Claude's output a placeholder to substitute (`MA=<id from above>`). It gets pasted literally and dies with a bash syntax error. Derive IDs inside the command. Twice now.
- `$HOME` on this machine is `/c/Users/James Maverick` — it contains a space. Quote every path variable; an unquoted one splits into extra arguments.
- Long documents are delivered as DOWNLOADS, never heredocs: this file contains literal backticks, which stall the shell inside a heredoc body (LR-078 amendment). Place them by copy, then byte-compare — existence is not correctness, and an `ls` that succeeds proves only that something is there.
