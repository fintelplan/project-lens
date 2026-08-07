# Lens Session Protocol -- standard open/close prompts
**Purpose: token-thrifty session boundaries. Paste verbatim; fill the <>. Adopted LENS-028; rewritten LENS-033 2026-08-06 to anchor every session to a declared target; synced to the agreed prompts LENS-033 2026-08-07.**

## OPEN -- paste as the first message of every Lens session
```
LENS-0XX OPEN | model: <model name>
1) Read docs/LENS_CONTRACT.md (law) + docs/LENS_TARGET_AND_ORDER.md
   (target + roots + order) + latest docs/NEXT_SESSION_BRIEF_*.md
   (session state) + memory.
2) Echo LOAD CHECK:
   CURRENT TARGET | THIS SESSION'S MISSION = top of the working order
   | expected head SHA | first gate | gates ack
   (BEV, LR gate list per LENS_CONTRACT.md, L2 one-question).
3) The mission is the TOP OF THE ORDER. It is not chosen freely and not
   chosen by what looks most broken. If you believe the order is wrong,
   say so and propose a re-order -- do not silently work something else.
4) Then step-0 commands only. All claims = leads, BEV before acting.
   If model differs from last session: LR-102 re-audit FIRST.
5) A run's log is not evidence until you have checked headSha AND the
   line count. A 35-line log means nothing ran.
```

## CLOSE -- paste when context reaches ~80%
```
LENS CLOSE (session-boundary rules apply -- see LENS_CONTRACT.md):
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
   If the target is ACHIEVED, run the PHASE TRANSITION ritual instead
   (LENS_CONTRACT.md) -- James declares the new target, not Claude.
5) Declare NEXT session's mission = new top of the order.
6) Rewrite NEXT_SESSION_BRIEF as SESSION STATE ONLY -- what shipped with
   SHAs, what is in flight, hazards found, LIVE vs BANKED. Reference order
   items BY NUMBER; never restate them (dual sources of truth killed S2-D).
7) List every claim this session made that was WRONG, and what the bytes
   said instead. Estimates included.
8) Append LR entries earned. Update memory. One-purpose docs commit,
   push command + ls-remote verify. Verify with git log --stat that the
   commit's message matches the files it actually touched.
```

## Notes
- Step-0 (unchanged): `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`,
  env load `set -a && source .env && set +a`, truth = `git ls-remote` (LR-104), health = Actions + Telegram.
- The OPEN prompt assumes the brief carries the state; if the brief is stale, say so in the LOAD CHECK and re-anchor from memory + `git log` before any work.
- Claude Code sessions use docs/LENS_LCLIFF_BUILD_PLAN.md task blocks by ID ("execute CC-3") — no free-form instructions.
