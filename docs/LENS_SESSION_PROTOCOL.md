# Lens Session Protocol -- standard open/close prompts
**Purpose: token-thrifty session boundaries. Paste verbatim; fill the <>. Adopted LENS-028; rewritten LENS-033 2026-08-06 to anchor every session to a declared target.**

## OPEN -- paste as the first message of every Lens session
```
LENS-0XX OPEN | model: <model name>
1) Read docs/LENS_CONTRACT.md (law) + docs/LENS_TARGET_AND_ORDER.md
   (target + roots + order) + latest docs/NEXT_SESSION_BRIEF_*.md
   (session state) + memory.
2) Echo LOAD CHECK:
   CURRENT TARGET | THIS SESSION'S MISSION = top of the working order
   | expected head SHA | first gate | gates ack
   (BEV, LR-078/080/092/094/099/101-104, L2 one-question).
3) The mission is the TOP OF THE ORDER. It is not chosen freely and not
   chosen by whatever looks most broken.
4) Then step-0 commands only. All claims = leads, BEV before acting.
   If model differs from last session: LR-102 re-audit FIRST.
```

## CLOSE -- paste when context reaches ~80%
```
LENS CLOSE (LR-101..104):
1) Did we complete the declared mission? Answer yes/no plainly.
2) Record EVERY finding from this session with its evidence. Never suppress.
3) Re-analyse: does each new finding join an existing root, or open a new
   one? A new root may re-rank items above it.
4) REGENERATE docs/LENS_TARGET_AND_ORDER.md -- dated, superseding, root to
   sub. Classify each item against the DECLARED TARGET (absolute rule: a
   silent live failure is urgent under any target). If the target itself
   changed, regenerate the whole order and say why.
5) Declare NEXT session's mission = new top of the order.
6) Rewrite NEXT_SESSION_BRIEF as SESSION STATE ONLY -- what shipped with
   SHAs, what is in flight, hazards found, LIVE vs BANKED. It must NOT
   duplicate the order (dual sources of truth are how S2-D died).
7) Append LR entries earned. Update memory. One-purpose docs commit,
   push command + ls-remote verify.
```

## Notes
- Step-0 (unchanged): `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`,
  env load `set -a && source .env && set +a`, truth = `git ls-remote` (LR-104), health = Actions + Telegram.
- The OPEN prompt assumes the brief carries the state; if the brief is stale, say so in the LOAD CHECK and re-anchor from memory + `git log` before any work.
- Claude Code sessions use docs/LENS_LCLIFF_BUILD_PLAN.md task blocks by ID ("execute CC-3") — no free-form instructions.
