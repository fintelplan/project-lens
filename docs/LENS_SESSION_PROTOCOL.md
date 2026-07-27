# Lens Session Protocol — standard open/close prompts
**Purpose: token-thrifty session boundaries (mirrors GNI's ritual). Paste these verbatim; fill the <>. Adopted LENS-028, 2026-07-27.**

## OPEN — paste as the first message of every Lens session
```
LENS-0XX OPEN | model: <model name>
1) Read latest docs/NEXT_SESSION_BRIEF_*.md + docs/LENS_LCLIFF_DECISIONS.md + memory.
2) Echo LOAD CHECK: expected head SHA | mission | first gate | gates ack
   (BEV, LR-078/080/092/094/099/101-104, L2 one-question).
3) Then step-0 commands only. All brief claims = leads, BEV before acting.
   If model differs from last session: LR-102 re-audit FIRST (trust tags reset).
```

## CLOSE — paste when context reaches ~80%
```
LENS CLOSE (LR-101..104):
1) Rewrite docs/NEXT_SESSION_BRIEF -- supersede, fold ALL live items, trust-tag every claim.
2) Append LR entries earned this session.  3) Update memory.
4) Ship-to-file, py_compile if code touched, one-purpose docs commit,
   give push command + ls-remote verify.
5) End with LIVE vs BANKED list written for the next model's hands.
```

## Notes
- Step-0 (unchanged): `printf '\e[?2004l' && cd C:/school/lens && source venv/Scripts/activate`,
  env load `set -a && source .env && set +a`, truth = `git ls-remote` (LR-104), health = Actions + Telegram.
- The OPEN prompt assumes the brief carries the state; if the brief is stale, say so in the LOAD CHECK and re-anchor from memory + `git log` before any work.
- Claude Code sessions use docs/LENS_LCLIFF_BUILD_PLAN.md task blocks by ID ("execute CC-3") — no free-form instructions.
