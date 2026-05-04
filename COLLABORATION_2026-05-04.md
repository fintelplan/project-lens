# Collaboration Notes — May 4, 2026 (LENS-022)

## Model performance: Sonnet 4.6 vs Opus 4.7

First full Sonnet session per LR-090. Performance assessment:

**What worked well:**
- Direct execution without narrating the protocol
- Brief verdicts (1-3 lines) without multi-paragraph self-analysis
- Caught own mistakes faster — when operator said "please remember error fighting,
  bird-eye view, policy, instructions, lessons learned" it self-corrected cleanly
- One-question discipline maintained throughout

**What went wrong:**
- Pattern Match Bias on patch scope: fixed S2-D syntax but didn't scan sibling
  files for same pattern → 3 S2F aggregators stayed broken 4 days
- T5 Mistral fallback: built the fallback but passed wrong model name → every
  Mistral call returned 400 Bad Request until operator caught it in logs
- Forgot to delete smoke test entity despite noting it early in session
- Session close docs were insufficient first time — operator called for full redo

**Verdict**: Sonnet 4.6 is correct for daily driver work (LR-090 confirmed).
The mistakes above are fixable with LR-092 and LR-093. Opus would have been overkill.

## What James does that works

- **Shows logs directly** — pastes terminal output or screenshots without asking
  if Claude needs them. This is the fastest diagnostic path.
- **"Please remember error fighting, bird-eye view..."** — consistent reminder when
  Claude starts moving too fast. Never dismisses it as nagging.
- **Asks "why" before accepting fixes** — "why do we miss this error?" leads to
  better rules than silently accepting the fix.
- **Screenshot evidence** — uploads GitHub Actions UI, Telegram messages, VSCode
  errors. Saves diagnostic time vs text descriptions.
- **Stays technically engaged** — knows when to ask "can we batch instead of reduce?"
  for S2-D. Preserves quality that Claude might have traded away for simplicity.

## What Claude should never do with James

- Narrate the protocol while executing it
- Give multi-paragraph self-analysis when 2 sentences suffice
- Fix one file and assume sibling files are fine (LR-092)
- Leave cleanup items in notes without a hard list (LR-093)
- Use fixed sleep when token-aware waiting is available
- Commit without `python -m py_compile` on ALL modified files

## Hard-won lessons this session

**"Same bug, different file"** — When a patch-generated bug appears in one file,
immediately check all files from the same origin commit. The LENS-021 entity
wiring touched 4 files (3 aggregators + helpers). The broken import pattern existed
in all 3 aggregators but only one was fixed and checked. Cost: 8 consecutive S2F
failures over 4 days.

**"Token-aware > time-aware"** — James's instinct to batch by token count rather
than fixed article splits was architecturally correct. Measuring actual cost and
waiting only when needed is always better than fixed delays or arbitrary splits.

**"Cascading failures"** — One manage-analyze failure cascades to refs not running,
which cascades to Regular Report showing 0 refs, which cascades to operator confusion.
Understanding the dependency chain prevents misdiagnosis.

## New rules summary (LENS-022)

- **LR-088**: State actors = entity_type='state_office' in lens_entities
- **LR-089**: Debugging gates are blocking (BIRD-EYE → DEEP ANALYSIS → SWOT first)
- **LR-090**: Model selection by task tier (Sonnet=daily, Opus=architecture)
- **LR-091**: LOCAL model testing: RAM limits, JSON gate, air-gap requirement for S3-E
- **LR-092**: Syntax verify ALL affected .py files before any commit — not just primary
- **LR-093**: Session close MUST include CLEANUP section with explicit deletion tasks

## Forward protocol — LENS-023 onward

- Default: Sonnet 4.6 adaptive (confirmed correct per LR-090)
- First task: always verify previous session's overnight cron before new work
- After any patch touching multiple files: `for f in *.py; do py_compile $f; done`
- Cleanup list in session close is MANDATORY before committing docs

---
**Collaboration update**: 21:00 Thai, May 4, 2026
