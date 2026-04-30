# Collaboration Notes — Apr 30, 2026

## What we learned about Claude model selection on Project Lens

**Empirical observation**: Opus 4.7 adaptive overfits to project lore on Project Lens
specifically. Three signals:
1. Pattern Match Bias on familiar templates ("ALTER TABLE for NOT NULL bug")
2. Verbose meta-narration when execution would do
3. Multi-paragraph self-analysis instead of brief verdicts

The marathon session (Apr 28-30, also Opus 4.7) handled the project well despite
the same model — but it was a session that legitimately needed Opus capabilities
(cross-lab evaluation across 5 labs, architecture v4 lock, decisions on 3 ratification
candidates LR-085/086/087). Today's session was T2 verification + a single-bug fix
across 3 files. That's Sonnet zone. Opus over-engineered it.

Mapped to a rule: **LR-090** — model selection by task tier.

## What works for James (Bro Alpha) regardless of model

- **Action first, not narration.** "Run X. While X runs: A/B/C?" — direct execution.
- **One question per turn.** Compound questions waste budget.
- **Brief verdicts.** "Verdict: X. Moving on." — 1-3 lines, not paragraphs.
- **Lettered options A/B/C.** With honest lean OR explicit "I don't have enough to lean."
- **Pause signals.** Short message after long Claude response = re-examine, don't push.
- **"Move on as we can"** = execute, don't recap.
- **"Where are we"** = prioritized to-do list, not narrative.
- **"BEV"** (bird-eye view) = hard stop on recommendations until full state shown.

## What doesn't work

- Narrating the protocol while running it ("now applying bird-eye view, step 1...")
- Self-flagellation when corrected ("you're right, I was being..."). One sentence
  acknowledgment + adjustment is enough.
- Premature leans without ground truth. Better to say "your call, here's data."
- Multiple memory edits when one would do. Tool calls cost cycles.
- Opus on Sonnet-zone work. Burns weekly limit, calendar drag.

## What worked today

1. Operator's correction on Pattern Match Bias was precise and immediate. Did
   not let Opus rationalize. Pushed back twice because first push-back wasn't
   absorbed deeply enough.
2. Operator's clarification of "previous session" specifically (the marathon)
   forced direct comparison instead of generic prior-Claude reference.
3. Operator's "you are senior full stack developer, answer your own question"
   was a clean delegation pattern — pushed Claude to take responsibility for
   the call rather than deferring up.
4. Plan limit screenshots gave hard data to inform the model-selection retrospective.

## Forward protocol — LENS-022 onward

- **Default model**: Sonnet 4.6 adaptive (3x sustainable pace per LR-090)
- **Opus 4.7 reserve**: S4-B architecture, Direction A web app, multi-pillar additions
- **Session close trigger**: 80% usage (LR-057), not the wall
- **Brief writing**: explicit, code-first, minimal narrative. Sonnet handles
  structured tasks well — over-narrative confuses it.

## Specific to handing off Opus → Sonnet

The next session brief (`NEXT_SESSION_BRIEF_LENS022_SONNET.md`) is written with
Sonnet's strengths in mind:
- Concrete code paths and file names
- Verification snippets ready to paste
- Hard rules with examples (not abstract principles)
- Direct task list with effort estimates
- Branches for diagnostic outcomes (rather than open-ended "investigate")

Sonnet 4.6 should not need to load extensive project lore to start productively.
The brief is self-contained.

---

**Collaboration update**: 16:30 Thai, Apr 30 2026
