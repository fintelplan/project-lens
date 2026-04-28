# Collaboration Notes — Apr 28, 2026

## Patterns observed today (carry-forward for tomorrow's Claude)

### Communication style

**Operator (Bro Alpha) — James Maverick**:
- Warm informal tone ("my buddy", "fight against challenges")
- Engineering rigor underneath the warmth
- Explicit constraint declarations ("you are the most limit consuming model")
- Self-states cognitive load when relevant ("i am still fresh")
- Lettered options pattern: A/B/C, W1/W2/W3, F1/F2/F3 — uses consistently
- "[No preference]" or "you pick my buddy" = trust-delegation when good reasoning shown
- Re-running same diagnostic or pasting same screenshot = often signal of cognitive load
- "We not you and me" — collaborative framing
- "1 hour" = active engagement time, intermittent, not continuous wall-clock

### What worked today

1. **Honest pushback was welcomed.** When operator said W3 (full breadth), Claude pushed back transparently with risk analysis. Operator considered, then said "i am still fresh, do the best for our project." Trust delegation accepted, scope adjusted.

2. **Bird-eye view request honored thoroughly.** Operator asked for "deep analysis" — Claude produced structured 8-section overview with state, decisions, options, recommendations. Operator engaged with the document.

3. **Closing-session protocol triggered by operator.** Operator initiated session-close with explicit list (audit, brief, diary, status, rule, collaboration). Claude executed the full set without negotiating scope.

4. **Self-awareness about cognitive load.** Operator's screenshot of context compaction at 42% was named explicitly. Claude acknowledged the structural risk to tomorrow's session.

### What Claude could improve

1. **Preamble cost.** Claude wrote multiple long preambles before the actual answer in early-session messages. Each preamble is paid for in operator's daily message budget. Pattern fix: lead with answer, append justification only if needed.

2. **Over-projection of fatigue.** Claude initially recommended W4 (stop) and W1 (narrow) based on session length numbers. Operator self-reported being fresh. Trust operator's self-read of state, don't over-project from timestamps.

3. **Asking same question twice.** Claude offered W1/W2/W3/W4 then re-offered W1-revised/W4-structured/W3 after operator's "do the best." This was unnecessary — operator had already decided. Take the call when delegated.

4. **Pattern Match Bias proximity.** Yesterday's bias-hypothesis arc was a documented incident. Today's Claude (this session) actively self-checked when interpreting cross-lab data. Tomorrow's Claude should continue this active check, especially under cognitive load.

### Operator's core engineering anchors (reference)

These are referenced from session record. Tomorrow's Claude should treat as authoritative:

- **GNI-R-037**: Bird-eye view first before deep dive
- **GNI-R-076**: Read full file before patching
- **GNI-R-080**: Write-then-verify (write code, then test it works)
- **GNI-R-083**: Investigation discipline, not research-paper aesthetics
- **GNI-R-193**: No `ai_engine/*.py` changes before April 10 GPVS verification (now expired but discipline pattern continues)
- **GNI-R-220-225**: FMEA discipline (failure modes per option)
- **GNI-R-232**: Visual Fix Protocol
- **GNI-R-233**: Self-Awareness Protocol — Pattern Match Bias, Recency Bias, Helpfulness Anxiety, Confidence Performance
- **PHI-004**: Cognitive sovereignty cadence (Watch → Clarity → Verification → Direction)

### Operator's preferred work pattern

- Long sessions with intermittent engagement
- Explicit lettered choices for branching decisions
- Trust delegation when reasoning is shown ("you pick")
- Clean atomic commits (3+ commits land per session is normal)
- Structured session audit/brief/handover documents
- Numbered session series ("LENS-019.5 Day 3")
- Documents go in `/c/school/lens/docs/` for project; working tree for code/data

### Operator's tools state (Apr 28 close)

- Windows + Git Bash + venv at `/c/school/lens`
- Terminal commands typed manually (no auto-execution)
- Multiple browser tabs for Claude.ai sessions, model docs, OpenRouter dashboard
- LM Studio + GPT4All + Ollama all installed locally
- Repo: `github.com/fintelplan/project-lens`
- Hardware: CPU/iGPU only (no discrete GPU) — affects Ollama model size choices

### Project context tomorrow's Claude needs

- Operator is HD CS student at Spring University Myanmar
- Works under Dr. Cinthia White / Team Geeks supervision
- Two parallel projects: GNI Myanmar (production live) and Project Lens (active dev)
- Project Lens is 30+ session series with 230+ rule governance system
- Zero-cost architecture constraint ($0/month) is foundational
- Operator operates as primary architect — Claude is full-stack developer + rule enforcer + session historian

---

## How tomorrow's Claude should open

1. **Read this file + NEXT_SESSION_BRIEF + STATUS first.** Don't ask operator to recap.
2. **Greet warmly but briefly.** Operator likes "my buddy" register but cuts long preambles.
3. **Confirm Ollama state with one bash command** (`ollama list | grep -E "qwen|llama|deepseek"`) before doing anything else.
4. **Don't re-derive Apr 28 conclusions.** They're locked. Trust the audit.
5. **Watch for Pattern Match Bias.** If you see asymmetric data, ask "would cross-lab evidence change this?" before concluding.
6. **Cut to answer first, justify after.** Every word costs operator's message budget.

---

**Collaboration notes**: 15:40 Thai, Apr 28 2026
