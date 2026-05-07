# Collaboration Notes — May 7, 2026 (LENS-022 Phase 2 close)

## Model performance: Sonnet 4.6 — Phase 2 Assessment

### What worked well

- **Root cause diagnosis over symptom fixes**: When the Forensic Report disappeared,
  didn't jump to "reduce articles" — traced the full chain: quota → exit code → trigger.
- **Architectural thinking**: LR-094 (7 accounts, one role each) is the kind of permanent
  structural fix that prevents the same issue from re-emerging as the system grows.
- **BEV discipline on Phase 2 open**: Read full logs before proposing anything. The entity
  extract log showing 99,559/100,000 TPD was the key evidence that unlocked the diagnosis.
- **3 docx reports built cleanly** with parallel structure — consistent 5-6 part format,
  retry logic, proper Telegram delivery, db logging.
- **S3 UnboundLocalError**: Caught by reading the log carefully, fixed with surgical 1-line
  change. Didn't over-engineer.

### What went wrong / watch for in LENS-023

- **Confirmation bias on "fixed"**: After applying yml fixes in 562e415, moved to session
  close without waiting for the next cron to confirm. Verification is mandatory before
  marking anything ✅ at session close — especially for overnight-only fixes.
- **GEMINI_S3B_API_KEY still shared**: Believed that adding a new secret name meant a new
  quota pool. The same Google account can have multiple API keys — they all share the same
  RPD. This cost one more cycle of S3-B failures. Always verify account separation, not just
  key name separation.
- **Context compaction mid-session**: Long sessions get compacted — some detail lost.
  This brief exists precisely to survive that. When in doubt: check the session audit file
  first, don't reconstruct from memory.

---

## What James does that works

- **Uploads logs as documents** — pastes terminal output directly. No "it failed, fix it."
  The actual log text is always attached. Fastest possible diagnostic path.
- **Screenshots for Telegram and GitHub Actions** — visual evidence of what arrived vs what
  didn't. Saves entire rounds of "did it send?" guessing.
- **"BEV" as a hard stop signal** — consistent, effective. When Claude starts moving too fast,
  one word resets the mode. No emotional friction.
- **Stays architecturally engaged** — noticed entity_extract TPD numbers in the log and asked
  the right question about shared keys. James reads logs too, not just Claude.
- **Trusts the system** — when a fix is applied but can't be confirmed until next cron,
  James accepts "next cron will confirm" and moves to session close. Doesn't demand
  real-time proof of everything.
- **Multi-day sessions** — LENS-022 ran Apr 30 → May 7. James stays with the problem until
  the machine is healthy, not just until the immediate fire is out.

---

## What Claude should never do with James

- Commit without `python -m py_compile` on ALL modified files (LR-092)
- Fix one file and assume sibling files from the same origin commit are fine
- Mark a fix ✅ before the next cron has confirmed it (especially overnight-only workflows)
- Confuse "new API key" with "new quota pool" — always verify account separation
- Narrate the diagnostic protocol while executing it
- Recommend reducing quality (article count, model capability) when architectural isolation
  is the correct fix — James will always choose architecture over shortcuts
- Leave session close docs incomplete — diary, status, rules, brief, collaboration are all
  mandatory (LR-093)

---

## Hard-won lessons — Phase 2

**"Silent failures are the worst bugs"**
A failing step produces a log. A trigger that never fires produces nothing. The Forensic
Report was broken for weeks with zero visible error. Diagnostic discipline for silent failures:
trace backwards from "what should have happened" → "what condition enables it" → "what
broke that condition." workflow_run requires success → manage-analyze must exit 0 → S2-A
must not fail → quota must be available → shared key must have headroom.

**"Separate key ≠ separate quota"**
Groq: all keys under the same org share TPD. Google: all keys under the same account share
RPD. Creating a new key from the same account does not create a new pool. Always create
a genuinely new account (new email) for quota isolation.

**"yml env vars don't auto-propagate to all steps"**
Job-level `env:` entries are accessible in that job's steps, but if a step calls a subprocess
that spawns its own Python interpreter, the env may not propagate. Explicit per-step `env:`
is the safe pattern. This caused S1 and S3 docx failures (Mistral 400 = key not found,
Telegram `sent=False` = keys not set).

**"Architecture compounds"**
Every key isolation decision made in LR-094 builds on the architecture established in LR-010
(GNI quota discipline) and LENS-010 (4-email Groq account architecture). The pattern is
always the same: when a provider has per-account limits, separate heavy consumers into
separate accounts. Don't fight the limits — route around them at the architecture level.

---

## Forward protocol — LENS-023 onward

- **Default model**: Sonnet 4.6 adaptive (LR-090 confirmed, no change)
- **First task every session**: Run verification script from brief. Check Telegram for docx
  files. Check GitHub Actions for Forensic Report. Don't start new work until status is known.
- **After any yml change**: Wait for the next cron run to confirm env vars actually reached
  the step. Don't mark ✅ until logs confirm.
- **Quota isolation rule (LR-094)**: Every new heavy consumer needs its own Groq account.
  New role = new email = new account = new TPD pool.
- **Gemini RPD (pending fix)**: S3-B and S2-B need genuinely separate Google accounts.
  Verify by checking if the account email is different from GEMINI_API_KEY's account.
- **S3-E fix**: detect GITHUB_ACTIONS env, skip Ollama check, route SambaNova directly.
  Read full file before touching (BEV).

---

**Collaboration update**: ~04:30 Thai, May 7, 2026
