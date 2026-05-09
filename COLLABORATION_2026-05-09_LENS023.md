# Collaboration Notes — May 7–9, 2026 (LENS-023)

## Model performance: Sonnet 4.6 — LENS-023 Final Assessment

### What worked well

- **Root cause discipline**: articles_used bug — traced through key check → hello test →
  prompt size → column content → exact token count. Never patched without evidence.
- **LR-092 sibling check**: applied every time. Caught quota_guard.py, README, Watch/Clarity/Verification,
  S3-B alongside S3-D fix, S3-C alongside S3 report.
- **PHI-002 respected after correction**: S3-E SambaNova option correctly rejected after full file read.
- **Full file before proposing**: S3-F built correctly because PHI-002/003/004 docs were read in full.
- **Patch discipline (LR-078)**: all fixes via downloadable .py files. Worked cleanly every time.
- **L2 respected**: sample_size constraint, timeout removal, source deletion — all proposed and approved before execution.
- **Confirmed before closing**: waited for PM cron screenshots, Forensic Report manual run, dedup cleanup count before writing docs.
- **Operator rule remembered**: "our yml, not my yml" — corrected immediately.

### What went wrong — watch for in LENS-024

- **BEV violation on S3-E**: Jumped to A/B options before reading full file. James: "please remember error fighting, bird-eye view..." After reading: SambaNova correctly rejected.
- **Wrong diagnosis on MISTRAL_API_KEY**: Claimed not in GitHub before verifying. Always check evidence before claiming.
- **Pattern Match Bias on 400**: Assumed key issue before getting response body.
- **LR-097 origin**: Dismissed James's concern about 30-minute limit citing "GitHub supports 6 hours." Never checked the actual yml. James was right — `timeout-minutes: 30` was there. Always read the yml.

---

## What James does that works

- **Uploads logs as documents** — full terminal output every time. Fastest diagnostic path.
- **Screenshots for Actions and Telegram** — visual evidence saves entire diagnostic rounds.
- **"Please remember error fighting, bird-eye view..."** — consistent reset. Never dismissive.
- **Root cause insistence** — "we need to make analysis to search root cause first, we are responsible developers." Prevents symptom patches.
- **"Our yml, not my yml"** — catches ownership framing errors immediately.
- **Checks past session docs** — "please see in past session if needed" maintains continuity.
- **Reads philosophies before building** — insisted on reading PHI-002/003/004 before S3-F. Result was architecturally correct.
- **Patient with large sessions** — LENS-023 ran 38 hours. James stayed with every issue until resolved.
- **Approves L2 before execution** — every schema and architecture change went through James.
- **Trusts ground truth** — "please give command like cat or others" — always wants to verify, not assume.

## What Claude should never do with James

- Jump to options before reading the full file (BEV gate, always blocking)
- Claim a secret/config is wrong without checking evidence first
- Diagnose HTTP errors from status code alone — always get `r.text`
- Fix one file and assume siblings are clean (LR-092)
- Violate PHI-002/003/004 even when fix seems easy
- Cite platform maximums without checking operator-set yml values (LR-097)
- Leave session close docs before verifying with actual cron results

---

## Hard-won lessons — LENS-023

**"Blob columns kill prompts silently"**
`articles_used` stored full JSON content — 392K chars × 4 lenses = 1.5M chars → 836K tokens.
`|| true` swallowed the error. S1 docx failed for unknown days with zero visible indication.
Rule LR-096: always check `len(str(value))` before using DB column in AI prompt.

**"Remove timeout = wait till finish"**
`timeout-minutes: 30` cancelled manage-analyze at 30m26s. The correct answer is not
a bigger number — it's removing the ceiling entirely. Pipeline completes when it completes.
Rule LR-097: read yml value before dismissing operator concerns about timeouts.

**"Dedup must be explicit, not assumed"**
Watch aggregator ran 2x/day, inserted same rows every time — 88 duplicates in 3 days.
PHI-004 cadence design requires once-per-day writing. Every aggregator writing to a shared
table must have a "already wrote today" guard. Always.

**"Verify every dead source, never assume"**
Of 18 "dead" sources from collection logs: 4 were truly 404, 8 had broken RSS, 4 returned
200 but had live RSS feeds (kept). Evidence-based audit, not assumption-based removal.

**"workflow_run resets after yml changes"**
GitHub silently drops workflow_run event subscriptions after the triggering workflow yml
is modified. Fix: one manual trigger resets the chain. Simple but invisible without knowing.

**"PHI-002 lives in the file header, not in grep"**
S3-E brief mentioned "redesigned to use SambaNova." The actual file said LOCAL only (PHI-002).
The code file is always ground truth over documentation about it.

**"Full philosophy before new builds"**
S3-F required reading PHI-002/003/004 in full before writing a single line.
The resulting code correctly addresses apparatus-people separation, cognitive sovereignty
cadence, alternative hypotheses, and data gating. Shortcuts would have violated all of these.

---

## Forward protocol — LENS-024 onward

- **Default model**: Sonnet 4.6 adaptive (LR-090, no change)
- **First task**: Verify cron results. Check Forensic Report auto-fired. Check drift findings count.
- **T3 calibration**: Run at 6-8 AM Thai if quota fresh — Cerebras best window.
- **Blob rule (LR-096)**: `len(str(value))` before ANY DB column in AI prompt.
- **400 rule (LR-095)**: `r.text[:200]` before any diagnosis.
- **Timeout rule (LR-097)**: Read yml before dismissing operator timeout concerns.
- **Dedup rule**: Every aggregator writing to shared table needs daily dedup check.
- **Forensic Report**: Watch for 2-3 consecutive auto-fires to confirm stable recovery.

---

**Collaboration update**: ~03:30 Thai, May 9, 2026
