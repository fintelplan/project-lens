# Collaboration Notes — May 12–17, 2026 (LENS-024)

## Session character

Shorter session than LENS-023 (38h). Focused: 3 bugs, 3 new sources, 1 architectural fix. Clean and efficient.

---

## What worked well

**Root cause discipline on pip failure**: Two failures (#81 at 9s, #82 at 24m 48s) had different root causes. Correctly identified: #81 was pip, #82 was code import. Did not conflate them.

**S2-C complete rewrite**: Rather than patching around the SDK, rewrote the full file cleanly with requests pattern — consistent with all other Mistral callers in the system.

**Architectural thinking on xlsx timing**: Rejected "shift cron earlier" as symptom chasing. Proposed separate yml with own schedule — correct solution. James approved C (separate independent yml) immediately.

**PHI-003 awareness on NDTV**: Correctly identified India as missing non-Western democratic voice. Framed NDTV addition as PHI-003 counterbalance, not just "more sources."

**LR-092 sibling check on sources**: Before removing dead sources, ran feedparser verification on ALL 18 candidates. Found 4 had live RSS (kept). Evidence-based, not assumption-based.

---

## What went wrong

**LR-098 violation — the core lesson of LENS-024**:
When removing `mistralai` from pip install, did NOT grep code/ for SDK imports. `lens_s2c_emotion.py` had `from mistralai.client import Mistral` — completely missed. Pipeline failed again at #82 with `ModuleNotFoundError`. This is exactly what LR-092 (sibling check) is for — it applies to BOTH yml AND code files.

The fix was correct (#93cc0b7) but the miss was avoidable. LR-098 now exists to prevent this class of error.

---

## James's style this session

- "not my yml, our yml, ok?" — caught ownership framing immediately. Claude corrected.
- "please remember error fighting, bird-eye view..." — used consistently as reset signal. Always worked.
- "i think this is not smart way" — on cron shifting. Correct. Led to the better architectural solution.
- "C" — one word approval after Claude laid out A/B/C options with honest lean. Efficient.
- Uploaded 17 Telegram/Actions screenshots for timing analysis — visual evidence is always better than memory.

---

## Hard-won lessons — LENS-024

**"pip and code are two separate things"**
Removing a package from pip install does NOT automatically fix code that imports that package. They are independent. Always check both. LR-098.

**"1of2 missing is a pipeline health signal"**
When only `2of2` appears for multiple days, the morning cron is failing. Check manage-analyze #81 (morning scheduled) first, not the ref system itself. The ref system was fine — the trigger was dead.

**"standalone yml is the correct answer for timing-critical outputs"**
A 30-min pipeline with 2-4h GitHub cron delay is never going to deliver at a precise time. Separate the lightweight output (xlsx, 37s) from the heavy pipeline (manage-analyze, 30min). Standalone yml with own schedule = predictable, independent, resilient.

**"India was missing"**
Zero India sources for a global OSINT system. NDTV fills an important gap — non-Western democratic perspective that PHI-003 requires for balanced apparatus analysis.

---

## Forward protocol — LENS-025 onward

- Add LR-095/096/097/098 to rules.md — 2 sessions overdue
- T3 steno calibration at 6-8 AM Thai
- S3-F data gate: ~10 more S3-A runs
- S4-B: July 2026
- Default model: Sonnet 4.6 adaptive (LR-090, unchanged)

---

**Collaboration update**: ~10:45 AM Thai, May 17, 2026
