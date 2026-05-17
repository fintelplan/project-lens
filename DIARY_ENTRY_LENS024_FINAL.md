# Project Lens Diary — LENS-024 (May 12–17, 2026)

## LENS-024 — The Pip-and-Code Session
*(May 12, 2026 ~18:00 → May 17, 2026 ~10:45 Thai)*

**Session window**: May 12–17, 2026
**Commits**: 6 (`8081cf4` → `e4d7ce3`)
**Model**: Claude Sonnet 4.6 adaptive (continued per LR-090)
**Last commit**: `e4d7ce3`

---

### Session origin

Opened with screenshots showing manage-analyze #81 failing at 9s, #82 failing at 24m 48s with different errors. James uploaded GitHub Actions and Telegram screenshots. Two distinct bugs presenting together made this an interesting diagnostic session.

### The mistralai double failure

**Bug 1 (#81, 9s)**: `mistralai` not on PyPI. Hardcoded in pip install line 31 of manage-analyze yml. Fixed: `dba12f2`.

**Bug 2 (#82, 24m 48s)**: After pip was fixed, pipeline ran 24 minutes then hit `ModuleNotFoundError: No module named 'mistralai'` at S2-C import. Different error, same package. `lens_s2c_emotion.py` had `from mistralai.client import Mistral` on line 17 — the full SDK import pattern, completely inconsistent with every other Mistral caller in the system (which all use `requests` directly).

LR-092 (sibling check) exists precisely to catch this. But it was applied to yml siblings, not code files. LR-098 now closes this gap: when removing a pip package, grep code/ for SDK imports.

S2-C was completely rewritten as `93cc0b7` — replaced the SDK client pattern with direct requests.post(), consistent with S3-D, S3-B, S2-B fallbacks. Confirmed working in #83: S2-C COMPLETE | 4 reports | steps=14 ✅.

### The xlsx timing mystery

From Telegram screenshots, `1of2` files were missing since May 10. Only `2of2` delivered each day. Investigation:

The morning cron (`28 1 UTC` = 8:28 AM Thai) had been failing at 9s (mistralai) since May 10. It never reached `lens_ref_system.py`. The evening cron kept producing `2of2`. `get_slot()` uses UTC hour: morning run (hour=1) → `1of2`, evening run (hour=13) → `2of2`.

First proposed fix: shift crons earlier. James: "i think this is not smart way, my buddy." Correct. Shifting crons is symptom chasing — if GitHub's delay changes, we're wrong again.

Real fix: separate `lens-ref-export.yml` with own schedule. Runs at `02:30 UTC` (09:30 AM Thai) and `14:30 UTC` (09:30 PM Thai). Only 4 secrets needed. pip install takes 15s vs 2min. Fully independent of manage-analyze health.

Results: both `1of2` and `2of2` delivered daily since May 13. May 16: `1of2` at 12:38 PM Thai, `2of2` at 10:34 PM Thai. Still 1-3h GitHub delay but at least never missing.

### CNN and NDTV — filling the coverage gap

"Why telegram output are not within expected time?" led to examining sources. James asked about NDTV. Zero India sources in Project Lens — significant gap for a global OSINT system.

NDTV World confirmed live (20 entries). Added as SRC-081 TIER1 India actor. PHI-003 requires non-Western democratic voices as counterbalance to apparatus analysis. NDTV fills this. Confirmed appearing in Telegram May 16 with Starbucks article preview card.

CNN World and CNN Business were added earlier in session (SRC-079/080) after James noticed CNN was arriving only through Google News indirect feeds. Direct RSS: 29 and 20 entries respectively.

### Session character

Shorter and more focused than LENS-023. Three bugs fixed, three sources added, one architectural improvement. The xlsx standalone yml is the cleanest solution of the session — correct architectural thinking beats cron adjustment every time.

LR-098 is the most important rule born from this session. The pip-vs-code inconsistency is a subtle but dangerous failure mode that LR-092 didn't fully cover.

---

**LENS-024 closed**: May 17, 2026 ~10:45 AM Thai by Sonnet 4.6
**Next**: LENS-025 — rules.md update, T3 calibration, S3-F data gate progress
