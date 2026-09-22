"""lens_canary_wave.py -- the canary's rows for ONE wave, and the lenses that did not speak.

CC-114 (LENS-045). The canary block, the S1 report and the Daily Brief each read
`order generated_at desc limit 4`. On 2026-09-22 evening Lens 1 failed (429_tpd), so
the fourth row was the MORNING's Sovereignty Check: one lens shown as two
perspectives, Foundation silently absent, and "4/4" on every surface -- the LENS-008
shape the canary doctrine exists to prevent. A missing lens is now named, never
filled (gas-mask arm 3).
"""
import re
from datetime import datetime, timedelta

LENSES = ("Foundation", "Physical Reality", "Causal Chain", "Sovereignty Check")  # Lens 1..4
WAVE_WINDOW_MIN = 90      # a wave's four lenses land within minutes; waves are ~12h apart
_PREFIX = re.compile(r"^\s*\[\s*([^\]\u2014]+?)\s*(?:\u2014|\])")


def lens_of(row: dict):
    m = _PREFIX.match(row.get("summary") or "")
    name = m.group(1).strip() if m else None
    return name if name in LENSES else None


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def select_wave(rows: list):
    """rows newest-first -> (this wave's rows in Lens 1..4 order, names of lenses missing)."""
    if not rows:
        return [], list(LENSES)
    newest = _ts(rows[0]["generated_at"])
    per, unreadable = {}, []
    for r in rows:
        if newest - _ts(r["generated_at"]) > timedelta(minutes=WAVE_WINDOW_MIN):
            continue
        name = lens_of(r)
        if name is None:
            unreadable.append(r)
        elif name not in per:
            per[name] = r
    if not per and unreadable:          # summaries lost their "[Lens -- ..." prefix: say so
        return unreadable[:4], ["(lens names unreadable)"]
    return [per[n] for n in LENSES if n in per], [n for n in LENSES if n not in per]


def fetch_wave(sb, cols: str):
    rows = (sb.table("lens_reports").select(cols)
            .order("generated_at", desc=True).limit(12).execute().data) or []
    return select_wave(rows)


def missing_note(missing: list) -> str:
    return (" -- MISSING: " + ", ".join(missing)) if missing else ""
