"""What each S2 analyst writes into injection_reports.confidence_score (CC-135, order item 3 Phase A).

The column holds six measures under one name (LR-296). ISO/IEC 11179 asks that one data element carry
one concept and one value domain; this registry is that definition, kept beside the readers until the
column is split (Phase B). Readers show a number under its own name and never rank, cap or take a max
across analysts. Write sites (LENS-049 bytes): lens_s2a_injection.py:410, lens_s2b_coordination.py:440,
lens_s2c_emotion.py:334, lens_s2d_adversary.py:601, lens_s2e_legitimacy.py:550, lens_s2_gap.py:237.
"""
from collections import Counter

UNNAMED = "unnamed measure"

# analyst: (key used in prompts, label shown to readers, who produces the number, value domain)
MEASURES = {
    "S2-A":   ("stated_confidence",     "model-stated confidence",        "model", "0-1; NONE rows 0.0"),
    "S2-B":   ("stated_confidence",     "model-stated confidence",        "model", "0-1; NONE rows 0.0"),
    "S2-C":   ("manipulation_score",    "manipulation score",             "model", "0-1"),
    "S2-D":   ("narrative_consistency", "narrative consistency",          "model", "0-1"),
    "S2-E":   ("low_legitimacy_share",  "share of low-legitimacy actors", "code",  "0-1, len(low)/len(actors)"),
    "S2-GAP": ("gap_quality",           "gap-analysis quality",           "model", "0-1"),
}

# The readers' own convention for a row that reports nothing (lens_telegram, lens_s2_step_report).
NON_FINDINGS = {"", "NONE", "NO_COORDINATION"}


def measure_key(analyst):
    return MEASURES.get(analyst, ("unnamed_measure",))[0]


def measure_label(analyst):
    m = MEASURES.get(analyst)
    return m[1] if m else UNNAMED


def is_finding(injection_type):
    return (injection_type or "").strip().upper() not in NON_FINDINGS


def top_finding(rows):
    """The most frequent finding type among rows -- a count, one quantity across analysts.
    Ties go to the alphabetically first type, so the line is the same on every read."""
    found = [r for r in rows if is_finding(r.get("injection_type"))]
    if not found:
        return {"n_findings": 0}
    counts = Counter(r.get("injection_type") for r in found)
    itype, n = sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))[0]
    analysts = sorted({str(r.get("analyst")) for r in found if r.get("injection_type") == itype})
    return {"injection_type": itype, "n": n, "n_findings": len(found), "analysts": analysts}
