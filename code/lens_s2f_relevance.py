"""lens_s2f_relevance.py -- CC-129 (LENS-048): S2-F calls the model only for a lens the article is about.

Bytes, LENS-048: in 30 days S2-F made 737 early_warning scorings and 92% came back
not_applicable (xi_office 95%, khamenei_office 97%, trump_office 84%) -- the Cloudflare budget
(~14-39 scorings a day) was spent on articles that do not concern the lens. Every reader of the
detections (the Watch, Clarity and Verification aggregators, the S2 report, the Compendium)
filters not_applicable = False, so those calls fed nothing.

Replay on those 737 rows with exactly the patterns below (LR-270): the gate skips 94% (xi),
74% (trump) and 90% (khamenei) of pairs; among the skipped pairs, 0, 0 and 1 carried operations
(the one: an Afghanistan piece scored under khamenei_office).

A lens with no pattern here is always relevant (fail open): a new lens is never silenced.
The text tested is the title plus the first TEXT_CHARS of the body -- what the model is shown.
"""
import re

TEXT_CHARS = 4500   # lens_framing_rubrics.ARTICLE_BODY_CHARS

KEYWORDS = {
    "xi_office":       r"\b(china|chinese|beijing|xi|jinping|ccp|pla|prc|taiwan|hong kong|tibet|xinjiang)\b",
    "trump_office":    r"\b(trump|white house|washington|u\.s\.|us|usa|united states|american|america|pentagon|state department)\b",
    "khamenei_office": r"\b(iran|iranian|tehran|khamenei|irgc|hormuz|persian gulf|hezbollah|houthis?|islamic republic)\b",
}
_RX = {k: re.compile(v, re.I) for k, v in KEYWORDS.items()}


def lens_key(lens):
    """'Khamenei Office' and 'khamenei_office' are one lens."""
    return (lens or "").strip().lower().replace(" ", "_")


def relevant_lenses(title, body, lenses):
    """The lenses, in order, that this article is about. Unknown lens -> relevant."""
    text = (title or "") + " " + (body or "")[:TEXT_CHARS]
    out = []
    for lens in lenses:
        rx = _RX.get(lens_key(lens))
        if rx is None or rx.search(text):
            out.append(lens)
    return out
