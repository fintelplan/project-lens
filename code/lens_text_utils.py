"""
lens_text_utils.py -- shared text extraction (LENS-033 CC-46).

visible_text(): strip markup, return what a reader would actually see.

Why this exists: article bodies arrive as HTML, and markup tokenises far
worse than prose. Measured 2026-08-06 on 200 real rows: markup is only
39% of CHARACTERS but runs at ~1.63 chars/token against ~4.2 for text, so
stripping it cuts S2-B's token count roughly 2.6x while removing nothing
a model would have read. Google News items are the extreme case -- their
entire body is a base64 tracking URL.

CONTRACT: never raises, never returns None. On any parse failure it
returns the input stripped. A text helper must not be able to take down
a position (LR-108 fail-safe shape).
"""
from __future__ import annotations
import re

_WS = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]+>")

try:
    import lxml.html as _LH
    _HAVE_LXML = True
except Exception:
    _HAVE_LXML = False


def visible_text(raw: str | None) -> str:
    """Return reader-visible text. Never raises."""
    if not raw:
        return ""
    if "<" not in raw:
        return _WS.sub(" ", raw).strip()
    if _HAVE_LXML:
        try:
            return _WS.sub(" ", _LH.fromstring(raw).text_content()).strip()
        except Exception:
            pass
    return _WS.sub(" ", _TAG.sub(" ", raw)).strip()


def extractor_name() -> str:
    """Which path is live -- log this so the wire is never a guess."""
    return "lxml" if _HAVE_LXML else "regex-fallback"
