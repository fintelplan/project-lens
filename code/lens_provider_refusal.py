"""lens_provider_refusal.py -- one line, one shape, for every provider refusal.

Item 11 (LENS-045), phase 1. SambaNova (402, 2026-07-28) and Cerebras (402,
~2026-08-17) died with nothing watching, and were found weeks later. Pinging
a models endpoint would not have caught Cerebras (GET /models answered 200
while inference answered 402), and an inference ping breathes a lens's quota.
So this watches what production already hears: the provider's own refusal.

Every live call site that gives up on a provider calls record_refusal() once,
and the log gains one line of one shape:

    PROVIDER_REFUSAL provider=... model=... class=... status=... reason=...

class is one of: payment, daily_quota, rate_limit, model_gone, auth, server,
other -- read from the status and the provider's own words. The reason is
redacted against the real environment values (CC-92's rule), and this module
never raises: a detector must not break the position it watches.
Phase 2 (LENS-046): the remaining sites, the per-provider summary, the death rule.
"""
import logging
import os
import re

log = logging.getLogger("provider_refusal")
_SECRET_NAME_HINTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "ACCOUNT_ID", "URL")
REFUSALS = []   # (provider, model, class, status) seen in this process


def _redact(text: str) -> str:
    for name, value in os.environ.items():
        if value and len(value) >= 12 and any(h in name.upper() for h in _SECRET_NAME_HINTS):
            text = text.replace(value, "***")
    return text


def _status_of(exc, text: str):
    for attr in ("status_code", "code"):
        v = getattr(exc, attr, None) if exc is not None else None
        if isinstance(v, int) and 100 <= v <= 599:
            return v
    m = re.search(r"(?:Error code: |^)(\d{3})\b", text or "")
    return int(m.group(1)) if m else None


def classify(status, text: str) -> str:
    t = (text or "").lower()
    if status == 402 or "payment_required" in t or "payment required" in t:
        return "payment"
    if status in (429, None) and ("daily free allocation" in t or "tokens per day" in t
                                  or "'code': 4006" in t):
        return "daily_quota"
    if status == 429:
        return "rate_limit"
    if status == 404 or "no longer available" in t or "model not found" in t or "model_not_found" in t:
        return "model_gone"
    if status in (401, 403):
        return "auth"
    if isinstance(status, int) and status >= 500:
        return "server"
    return "other"


def record_refusal(provider: str, model: str, exc=None, status=None, text=None):
    """Log one PROVIDER_REFUSAL line. Returns the class, or None if it could not."""
    try:
        if text is None:
            text = str(exc) if exc is not None else ""
        st = status if status is not None else _status_of(exc, text)
        cls = classify(st, text)
        REFUSALS.append((provider, model, cls, st))
        reason = _redact(" ".join(str(text).split()))[:200]
        log.warning(f"PROVIDER_REFUSAL provider={provider} model={model} class={cls} "
                    f"status={st} reason={reason}")
        return cls
    except Exception:
        return None
