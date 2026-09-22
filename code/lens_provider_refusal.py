"""lens_provider_refusal.py -- one line, one shape, for every provider refusal, STORED.

Item 11 (LENS-045). SambaNova (402, 2026-07-28) and Cerebras (402, ~2026-08-17)
died with nothing watching, and were found weeks later. Pinging a models
endpoint would not have caught Cerebras (GET /models answered 200 while
inference answered 402), and an inference ping breathes a lens's quota. So this
watches what production already hears: the provider's own refusal.

Phase 1 (CC-102): each live call site that gives up calls record_refusal() once:

    PROVIDER_REFUSAL provider=... model=... class=... status=... reason=...

Phase 2 (CC-103), ITIL 4 monitoring and event management -- record, store,
provide: class maps to a severity (exception = the provider may be gone:
payment, model_gone, auth; warning = it recovers: daily_quota, rate_limit,
server, other). Events are buffered in memory and written to
lens_provider_events ONCE, at process exit, in one batch -- never per call,
so a refusal storm cannot slow the air supply (gas-mask arm 2). Only inside
GitHub Actions: local tests and probes do not write to the production table.

This module never raises: a detector must not break the position it watches.
"""
import atexit
import logging
import os
import re
import sys

log = logging.getLogger("provider_refusal")
_SECRET_NAME_HINTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "ACCOUNT_ID", "URL")
EXCEPTION_CLASSES = ("payment", "model_gone", "auth")
REFUSALS = []    # (provider, model, class, status) seen in this process
_PENDING = []    # rows not yet stored
_ATEXIT = {"registered": False}


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


def severity(cls: str) -> str:
    return "exception" if cls in EXCEPTION_CLASSES else "warning"


def record_refusal(provider: str, model: str, exc=None, status=None, text=None):
    """Log one PROVIDER_REFUSAL line and buffer it. Returns the class, or None."""
    try:
        if text is None:
            text = str(exc) if exc is not None else ""
        st = status if status is not None else _status_of(exc, text)
        cls = classify(st, text)
        sev = severity(cls)
        reason = _redact(" ".join(str(text).split()))[:200]
        REFUSALS.append((provider, model, cls, st))
        _PENDING.append({"provider": str(provider), "model": str(model), "class": cls,
                         "severity": sev, "status": st,
                         "source": os.path.basename(sys.argv[0] or "") or None,
                         "run_id": os.environ.get("GITHUB_RUN_ID") or "local",
                         "reason": reason})
        if not _ATEXIT["registered"]:
            atexit.register(flush)
            _ATEXIT["registered"] = True
        log.warning(f"PROVIDER_REFUSAL provider={provider} model={model} class={cls} "
                    f"status={st} reason={reason}")   # shape unchanged since CC-102
        return cls
    except Exception:
        return None


def _aggregate(rows: list) -> list:
    """CC-104: one row per (provider, model, class, status, source, run_id), counted in n.

    Collection alone produced 133 refusals in one run on 2026-09-22; one row each
    would be ~270 rows a day on a database that has already hit its fair-use limit.
    The first reason is kept.
    """
    out = {}
    for r in rows:
        k = (r["provider"], r["model"], r["class"], r["status"], r["source"], r["run_id"])
        if k in out:
            out[k]["n"] += 1
        else:
            out[k] = dict(r, n=1)
    return list(out.values())


def flush() -> int:
    """Write the buffered events in one request. Returns rows stored; never raises."""
    try:
        if not _PENDING:
            return 0
        rows = _aggregate(list(_PENDING))
        _PENDING.clear()
        if os.environ.get("GITHUB_ACTIONS") != "true":
            log.info(f"PROVIDER_EVENTS not stored: not running in Actions ({len(rows)} events)")
            return 0
        url = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY") or ""
        if not url or not key:
            log.warning(f"PROVIDER_EVENTS NOT stored: no Supabase env ({len(rows)} events)")
            return 0
        import requests
        r = requests.post(f"{url}/rest/v1/lens_provider_events", json=rows, timeout=10,
                          headers={"apikey": key, "Authorization": f"Bearer {key}",
                                   "Content-Type": "application/json",
                                   "Prefer": "return=minimal"})
        if r.status_code in (200, 201, 204):
            log.info(f"PROVIDER_EVENTS stored: {len(rows)} rows, "
                     f"{sum(r['n'] for r in rows)} events")
            return len(rows)
        log.warning(f"PROVIDER_EVENTS NOT stored: HTTP {r.status_code} "
                    f"{_redact(r.text)[:160]} ({len(rows)} events)")
        return 0
    except Exception as e:
        try:
            log.warning(f"PROVIDER_EVENTS NOT stored: {type(e).__name__} {_redact(str(e))[:120]}")
        except Exception:
            pass
        return 0
