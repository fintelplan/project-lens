"""lens_provider_health.py -- PROVIDER HEALTH for the operator. Item 11, CC-105 (LENS-045).

Reads lens_provider_events (CC-103/104) for the last WINDOW_HOURS and sends ONE
plain-text Telegram message. It is its own step at the end of the Manager +
Analyze wave, in its own process: operator information, never the canary's
voice (item 11 ruling b). It makes no provider call (ruling a: passive).

    DOWN       a payment / model_gone / auth refusal in 2 or more runs
    EXCEPTION  the same, in one run so far -- watching
    WARN       daily_quota / rate_limit / server / other

Known limit: successes are not stored, so DOWN means "refused in 2+ runs", not
"no success at all". Exception classes do not clear on retry, which is why the
rule is taken as sufficient -- an assumption, written down on purpose.

Always exits 0. The message is the loud part; a dead primary with a working
fallback (S3-B's Gemini) must not turn every wave red and empty red of meaning.
"""
import os
import sys
from datetime import datetime, timezone, timedelta

WINDOW_HOURS = 36
WATCHED = ("cloudflare", "cohere", "google", "groq", "mistral")
EXCEPTION_CLASSES = ("payment", "model_gone", "auth")
HEADER = "PROVIDER HEALTH (operator, not the canary)"


def fetch(url: str, key: str, since_iso: str) -> list:
    import requests
    r = requests.get(f"{url}/rest/v1/lens_provider_events",
                     params={"select": "provider,model,class,severity,n,run_id,created_at",
                             "created_at": f"gte.{since_iso}",
                             "order": "created_at.asc", "limit": "1000"},
                     headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return r.json()


def summarize(rows: list):
    groups = {}
    for r in rows:
        k = (r.get("provider") or "?", r.get("model") or "?", r.get("class") or "?")
        g = groups.setdefault(k, {"n": 0, "runs": set(), "first": str(r.get("created_at") or "")})
        g["n"] += int(r.get("n") or 1)
        g["runs"].add(r.get("run_id") or "?")
    out = []
    for (p, m, c), g in groups.items():
        runs = len(g["runs"])
        if c in EXCEPTION_CLASSES and runs >= 2:
            tag, rank = "DOWN", 0
        elif c in EXCEPTION_CLASSES:
            tag, rank = "EXCEPTION (1 run, watching)", 1
        else:
            tag, rank = "WARN", 2
        since = g["first"][:16].replace("T", " ")
        out.append((rank, f"{tag}  {p}/{m}  {c} x{g['n']} in {runs} run{'' if runs == 1 else 's'} since {since}Z"))
    lines = [t for _, t in sorted(out)]
    seen = {k[0] for k in groups}
    quiet = [p for p in WATCHED if p not in seen]
    return lines, quiet


def compose(lines: list, quiet: list, hours: int = WINDOW_HOURS) -> str:
    parts = [f"{HEADER} -- last {hours}h"]
    parts += lines if lines else ["No provider refusal stored."]
    if quiet:
        parts.append("No refusal stored for: " + ", ".join(quiet))
    parts.append("DOWN = payment/model_gone/auth refused in 2+ runs. Successes are not stored.")
    return "\n".join(parts)


def send(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("[HEALTH] Telegram keys not set -- not sent")
        return False
    try:
        import requests
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text}, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"[HEALTH] Telegram send failed: {type(e).__name__}")
        return False


def main() -> int:
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=WINDOW_HOURS)).isoformat()
    try:
        url = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY") or ""
        if not url or not key:
            raise RuntimeError("no Supabase env")
        lines, quiet = summarize(fetch(url, key, since))
        text = compose(lines, quiet)
    except Exception as e:
        text = f"{HEADER}\nprovider health unavailable: {type(e).__name__}: {str(e)[:80]}"
    print(text)
    print(f"[HEALTH] sent={send(text)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
