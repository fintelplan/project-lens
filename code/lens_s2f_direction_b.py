"""
lens_s2f_direction_b.py -- Direction B Forensic Report Delivery
LENS-020 | Project Lens | send-once rewrite CC-120 (LENS-047)

PHI-004 Phase 1 -- Direction B (operator-gated):
  All Verification findings go to operator review ONLY.
  No public exposure until operator ratifies (Direction A, Phase 2).

Sends Verification findings as Telegram messages to the operator, ONCE per state change
(completion test L3.4). The decision is lens_s2f_delivery_rules.decide(); every message
sent is written to the append-only ledger lens_s2f_deliveries, which is also the memory
of what the operator has already been told:
  NEW      a finding appears (or reappears after CLEARED)
  CHANGED  its persistent operations change and the change has held HOLD_DAYS daily checks
  CLEARED  it has not been confirmed on CLEAR_ALIVE_DAYS days on which the aggregators ran
Unchanged findings send nothing.

Before CC-120 this step re-sent the newest five unreviewed HIGH rows on every run (one
finding, ten messages a day), stamped the delivery into operator_notes -- the operator's
own field -- and never announced a finding that stopped being confirmed. operator_notes
is no longer written.

Fails loudly (exit 1): no database, ledger unreadable, a Telegram send refused, or a ledger
write refused after a send. A failed send writes no ledger row, so the next run tries again.
Operator review is unchanged: mark reviewed_by_operator=True in Supabase.

Uses existing TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID secrets.
Byte check per run: the line "S2F_DELIVERY new=.. changed=.. cleared=.. quiet=.. failed=..".
"""

import html
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [S2F-DIRB] %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("s2f_direction_b")

# Configured before these imports so the aggregator's own logging setup cannot relabel us.
from lens_s2f_delivery_rules import (HOLD_DAYS, CLEAR_ALIVE_DAYS, OPEN_KINDS,   # noqa: E402
                                     daily_series, decide)
from lens_s2f_verification_aggregator import (VERIFICATION_RECUR_MIN,           # noqa: E402
                                              VERIFICATION_MIN_ARTICLES,
                                              VERIFICATION_MIN_OPS,
                                              VERIFICATION_WINDOW_DAYS)

TELEGRAM_CAPTION_CAP = 4000  # Telegram message max is 4096 chars
FINDINGS = "lens_drift_findings"
LEDGER = "lens_s2f_deliveries"
LOOKBACK_DAYS = 2 * VERIFICATION_WINDOW_DAYS

X, GE, DASH = "\u00d7", "\u2265", "\u2014"
MARK = {"NEW": "\U0001F534", "CHANGED": "\U0001F7E0", "CLEARED": "\u26AA"}


def _get_supabase_client():
    try:
        from supabase import create_client
    except ImportError:
        return None
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


def send_telegram_text(message: str) -> bool:
    """Send a text message to operator via Telegram."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, data={
            "chat_id": chat_id,
            "text": message[:TELEGRAM_CAPTION_CAP],
            "parse_mode": "HTML",
        }, timeout=30)
        if resp.status_code == 200:
            log.info("Telegram message sent")
            return True
        log.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Telegram send failed: {str(e)[:200]}")
        return False


def _fetch_all(client, table, cols, since, order="created_at", **eq):
    out, start = [], 0
    while True:
        q = client.table(table).select(cols).gte(order, since)
        for k, v in eq.items():
            q = q.eq(k, v)
        page = q.order(order).range(start, start + 999).execute().data or []
        out += page
        if len(page) < 1000:
            return out
        start += 1000


def _ops_with_counts(ops, row):
    counts = (row or {}).get("framing_mean") or {}
    ranked = sorted(ops, key=lambda op: (-(counts.get(op) or 0), op))
    return ", ".join(f"{html.escape(op)} ({counts[op]})" if op in counts else html.escape(op)
                     for op in ranked) or "none"


def format_event(voice: str, lens: str, ev: dict) -> str:
    """One Telegram message for one decided event."""
    kind, row = ev["kind"], ev.get("row") or {}
    head = (f"{MARK[kind]} <b>S2-F VERIFICATION {DASH} "
            f"{'NO LONGER CONFIRMED' if kind == 'CLEARED' else kind}</b>\n"
            f"<b>{html.escape(voice)}</b> {X} {html.escape(lens)}\n")
    if kind == "NEW":
        body = (f"Confirmed {ev['last_day']}: {row.get('sample_size', '?')} articles in "
                f"{VERIFICATION_WINDOW_DAYS} days; {len(ev['reported_ops'])} operations each in "
                f"{GE}{VERIFICATION_RECUR_MIN} articles:\n"
                f"{_ops_with_counts(ev['reported_ops'], row)}\n\n"
                f"Sent once. You hear about this finding again only if its operations change "
                f"(held for {HOLD_DAYS} daily checks) or it stops being confirmed.\n")
    elif kind == "CHANGED":
        was = len(set(ev["reported_ops"]) - set(ev["added"]) | set(ev["dropped"]))
        body = ""
        if ev["added"]:
            body += (f"Added {DASH} in {GE}{VERIFICATION_RECUR_MIN} articles on each of the "
                     f"last {HOLD_DAYS} daily checks: {_ops_with_counts(ev['added'], row)}\n")
        if ev["dropped"]:
            body += (f"Dropped {DASH} below {VERIFICATION_RECUR_MIN} articles on each of the "
                     f"last {HOLD_DAYS} daily checks: {html.escape(', '.join(ev['dropped']))}\n")
        body += (f"Now {len(ev['reported_ops'])} persistent operations (was {was}). "
                 f"Latest check {ev['last_day']}: {row.get('sample_size', '?')} articles.\n")
    else:
        since = (f"Last confirmed {ev['last_day']}; since then the S2-F aggregators ran on "
                 f"{ev['alive_since']} days" if ev.get("last_day") else
                 f"Not confirmed in the last {LOOKBACK_DAYS} days; the S2-F aggregators ran")
        body = (f"{since} without confirming it (Verification needs {GE}"
                f"{VERIFICATION_MIN_ARTICLES} articles and {GE}{VERIFICATION_MIN_OPS} persistent "
                f"operations in {VERIFICATION_WINDOW_DAYS} days).\n"
                f"This does not say why: a real change and a gap in collection look the same "
                f"here.\n")
    tail = ("\n\u26A0\uFE0F <b>Direction B " + DASH + " operator review.</b> "
            "Mark reviewed_by_operator in Supabase before any Direction A use."
            if kind != "CLEARED" else "")
    return head + body + tail


def _ledger_row(voice, lens, ev):
    detail = {"NEW": f"{len(ev['reported_ops'])} ops",
              "CHANGED": f"+{','.join(ev['added'])} -{','.join(ev['dropped'])}",
              "CLEARED": f"last_day={ev.get('last_day')} alive_since={ev.get('alive_since')}"}
    return {"voice_name": voice, "state_actor_lens": lens, "kind": ev["kind"],
            "reported_ops": ev["reported_ops"],
            "finding_id": str((ev.get("row") or {}).get("id") or "") or None,
            "detail": detail[ev["kind"]]}


def run_direction_b_delivery(dry_run: bool = False) -> int:
    """Decide and send. Returns the number of failures (0 = green)."""
    client = _get_supabase_client()
    if not client:
        log.error("No Supabase client")
        return 1

    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    since = (now - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    try:
        high = _fetch_all(client, FINDINGS,
                          "id,created_at,state_actor_lens,sample_size,framing_mean,finding_phrasing",
                          since, finding_confidence="HIGH")
        alive = {str(r["created_at"])[:10] for r in _fetch_all(client, FINDINGS, "created_at", since)}
    except Exception as e:
        log.error(f"Findings fetch failed: {str(e)[:200]}")
        return 1
    try:
        entries = client.table(LEDGER).select("voice_name,state_actor_lens,kind,reported_ops,sent_at") \
            .order("sent_at", desc=True).limit(5000).execute().data or []
    except Exception as e:
        if not dry_run:
            log.error(f"Ledger {LEDGER} unreadable -- sending nothing: {str(e)[:200]}")
            return 1
        log.warning(f"[DRY RUN] ledger unreadable, treated as empty: {str(e)[:120]}")
        entries = []

    last = {}
    for e in entries:
        last.setdefault((e["voice_name"], e["state_actor_lens"]), e)

    series, unparsed = daily_series(high, VERIFICATION_RECUR_MIN)
    if unparsed:
        log.warning(f"{len(unparsed)} HIGH rows without a 'VERIFICATION FINDING (lens): voice' "
                    f"header were skipped: ids {[r.get('id') for r in unparsed][:5]}")
    log.info(f"Direction B: {len(high)} HIGH rows since {since}, {len(series)} findings, "
             f"{len(alive)} aggregator days, {len(last)} findings in the ledger")

    counts = {"NEW": 0, "CHANGED": 0, "CLEARED": 0, "quiet": 0, "failed": 0}
    for pair in sorted(set(series) | {p for p, e in last.items() if e["kind"] in OPEN_KINDS}):
        voice, lens = pair
        ev = decide(series.get(pair, []), last.get(pair), alive, today)
        if ev is None:
            counts["quiet"] += 1
            continue
        msg = format_event(voice, lens, ev)
        if dry_run:
            log.info(f"[DRY RUN] would send {ev['kind']} for {voice} x {lens}:\n{msg}")
            counts[ev["kind"]] += 1
            continue
        if not send_telegram_text(msg):
            counts["failed"] += 1
            continue
        try:
            client.table(LEDGER).insert(_ledger_row(voice, lens, ev)).execute()
        except Exception as e:
            counts["failed"] += 1
            log.error(f"SENT but ledger write refused -- the next run will send it again: "
                      f"{voice} x {lens} {ev['kind']}: {str(e)[:200]}")
            continue
        counts[ev["kind"]] += 1
        log.info(f"{ev['kind']}: {voice} x {lens}")

    log.info("S2F_DELIVERY new={NEW} changed={CHANGED} cleared={CLEARED} quiet={quiet} "
             "failed={failed}".format(**counts) + (" (dry run)" if dry_run else ""))
    return counts["failed"]


if __name__ == "__main__":
    if not os.environ.get("GITHUB_ACTIONS"):
        from dotenv import load_dotenv
        load_dotenv()
    dry = "--dry" in sys.argv
    failures = run_direction_b_delivery(dry_run=dry)
    print(f"\nDirection B: {failures} failure(s)")
    sys.exit(1 if failures else 0)
