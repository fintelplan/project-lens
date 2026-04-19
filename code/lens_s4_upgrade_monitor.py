"""
lens_s4_upgrade_monitor.py
S4-E: Upgrade Trigger Monitor

Watches Table 8 thresholds from the architecture document.
No AI needed — pure database counts.
Fires Telegram alert when any threshold is crossed.

Table 8 thresholds:
  S1: 100 complete runs
  S2: 50 injection reports
  S3: 90 days continuous operation

Runs at end of every cron — after Mission Analyst completes.
Cost: $0. Zero quota impact.
"""

import os, json, logging, requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ── Canonical cycle + era boundary (LENS-014 O1) ──────────────────────────────
from lens_cycle import DAY_1_UTC, CANONICAL_CYCLES, LEGACY_CYCLES

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [S4-E] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("S4-E")

# Table 8 thresholds
THRESHOLD_S1_RUNS     = 100
THRESHOLD_S2_REPORTS  = 50
THRESHOLD_S3_DAYS     = 90


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE credentials missing")
    return create_client(url, key)


def count_s1_runs_legacy(sb: Client) -> int:
    """Count S1 production runs from the LEGACY era (before DAY_1_UTC).

    Legacy era used mixed labels: morning/afternoon/evening/midnight
    (per LENS-005 FIX-021 and earlier). These rows preserve operational
    history from before the LENS-014 O1 canonical cycle rollout.
    """
    try:
        r = sb.table("lens_reports") \
            .select("id", count="exact") \
            .in_("cycle", LEGACY_CYCLES) \
            .lt("generated_at", DAY_1_UTC.isoformat()) \
            .execute()
        return r.count or len(r.data or [])
    except Exception as e:
        log.warning(f"S1 legacy count failed: {e}")
        return 0


def count_s1_runs_new(sb: Client) -> int:
    """Count S1 production runs from the NEW era (at or after DAY_1_UTC).

    New era uses canonical cycle labels: 2of1 / 2of2. All rows after
    Day 1 (April 17, 2026 UTC) should carry canonical labels per
    LENS-014 O1 (lens_cycle.py).
    """
    try:
        r = sb.table("lens_reports") \
            .select("id", count="exact") \
            .in_("cycle", CANONICAL_CYCLES) \
            .gte("generated_at", DAY_1_UTC.isoformat()) \
            .execute()
        return r.count or len(r.data or [])
    except Exception as e:
        log.warning(f"S1 new count failed: {e}")
        return 0


def count_s1_runs(sb: Client) -> int:
    """Total S1 production runs across both eras (legacy + new).

    Preserves full operational history for upgrade-trigger accounting.
    S4-E threshold check uses this total, ensuring pre-Day-1 experience
    counts toward maturity. Individual eras accessible via the
    count_s1_runs_legacy() and count_s1_runs_new() helpers.
    """
    return count_s1_runs_legacy(sb) + count_s1_runs_new(sb)


def count_s2_reports(sb: Client) -> int:
    """Count total injection reports in injection_reports table."""
    try:
        r = sb.table("injection_reports") \
            .select("id", count="exact") \
            .execute()
        return r.count or len(r.data or [])
    except Exception as e:
        log.warning(f"S2 count failed: {e}")
        return 0


def count_s3_days(sb: Client) -> int:
    """Count distinct days with S3 reports — approximates continuous operation."""
    try:
        r = sb.table("lens_system3_reports") \
            .select("generated_at") \
            .not_.is_("generated_at", "null") \
            .execute()
        dates = set(
            row["generated_at"][:10]
            for row in (r.data or [])
            if row.get("generated_at")
        )
        return len(dates)
    except Exception as e:
        log.warning(f"S3 days count failed: {e}")
        return 0


def send_telegram(message: str) -> bool:
    """Send upgrade trigger alert via Telegram."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("Telegram credentials missing — cannot send upgrade alert")
        return False
    try:
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def fetch_alert_state(sb: Client, threshold_id: str):
    """Fetch crossing-edge state row for a given threshold_id.
    Returns the row dict if a state row exists, else None.
    Returns None on read failure as graceful fallback (LR-080) — None means
    "treat as fresh" and the original fire-every-run behaviour is preserved
    rather than silent-failing into an unknown alert state.
    """
    try:
        r = (
            sb.table("lens_s4_alert_state")
              .select("threshold_id,last_count,threshold_value,last_alerted_at,updated_at")
              .eq("threshold_id", threshold_id)
              .limit(1)
              .execute()
        )
        rows = r.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.warning(f"Alert state fetch failed for {threshold_id}: {e} - degrading to fire-every-run")
        return None


def upsert_alert_state(sb: Client, threshold_id: str, current: int,
                       threshold_value: int, alerted: bool,
                       prev_alerted_at) -> bool:
    """Upsert one row in lens_s4_alert_state.
    last_alerted_at: bumped to now() iff alerted=True this run; else preserved
    from prev_alerted_at (which may be None on first insert — column stays NULL).
    Returns True on success, False on logged failure (non-fatal — alerting itself
    still works via the should_alert path; only the dedupe state is at risk).
    """
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        record = {
            "threshold_id": threshold_id,
            "last_count": current,
            "threshold_value": threshold_value,
            "updated_at": now_iso,
        }
        if alerted:
            record["last_alerted_at"] = now_iso
        elif prev_alerted_at is not None:
            record["last_alerted_at"] = prev_alerted_at
        # else: omit -> DB column stays NULL on first insert
        (
            sb.table("lens_s4_alert_state")
              .upsert(record, on_conflict="threshold_id")
              .execute()
        )
        return True
    except Exception as e:
        log.warning(f"Alert state upsert failed for {threshold_id}: {e}")
        return False


def run_upgrade_monitor() -> dict:
    """
    S4-E: Check all Table 8 upgrade thresholds.
    Fires Telegram alert on first crossing of any threshold.
    """
    log.info("=== S4-E UPGRADE TRIGGER MONITOR ===")

    try:
        sb = get_supabase()
    except Exception as e:
        log.error(f"Supabase init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    s1_runs_legacy = count_s1_runs_legacy(sb)
    s1_runs_new    = count_s1_runs_new(sb)
    s1_runs        = s1_runs_legacy + s1_runs_new
    s2_reports     = count_s2_reports(sb)
    s3_days        = count_s3_days(sb)

    log.info(f"S1 runs: {s1_runs}/{THRESHOLD_S1_RUNS} "
             f"(legacy: {s1_runs_legacy}, new: {s1_runs_new}) | "
             f"S2 reports: {s2_reports}/{THRESHOLD_S2_REPORTS} | "
             f"S3 days: {s3_days}/{THRESHOLD_S3_DAYS}")

    alerts = []

    # Check S1 threshold (crossing-edge gate - LENS-017 I7)
    s1_state = fetch_alert_state(sb, "S1_RUNS")
    s1_should_alert = (
        s1_runs >= THRESHOLD_S1_RUNS
        and (
            s1_state is None
            or s1_state["threshold_value"] != THRESHOLD_S1_RUNS
            or s1_state["last_count"] < THRESHOLD_S1_RUNS
        )
    )
    if s1_should_alert:
        alerts.append(
            f"🔔 S1 UPGRADE TRIGGER: {s1_runs} complete runs reached "
            f"(legacy: {s1_runs_legacy}, new: {s1_runs_new}).\n"
            f"Table 8 threshold: {THRESHOLD_S1_RUNS}.\n"
            f"Action: S2 corrections can now feed back to S1 source scoring (5-B)."
        )
        log.info(f"S1 THRESHOLD CROSSED (edge): {s1_runs} >= {THRESHOLD_S1_RUNS} "
                 f"(legacy: {s1_runs_legacy}, new: {s1_runs_new})")
    elif s1_runs >= THRESHOLD_S1_RUNS:
        log.info(f"S1 above threshold ({s1_runs} >= {THRESHOLD_S1_RUNS}) - alert suppressed (already fired)")
    upsert_alert_state(
        sb, "S1_RUNS", s1_runs, THRESHOLD_S1_RUNS,
        alerted=s1_should_alert,
        prev_alerted_at=(s1_state.get("last_alerted_at") if s1_state else None),
    )

    # Check S2 threshold (crossing-edge gate - LENS-017 I7)
    s2_state = fetch_alert_state(sb, "S2_REPORTS")
    s2_should_alert = (
        s2_reports >= THRESHOLD_S2_REPORTS
        and (
            s2_state is None
            or s2_state["threshold_value"] != THRESHOLD_S2_REPORTS
            or s2_state["last_count"] < THRESHOLD_S2_REPORTS
        )
    )
    if s2_should_alert:
        alerts.append(
            f"🔔 S2 UPGRADE TRIGGER: {s2_reports} injection reports reached.\n"
            f"Table 8 threshold: {THRESHOLD_S2_REPORTS}.\n"
            f"Action: S3 patterns can now feed back to S2 immune rules (5-A).\n"
            f"Action: Phase 2 RL — simple EMA source weight adjustment ready."
        )
        log.info(f"S2 THRESHOLD CROSSED (edge): {s2_reports} >= {THRESHOLD_S2_REPORTS}")
    elif s2_reports >= THRESHOLD_S2_REPORTS:
        log.info(f"S2 above threshold ({s2_reports} >= {THRESHOLD_S2_REPORTS}) - alert suppressed (already fired)")
    upsert_alert_state(
        sb, "S2_REPORTS", s2_reports, THRESHOLD_S2_REPORTS,
        alerted=s2_should_alert,
        prev_alerted_at=(s2_state.get("last_alerted_at") if s2_state else None),
    )

    # Check S3 threshold (crossing-edge gate - LENS-017 I7)
    s3_state = fetch_alert_state(sb, "S3_DAYS")
    s3_should_alert = (
        s3_days >= THRESHOLD_S3_DAYS
        and (
            s3_state is None
            or s3_state["threshold_value"] != THRESHOLD_S3_DAYS
            or s3_state["last_count"] < THRESHOLD_S3_DAYS
        )
    )
    if s3_should_alert:
        alerts.append(
            f"🔔 S3 UPGRADE TRIGGER: {s3_days} days of continuous operation.\n"
            f"Table 8 threshold: {THRESHOLD_S3_DAYS}.\n"
            f"Action: S4-C Calibration Analyst ready to build.\n"
            f"Action: 180-day S3-D window can now activate.\n"
            f"Action: Phase 3 DL classifier on prediction-outcome pairs ready."
        )
        log.info(f"S3 THRESHOLD CROSSED (edge): {s3_days} >= {THRESHOLD_S3_DAYS}")
    elif s3_days >= THRESHOLD_S3_DAYS:
        log.info(f"S3 above threshold ({s3_days} >= {THRESHOLD_S3_DAYS}) - alert suppressed (already fired)")
    upsert_alert_state(
        sb, "S3_DAYS", s3_days, THRESHOLD_S3_DAYS,
        alerted=s3_should_alert,
        prev_alerted_at=(s3_state.get("last_alerted_at") if s3_state else None),
    )

    # Progress report (always — so Bro Alpha can track progress)
    s1_pct = min(100, int(s1_runs / THRESHOLD_S1_RUNS * 100))
    s2_pct = min(100, int(s2_reports / THRESHOLD_S2_REPORTS * 100))
    s3_pct = min(100, int(s3_days / THRESHOLD_S3_DAYS * 100))

    log.info(f"Progress: S1 {s1_pct}% | S2 {s2_pct}% | S3 {s3_pct}%")

    if alerts:
        full_message = "🚀 PROJECT LENS — UPGRADE TRIGGER ALERT\n\n" + "\n\n".join(alerts)
        sent = send_telegram(full_message)
        log.info(f"Upgrade alert sent via Telegram: {sent}")

    return {
        "status":      "OK",
        "s1_runs":        s1_runs,
        "s1_runs_legacy": s1_runs_legacy,
        "s1_runs_new":    s1_runs_new,
        "s2_reports":  s2_reports,
        "s3_days":     s3_days,
        "thresholds":  {
            "s1": f"{s1_runs}/{THRESHOLD_S1_RUNS} ({s1_pct}%)",
            "s2": f"{s2_reports}/{THRESHOLD_S2_REPORTS} ({s2_pct}%)",
            "s3": f"{s3_days}/{THRESHOLD_S3_DAYS} ({s3_pct}%)",
        },
        "alerts_fired": len(alerts),
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    import json
    result = run_upgrade_monitor()
    print(json.dumps(result, indent=2))
