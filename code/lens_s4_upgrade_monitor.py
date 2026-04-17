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


def count_s1_runs(sb: Client) -> int:
    """Count distinct S1 run_ids in lens_reports."""
    try:
        r = sb.table("lens_reports").select("id").execute()
        run_ids = set(row["id"] for row in (r.data or []))
        return len(run_ids)
    except Exception as e:
        log.warning(f"S1 count failed: {e}")
        return 0


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

    s1_runs    = count_s1_runs(sb)
    s2_reports = count_s2_reports(sb)
    s3_days    = count_s3_days(sb)

    log.info(f"S1 runs: {s1_runs}/{THRESHOLD_S1_RUNS} | "
             f"S2 reports: {s2_reports}/{THRESHOLD_S2_REPORTS} | "
             f"S3 days: {s3_days}/{THRESHOLD_S3_DAYS}")

    alerts = []

    # Check S1 threshold
    if s1_runs >= THRESHOLD_S1_RUNS:
        alerts.append(
            f"🔔 S1 UPGRADE TRIGGER: {s1_runs} complete runs reached.\n"
            f"Table 8 threshold: {THRESHOLD_S1_RUNS}.\n"
            f"Action: S2 corrections can now feed back to S1 source scoring (5-B)."
        )
        log.info(f"S1 THRESHOLD CROSSED: {s1_runs} >= {THRESHOLD_S1_RUNS}")

    # Check S2 threshold
    if s2_reports >= THRESHOLD_S2_REPORTS:
        alerts.append(
            f"🔔 S2 UPGRADE TRIGGER: {s2_reports} injection reports reached.\n"
            f"Table 8 threshold: {THRESHOLD_S2_REPORTS}.\n"
            f"Action: S3 patterns can now feed back to S2 immune rules (5-A).\n"
            f"Action: Phase 2 RL — simple EMA source weight adjustment ready."
        )
        log.info(f"S2 THRESHOLD CROSSED: {s2_reports} >= {THRESHOLD_S2_REPORTS}")

    # Check S3 threshold
    if s3_days >= THRESHOLD_S3_DAYS:
        alerts.append(
            f"🔔 S3 UPGRADE TRIGGER: {s3_days} days of continuous operation.\n"
            f"Table 8 threshold: {THRESHOLD_S3_DAYS}.\n"
            f"Action: S4-C Calibration Analyst ready to build.\n"
            f"Action: 180-day S3-D window can now activate.\n"
            f"Action: Phase 3 DL classifier on prediction-outcome pairs ready."
        )
        log.info(f"S3 THRESHOLD CROSSED: {s3_days} >= {THRESHOLD_S3_DAYS}")

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
        "s1_runs":     s1_runs,
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
