"""
lens_s2f_direction_b.py — Direction B Forensic Report Delivery
LENS-020 | Project Lens

PHI-004 Phase 1 — Direction B (operator-gated):
  All Verification findings go to operator review ONLY.
  No public exposure until operator ratifies (Direction A, Phase 2).

Sends Verification findings as Telegram messages to operator.
Format: text message (not docx — findings are concise enough for Telegram).
Operator reviews and marks reviewed_by_operator=True in Supabase before Direction A.

Uses existing TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID secrets.
"""

import os
import sys
import logging
import requests
from datetime import datetime, timezone

log = logging.getLogger("s2f_direction_b")

TELEGRAM_CAPTION_CAP = 4000  # Telegram message max is 4096 chars


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
            log.info("Telegram message sent ✅")
            return True
        else:
            log.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Telegram send failed: {str(e)[:200]}")
        return False


def format_verification_finding(finding: dict) -> str:
    """Format a Verification finding for Telegram delivery."""
    lens = finding.get("state_actor_lens", "unknown")
    phrasing = finding.get("finding_phrasing", "")
    sample = finding.get("sample_size", 0)
    created = finding.get("created_at", "")[:10]
    persistent = finding.get("framing_mean", {})
    top_ops = sorted(persistent.items(), key=lambda x: x[1], reverse=True)[:5]

    msg = (
        f"🔴 <b>S2-F VERIFICATION FINDING</b>\n"
        f"<b>Lens:</b> {lens}\n"
        f"<b>Date:</b> {created}\n"
        f"<b>Sample:</b> {sample} articles\n"
        f"<b>Confidence:</b> HIGH\n\n"
        f"<b>Top persistent operations:</b>\n"
    )
    for op_id, count in top_ops:
        msg += f"  • {op_id}: {count} articles\n"

    msg += f"\n<b>Full finding:</b>\n{phrasing[:800]}...\n\n"
    msg += f"⚠️ <b>Direction B — Operator review required.</b>\n"
    msg += f"Mark reviewed in Supabase before Direction A delivery."

    return msg


def run_direction_b_delivery(dry_run: bool = False) -> int:
    """Fetch undelivered HIGH-confidence findings and send to operator.

    Returns count of findings delivered.
    """
    client = _get_supabase_client()
    if not client:
        log.error("No Supabase client")
        return 0

    # Fetch unreviewed HIGH findings not yet delivered
    try:
        response = client.table("lens_drift_findings") \
            .select("*") \
            .eq("finding_confidence", "HIGH") \
            .eq("reviewed_by_operator", False) \
            .order("created_at", desc=True) \
            .limit(5) \
            .execute()
        findings = response.data or []
    except Exception as e:
        log.error(f"Findings fetch failed: {str(e)[:200]}")
        return 0

    log.info(f"Direction B: {len(findings)} unreviewed HIGH findings")

    if not findings:
        log.info("No new HIGH findings to deliver")
        return 0

    delivered = 0
    for finding in findings:
        msg = format_verification_finding(finding)

        if dry_run:
            log.info(f"[DRY RUN] Would send:\n{msg[:200]}...")
            delivered += 1
            continue

        sent = send_telegram_text(msg)
        if sent:
            # Mark as operator_notes with delivery timestamp
            try:
                client.table("lens_drift_findings") \
                    .update({"operator_notes": f"Direction B delivered at {datetime.now(timezone.utc).isoformat()}"}) \
                    .eq("id", finding["id"]) \
                    .execute()
            except Exception:
                pass
            delivered += 1

    log.info(f"Direction B delivery: {delivered}/{len(findings)} findings sent")
    return delivered


if __name__ == "__main__":
    if not os.environ.get("GITHUB_ACTIONS"):
        from dotenv import load_dotenv
        load_dotenv()
    dry = "--dry" in sys.argv
    count = run_direction_b_delivery(dry_run=dry)
    print(f"\nDirection B: {count} findings delivered to operator")
