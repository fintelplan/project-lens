"""
lens_telegram.py — Telegram Alert System
Project Lens | LENS-011
"""
import os, json, logging, requests
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TELEGRAM] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("telegram")
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

def send_message(text, parse_mode="HTML"):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN","")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID","")
    if not token or not chat_id:
        log.warning("Telegram keys not set — skipping"); return False
    try:
        r = requests.post(TELEGRAM_API.format(token=token),
            json={"chat_id":chat_id,"text":text,"parse_mode":parse_mode}, timeout=10)
        if r.status_code == 200: log.info("Message sent OK"); return True
        log.error(f"Telegram error {r.status_code}: {r.text[:200]}"); return False
    except Exception as e:
        log.error(f"Telegram send failed: {e}"); return False

def fetch_latest(run_id=None):
    from supabase import create_client
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    ma = (sb.table("lens_macro_reports").select("threat_level,executive_summary,key_findings,quality_score,run_id,created_at").order("created_at",desc=True).limit(1).execute().data or [{}])[0]
    rid = run_id or ma.get("run_id","")
    s2  = sb.table("injection_reports").select("analyst,injection_type,confidence_score").eq("run_id",rid).order("confidence_score",desc=True).limit(5).execute().data or []
    s3  = (sb.table("lens_system3_reports").select("summary,first_domino,patterns_found,position,generated_at").eq("position","S3-A").order("generated_at",desc=True).limit(1).execute().data or [{}])[0]
    s1  = sb.table("lens_reports").select("summary,quality_score,cycle,generated_at").order("generated_at",desc=True).limit(4).execute().data or []
    return {"ma":ma,"s2":s2,"s3":s3,"s1":s1}

def format_daily_brief(data):
    ma,s2,s3,s1 = data["ma"],data["s2"],data["s3"],data["s1"]
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    threat = ma.get("threat_level","UNKNOWN")
    emoji  = {"CRITICAL":"🔴","HIGH":"🟠","ELEVATED":"🟡","MODERATE":"🟢","LOW":"⚪"}.get(threat,"❓")
    avg_q  = round(sum(r.get("quality_score") or 0 for r in s1)/len(s1),1) if s1 else 0
    top_s2 = s2[0] if s2 else {}
    try:
        pats = json.loads(s3.get("patterns_found","[]")) if isinstance(s3.get("patterns_found","[]"),str) else s3.get("patterns_found",[])
        pcnt = len(pats)
    except: pcnt = 0
    lines = [
        "<b>🔭 PROJECT LENS — Daily Brief</b>",
        f"<code>{now}</code>","",
        f"{emoji} <b>THREAT: {threat}</b>","",
        "<b>━━ SYSTEM 1 ━━</b>",
        f"Lenses: {len(s1)}/4 | Avg quality: {avg_q}/10","",
        "<b>━━ SYSTEM 2 ━━</b>",
        f"Top injection: <code>{top_s2.get('injection_type','none')}</code> ({top_s2.get('analyst','?')}, conf={top_s2.get('confidence_score',0):.2f})","",
        "<b>━━ SYSTEM 3 ━━</b>",
        f"Patterns: {pcnt} detected",
        (s3.get("summary") or "No pattern report yet")[:200],
    ]
    if s3.get("first_domino"):
        lines += ["", f"⚠️ <b>First Domino:</b> {s3['first_domino'][:150]}"]
    lines += ["","<b>━━ MISSION ANALYST ━━</b>",
        (ma.get("executive_summary") or "No macro report yet")[:300],"",
        f"<i>Quality: {ma.get('quality_score',0):.2f} | Run: {ma.get('run_id','?')}</i>"]
    return "\n".join(lines)

def format_critical_alert(reason, signal, threat):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return "\n".join([
        "🚨 <b>PROJECT LENS — CRITICAL ALERT</b>",
        f"<code>{now}</code>","",
        f"<b>Threat: {threat}</b>",f"Reason: {reason}","",
        "<b>Signal:</b>",f"{signal[:400]}","",
        "<i>Immediate action may be required.</i>"])

def send_daily_brief(run_id=None):
    try:
        result = send_message(format_daily_brief(fetch_latest(run_id)))
        # S4 RLHF reminder — rate this report for calibration
        rating_msg = (
            "\u2b50 <b>Rate this report (S4 RLHF)</b>\n"
            "Run: <code>python code/lens_rate.py 4</code>\n"
            "Scale: 1=Poor 2=Weak 3=Adequate 4=Good 5=Excellent\n"
            "Add note: <code>python code/lens_rate.py 4 'good signal'</code>"
        )
        send_message(rating_msg)
        return result
    except Exception as e:
        log.error(f"Daily brief failed: {e}"); return False

def send_critical_alert(reason, signal, threat="CRITICAL"):
    return send_message(format_critical_alert(reason, signal, threat))

if __name__ == "__main__":
    from dotenv import load_dotenv; load_dotenv()
    import sys
    if len(sys.argv)>1 and sys.argv[1]=="test":
        ok = send_message("🔭 <b>Project Lens — Test Alert</b>\n\nTelegram connection verified.")
        print("SENT OK" if ok else "SEND FAILED")
    else:
        ok = send_daily_brief()
        print("BRIEF SENT" if ok else "BRIEF FAILED")
