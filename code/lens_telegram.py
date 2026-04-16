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


# ── Intelligence Step Reports ─────────────────────────────────────────────────

def _get_sb():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def send_s1_intelligence(run_id=None):
    """Message 1: What the canary sees. Plain English. The actual picture."""
    try:
        from datetime import datetime, timezone, timedelta
        sb = _get_sb()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        s1 = sb.table("lens_reports") \
            .select("domain_focus,summary,quality_score,cycle,generated_at") \
            .order("generated_at", desc=True).limit(4).execute().data or []
        if not s1:
            return False
        cycle = s1[0].get("cycle", "") if s1 else ""
        avg_q = round(sum(r.get("quality_score") or 0 for r in s1) / len(s1), 1) if s1 else 0
        lines = [
            "🔭 <b>WHAT THE CANARY SEES</b>",
            f"<i>{cycle} | {now} | quality {avg_q}/10</i>", "",
        ]
        for r in s1:
            focus   = (r.get("domain_focus", "") or "").strip()
            summary = (r.get("summary", "") or "").strip()
            if summary:
                lines += [f"<b>{focus}</b>", summary[:350], ""]
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            arts = sb.table("lens_raw_articles") \
                .select("title,url,source_name") \
                .gte("created_at", cutoff).order("created_at", desc=True).limit(20).execute().data or []
            if arts:
                lines.append("<b>What the canary read:</b>")
                seen_urls, seen_src, count = set(), set(), 0
                for a in arts:
                    url, title, src = a.get("url",""), (a.get("title","") or "")[:80], a.get("source_name","")
                    if url and title and url not in seen_urls and src not in seen_src:
                        lines.append(f"  🔗 <a href=\"{url}\">{title}</a> <i>[{src}]</i>")
                        seen_urls.add(url); seen_src.add(src); count += 1
                    if count >= 5:
                        break
        except Exception:
            pass
        return send_message("\n".join(lines))
    except Exception as e:
        log.error(f"send_s1_intelligence failed: {e}"); return False


def send_s2_intelligence(run_id=None):
    """Message 2: How the information is being shaped. Who. Why. What you missed."""
    try:
        from datetime import datetime, timezone, timedelta
        import json as _json
        sb = _get_sb()
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")

        def ev(raw):
            if not raw: return {}
            if isinstance(raw, dict): return raw
            try: return _json.loads(raw)
            except: return {}

        def phrases(raw):
            if not raw: return []
            if isinstance(raw, list): return raw
            try: return _json.loads(raw)
            except: return [str(raw)]

        inj = sb.table("injection_reports") \
            .select("analyst,injection_type,confidence_score,flagged_phrases,evidence") \
            .order("created_at", desc=True).limit(15).execute().data or []
        if not inj:
            return False

        lines = ["🔬 <b>HOW THE INFORMATION IS BEING SHAPED</b>", f"<i>System 2 | {now}</i>", ""]

        s2a = next((i for i in inj if i.get("analyst") == "S2-A"), None)
        if s2a:
            itype = s2a.get("injection_type", "UNKNOWN")
            conf  = s2a.get("confidence_score", 0) or 0
            e     = ev(s2a.get("evidence"))
            desc  = e.get("description","") or e.get("q1","") or e.get("raw","")
            ph    = " · ".join(str(p) for p in phrases(s2a.get("flagged_phrases"))[:4] if p)
            lines += [f"<b>Injection method:</b> {itype} ({conf:.0%})"]
            if desc: lines.append(str(desc)[:250])
            if ph:   lines.append(f"<b>Trigger language:</b> <code>{ph[:150]}</code>")
            lines.append("")

        s2c = next((i for i in inj if i.get("analyst") == "S2-C"), None)
        if s2c:
            e = ev(s2c.get("evidence"))
            frame = e.get("dominant_emotion","") or e.get("q3","") or e.get("frame","")
            if frame:
                lines += [f"<b>Emotional frame deployed:</b>", str(frame)[:200], ""]

        s2d = next((i for i in inj if i.get("analyst") == "S2-D"), None)
        if s2d:
            e = ev(s2d.get("evidence"))
            nar = e.get("primary_narrative","") or e.get("q1","") or e.get("narrative","")
            if nar:
                lines += ["<b>What the adversary wants you to believe:</b>", str(nar)[:280], ""]

        s2b = next((i for i in inj if i.get("analyst") == "S2-B"), None)
        if s2b:
            itype = s2b.get("injection_type","")
            conf  = s2b.get("confidence_score",0) or 0
            if itype and itype not in ("NO_COORDINATION","NONE","") and conf > 0.3:
                e = ev(s2b.get("evidence"))
                detail = e.get("description","") or e.get("raw","")
                lines += [f"<b>Coordination detected:</b> {itype} ({conf:.0%})"]
                if detail: lines.append(str(detail)[:180])
                lines.append("")

        gap = next((i for i in inj if i.get("analyst") == "S2-GAP"), None)
        if gap:
            e = ev(gap.get("evidence"))
            key = e.get("key_gap_finding","")
            missed = e.get("missed_by_s1",[]) or []
            if key:
                lines += ["<b>What the canary missed (Broken Window):</b>", str(key)[:250]]
            for m in (missed[:2] if isinstance(missed, list) else []):
                if isinstance(m, dict):
                    story = m.get("story","")
                    why   = m.get("why_significant","")
                    if story:
                        lines.append(f"  → {story[:120]}")
                        if why: lines.append(f"    <i>{why[:100]}</i>")
            lines.append("")

        s2e = next((i for i in inj if i.get("analyst") == "S2-E"), None)
        if s2e:
            e = ev(s2e.get("evidence"))
            verdict = e.get("verdict","") or e.get("legitimacy_verdict","") or e.get("q6","")
            if verdict:
                lines += ["<b>Cui Bono — who benefits from today's information environment:</b>",
                          str(verdict)[:220], ""]

        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
            tiercd = sb.table("lens_tiercd_data") \
                .select("tier,source_name,title,url,significance") \
                .gte("created_at", cutoff).order("created_at", desc=True).limit(6).execute().data or []
            high = [t for t in tiercd if t.get("significance") == "HIGH"]
            if high:
                lines.append("<b>Physical ground truth (cannot be narratively distorted):</b>")
                for t in high[:3]:
                    url   = t.get("url","")
                    title = (t.get("title","") or "")[:80]
                    icon  = "🏛" if t.get("tier") == "TIER_D" else "📊"
                    lines.append(f"  {icon} <a href=\"{url}\">{title}</a>" if url else f"  {icon} {title}")
        except Exception:
            pass

        return send_message("\n".join(lines))
    except Exception as e:
        log.error(f"send_s2_intelligence failed: {e}"); return False


def send_s3_intelligence(run_id=None):
    """Message 3: What is actually being built beneath the noise."""
    try:
        from datetime import datetime, timezone, timedelta
        sb = _get_sb()
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")

        s3_all = sb.table("lens_system3_reports") \
            .select("position,summary,first_domino,quality_score,generated_at") \
            .order("generated_at", desc=True).limit(8).execute().data or []
        if not s3_all:
            return False

        s3a = next((r for r in s3_all if r.get("position") == "S3-A"), None)
        s3b = next((r for r in s3_all if r.get("position") == "S3-B"), None)
        s3d = next((r for r in s3_all if r.get("position") == "S3-D"), None)
        s3c = next((r for r in s3_all if r.get("position") == "S3-C"), None)
        if not s3a:
            return False

        lines = ["📚 <b>WHAT IS ACTUALLY BEING BUILT</b>", f"<i>System 3 | {now}</i>", ""]

        if s3a:
            summary = (s3a.get("summary","") or "").strip()
            if summary:
                lines += ["<b>The pattern forming over 7 days:</b>", summary[:400], ""]
            dom = (s3a.get("first_domino","") or "").strip()
            if dom:
                lines += ["⚠️ <b>If current patterns continue, this becomes inevitable:</b>",
                          dom[:300], ""]

        if s3b:
            hist = (s3b.get("summary","") or "").strip()
            if hist:
                lines += ["📖 <b>We have seen this before:</b>", hist[:280], ""]

        if s3d:
            struct = (s3d.get("summary","") or "").strip()
            s3d_dom = (s3d.get("first_domino","") or "").strip()
            if struct:
                lines += ["🏗 <b>What has changed structurally in 30 days:</b>", struct[:280]]
                if s3d_dom: lines.append(f"<b>Structural first domino:</b> {s3d_dom[:150]}")
                lines.append("")

        if s3c:
            drift = (s3c.get("summary","") or "").strip()
            if drift:
                lines += ["📊 <b>Analytical drift check (weekly):</b>", drift[:200], ""]

        try:
            pred = sb.table("lens_predictions") \
                .select("prediction,confidence,verification_date") \
                .order("created_at", desc=True).limit(1).execute().data or []
            if pred:
                p = pred[0]
                pt = (p.get("prediction","") or "").strip()
                if pt:
                    lines += [
                        "🌱 <b>Prediction recorded — System 4 will verify this:</b>",
                        pt[:200],
                        f"<i>Confidence {(p.get('confidence',0) or 0):.0%} · Verify by {p.get('verification_date','?')}</i>"
                    ]
        except Exception:
            pass

        return send_message("\n".join(lines))
    except Exception as e:
        log.error(f"send_s3_intelligence failed: {e}"); return False

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
