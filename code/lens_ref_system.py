"""
lens_ref_system.py v2
Project Lens — Article Reference System

FREE TIER (4x/day):  20260417_ProjectLens_Refs_40f1.xlsx
SONNET  (2x/day):   20260417_ProjectLens_Refs_20f1.xlsx

Each Excel has 2 sheets:
  Sheet 1 — All Collected  (every article from this cron window)
  Sheet 2 — Selected       (articles S2 actually used for findings)

Usage:
  python code/lens_ref_system.py --mode free    (called from free tier cron)
  python code/lens_ref_system.py --mode sonnet  (called from sonnet workflow)
"""

import os, json, logging, sys, tempfile
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [REF-SYS] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("REF_SYS")


def get_supabase():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


# ── Slot detection ────────────────────────────────────────────────────────────

def get_slot(mode: str) -> str:
    """Detect which run slot we are in based on UTC hour."""
    hour = datetime.now(timezone.utc).hour

    if mode == "free":
        # Free tier: 04:28 / 13:28 / 17:28 / 21:28 UTC
        if   hour < 6:   return "f1"
        elif hour < 15:  return "f2"
        elif hour < 19:  return "f3"
        else:            return "f4"
    else:
        # Sonnet: 03:28 / 15:28 UTC
        if hour < 9:  return "f1"
        else:         return "f2"


# ── Assign REF numbers ────────────────────────────────────────────────────────

def assign_refs(sb, hours_back: int = 6) -> list:
    """Assign REF-YYYYMMDD-NNNN to new articles. Returns list of ref rows."""
    today     = datetime.now(timezone.utc).strftime("%Y%m%d")
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cutoff    = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

    # Fetch new articles
    try:
        r = sb.table("lens_raw_articles") \
            .select("id,title,url,source_id,domain,collected_at") \
            .gte("collected_at", cutoff) \
            .order("collected_at", desc=False).execute()
        articles = r.data or []
    except Exception as e:
        log.error(f"Fetch articles failed: {e}")
        return []

    if not articles:
        log.info("No articles in window")
        return []

    # Get already-referenced article IDs
    try:
        ex = sb.table("lens_article_refs") \
            .select("raw_article_id") \
            .eq("collected_date", today_iso).execute()
        existing_ids = {r["raw_article_id"] for r in (ex.data or [])}
    except Exception:
        existing_ids = set()

    new_articles = [a for a in articles if a.get("id") not in existing_ids]

    if not new_articles:
        log.info("All articles already have REF IDs")
        # Return existing refs for this window
        try:
            r2 = sb.table("lens_article_refs") \
                .select("ref_id,collected_date,domain,source_name,title,url,raw_article_id") \
                .eq("collected_date", today_iso) \
                .order("ref_id", desc=False).execute()
            return r2.data or []
        except Exception:
            return []

    # Get max sequence for today
    try:
        mx = sb.table("lens_article_refs") \
            .select("ref_id") \
            .like("ref_id", f"REF-{today}-%") \
            .order("ref_id", desc=True).limit(1).execute()
        last_seq = int(mx.data[0]["ref_id"].split("-")[-1]) if mx.data else 0
    except Exception:
        last_seq = 0

    # Source name map
    source_map = {}
    try:
        src = sb.table("lens_sources").select("id,name").execute()
        source_map = {s["id"]: s["name"] for s in (src.data or [])}
    except Exception:
        pass

    # Build rows
    rows = []
    for i, art in enumerate(new_articles):
        seq = last_seq + i + 1
        rows.append({
            "ref_id":         f"REF-{today}-{seq:04d}",
            "collected_date": today_iso,
            "domain":         (art.get("domain") or "GENERAL").upper(),
            "source_name":    source_map.get(art.get("source_id"), "Unknown"),
            "title":          (art.get("title") or "")[:300],
            "url":            art.get("url", ""),
            "raw_article_id": art.get("id"),
        })

    # Insert in batches
    inserted = 0
    for i in range(0, len(rows), 50):
        batch = rows[i:i+50]
        try:
            sb.table("lens_article_refs").upsert(
                batch, on_conflict="raw_article_id").execute()
            inserted += len(batch)
        except Exception as e:
            log.error(f"Insert batch failed: {e}")

    log.info(f"Referenced {inserted} new articles (seq {last_seq+1} → {last_seq+inserted})")

    # Return all refs for today
    try:
        r3 = sb.table("lens_article_refs") \
            .select("ref_id,collected_date,domain,source_name,title,url,raw_article_id") \
            .eq("collected_date", today_iso) \
            .order("ref_id", desc=False).execute()
        return r3.data or []
    except Exception:
        return rows


# ── Get selected articles (S2 used these) ────────────────────────────────────

def get_selected_articles(sb, all_refs: list, hours_back: int = 6) -> list:
    """
    Find articles S2 actually selected for findings.
    Strategy:
      1. Get latest injection_reports (last 6h)
      2. Extract flagged phrases + adversarial source articles
      3. Match against all_refs by URL or source name
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

    # Get S2 injection findings
    try:
        r = sb.table("injection_reports") \
            .select("analyst,injection_type,confidence_score,flagged_phrases,evidence") \
            .gte("created_at", cutoff) \
            .order("confidence_score", desc=True).execute()
        injections = r.data or []
    except Exception as e:
        log.warning(f"Injection reports fetch failed: {e}")
        injections = []

    # Adversarial source names (S2-D always uses these)
    adversarial_sources = {
        "TASS", "RT", "Kremlin", "Global Times", "Xinhua",
        "PressTV", "Press TV", "Iran Press", "Tasnim News",
        "Sputnik", "RIA Novosti"
    }

    # Build selection set
    selected = []
    seen_refs = set()

    # Collect flagged phrases from S2-A/C/E
    flagged_phrases = set()
    for inj in injections:
        ph = inj.get("flagged_phrases")
        if ph:
            if isinstance(ph, list):
                flagged_phrases.update(str(p).lower() for p in ph if p)
            elif isinstance(ph, str):
                try:
                    parsed = json.loads(ph)
                    if isinstance(parsed, list):
                        flagged_phrases.update(str(p).lower() for p in parsed if p)
                except Exception:
                    flagged_phrases.add(ph.lower())

    # Match refs against selection criteria
    analyst_map = {inj.get("analyst"): inj for inj in injections}

    for ref in all_refs:
        ref_id      = ref.get("ref_id","")
        source      = ref.get("source_name","")
        title       = (ref.get("title","") or "").lower()
        reason      = None
        s2_position = None
        finding     = None

        if ref_id in seen_refs:
            continue

        # Rule 1: adversarial source → S2-D selected it
        if source in adversarial_sources:
            reason      = "Adversarial source"
            s2_position = "S2-D"
            inj         = analyst_map.get("S2-D")
            finding     = inj.get("injection_type","") if inj else "Adversary narrative"

        # Rule 2: title contains flagged phrase → S2-A/C/E selected it
        elif flagged_phrases:
            for phrase in flagged_phrases:
                if phrase and len(phrase) > 3 and phrase in title:
                    reason      = f"Flagged phrase: {phrase}"
                    s2_position = "S2-A"
                    inj         = analyst_map.get("S2-A")
                    finding     = inj.get("injection_type","") if inj else "Phrase sync"
                    break

        if reason:
            seen_refs.add(ref_id)
            selected.append({
                **ref,
                "s2_position": s2_position,
                "finding":     finding,
                "reason":      reason,
            })

    log.info(f"Selected: {len(selected)} articles used by S2")
    return selected


# ── Build Excel ───────────────────────────────────────────────────────────────

def build_excel(all_refs: list, selected: list,
                date_str: str, filename: str, mode: str, slot: str) -> str:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment

    wb = openpyxl.Workbook()

    domain_colors = {
        "MILITARY":  "FFE0E0", "FINANCE":   "E0F0FF",
        "NARRATIVE": "FFF0E0", "POWER":     "F0E0FF",
        "TECH":      "E0FFE0", "NETWORK":   "E0FFFF",
        "RESOURCE":  "FFFDE0", "GENERAL":   "F5F5F5",
    }

    header_fill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=10)
    selected_fill = PatternFill(start_color="1a1a2e", end_color="1a3a2e", fill_type="solid")

    def make_sheet(ws, rows, columns, col_widths, title_row=None):
        # Header
        for col, (header, width) in enumerate(zip(columns, col_widths), 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.column_dimensions[ws.cell(1, col).column_letter].width = width
        ws.row_dimensions[1].height = 18
        ws.freeze_panes = "A2"

        # Data
        for row_i, row_data in enumerate(rows, 2):
            domain = (row_data.get("domain") or "GENERAL").upper()
            fill_color = domain_colors.get(domain, "FFFFFF")
            row_fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")

            for col_i, key in enumerate(columns, 1):
                val = row_data.get(key.lower().replace(" ","_")
                                   .replace("-","_"), "")
                # Map display column to data key
                key_map = {
                    "REF ID":      "ref_id",
                    "Date":        "collected_date",
                    "Domain":      "domain",
                    "Source":      "source_name",
                    "Title":       "title",
                    "URL":         "url",
                    "S2 Position": "s2_position",
                    "Finding":     "finding",
                    "Reason":      "reason",
                }
                val = row_data.get(key_map.get(key, key), "")
                cell = ws.cell(row=row_i, column=col_i, value=val)
                cell.fill = row_fill
                cell.alignment = Alignment(vertical="center")
                if key == "URL" and val:
                    cell.hyperlink = val
                    cell.font = Font(color="0563C1", underline="single")

    # Sheet 1 — All Collected
    ws1 = wb.active
    ws1.title = "All Collected"
    make_sheet(ws1, all_refs,
        ["REF ID", "Domain", "Date", "Source", "Title", "URL"],
        [22, 13, 13, 20, 55, 55])

    # Sheet 2 — Selected
    ws2 = wb.create_sheet("Selected (S2 Used)")
    make_sheet(ws2, selected,
        ["REF ID", "Domain", "Date", "Source", "Title", "URL", "S2 Position", "Finding"],
        [22, 13, 13, 20, 45, 45, 14, 25])

    # Sheet 3 — Summary
    ws3 = wb.create_sheet("Summary")
    ws3["A1"] = f"Project Lens — Article References"
    ws3["A2"] = f"Date: {date_str}"
    ws3["A3"] = f"Mode: {'Free Tier 4x/day' if mode=='free' else 'Sonnet 4.6 2x/day'}"
    ws3["A4"] = f"Slot: {slot}"
    ws3["A5"] = f"Total collected: {len(all_refs)}"
    ws3["A6"] = f"Total selected: {len(selected)}"

    domain_counts = {}
    for ref in all_refs:
        d = ref.get("domain","GENERAL")
        domain_counts[d] = domain_counts.get(d,0) + 1

    ws3["A8"] = "By Domain:"
    for i, (d, c) in enumerate(sorted(domain_counts.items(), key=lambda x:-x[1]), 9):
        ws3[f"A{i}"] = d; ws3[f"B{i}"] = c

    source_counts = {}
    for ref in all_refs:
        s = ref.get("source_name","Unknown")
        source_counts[s] = source_counts.get(s,0) + 1

    ws3["D8"] = "Top Sources:"
    for i, (s, c) in enumerate(sorted(source_counts.items(), key=lambda x:-x[1])[:15], 9):
        ws3[f"D{i}"] = s; ws3[f"E{i}"] = c

    out = os.path.join(tempfile.gettempdir(), filename)
    wb.save(out)
    log.info(f"Excel saved: {out} ({len(all_refs)} collected, {len(selected)} selected)")
    return out


# ── Telegram send ─────────────────────────────────────────────────────────────

def send_telegram(path: str, filename: str, date_str: str,
                  total: int, selected: int, mode: str, slot: str) -> bool:
    import requests
    token   = os.environ.get("TELEGRAM_BOT_TOKEN","")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID","")
    if not token or not chat_id: return False
    try:
        tier = "Free Tier 4x" if mode == "free" else "Sonnet 4.6 2x"
        caption = (
            f"📊 {filename}\n"
            f"{date_str} | {tier} | {slot}\n\n"
            f"All collected: {total}\n"
            f"Selected by S2: {selected}\n\n"
            f"Sheet 1: All Collected\n"
            f"Sheet 2: Selected (S2 Used)\n"
            f"Sheet 3: Summary"
        )
        url = f"https://api.telegram.org/bot{token}/sendDocument"
        with open(path,"rb") as f:
            resp = requests.post(url,
                data={"chat_id": chat_id, "caption": caption},
                files={"document": (filename, f,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                timeout=60)
        ok = resp.status_code == 200
        log.info(f"Telegram: {'OK' if ok else 'FAILED'}")
        return ok
    except Exception as e:
        log.error(f"Telegram failed: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run(mode: str = "free") -> dict:
    import time
    start    = time.time()
    slot     = get_slot(mode)
    today    = datetime.now(timezone.utc).strftime("%Y%m%d")
    thai     = (datetime.now(timezone.utc) + timedelta(hours=7))
    date_str = thai.strftime("%B %d, %Y %I:%M %p")

    # File naming
    prefix   = "40" if mode == "free" else "20"
    filename = f"{today}_ProjectLens_Refs_{prefix}{slot}.xlsx"

    log.info(f"=== REF SYSTEM START | mode={mode} slot={slot} file={filename} ===")

    try:
        sb = get_supabase()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    all_refs = assign_refs(sb, hours_back=6)
    if not all_refs:
        log.warning("No articles found — skipping export")
        return {"status": "NO_DATA"}

    selected  = get_selected_articles(sb, all_refs, hours_back=6)
    xlsx_path = build_excel(all_refs, selected, date_str, filename, mode, slot)
    sent      = send_telegram(xlsx_path, filename, date_str,
                               len(all_refs), len(selected), mode, slot)

    elapsed = round(time.time() - start, 1)
    log.info(f"=== DONE | {len(all_refs)} collected | {len(selected)} selected | {elapsed}s ===")
    return {
        "status":   "OK",
        "file":     filename,
        "collected": len(all_refs),
        "selected":  len(selected),
        "sent":      sent,
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    mode = "sonnet" if "--mode" in sys.argv and sys.argv[sys.argv.index("--mode")+1] == "sonnet" else "free"
    result = run(mode=mode)
    print(json.dumps(result, indent=2))
