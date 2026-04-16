"""
lens_fetch_tierd.py
Tier D Sources — Primary Documents

The gap between primary document and how it is reported IS the intelligence.
When OFAC designates a new entity and MSM misreports it — that gap IS the signal.

Sources:
  Federal Register  — US policy documents (free JSON API, no key)
  OFAC SDN          — US sanctions designations (free XML, no key)
  World Bank        — new project approvals (free JSON, no key)

Output: lens_tiercd_data (tier=TIER_D)
Cadence: 2x/day (alongside main cron)
Cost: $0 — all free public APIs, no key needed

PHI-002: Primary documents cannot be narratively reframed at source.
         The gap between what was signed and what was reported = the intelligence.
"""

import os, json, time, logging, xml.etree.ElementTree as ET
from datetime import datetime, date, timezone, timedelta
from typing import Optional
import requests
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [TIER-D] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("TIER_D")

HEADERS = {
    "User-Agent": "ProjectLens/1.0 (academic research)",
    "Accept": "application/json,application/xml,text/xml,*/*",
}
TIMEOUT = 20
LOOKBACK_DAYS = 2  # fetch last 2 days of primary docs


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("Supabase credentials missing")
    return create_client(url, key)


def already_saved(sb: Client, source_id: str, title_hash: str) -> bool:
    """Avoid duplicate records."""
    try:
        r = sb.table("lens_tiercd_data") \
            .select("id") \
            .eq("source_id", source_id) \
            .eq("raw_snippet", title_hash) \
            .limit(1).execute()
        return bool(r.data)
    except Exception:
        return False


def save_records(sb: Client, records: list) -> int:
    saved = 0
    for rec in records:
        try:
            sb.table("lens_tiercd_data").insert(rec).execute()
            saved += 1
        except Exception as e:
            log.warning(f"Save failed: {e}")
    return saved


# ── Federal Register ──────────────────────────────────────────────────────────
def fetch_federal_register(sb: Client) -> int:
    """
    US Federal Register — executive orders, rules, notices.
    Signal: the gap between what policy changes and how media frames it.
    Free JSON API, no key needed.
    """
    SOURCE_ID = "FED_REGISTER"
    try:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        url = (f"https://www.federalregister.gov/api/v1/documents.json"
               f"?per_page=10&order=newest"
               f"&conditions[publication_date][gte]={since}"
               f"&fields[]=title&fields[]=abstract&fields[]=publication_date"
               f"&fields[]=agency_names&fields[]=document_number&fields[]=html_url"
               f"&conditions[type][]=RULE&conditions[type][]=PRORULE"
               f"&conditions[type][]=NOTICE")

        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        docs = data.get("results", [])

        records = []
        for doc in docs[:8]:
            title    = doc.get("title", "")[:300]
            abstract = (doc.get("abstract", "") or "")[:500]
            pub_date = doc.get("publication_date", "")
            agencies = ", ".join(doc.get("agency_names", [])[:3])
            doc_num  = doc.get("document_number", "")
            doc_url  = doc.get("html_url", "")

            # significance: EO > final rule > proposed rule > notice
            significance = "MEDIUM"
            tl = title.lower()
            if any(kw in tl for kw in ["executive order", "national security", "sanctions", "emergency"]):
                significance = "HIGH"

            title_hash = doc_num or title[:50]
            if already_saved(sb, SOURCE_ID, title_hash):
                continue

            records.append({
                "tier":        "TIER_D",
                "source_id":   SOURCE_ID,
                "source_name": "US Federal Register",
                "data_type":   "POLICY_DOC",
                "title":       title,
                "summary":     f"[{agencies}] {pub_date}: {abstract}",
                "countries":   ["US"],
                "significance": significance,
                "url":         doc_url,
                "raw_snippet": title_hash,
                "fetch_date":  str(date.today()),
            })

        saved = save_records(sb, records)
        log.info(f"Federal Register: {saved}/{len(docs)} new records")
        return saved

    except Exception as e:
        log.error(f"Federal Register fetch failed: {e}")
        return 0


# ── OFAC Sanctions ────────────────────────────────────────────────────────────
def fetch_ofac_designations(sb: Client) -> int:
    """
    OFAC SDN (Specially Designated Nationals) consolidated list.
    Signal: who just got sanctioned. Gap analysis: did media report it?
    Free XML, no key needed.
    """
    SOURCE_ID = "OFAC_SDN"
    try:
        # OFAC recent actions RSS/updates page
        url = "https://www.treasury.gov/ofac/downloads/sdn.xml"

        resp = requests.get(url, headers={**HEADERS, "Accept": "text/xml,application/xml,*/*"},
                           timeout=30)  # this file is large
        resp.raise_for_status()

        # Parse XML — get count and recent entries
        root = ET.fromstring(resp.content[:500000])  # limit parse to first 500KB

        # Count total entries
        ns = ""
        entries = root.findall(".//sdnEntry") or root.findall("sdnEntry")
        if not entries:
            # Try with namespace
            for child in root:
                tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                if 'sdn' in tag.lower() or 'entry' in tag.lower():
                    entries = list(root.iter(child.tag))
                    break

        total_count = len(entries)

        # Get recently added (last 10 for summary)
        recent_names = []
        for entry in entries[-10:]:
            # Try to find name
            for child in entry:
                tag = child.tag.split('}')[-1].lower()
                if tag in ('lastname', 'sdnname', 'name'):
                    recent_names.append(child.text or "")
                    break

        summary = (f"OFAC SDN List: {total_count} designated entities. "
                  f"Recent entries include: {', '.join(recent_names[:5])}" if recent_names
                  else f"OFAC SDN List: {total_count} designated entities total.")

        title_hash = f"OFAC-{date.today()}-{total_count}"
        if already_saved(sb, SOURCE_ID, title_hash):
            log.info("OFAC: already saved today — skip")
            return 0

        record = {
            "tier":        "TIER_D",
            "source_id":   SOURCE_ID,
            "source_name": "OFAC Specially Designated Nationals List",
            "data_type":   "SANCTIONS",
            "title":       f"OFAC SDN List Status — {date.today()}",
            "summary":     summary,
            "countries":   ["US"],
            "significance": "HIGH",
            "url":         "https://ofac.treasury.gov/sanctions-list-service",
            "raw_snippet": title_hash,
            "fetch_date":  str(date.today()),
        }
        saved = save_records(sb, [record])
        log.info(f"OFAC: {saved} record saved (total SDN entries: {total_count})")
        return saved

    except Exception as e:
        log.error(f"OFAC fetch failed: {e}")
        return 0


# ── World Bank Projects ───────────────────────────────────────────────────────
def fetch_worldbank_projects(sb: Client) -> int:
    """
    World Bank new project approvals.
    Signal: where development finance is flowing — structural, not narrative.
    Free JSON API, no key needed.
    """
    SOURCE_ID = "WORLD_BANK_PROJECTS"
    try:
        since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        url = (f"https://search.worldbank.org/api/v2/projects"
               f"?format=json&rows=10&os=0"
               f"&fl=id,project_name,countryname,totalamt,impagency,boardapprovaldate,sector1"
               f"&strdate={since}")

        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        projects = data.get("projects", {})
        if isinstance(projects, dict):
            project_list = list(projects.values())
        else:
            project_list = projects or []

        records = []
        for proj in project_list[:8]:
            name     = proj.get("project_name", "")[:200]
            country  = proj.get("countryname", "?")
            amount   = proj.get("totalamt", 0)
            agency   = proj.get("impagency", "")
            approved = proj.get("boardapprovaldate", "")[:10]
            sector   = proj.get("sector1", {})
            if isinstance(sector, dict):
                sector = sector.get("Name", "")

            proj_id = proj.get("id", name[:30])
            if already_saved(sb, SOURCE_ID, proj_id):
                continue

            amount_str = f"${amount/1e6:.1f}M" if amount else "undisclosed"
            summary = (f"World Bank [{country}] {amount_str} approved {approved}: "
                      f"{name} | Sector: {sector} | Agency: {agency}")

            # High significance: large amount or strategic countries
            significance = "HIGH" if (amount and amount > 500000000) or \
                           any(c in country for c in ["China", "Russia", "Iran", "Myanmar"]) \
                           else "MEDIUM"

            records.append({
                "tier":        "TIER_D",
                "source_id":   SOURCE_ID,
                "source_name": "World Bank Project Database",
                "data_type":   "DEVELOPMENT_FINANCE",
                "title":       f"WB {country}: {name[:100]}",
                "summary":     summary,
                "countries":   [country],
                "significance": significance,
                "url":         f"https://projects.worldbank.org/en/projects-operations/project-detail/{proj_id}",
                "raw_snippet": proj_id,
                "fetch_date":  str(date.today()),
            })

        saved = save_records(sb, records)
        log.info(f"World Bank projects: {saved} new records")
        return saved

    except Exception as e:
        log.error(f"World Bank projects fetch failed: {e}")
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def run_fetch_tierd() -> dict:
    log.info("=== TIER D FETCH START ===")
    try:
        sb = get_supabase()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    total = 0
    total += fetch_federal_register(sb)
    time.sleep(2)
    total += fetch_ofac_designations(sb)
    time.sleep(2)
    total += fetch_worldbank_projects(sb)

    log.info(f"=== TIER D FETCH COMPLETE | {total} records saved ===")
    return {"status": "OK", "records_saved": total}


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_fetch_tierd()
    print(json.dumps(result, indent=2))
