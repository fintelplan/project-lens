"""
lens_fetch_tierc.py
Tier C Sources — Data That Cannot Lie

Physical ground truth. No narrative can permanently distort trade flows,
ship movements, or reserve holdings. These are the reality anchors.

Sources:
  IMF COFER    — central bank reserve compositions (quarterly)
  World Bank   — trade indicators + country data
  UN Comtrade  — trade flow data (requires free API key)

Output: lens_tiercd_data (tier=TIER_C)
Cadence: daily (alongside fetch_text.py)
Cost: $0 — all free public APIs

PHI-002: Physical data = the only defense against coordinated narrative pollution.
         When all sources say X but trade flows say Y — Y is real.
"""

import os, json, time, logging, hashlib
from datetime import datetime, date, timezone, timedelta
from typing import Optional
import requests
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [TIER-C] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("TIER_C")

HEADERS = {
    "User-Agent": "ProjectLens/1.0 (academic research; contact: public)",
    "Accept": "application/json",
}
TIMEOUT = 15


def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("Supabase credentials missing")
    return create_client(url, key)


def already_fetched_today(sb: Client, source_id: str) -> bool:
    """Skip if this source was already fetched today."""
    try:
        r = sb.table("lens_tiercd_data") \
            .select("id") \
            .eq("source_id", source_id) \
            .eq("fetch_date", str(date.today())) \
            .limit(1).execute()
        return bool(r.data)
    except Exception:
        return False


def save_records(sb: Client, records: list) -> int:
    """Save list of Tier C records. Returns count saved."""
    if not records:
        return 0
    saved = 0
    for rec in records:
        try:
            sb.table("lens_tiercd_data").insert(rec).execute()
            saved += 1
        except Exception as e:
            log.warning(f"Save failed for {rec.get('source_id','?')}: {e}")
    return saved


# ── IMF COFER — Reserve Compositions ─────────────────────────────────────────
def fetch_imf_cofer(sb: Client) -> int:
    """
    IMF Currency Composition of Official Foreign Exchange Reserves.
    Tracks how central banks allocate reserves — dollar share, yuan share, etc.
    Signal: reserve de-dollarization is structural, not narrative.
    """
    SOURCE_ID = "IMF_COFER"
    if already_fetched_today(sb, SOURCE_ID):
        log.info("IMF COFER: already fetched today — skip")
        return 0

    try:
        # IMF SDMX JSON API — COFER dataset
        url = ("https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/"
               "COFER/Q.W00.RAFA_USD+RAFAEUR_USD+RAFAJPY_USD+RAFAGBP_USD+RAFACNY_USD"
               "?startPeriod=2024-Q1")
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        series = (data.get("CompactData", {})
                     .get("DataSet", {})
                     .get("Series", []))

        if not series:
            log.warning("IMF COFER: no series data returned")
            return 0

        # Parse: extract latest values for each currency
        summaries = []
        currency_map = {
            "RAFA_USD":    "USD",
            "RAFAEUR_USD": "EUR",
            "RAFAJPY_USD": "JPY",
            "RAFAGBP_USD": "GBP",
            "RAFACNY_USD": "CNY",
        }

        if isinstance(series, dict):
            series = [series]

        for s in series:
            indicator = s.get("@INDICATOR", "")
            currency = currency_map.get(indicator, indicator)
            obs = s.get("Obs", [])
            if isinstance(obs, dict):
                obs = [obs]
            if obs:
                latest = obs[-1]
                period  = latest.get("@TIME_PERIOD", "?")
                value   = latest.get("@OBS_VALUE", "?")
                summaries.append(f"{currency}: {value} USD bn ({period})")

        if not summaries:
            return 0

        summary = "IMF COFER Global Reserves: " + " | ".join(summaries)
        record = {
            "tier":        "TIER_C",
            "source_id":   SOURCE_ID,
            "source_name": "IMF Currency Composition of Official Foreign Exchange Reserves",
            "data_type":   "RESERVE_COMPOSITION",
            "title":       f"IMF COFER Reserve Data — {date.today()}",
            "summary":     summary,
            "countries":   ["GLOBAL"],
            "significance": "HIGH",
            "url":         "https://data.imf.org/regular.aspx?key=41175",
            "raw_snippet": json.dumps(summaries)[:1000],
            "fetch_date":  str(date.today()),
        }
        saved = save_records(sb, [record])
        log.info(f"IMF COFER: saved {saved} record")
        return saved

    except Exception as e:
        log.error(f"IMF COFER fetch failed: {e}")
        return 0


# ── World Bank — Trade & Economic Indicators ──────────────────────────────────
def fetch_worldbank_indicators(sb: Client) -> int:
    """
    World Bank key trade and economic indicators.
    Signal: GDP shifts, trade balance changes, FDI flows — structural not narrative.
    """
    SOURCE_ID = "WORLD_BANK_INDICATORS"
    if already_fetched_today(sb, SOURCE_ID):
        log.info("World Bank indicators: already fetched today — skip")
        return 0

    # Key indicators: trade as % GDP, current account, FDI
    indicators = [
        ("NE.TRD.GNFS.ZS", "Trade (% of GDP)"),
        ("BN.CAB.XOKA.CD",  "Current account balance (USD)"),
        ("BX.KLT.DINV.CD.WD", "FDI net inflows (USD)"),
    ]

    # Key countries to track
    countries = ["CN", "US", "RU", "DE", "IN", "SA", "TR"]
    records = []

    for indicator_code, indicator_name in indicators:
        try:
            url = (f"https://api.worldbank.org/v2/country/"
                   f"{';'.join(countries)}/indicator/{indicator_code}"
                   f"?format=json&mrv=2&per_page=20")
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()

            if not isinstance(raw, list) or len(raw) < 2:
                continue

            data_points = raw[1] or []
            summaries = []
            for dp in data_points[:7]:  # one per country
                country = dp.get("country", {}).get("value", "?")
                year    = dp.get("date", "?")
                value   = dp.get("value")
                if value is not None:
                    summaries.append(f"{country} ({year}): {value:.2f}")

            if summaries:
                records.append({
                    "tier":        "TIER_C",
                    "source_id":   SOURCE_ID,
                    "source_name": "World Bank Open Data",
                    "data_type":   "ECONOMIC_INDICATOR",
                    "title":       f"{indicator_name} — {date.today()}",
                    "summary":     f"{indicator_name}: " + " | ".join(summaries),
                    "countries":   countries,
                    "significance": "MEDIUM",
                    "url":         f"https://data.worldbank.org/indicator/{indicator_code}",
                    "raw_snippet": json.dumps(summaries)[:1000],
                    "fetch_date":  str(date.today()),
                })
            time.sleep(1)

        except Exception as e:
            log.warning(f"World Bank {indicator_code} failed: {e}")
            continue

    saved = save_records(sb, records)
    log.info(f"World Bank indicators: saved {saved} records")
    return saved


# ── UN Comtrade — Trade Flows ─────────────────────────────────────────────────
def fetch_un_comtrade(sb: Client) -> int:
    """
    UN Comtrade trade flow data.
    Requires free API key: comtradeplus.un.org (register once, free tier).
    Signal: actual trade volumes — cannot be narratively distorted.
    """
    SOURCE_ID = "UN_COMTRADE"
    api_key = os.environ.get("COMTRADE_API_KEY", "")

    if not api_key:
        log.info("UN Comtrade: COMTRADE_API_KEY not set — skipping")
        log.info("  Register free at: https://comtradeplus.un.org")
        log.info("  Add COMTRADE_API_KEY to .env and GitHub Secrets")
        return 0

    if already_fetched_today(sb, SOURCE_ID):
        log.info("UN Comtrade: already fetched today — skip")
        return 0

    try:
        # Preview endpoint — total trade for major reporters
        url = ("https://comtradeapi.un.org/public/v1/preview/C/A/HS"
               "?reporterCode=156,840,643,276,356"  # CN, US, RU, DE, IN
               "&period=2023&cmdCode=TOTAL&flowCode=X,M")
        headers = {**HEADERS, "Ocp-Apim-Subscription-Key": api_key}
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        records_raw = data.get("data", [])
        if not records_raw:
            log.warning("UN Comtrade: no data returned")
            return 0

        summaries = []
        for item in records_raw[:10]:
            reporter = item.get("reporterDesc", "?")
            flow     = item.get("flowDesc", "?")
            value    = item.get("primaryValue", 0)
            year     = item.get("period", "?")
            summaries.append(f"{reporter} {flow} ({year}): ${value/1e9:.1f}B")

        record = {
            "tier":        "TIER_C",
            "source_id":   SOURCE_ID,
            "source_name": "UN Comtrade — Global Trade Flows",
            "data_type":   "TRADE_FLOW",
            "title":       f"UN Comtrade Trade Flows — {date.today()}",
            "summary":     "Global trade flows: " + " | ".join(summaries),
            "countries":   ["CN", "US", "RU", "DE", "IN"],
            "significance": "HIGH",
            "url":         "https://comtradeplus.un.org",
            "raw_snippet": json.dumps(summaries)[:1000],
            "fetch_date":  str(date.today()),
        }
        saved = save_records(sb, [record])
        log.info(f"UN Comtrade: saved {saved} record")
        return saved

    except Exception as e:
        log.error(f"UN Comtrade fetch failed: {e}")
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def run_fetch_tierc() -> dict:
    log.info("=== TIER C FETCH START ===")
    try:
        sb = get_supabase()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    total = 0
    total += fetch_imf_cofer(sb)
    total += fetch_worldbank_indicators(sb)
    total += fetch_un_comtrade(sb)

    log.info(f"=== TIER C FETCH COMPLETE | {total} records saved ===")
    return {"status": "OK", "records_saved": total}


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_fetch_tierc()
    print(json.dumps(result, indent=2))
