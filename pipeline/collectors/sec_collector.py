# pipeline/collectors/sec_collector.py
#
# SEC EDGAR Collector — fetches 8-K filings and Form 4 insider trades
# for all US watchlist tickers via SEC's official free API.
#
# EDGAR API docs: https://efts.sec.gov/LATEST/search-index
# No API key required. SEC requests a User-Agent identifying your app.
# Rate limit: 10 requests/second — we stay well under this.

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import requests

from pipeline.models import SourceTier, Market, RawItem

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent / "config"
CIK_MAP_PATH = CONFIG_DIR / "sec_cik_map.json"

# SEC requires a descriptive User-Agent for automated access
# Format: "AppName/Version Contact-Email"
HEADERS = {
    "User-Agent":      "FINTEL-Pipeline/1.0 personal-investment-research@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Host":            "efts.sec.gov",
}

HEADERS_DATA = {
    "User-Agent":      "FINTEL-Pipeline/1.0 personal-investment-research@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Host":            "data.sec.gov",
}

# 8-K item numbers that are high-signal for trading
# Others exist but generate noise without meaningful price impact
HIGH_SIGNAL_8K_ITEMS = {
    "1.01", "2.01", "2.02", "2.06",
    "4.01", "5.02", "7.01", "8.01"
}

ITEM_DECRIPTIONS = {
    "1.01": "Material Definitive Agreement",
    "2.01": "Completion of Acquisition or Disposition",
    "2.02": "Results of Operations / Earnings",
    "2.06": "Material Impairment",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "5.02": "Departure/Election of Directors or Principal Officers",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Material Events"
}

def _load_cik_map() -> dict:
    """Returns {ticker: padded_cik} e.g. {"NVDA": "0001045810"}"""
    with open(CIK_MAP_PATH, "r") as f:
        data = json.load(f)

    return {k: v for k, v in data.items() if not k.startswith("_")}


def _parse_sec_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    SEC dates are YYYY-MM-DD format (e.g. "2026-07-02").
    Filings go live approximately 9am ET.
    ET = UTC-4 (EDT) or UTC-5 (EST). Use 14:00 UTC as approximation.
    """
    if not date_str:
        return None
    
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        # SEC filings go live ~9am EST. EST = UTC-5, so approx 14:00 UTC.
        return dt.replace(hour=14, tzinfo=timezone.utc)
    except Exception:
        return None


def _build_filing_url(cik: str, adsh: str) -> str:
    """
    Build direct URL to the filing index on SEC website.

    EDGAR URL format: https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{adsh_no_dashes}/{adsh}-index.htm

    cik_numeric = CIK without leading zeros e.g. "0001045810" → "1045810"\n
    adsh_no_dashes = adsh with hyphens removed e.g. "0001045810-26-000060" → "000104581026000060"
    """
    if not adsh:
        return "https://www.sec.gov"
    
    cik_numeric = str(int(cik))
    adsh_no_dashes = adsh.replace("-", "")

    return f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{adsh_no_dashes}/{adsh}-index.htm"


def _fetch_8k_filings(ticker: str, cik: str, days_back: int = 3) -> List[RawItem]:
    """
    Fetch recent 8-K filings for one ticker via EDGAR full-text search.

    Uses text search for ticker mention, then verifies CIK to eliminate
    false positives (other companies mentioning our ticker in their filings).

    days_back=3 covers weekends: Monday run catches Friday filings.
    """
    items: List[RawItem] = []

    since_date = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q":         f'{ticker}',   # quoted = exact match
        "forms":     "8-K",
        "dateRange": "custom",
        "startdt":   since_date,
        "enddt":     today,
    }

    try:
        response = requests.get(url=url, params=params, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        hits = data.get("hits", {}).get("hits", [])
        for hit in hits:
            source = hit.get("_source", {})

            # ── CIK verification — eliminates false positives ──────
            result_ciks = source.get("ciks", [])
            if not result_ciks or result_ciks[0] != cik:
                logger.debug(f"SEC 8-K [{ticker}]: skipping — not primary filer. ciks={result_ciks}")
                continue

            # ── Form type verification ────────────────
            root_forms = source.get("root_forms", [])
            if '8-K' not in root_forms:
                continue

            adsh = source.get("adsh", "")
            file_date = source.get("file_date", "")
            period = source.get("period_ending", "")
            filing_items = source.get("items", [])
            display_names = source.get("display_names", [])
            entity_name = display_names[0] if display_names else ticker

            # ── Item signal filtering ──────────────
            high_signal_items = [item for item in filing_items if item in HIGH_SIGNAL_8K_ITEMS]

            # Skip 8-Ks with no high-signal items
            # (but only if items list is non-empty — some 8-Ks don't tag items)
            if filing_items and not high_signal_items:
                logger.debug(f"SEC 8-K [{ticker}]: skipping low-signal items {filing_items}")
                continue

            item_desc = ", ".join(
                ITEM_DECRIPTIONS.get(item, f"Item {item}") for item in high_signal_items
            ) if high_signal_items else "Material Event"

            headline = (
                f"[{ticker}] 8-K: {item_desc} | "
                f"Period: {period} | Filed: {file_date}"
            )

            snippet = (
                f"SEC 8-K | {entity_name} | "
                f"Filed: {file_date} | "
                f"Items: {', '.join(high_signal_items) if high_signal_items else 'see filing'} | "
                f"{item_desc}"
            )

            items.append(RawItem(
                source_id="sec_edgar_8k0",
                source_name="SEC EDGAR — 8-K Material Events",
                source_tier=SourceTier.OFFICIAL,
                headline=headline,
                snippet=snippet,
                url=_build_filing_url(result_ciks[0], adsh),
                published_at=_parse_sec_date(file_date),
                market=Market.US,
                raw_payload=source
            ))

    except Exception as e:
        logger.warning(f"SEC 8-K [{ticker}]: {e}", exc_info=True)

    return items


def _fetch_form4_filings(ticker: str, cik: str, days_back: int = 3) -> List[RawItem]:
    """
    Fetch Form 4 insider trade filings for one ticker.

    Same CIK verification applied — text search can return Form 4
    filings from other companies that mention this ticker.

    Form 4 transaction codes:
      P = Purchase (insider buying — bullish signal)
      S = Sale     (insider selling — weak signal, many reasons to sell)
      A = Award    (compensation grant — not a market signal)
    """
    items: List[RawItem] = []

    since_date = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q":         f'{ticker}',   # quoted = exact match
        "forms":     "4",
        "dateRange": "custom",
        "startdt":   since_date,
        "enddt":     today,
    }

    try:
        response = requests.get(url=url, params=params, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        hits = data.get("hits", {}).get("hits", [])
        for hit in hits:
            source = hit.get("_source")

            result_ciks = source.get("ciks", [])
            if cik not in result_ciks:
                continue

            root_forms = source.get("root_forms", [])
            if "4" not in root_forms and "4" not in source.get("form", ""):
                continue

            adsh          = source.get("adsh", "")
            file_date     = source.get("file_date", "")
            display_names = source.get("display_names", [])

            insider_name  = display_names[0] if display_names else "Unknown Insider"
            company_name  = display_names[1] if len(display_names) > 1 else ticker

            headline = (
                f"[{ticker}] Form 4 Insider Trade | "
                f"Insider: {insider_name} | "
                f"Company: {company_name} | "
                f"Filed: {file_date}"
            )
            snippet = (
                f"SEC Form 4 | {ticker} | "
                f"Insider: {insider_name} | "
                f"Filed: {file_date} | "
                f"P=Purchase(bullish), S=Sale(neutral-bearish), A=Award(non-signal)"
            )

            filing_cik = result_ciks[0] if result_ciks else cik
            items.append(RawItem(
                source_id    = "sec_edgar_form4",
                source_name  = "SEC EDGAR — Form 4 Insider Trades",
                source_tier  = SourceTier.OFFICIAL,
                headline     = headline,
                snippet      = snippet,
                url          = _build_filing_url(filing_cik, adsh),
                published_at = _parse_sec_date(file_date),
                market       = Market.US,
                raw_payload  = source,
            ))

    except Exception as e:
        logger.warning(f"SEC Form 4 [{ticker}]: {e}", exc_info=True)

    return items


def collect_sec(session_type: str) -> List[RawItem]:
    """
    Public entry point — fetches 8-K + Form 4 for all US watchlist tickers.
    Only runs during US market sessions.
    """
    if session_type not in {"US_OPEN", "US_CLOSE"}:
        return []
    
    cik_map = _load_cik_map()
    all_items: List[RawItem] = []

    for ticker, cik in cik_map.items():
        logger.info(f"SEC EDGAR: fetching {ticker} (CIK: {cik})")
        all_items.extend(_fetch_8k_filings(ticker=ticker, cik=cik, days_back=10))
        all_items.extend(_fetch_form4_filings(ticker=ticker, cik=cik, days_back=10))

    logger.info(f"SEC EDGAR: {len(all_items)} total items across {len(cik_map)} tickers")
    return all_items
