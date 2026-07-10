# pipeline/collectors/bse_collector.py
#
# BSE Collector — fetches corporate announcements from Bombay Stock Exchange.
#
# APPROACH: per-scrip fetching, not category-wide pagination.
# Reason: fetching all companies by category returns 2000+ announcements
# across 50+ pages. Our 33 watchlist tickers could appear on any page.
# Fetching per-scripcode returns only that company's announcements —
# always fits in one page, zero risk of missing anything for our watchlist.
# BSE library docs confirm: "it is faster and more bandwidth efficient
# to lookup by scripcode."
#
# DATE WINDOW: 72 hours (not 24) to handle weekends correctly.
# Monday NSE_OPEN: Friday's post-close filings are ~60h old.
# 24h window misses them. 72h catches everything. Deduplication
# (M2.5 DeduplicatorAgent via SHA-256 hash) handles any repeats.
#
# CATEGORY STRINGS: BSE's actual CATEGORYNAME values are used throughout.
# These must match exactly with NoiseScorerAgent's category weight table
# in agent_graph.md — any mismatch silently gives +0 modifier.
# Run test_bse_live.py and check real category names if anything
# scores unexpectedly low downstream.

import json
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from bse import BSE

from pipeline.models import Market, RawItem, SourceTier

logger = logging.getLogger(__name__)

CONFIG_DIR       = Path(__file__).parent.parent / "config"
SCRIP_CODES_PATH = CONFIG_DIR / "bse_scrip_codes.json"

# ── Date window ───────────────────────────────────────────────
# 72h covers full weekends + any BSE filing delays.
# DeduplicatorAgent handles repeat items via SHA-256 hashing.
LOOKBACK_HOURS = 72

# ── Time horizon map ──────────────────────────────────────────
# Keys are BSE's actual CATEGORYNAME strings returned by the API.
# CRITICAL: these strings must match NoiseScorerAgent's category
# weight table exactly. Update both places together if either changes.
# Fallback for unrecognised categories: "BOTH" (safe default).
# If you see unexpected "BOTH" in digests, check what CATEGORYNAME
# BSE actually returned and add it here.
HORIZON_MAP = {
    # SHORT_TERM: direct near-term price catalysts
    "Financial Results":            "SHORT_TERM",
    "Board Meeting":                "SHORT_TERM",
    "Outcome of Board Meeting":     "SHORT_TERM",
    "Corp. Action":                 "SHORT_TERM",
    "Dividend":                     "SHORT_TERM",
    "Bonus":                        "SHORT_TERM",
    "Buyback":                      "SHORT_TERM",
    "Rights":                       "SHORT_TERM",
    "Split":                        "SHORT_TERM",

    # LONG_TERM: structural changes, takes weeks/months to play out
    "Merger/Amalgamation":          "LONG_TERM",
    "Acquisition":                  "LONG_TERM",
    "Insider Trading / SAST":       "LONG_TERM",
    "Change in Directors":          "LONG_TERM",
    "Change in Management":         "LONG_TERM",
    "Fund Raising":                 "LONG_TERM",
    "Change in Shareholding":       "LONG_TERM",
    "Related Party Transactions":   "LONG_TERM",

    # BOTH: depends on specifics, let SignalGeneratorAgent decide
    "Regulatory":                   "BOTH",
    "Credit Rating":                "BOTH",
    "General":                      "BOTH",
    "Updates":                      "BOTH",
    "Others":                       "BOTH",
    "New Listing":                  "BOTH",
    "AGM/EGM":                      "BOTH",
}

def _load_scrip_map() -> dict:
    """
    Returns {ticker_display: scrip_code} from bse_scrip_codes.json.
    e.g. {"RELIANCE": "500325", "HDFCBANK": "500180", ...}
    """
    with open(SCRIP_CODES_PATH, "r") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_")}


def _parse_bse_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse BSE date strings to UTC datetime.
    BSE returns various formats — dateutil handles all of them.
    BSE timestamps are IST (UTC+5:30) — converts to UTC.
    """
    if not date_str:
        return None
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(date_str, dayfirst=True)
        if dt.tzinfo is None:
            # Assume IST, convert to UTC
            dt = (dt - timedelta(hours=5, minutes=30)).replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _announcement_to_raw_item(
    announcement: dict,
    ticker: str,
    scrip_code: str,
) -> Optional[RawItem]:
    """
    Convert a single BSE announcement dict to a RawItem.

    BSE library returns these fields per announcement:
      SCRIP_CD      — numeric scrip code e.g. "500325"
      SCRIP_NAME    — company name
      CATEGORYNAME  — announcement type e.g. "Financial Results"
      HEADLINE      — subject/announcement text
      NEWS_DT       — filing datetime string
      ATTACHMENTNAME — PDF filename (if any)

    The category_label is embedded in the headline so NoiseScorerAgent
    can extract it for the category weight modifier without parsing
    the raw_payload. Format: "[TICKER] [CATEGORY] headline text"
    """
    headline_raw = (announcement.get("HEADLINE") or "").strip()
    if not headline_raw:
        return None

    category_label = (announcement.get("CATEGORYNAME") or "General").strip()
    horizon = HORIZON_MAP.get(category_label, "BOTH")

    # Structured headline: ticker + category visible to all downstream agents
    headline = f"[{ticker}] [{category_label}] {headline_raw}"
    attachment = announcement.get("ATTACHMENTNAME", "")
    url = (
        f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attachment}"
        if attachment else f"https://www.bseindia.com/corporates/ann.html?scripcd={scrip_code}"
    )

    snippet = (
        f"BSE Corporate Filing | {ticker} (Scrip: {scrip_code}) | "
        f"Category: {category_label} | "
        f"Horizon: {horizon}"
    )

    return RawItem(
        source_id    = "bse_filings",
        source_name  = "BSE Corporate Filings",
        source_tier  = SourceTier.OFFICIAL,
        headline     = headline,
        snippet      = snippet,
        url          = url,
        published_at = _parse_bse_date(announcement.get("NEWS_DT")),
        market       = Market.INDIA,
        raw_payload  = announcement,
    )


def collect_bse(session_type: str) -> List[RawItem]:
    """
    Fetch BSE corporate announcements for all watchlist India tickers.
    Only runs during India market sessions.

    Calls BSE once per watchlist ticker (per-scrip approach).
    Total API calls per run = number of tickers in scrip_codes.json (33).
    bse library handles rate limiting and session cookies internally.
    """
    if session_type not in {"NSE_OPEN", "NSE_MID", "NSE_CLOSE"}:
        return []

    scrip_map  = _load_scrip_map()
    all_items: List[RawItem] = []

    today     = datetime.now(timezone.utc)
    from_date = today - timedelta(hours=LOOKBACK_HOURS)

    logger.info(
        f"BSE: fetching {len(scrip_map)} tickers | "
        f"window: last {LOOKBACK_HOURS}h ({from_date.strftime('%Y-%m-%d %H:%M')} UTC → now)"
    )

    # bse library requires a folder for cookie persistence.
    # Using tempfile means cookies are ephemeral (re-auth each run).
    # Acceptable for our 5-session-per-day cadence.
    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            bse_client = BSE(download_folder=tmp_dir)

            for ticker, scrip_code in scrip_map.items():
                try:
                    data = bse_client.announcements(
                        scripcode = scrip_code,
                        from_date = from_date,
                        to_date   = today,
                        page_no   = 1,
                        # No category filter: fetch all announcement types
                        # for this ticker. Category-based scoring happens
                        # in NoiseScorerAgent via the weight modifier table,
                        # not at collection time.
                    )

                    announcements = data.get("Table", [])
                    total_available = (
                        data.get("Table1", [{}])[0].get("ROWCNT", 0)
                        if data.get("Table1") else 0
                    )

                    if not announcements:
                        logger.debug(f"BSE [{ticker}]: no announcements in window")
                        continue

                    # Flag if a single company has more than one page of results.
                    # This would be unusual (>40 announcements in 72h for one company)
                    # but worth knowing. Could indicate a data flood or BSE anomaly.
                    if total_available > len(announcements):
                        logger.warning(
                            f"BSE [{ticker}]: {total_available} total announcements "
                            f"but only {len(announcements)} on page 1. "
                            f"Consider adding pagination for this ticker."
                        )

                    items_for_ticker = []
                    for announcement in announcements:
                        item = _announcement_to_raw_item(
                            announcement = announcement,
                            ticker       = ticker,
                            scrip_code   = scrip_code,
                        )
                        if item:
                            items_for_ticker.append(item)

                    logger.info(
                        f"BSE [{ticker}]: {len(items_for_ticker)} items "
                        f"from {len(announcements)} announcements"
                    )
                    all_items.extend(items_for_ticker)

                except Exception as e:
                    # Per-ticker failure: log and continue to next ticker.
                    # One company's API failure never stops the rest.
                    logger.warning(
                        f"BSE [{ticker} | {scrip_code}]: failed — {e}",
                        exc_info=True,
                    )

            bse_client.exit()

        except Exception as e:
            logger.warning(
                f"BSE: client initialisation failed — {e}",
                exc_info=True,
            )

    # Summary log: useful for spotting which tickers consistently return nothing
    logger.info(
        f"BSE collection complete | "
        f"Session: {session_type} | "
        f"Total items: {len(all_items)} | "
        f"Tickers processed: {len(scrip_map)}"
    )
    return all_items
