# pipeline/collectors/nse_collector.py
#
# NSE Collector — fetches corporate announcements via official NSE RSS feeds.
#
# Previous approach: session-cookie API scraping → fragile, 403-prone
# Current approach: official RSS feeds from nsearchives.nseindia.com → stable,
# no authentication, no bot detection, officially supported by NSE.
#
# These feeds return ALL listed companies (~2000+). RelevanceFilterAgent
# downstream narrows this to our 33 India watchlist tickers via keyword matching.
#
# FII/DII flows: no RSS equivalent exists. Retained as a separate API call
# for NSE_CLOSE session only.

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import feedparser
import requests

from pipeline.models import RawItem, SourceTier, Market

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent / "config"
WATCHLIST_PATH = CONFIG_DIR / "watchlist.json"

# ── NSE RSS Feed Registry ──────────────────────────────────────
# Only feeds with meaningful signal value for trading decisions.
# Skipped: brsr, annual_reports, corporate_governance, secretarial_compliance,
# voting_results, investor_complaints, share_transfers, unitholding_patterns,
# sast_reason_for_encumbrance (context-only, surfaced when Reg31 fires)

NSE_RSS_FEEDS = [
    {
        "id":          "nse_announcements",
        "name":        "NSE Online Announcements",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
        "category":    "Announcement",
        "signal_tier": "high",
        "sessions":    {"NSE_OPEN", "NSE_MID", "NSE_CLOSE"},
    },
    {
        "id":          "nse_board_meetings",
        "name":        "NSE Board Meetings",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Board_Meetings.xml",
        "category":    "Board Meeting",
        "signal_tier": "high",
        "sessions":    {"NSE_OPEN", "NSE_MID", "NSE_CLOSE"},
    },
    {
        "id":          "nse_financial_results",
        "name":        "NSE Financial Results",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Financial_Results.xml",
        "category":    "Financial Results",
        "signal_tier": "high",
        "sessions":    {"NSE_OPEN", "NSE_MID", "NSE_CLOSE"},
    },
    {
        "id":          "nse_xbrl_financials",
        "name":        "NSE Integrated Filing Financials (XBRL)",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Integrated_Filing_Financials.xml",
        "category":    "Financial Results",
        "signal_tier": "high",
        "sessions":    {"NSE_CLOSE"},   # XBRL filed same day as results, check at close
    },
    {
        "id":          "nse_insider_trading",
        "name":        "NSE Insider Trading Disclosures",
        "url":         "https://nsearchives.nseindia.com/content/RSS/InsiderTrading.xml",
        "category":    "Insider Trading",
        "signal_tier": "high",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_corporate_actions",
        "name":        "NSE Corporate Actions",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Corporate_action.xml",
        "category":    "Corporate Action",
        "signal_tier": "high",
        "sessions":    {"NSE_OPEN", "NSE_CLOSE"},
    },
    {
        "id":          "nse_buyback",
        "name":        "NSE Daily Buyback",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Daily_Buyback.xml",
        "category":    "Buyback",
        "signal_tier": "high",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_sast_reg29",
        "name":        "NSE SAST Regulation 29 (Stake Acquisition 5%+)",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Sast_Regulation29.xml",
        "category":    "Acquisition",
        # SAST Regulation 29: disclosure when any entity acquires or
        # disposes 5%+ of a company's shares. Signals large institutional
        # interest or a potential takeover play. High impact on target stock.
        "signal_tier": "high",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_sast_reg31",
        "name":        "NSE SAST Regulation 31 (Promoter Pledge)",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Sast_Regulation31.xml",
        "category":    "Promoter Pledge",
        # SAST Regulation 31: promoter pledging company shares as collateral.
        # Bearish signal — promoters pledge when they need liquidity urgently.
        # High pledge % (>50% of promoter holding) is a serious red flag.
        # Classic warning sign in Indian markets before stock price crashes.
        "signal_tier": "high",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_shareholding",
        "name":        "NSE Shareholding Pattern",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Shareholding_Pattern.xml",
        "category":    "Shareholding Pattern",
        "signal_tier": "medium",
        "sessions":    {"NSE_CLOSE"},   # Published quarterly
    },
    {
        "id":          "nse_related_party",
        "name":        "NSE Related Party Transactions",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Related_Party_Trans.xml",
        "category":    "Related Party Transaction",
        "signal_tier": "medium",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_deviation",
        "name":        "NSE Statement of Deviation",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Statement_Of_Deviation.xml",
        "category":    "Fund Usage Deviation",
        # Company used IPO/QIP proceeds differently from stated purpose.
        # Governance red flag — always treat as bearish when it appears.
        "signal_tier": "medium",
        "sessions":    {"NSE_CLOSE"},
    },
    {
        "id":          "nse_offer_documents",
        "name":        "NSE Offer Documents",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Offer_Documents.xml",
        "category":    "Offer Document",
        "signal_tier": "medium",
        "sessions":    {"NSE_OPEN", "NSE_CLOSE"},
    },
    {
        "id":          "nse_circulars",
        "name":        "NSE Exchange Circulars",
        "url":         "https://nsearchives.nseindia.com/content/RSS/Circulars.xml",
        "category":    "Exchange Circular",
        # Policy changes from NSE: margin requirements, settlement rules,
        # circuit breakers, new regulations. Affects all traders.
        "signal_tier": "medium",
        "sessions":    {"NSE_OPEN"},
    },
]

# ── FII/DII API (no RSS alternative) ──────────────────────────
NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
NSE_BASE_URL    = "https://www.nseindia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9"
}

# ── Time horizon mapping ──────────────────────────────────────
HORIZON_MAP = {
    "Financial Results": "SHORT_TERM",
    "Board Meeting": "SHORT_TERM",
    "Corporate Action": "SHORT_TERM",
    "Buyback": "SHORT_TERM",
    "Acquisition": "LONG_TERM",
    "Insider Trading": "LONG_TERM",
    "Promoter Pledge": "LONG_TERM",
    "Shareholding Pattern": "LONG_TERM",
    "Related Party Transaction": "LONG_TERM",
    "Offer Document": "LONG_TERM",
    "Fund Usage Deviation": "BOTH",
    "Exchange Circular": "BOTH",
    "Announcement": "BOTH"
}

def _parse_rss_date(entry: dict) -> Optional[datetime]:
    """Parse feedparser's published_parsed struct_time to UTC datetime."""
    import calendar
    parsed = entry.get("published_parsed")
    if not parsed:
        return None
    try:
        return datetime.fromtimestamp(calendar.timegm(parsed), tz=timezone.utc)
    except Exception:
        return None

def _entry_to_raw_item(entry: dict, feed_config: dict) -> Optional[RawItem]:
    """Convert a single RSS entry to a RawItem."""
    headline = (entry.get("title") or "").strip()
    if not headline:
        return None
    
    category = feed_config["category"]
    horizon = HORIZON_MAP.get(category, "BOTH")
    
    snippet = (
        f"NSE {category} | "
        f"Time Horizon: {horizon} | "
        f"{(entry.get('summary') or '')[:300]}"
    )

    return RawItem(
        source_id=feed_config["id"],
        source_name=feed_config["name"],
        source_tier=SourceTier.OFFICIAL,
        headline=headline,
        snippet=snippet,
        url=entry.get("link", NSE_BASE_URL),
        published_at=_parse_rss_date(entry),
        market=Market.INDIA,
        category=category,
        raw_payload=dict(entry)
    )


def collect_nse_announcements(session_type: str) -> List[RawItem]:
    """
    Fetch all applicable NSE RSS feeds for the current session.
    Returns RawItems for all entries — RelevanceFilterAgent downstream
    narrows to watchlist tickers via keyword matching.
    """
    active_feeds=[
        f for f in NSE_RSS_FEEDS
        if session_type in f['sessions']
    ]

    if not active_feeds:
        return []
    
    # Seed NSE session cookies once — reuse for all feed fetches
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get(NSE_BASE_URL, timeout=15)
        time.sleep(1)
    except Exception as e:
        logger.warning(f"NSE RSS: session seed failed ({e}) — attempting feeds anyway")

    all_items: List[RawItem] = []
    for feed_config in active_feeds:
        try:
            response = None
            for attempt in range(2):  # 1 attempt + 1 retry
                try:
                    response = session.get(feed_config['url'], timeout=30)
                    response.raise_for_status()
                    break  # success — exit retry loop
                except requests.exceptions.Timeout:
                    if attempt == 0:
                        logger.warning(
                            f"NSE RSS [{feed_config['id']}]: timeout on attempt 1 "
                            f"— waiting 5s before retry"
                        )
                        time.sleep(5)
                    else:
                        raise  # second timeout — let outer except handle it

            if response is None:
                continue

            feed = feedparser.parse(response.content)
            if feed.bozo:
                logger.warning(
                    f"NSE RSS [{feed_config['id']}]: malformed XML "
                    f"({type(feed.bozo_exception).__name__}) — parsing anyway"
                )

            items_from_feed = []
            for entry in feed.entries:
                item = _entry_to_raw_item(entry=entry, feed_config=feed_config)
                if item:
                    items_from_feed.append(item)

            logger.info(f"NSE RSS [{feed_config['id']}]: {len(items_from_feed)} items")
            all_items.extend(items_from_feed)

            time.sleep(2)  # respectful crawling — increased from 0.5s

        except Exception as e:
            logger.warning(
                f"NSE RSS [{feed_config['id']}]: failed — {e}",
                exc_info=True,
            )

    return all_items

def collect_fii_dii(session_type: str) -> List[RawItem]:
    """
    Fetch FII/DII daily flow data via NSE API.
    No RSS alternative exists for this data.
    Runs only at NSE_CLOSE — NSE publishes after market close.

    API response: List of category dicts — one per institutional type.
    Fields: buyValue, sellValue, netValue, category, date
    Known categories as of Jul 2026: 'FII/FPI', 'DII'
    Category names have changed before (FII → FII/FPI in 2014).
    We match flexibly — don't assume exact strings or row count.
    """
    if session_type != "NSE_CLOSE":
        return []

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get(NSE_BASE_URL, timeout=15)
        time.sleep(1)

        response = session.get(NSE_FII_DII_URL, timeout=15)
        response.raise_for_status()
        data = response.json()

        if not data or not isinstance(data, list):
            logger.warning(f"NSE FII/DII: unexpected response shape: {type(data)}")
            return []

        # Log all categories found — catches API changes immediately
        categories_found = [str(row.get("category", "UNKNOWN")) for row in data]
        logger.info(f"NSE FII/DII: categories in response: {categories_found}")

        # Aggregate by group — flexible matching, handles N rows
        # Foreign group: any category containing FII or FPI
        # Domestic group: any category containing DII
        # Unknown categories: logged as warning, not silently dropped
        foreign_buy  = 0.0
        foreign_sell = 0.0
        foreign_net  = 0.0
        domestic_buy  = 0.0
        domestic_sell = 0.0
        domestic_net  = 0.0
        date_str      = ""
        unknown_cats  = []

        for row in data:
            cat      = str(row.get("category", "")).upper()
            buy      = float(row.get("buyValue",  0) or 0)
            sell     = float(row.get("sellValue", 0) or 0)
            net      = float(row.get("netValue",  0) or 0)
            date_str = row.get("date", date_str)

            if "FII" in cat or "FPI" in cat:
                foreign_buy  += buy
                foreign_sell += sell
                foreign_net  += net
            elif "DII" in cat:
                domestic_buy  += buy
                domestic_sell += sell
                domestic_net  += net
            else:
                # Unknown category — log it, don't silently ignore
                unknown_cats.append(cat)
                logger.warning(
                    f"NSE FII/DII: unrecognised category '{cat}' — "
                    f"not included in foreign/domestic aggregates. "
                    f"Consider updating the collector if this persists."
                )

        if foreign_net == 0 and domestic_net == 0:
            logger.warning(
                f"NSE FII/DII: all values are zero after aggregation. "
                f"Categories found: {categories_found}. "
                f"Raw sample: {data[0] if data else 'empty'}"
            )

        # Sentiment based on foreign institutional flows
        # Threshold: ±500 Cr = significant institutional activity
        if foreign_net > 500:
            sentiment = "BULLISH"
        elif foreign_net < -500:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"

        headline = (
            f"FII/DII Flows {date_str}: "
            f"FII/FPI ₹{foreign_net:,.2f} Cr | "
            f"DII ₹{domestic_net:,.2f} Cr | "
            f"{sentiment}"
        )
        snippet = (
            f"FII/FPI Buy: ₹{foreign_buy:,.2f} Cr | "
            f"FII/FPI Sell: ₹{foreign_sell:,.2f} Cr | "
            f"DII Buy: ₹{domestic_buy:,.2f} Cr | "
            f"DII Sell: ₹{domestic_sell:,.2f} Cr"
            + (f" | Unknown categories: {unknown_cats}" if unknown_cats else "")
        )

        logger.info(
            f"NSE FII/DII: {sentiment} | "
            f"FII/FPI ₹{foreign_net:,.2f} Cr | "
            f"DII ₹{domestic_net:,.2f} Cr"
        )

        return [RawItem(
            source_id    = "nse_fii_dii",
            source_name  = "NSE FII/DII Daily Flows",
            source_tier  = SourceTier.OFFICIAL,
            headline     = headline,
            snippet      = snippet,
            url          = NSE_FII_DII_URL,
            market       = Market.INDIA,
            raw_payload  = {"rows": data, "categories_found": categories_found},
        )]

    except Exception as e:
        logger.warning(f"NSE FII/DII: failed — {e}", exc_info=True)
        return []

def collect_nse(session_type: str) -> List[RawItem]:
    """Public entry point — RSS feeds + FII/DII API."""
    items: List[RawItem] = []
    items.extend(collect_nse_announcements(session_type=session_type))
    items.extend(collect_fii_dii(session_type=session_type))
    return items
