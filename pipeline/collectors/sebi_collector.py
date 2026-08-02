# pipeline/collectors/sebi_collector.py
#
# SEBI Collector — fetches from SEBI's own official RSS feed.
#
# sources.json originally described this as a scraper against
# sebi.gov.in/sebiweb/... but SEBI actually publishes a combined
# official RSS feed (verified live, current data as of this build):
#   https://www.sebi.gov.in/sebirss.xml
# covering Press Releases, Circulars, and Orders/Rulings in one feed.
# No scraping needed — same clean pattern as RBI/Fed.
#
# NOISE WARNING (from a live pull of the feed):
# The feed is dominated by individual-investor enforcement paperwork —
# "Notice of Demand against <name>", "Appeal No. X filed by <name>",
# recovery certificates against defaulters in illiquid stock-option
# cases. None of this is market-moving signal. Only three link-path
# categories carry real signal:
#   /legal/circulars/                          — regulatory policy
#   /media-and-notifications/press-releases/   — SEBI announcements
#   /enforcement/orders/                       — orders against firms
#     (but this path ALSO mixes in bare "Appeal No. X filed by <name>"
#      docket entries — filtered out below by title pattern)
# /enforcement/recovery-proceedings/ is skipped entirely.
#
# THIS FILTERING RULE IS A FIRST PASS — confirm it matches what you
# actually want kept once you see real output.

import calendar
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional

import feedparser

from pipeline.models import RawItem, SourceTier, Market

logger = logging.getLogger(__name__)

SEBI_RSS_URL = "https://www.sebi.gov.in/sebirss.xml"

PATH_CATEGORY_MAP = {
    "/legal/circulars/": "Circular",
    "/media-and-notifications/press-releases/": "Press Release",
    "/enforcement/orders/": "Enforcement Order"
}

SKIPPED_PATH_PREFIXES = (
    "/enforcement/recovery-proceedings/",
)

# Bare docket filings under /enforcement/orders/ — a filing number and
# a name, not an actual decision. Not market information.
DOCKET_TITLE_PATTERN = re.compile(r"^Appeal No\.\s*\d+", re.IGNORECASE)

def _category_from_url(url: str) -> Optional[str]:
    for prefix in SKIPPED_PATH_PREFIXES:
        if prefix in url:
            return None

    for path_segement, label in PATH_CATEGORY_MAP.items():
        if path_segement in url:
            return label

    return None


def _parse_sebi_date(entry: dict) -> Optional[datetime]:
    parsed = entry.get("published_parsed")
    if not parsed:
        return None

    try:
        return datetime.fromtimestamp(calendar.timegm(parsed), tz=timezone.utc)
    except Exception:
        return None


def _entry_to_raw_item(entry: dict) -> Optional[RawItem]:
    headline = (entry.get("title") or "").strip()
    url = entry.get("link", "")
    if not headline or not url:
        return None

    if DOCKET_TITLE_PATTERN.match(headline):
        return None

    category = _category_from_url(url)
    if category is None:
        return None

    snippet = f"SEBI {category} | {(entry.get("summary") or "")[:300]}"
    return RawItem(
        source_id="sebi_disclosures",
        source_name="SEBI Regulatory Disclosures",
        source_tier=SourceTier.OFFICIAL,
        headline=headline,
        snippet=snippet,
        url=url,
        published_at=_parse_sebi_date(entry),
        market=Market.INDIA,
        category=category,
        raw_payload=dict(entry),
    )


def collect_sebi(session_type: str) -> List[RawItem]:
    """
    Fetch SEBI's combined RSS feed — one feed for circulars, press
    releases, and orders, no category-specific URL exists.
    Matches sources.json's original poll_sessions for sebi_disclosures
    (NSE_CLOSE — pulled once daily after market close).
    """
    if session_type != "NSE_CLOSE":
        return []

    try:
        feed = feedparser.parse(SEBI_RSS_URL)
        if feed.bozo:
            logger.warning(f"SEBI RSS: malformed XML ({type(feed.bozo_exception).__name__}) — parsing anyway")

    except Exception as e:
        logger.warning(f"SEBI RSS: fetch failed — {e}", exc_info=True)
        return []

    items: List[RawItem] = []
    skipped = 0
    for entry in feed.entries:
        item = _entry_to_raw_item(entry)
        if item:
            items.append(item)
        else:
            skipped += 1

    logger.info(
        f"SEBI RSS: {len(feed.entries)} entries → {len(items)} kept, "
        f"{skipped} skipped (recovery-proceedings/docket noise)"
    )
    return items
