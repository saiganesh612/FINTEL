# pipeline/collectors/rss_collector.py
#
# RSS Collector — fetches headlines from all RSS/Atom news feeds
# applicable to the current market session.
#
# RSS  = Really Simple Syndication (news feed standard)
# Atom = A newer, similar feed standard (feedparser handles both)
#
# This adapter's contract:
#   Input:  session_type (str) — which market session is running
#   Output: List[RawItem]      — standardised items ready for the pipeline
#
# One failure in one feed never kills the whole collector.
# Each feed is wrapped in its own try/except.

import calendar
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

import feedparser

from pipeline.models import RawItem, Market, SourceTier

logger = logging.getLogger(__name__)

SOURCES_PATH = Path(__file__).parent.parent / "config" / "sources.json"

# ─────────────────────────────────────────────
# TIER MAP
#
# sources.json stores sources under "tier_1", "tier_2" etc.
# We need to convert those string keys to SourceTier enum values
# so RawItem validation works correctly.
# ─────────────────────────────────────────────
TIER_MAP = {
    1: SourceTier.OFFICIAL,
    2: SourceTier.WIRE,
    3: SourceTier.MEDIA,
    4: SourceTier.COMMUNITY
}

MARKET_MAP = {
    "india": Market.INDIA,
    "us": Market.US,
    "global": Market.GLOBAL
}

def _load_rss_sources(session_type: str) -> List[dict]:
    """
    Load sources.json and return only RSS sources
    that are configured to run in the given session.

    Why we inject tier number here:
    sources.json organises sources by tier (tier_1, tier_2...) as top-level keys.
    Individual source objects don't have a 'source_tier' field — the tier is
    implied by which list they're in. We add it explicitly here so every
    source dict carries its own tier number before we process it.
    This avoids passing tier around as a separate variable everywhere.
    """
    with open(SOURCES_PATH, 'r', encoding='utf-8') as f:
        sources_data = json.load(f)

    rss_sources = []
    for tier_key, tier_number in [("tier_1", 1), ("tier_2", 2), ("tier_3", 3), ("tier_4", 4)]:
        for source in sources_data.get(tier_key, []):
            # Only include RSS/Atom type sources
            if source.get("type") != "rss":
                continue
            if session_type not in source.get("poll_sessions", []):
                continue
            # Inject the tier number directly into the source dict
            rss_sources.append({**source, "source_tier": tier_number})

    return rss_sources


def _parse_published_date(entry: dict) -> Optional[datetime]:
    parsed = entry.get("published_parsed")
    if parsed is None:
        return None
    
    try:
        timestamp = calendar.timegm(parsed) # struct_time -> Unix timestamp (UTC)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except Exception:
        return None
    

def _entry_to_raw_item(entry: dict, source: dict) -> Optional[RawItem]:
    """
    Convert single feedparse entry to internal RawItem
    """
    headline = (entry.get("title") or "").strip()
    if not headline:
        # An Item with no headline is useless - ignore it
        return None
    
    snippet = entry.get("summary") or entry.get("description") or ""
    url = entry.get("link") or ""
    published_at = _parse_published_date(entry)
    market = MARKET_MAP.get(source.get("market", "global"), Market.GLOBAL)
    tier = TIER_MAP.get(source["source_tier"], SourceTier.MEDIA)

    try:
        raw = dict(entry)
    except Exception:
        raw = {}

    return RawItem(
        source_id=source["id"],
        source_name=source["name"],
        source_tier=tier,
        headline=headline,
        snippet=snippet,
        url=url,
        published_at=published_at,
        market=market,
        raw_payload=raw
    )


def collect_rss(session_type: str) -> List[RawItem]:
    """
    Fetch and parse all RSS feeds applicable to the given session.

    Args:
        session_type: One of NSE_OPEN | NSE_MID | NSE_CLOSE | US_OPEN | US_CLOSE

    Returns:
        List[RawItem] — all items collected from all applicable RSS sources.
        Empty list if all sources fail (never raises an exception to the caller).

    Error handling strategy:
        Each source is wrapped in its own try/except.
        One feed being down never affects the others.
        Errors are logged as WARNING (not ERROR) because a temporary feed
        outage is expected and non-critical for the pipeline.
    """
    sources = _load_rss_sources(session_type=session_type)
    all_items: List[RawItem] = []

    if not sources:
        logger.info(f"No RSS sources configured for session: {session_type}")
        return all_items
    
    logger.info(f"Fetching {len(sources)} RSS source(s) for session: {session_type}")
    for source in sources:
        source_id = source["id"]
        url = source.get("url", "")

        if not url:
            logger.warning(f"[{source_id}] No URL configured - skipping")
            continue

        try:
            feed = feedparser.parse(url)
            if feed.bozo:
                logger.warning(
                    f"[{source_id}] Malformed feed XML "
                    f"({type(feed.bozo_exception).__name__}) — parsing anyway"
                )

            # feed.entries = list of individual news items in feed
            items_from_source: List[RawItem] = []
            for entry in feed.entries:
                item = _entry_to_raw_item(entry=entry, source=source)
                if item is not None:
                    items_from_source.append(item)

            logger.info(f"[{source_id}] Collected {len(items_from_source)} items")
            all_items.extend(items_from_source)

        except Exception as e:
            # Catch-all: network timeout, DNS failure, unexpected format, etc.
            # We log with exc_info=True which includes the full stack trace
            # in the log output — essential for debugging
            logger.warning(
                f"[{source_id}] Failed to fetch — {type(e).__name__}: {e}",
                exc_info=True,
            )

    logger.info(
        f"RSS collection complete. "
        f"Total items: {len(all_items)} from {len(sources)} sources"
    )
    return all_items
