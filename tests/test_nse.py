# tests/test_nse.py
#
# Manual live test for the updated NSE collector.
# Uses official RSS feeds — no session cookie needed.
#
# Run: python tests/test_nse.py
#
# What this tests:
#   1. Each RSS feed URL is reachable and returns parseable XML
#   2. RawItems are produced with correct structure
#   3. FII/DII API still works (session-cookie, only for NSE_CLOSE)
#   4. Different sessions activate the right feeds

import logging
import os
import sys

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

from pipeline.collectors.nse_collector import (
    collect_nse,
    collect_nse_announcements,
    collect_fii_dii,
    NSE_RSS_FEEDS,
)
from pipeline.models import SourceTier, Market


def separator(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def test_individual_feeds():
    """
    Test each RSS feed URL independently.
    Shows exactly which feeds are reachable and how many items each returns.
    Useful for identifying if a specific feed URL has changed.
    """
    separator("TEST 1 — Individual Feed Health Check")

    import feedparser
    import time

    results = []
    for feed_config in NSE_RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_config["url"])
            count = len(feed.entries)
            bozo  = feed.bozo
            status = "⚠ BOZO" if bozo else "✓ OK"
            print(f"  {status} | {feed_config['id']:<30} | {count:>4} entries")
            results.append((feed_config["id"], count, bozo))
            time.sleep(0.3)  # respectful crawling
        except Exception as e:
            print(f"  ✗ FAIL | {feed_config['id']:<30} | {e}")
            results.append((feed_config["id"], 0, True))

    working = sum(1 for _, count, _ in results if count > 0)
    print(f"\n  Summary: {working}/{len(NSE_RSS_FEEDS)} feeds returned data")
    return results


def test_session_routing():
    """
    Test that each session type activates the correct subset of feeds.
    NSE_OPEN and NSE_MID should activate fewer feeds than NSE_CLOSE.
    """
    separator("TEST 2 — Session Routing")

    for session in ["NSE_OPEN", "NSE_MID", "NSE_CLOSE"]:
        active = [f for f in NSE_RSS_FEEDS if session in f["sessions"]]
        feed_names = [f["id"].replace("nse_", "") for f in active]
        print(f"  {session:<12} → {len(active)} feeds: {', '.join(feed_names)}")


def test_raw_item_structure():
    """
    Fetch NSE_OPEN session and validate RawItem structure.
    Every item must have required fields populated correctly.
    """
    separator("TEST 3 — RawItem Structure Validation (NSE_OPEN)")

    items = collect_nse_announcements("NSE_OPEN")
    print(f"\n  Total items collected: {len(items)}")

    if not items:
        print("  ⚠ No items returned — market may be closed or feeds are empty")
        return

    errors = []
    for i, item in enumerate(items):
        if not item.headline:
            errors.append(f"Item {i}: empty headline")
        if not item.source_id:
            errors.append(f"Item {i}: missing source_id")
        if item.source_tier != SourceTier.OFFICIAL:
            errors.append(f"Item {i}: wrong tier {item.source_tier}, expected OFFICIAL")
        if item.market != Market.INDIA:
            errors.append(f"Item {i}: wrong market {item.market}, expected INDIA")
        if item.fetched_at is None:
            errors.append(f"Item {i}: missing fetched_at")

    if errors:
        print(f"\n  ✗ Structural errors found:")
        for e in errors:
            print(f"    {e}")
    else:
        print(f"  ✓ All {len(items)} items have valid structure")

    # Print first 5 items as sample
    print(f"\n  --- Sample items (first 5) ---")
    for item in items[:5]:
        print(f"\n  Source  : {item.source_id}")
        print(f"  Headline: {item.headline[:100]}")
        print(f"  Snippet : {item.snippet[:120]}")
        print(f"  URL     : {item.url[:80]}")
        print(f"  Pub at  : {item.published_at}")


def test_nse_close_session():
    """
    Test NSE_CLOSE session specifically — activates all feeds including
    high-value ones: XBRL financials, insider trading, FII/DII, buyback,
    SAST Reg29/31, shareholding pattern.
    """
    separator("TEST 4 — NSE_CLOSE Full Session")

    items = collect_nse_announcements("NSE_CLOSE")
    print(f"\n  Total items: {len(items)}")

    # Group by source_id to see distribution
    from collections import Counter
    counts = Counter(item.source_id for item in items)
    print("\n  Items per feed:")
    for source_id, count in sorted(counts.items()):
        print(f"    {source_id:<35} {count:>4} items")


def test_fii_dii():
    """
    Test FII/DII API separately — this still uses session-cookie approach
    since no RSS alternative exists for this data.
    Only meaningful after market close (NSE publishes data post 15:30 IST).
    """
    separator("TEST 5 — FII/DII API (NSE_CLOSE only)")

    items = collect_fii_dii("NSE_CLOSE")

    if not items:
        print("\n  ⚠ No FII/DII data returned")
        print("  This is expected if:")
        print("    - Market hasn't closed yet today")
        print("    - It's a weekend or market holiday")
        print("    - NSE session cookie was rejected (403)")
        return

    item = items[0]
    print(f"\n  ✓ FII/DII data received")
    print(f"  Headline: {item.headline}")
    print(f"  Snippet : {item.snippet}")
    print(f"  Tier    : {item.source_tier}")


def test_full_collect_nse():
    """
    End-to-end test of the public collect_nse() function.
    This is what CollectorAgent actually calls.
    """
    separator("TEST 6 — Full collect_nse() End-to-End")

    for session in ["NSE_OPEN", "NSE_CLOSE"]:
        items = collect_nse(session)
        print(f"\n  {session}: {len(items)} total items")
        if items:
            tiers = set(item.source_tier.name for item in items)
            sources = set(item.source_id for item in items)
            print(f"    Tiers   : {tiers}")
            print(f"    Sources : {len(sources)} unique sources")


if __name__ == "__main__":
    print("\nNSE Collector Live Test")
    print("Tests against real NSE RSS feeds — internet required")
    print("FII/DII test only meaningful after 15:30 IST on trading days")

    # test_individual_feeds()
    # test_session_routing()
    # test_raw_item_structure()
    # test_nse_close_session()
    test_fii_dii()
    # test_full_collect_nse()

    print("\n" + "=" * 60)
    print("  All tests complete")
    print("=" * 60 + "\n")