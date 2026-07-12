# tests/test_bse_live.py
# Run: python -m tests.test_bse_live

import logging
import os
import sys

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)

from pipeline.collectors.bse_collector import collect_bse, HORIZON_MAP


def separator(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def test_session_routing():
    """Verify only India sessions trigger BSE collection."""
    separator("TEST 1 — Session Routing")
    for session in ["NSE_OPEN", "NSE_MID", "NSE_CLOSE", "US_OPEN", "US_CLOSE"]:
        items = collect_bse(session)
        expected = session in {"NSE_OPEN", "NSE_MID", "NSE_CLOSE"}
        status = "✓" if (len(items) >= 0 and expected) or (not expected and len(items) == 0) else "✗"
        runs = "runs" if expected else "skips (correct)"
        print(f"  {status} {session:<14} → {runs} | {len(items)} items")


def test_item_structure():
    """Validate RawItem structure on real BSE data."""
    separator("TEST 2 — RawItem Structure (NSE_OPEN)")
    from pipeline.models import SourceTier, Market

    items = collect_bse("NSE_OPEN")
    print(f"\n  Total items collected: {len(items)}")

    if not items:
        print("  ⚠ No items — market may be closed or no filings in last 72h")
        return

    errors = []
    category_names_seen = set()

    for i, item in enumerate(items):
        if not item.headline:
            errors.append(f"Item {i}: empty headline")
        if item.source_tier != SourceTier.OFFICIAL:
            errors.append(f"Item {i}: wrong tier {item.source_tier}")
        if item.market != Market.INDIA:
            errors.append(f"Item {i}: wrong market {item.market}")
        if item.source_id != "bse_filings":
            errors.append(f"Item {i}: wrong source_id {item.source_id}")
        if item.fetched_at is None:
            errors.append(f"Item {i}: missing fetched_at")

        # Extract category from headline format: [TICKER] [CATEGORY] text
        try:
            parts = item.headline.split("] [")
            if len(parts) >= 2:
                cat = parts[1].rstrip("]").split("]")[0]
                category_names_seen.add(cat)
        except Exception:
            pass

    if errors:
        print(f"\n  ✗ Structural errors:")
        for e in errors:
            print(f"    {e}")
    else:
        print(f"  ✓ All {len(items)} items have valid structure")

    # This is the most important output — shows actual BSE CATEGORYNAME values
    # Compare these against HORIZON_MAP keys in bse_collector.py
    # Any missing here means those categories get BOTH horizon by default
    print(f"\n  Categories seen in this run (check against HORIZON_MAP):")
    for cat in sorted(category_names_seen):
        in_map = "✓ mapped" if cat in HORIZON_MAP else "⚠ not in HORIZON_MAP (defaults to BOTH)"
        print(f"    {cat:<40} {in_map}")


def test_sample_output():
    """Print first 5 items in detail."""
    separator("TEST 3 — Sample Output")
    items = collect_bse("NSE_CLOSE")
    print(f"\n  Total items: {len(items)}")
    for item in items[:5]:
        print(f"\n  Headline : {item.headline}")
        print(f"  Snippet  : {item.snippet}")
        print(f"  URL      : {item.url[:80]}")
        print(f"  Pub at   : {item.published_at}")


def test_per_ticker_coverage():
    """Check which tickers returned data vs which returned nothing."""
    separator("TEST 4 — Per-Ticker Coverage")
    import json
    from pathlib import Path

    scrip_path = Path(__file__).parent.parent / "pipeline" / "config" / "bse_scrip_codes.json"
    with open(scrip_path) as f:
        scrip_map = {k: v for k, v in json.load(f).items() if not k.startswith("_")}

    items = collect_bse("NSE_CLOSE")

    # Count items per ticker from headline
    from collections import Counter
    ticker_counts = Counter()
    for item in items:
        try:
            ticker = item.headline.split("]")[0].lstrip("[")
            ticker_counts[ticker] += 1
        except Exception:
            pass

    tickers_with_data    = {t for t in scrip_map if ticker_counts[t] > 0}
    tickers_without_data = set(scrip_map.keys()) - tickers_with_data

    print(f"\n  Tickers with data    : {len(tickers_with_data)}")
    print(f"  Tickers without data : {len(tickers_without_data)} (no filings in last 72h — normal)")
    if tickers_without_data:
        print(f"  No data: {', '.join(sorted(tickers_without_data))}")


if __name__ == "__main__":
    print("\nBSE Collector Live Test — per-scrip approach")

    test_session_routing()
    test_item_structure()
    test_sample_output()
    test_per_ticker_coverage()

    print(f"\n{'=' * 60}")
    print("  All tests complete")
    print(f"{'=' * 60}\n")