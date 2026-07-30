# tests/test_relevance_filter.py
# Run: python -m tests.test_relevance_filter

import logging
import os
import sys
from datetime import datetime, timezone

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

from pipeline.models import RawItem, SourceTier, Market
from pipeline.storage.base import InMemoryStorage, compute_hash
from pipeline.agents.deduplicator import DeduplicatedItem, run_deduplicator
from pipeline.agents.relevance_filter import (
    run_relevance_filter, RelevanceTag, FilteredItem
)


def make_deduped(
    headline:  str,
    source_id: str = "et_markets",
    tier:      SourceTier = SourceTier.MEDIA,
    snippet:   str = "",
    market:    Market = Market.INDIA,
) -> DeduplicatedItem:
    item = RawItem(
        source_id   = source_id,
        source_name = source_id,
        source_tier = tier,
        headline    = headline,
        snippet     = snippet,
        url         = "https://example.com",
        market      = market,
        published_at = datetime.now(timezone.utc),
    )
    return DeduplicatedItem(
        item                  = item,
        item_hash             = compute_hash(headline, source_id),
        corroboration_count   = 1,
        corroborating_sources = [],
        is_wire_reprint       = False,
    )


def separator(title):
    print(f"\n{'=' * 55}\n  {title}\n{'=' * 55}")


def test_ticker_keyword_match():
    separator("TEST 1 — Ticker keyword match (Pass 1)")

    items = [
        make_deduped("HDFC Bank Q1 profit rises 35%"),
        make_deduped("Infosys wins $500M cloud deal from US client"),
        make_deduped("Random unrelated news about cricket"),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 2, f"Expected 2, got {len(result)}"
    tags = {r.item.headline: r.tag for r in result}
    assert tags["HDFC Bank Q1 profit rises 35%"] == RelevanceTag.RELEVANT
    assert tags["Infosys wins $500M cloud deal from US client"] == RelevanceTag.RELEVANT
    print(f"  ✓ 2/3 kept | cricket article correctly dropped")


def test_sector_keyword_match():
    separator("TEST 2 — Sector keyword match (Pass 2)")

    items = [
        make_deduped("Information Technology(IT) sector stocks rally on weak dollar"),
        make_deduped("Pharma & Healthcare stocks under pressure"),
        make_deduped("Unrelated sports news today"),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 2, f"Expected 2, got {len(result)}"
    print(f"  ✓ Sector headlines kept: {[r.item.headline for r in result]}")


def test_macro_source_pass():
    separator("TEST 3 — Tier 1 macro source (Pass 3)")

    items = [
        make_deduped(
            "RBI Policy Statement Q2 2026",
            source_id = "rbi_press",
            tier      = SourceTier.OFFICIAL,
            snippet   = "repo rate held at 6.5%"
        ),
        make_deduped(
            "Federal Reserve FOMC Statement July 2026",
            source_id = "fed_press",
            tier      = SourceTier.OFFICIAL,
            market    = Market.US,
        ),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 2
    for r in result:
        assert r.tag == RelevanceTag.MACRO, f"Expected MACRO, got {r.tag}"
    print(f"  ✓ Both RBI and Fed items tagged MACRO regardless of content")


def test_macro_keyword_match():
    separator("TEST 4 — Macro keyword match (Pass 4)")

    items = [
        make_deduped("India CPI inflation rises to 5.2% in June"),
        make_deduped("Crude oil prices fall 3% on demand concerns"),
        make_deduped("US GDP grows 2.1% in Q2 2026"),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 3
    for r in result:
        assert r.tag == RelevanceTag.MACRO
    print(f"  ✓ All 3 macro headlines kept with MACRO tag")
    for r in result:
        print(f"    {r.item.headline[:55]} → sectors: {r.sector_impact}")


def test_sector_impact_populated_for_macro():
    separator("TEST 5 — Sector impact populated for MACRO items")

    items = [
        make_deduped(
            "RBI cuts repo rate by 50bps",
            snippet = "monetary policy committee reduces repo rate"
        ),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 1
    r = result[0]
    assert r.tag == RelevanceTag.MACRO
    assert len(r.sector_impact) > 0, "Sector impact should be populated for rate cut"
    print(f"  ✓ Sectors affected by rate cut: {r.sector_impact}")


def test_hard_drop_irrelevant():
    separator("TEST 6 — Irrelevant items hard dropped")

    items = [
        make_deduped("Manchester United signs new striker"),
        make_deduped("Weather forecast: heavy rain expected in Mumbai tomorrow"),
        make_deduped("New Bollywood film releases this Friday"),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 0, f"Expected 0, got {len(result)}: Result:{result}"
    print(f"  ✓ All 3 irrelevant items correctly dropped")


def test_empty_input():
    separator("TEST 7 — Empty input")
    result = run_relevance_filter([])
    assert result == []
    print("  ✓ Empty input returns empty list")


def test_us_ticker_keyword():
    separator("TEST 8 — US ticker keyword match")

    items = [
        make_deduped("NVIDIA reports record GPU sales", market=Market.US),
        make_deduped("Blackwell architecture ships ahead of schedule", market=Market.US),
        make_deduped("GLP-1 drug Mounjaro approved for new indication", market=Market.US),
    ]
    result = run_relevance_filter(items)

    assert len(result) == 3
    print(f"  ✓ US tickers matched: {[r.item.headline for r in result]}")


def test_matched_keywords_populated():
    separator("TEST 9 — Matched keywords populated on FilteredItem")

    items = [make_deduped("TCS wins multi-year IT contract")]
    result = run_relevance_filter(items)

    assert len(result) == 1
    assert len(result[0].matched_keywords) > 0
    print(f"  ✓ Matched keywords: {result[0].matched_keywords}")


if __name__ == "__main__":
    print("\nRelevanceFilterAgent Tests — no network, no API calls")

    test_ticker_keyword_match()
    test_sector_keyword_match()
    test_macro_source_pass()
    test_macro_keyword_match()
    test_sector_impact_populated_for_macro()
    test_hard_drop_irrelevant()
    test_empty_input()
    test_us_ticker_keyword()
    test_matched_keywords_populated()

    print(f"\n{'=' * 55}")
    print("  All relevance filter tests complete")
    print(f"{'=' * 55}\n")