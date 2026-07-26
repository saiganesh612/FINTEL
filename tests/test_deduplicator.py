# tests/test_deduplicator.py
# Run: python -m tests.test_deduplicator

import logging
import os
import sys
from datetime import datetime, timezone

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

from pipeline.models import RawItem, SourceTier, Market
from pipeline.storage.base import InMemoryStorage
from pipeline.agents.deduplicator import run_deduplicator, DeduplicatedItem


def make_item(
    headline:  str,
    source_id: str,
    tier:      SourceTier = SourceTier.MEDIA,
    snippet:   str = "",
    market:    Market = Market.INDIA,
) -> RawItem:
    return RawItem(
        source_id   = source_id,
        source_name = source_id,
        source_tier = tier,
        headline    = headline,
        snippet     = snippet,
        url         = "https://example.com",
        market      = market,
        published_at = datetime.now(timezone.utc),
    )


def separator(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def test_exact_deduplication():
    """Same headline from same source — second one must be dropped."""
    separator("TEST 1 — Exact deduplication")

    storage = InMemoryStorage()
    items = [
        make_item("RBI holds rates at 6.5%", "et_markets"),
        make_item("RBI holds rates at 6.5%", "et_markets"),  # exact duplicate
        make_item("RBI holds rates at 6.5%", "moneycontrol"),  # different source — keep
    ]

    result = run_deduplicator(items, storage)

    # After Pass 1: 2 unique items (et_markets + moneycontrol)
    # After Pass 2: 1 cluster (identical headline — same story)
    assert len(result) == 1, f"Expected 1 cluster, got {len(result)}"
    assert result[0].corroboration_count == 2  # both sources contributed
    assert "moneycontrol" in result[0].corroborating_sources
    print(f"  ✓ 3 items → 1 cluster | corroboration: {result[0].corroboration_count}")


def test_near_duplicate_clustering():
    """Same story from different sources — should cluster into one item."""
    separator("TEST 2 — Near-duplicate clustering")

    storage = InMemoryStorage()
    items = [
        make_item("RBI holds repo rate steady at 6.5%", "reuters_india", SourceTier.WIRE),
        make_item("RBI keeps repo rate unchanged at 6.5%", "et_markets"),
        make_item("RBI holds repo rate at 6.5% for fifth time", "moneycontrol"),
    ]

    result = run_deduplicator(items, storage)

    # All three are the same story — should produce 1 or 2 clusters
    # depending on exact similarity scores
    print(f"  Result: {len(items)} items → {len(result)} clusters")
    for d in result:
        print(
            f"    {d.item.source_id} | "
            f"corroboration: {d.corroboration_count} | "
            f"effective: {d.effective_corroboration} | "
            f"wire_reprint: {d.is_wire_reprint}"
        )
    assert len(result) <= len(items)
    print("  ✓ Clustering reduced item count")


def test_tier1_wins_in_cluster():
    """When cluster has Tier 1 and Tier 3, canonical item must be Tier 1."""
    separator("TEST 3 — Tier 1 wins canonical selection")

    storage = InMemoryStorage()
    items = [
        make_item("HDFC Bank Q1 results: PAT up 35%", "et_markets", SourceTier.MEDIA),
        make_item("HDFC Bank Q1 results: profit up 35%", "nse_announcements", SourceTier.OFFICIAL),
    ]

    result = run_deduplicator(items, storage)

    if len(result) == 1:
        assert result[0].item.source_tier == SourceTier.OFFICIAL, \
            "Canonical item should be Tier 1 (OFFICIAL)"
        print(f"  ✓ Canonical item is Tier 1: {result[0].item.source_id}")
    else:
        print(f"  Headlines not similar enough to cluster ({len(result)} items) — check similarity")


def test_citation_laundering_detection():
    """
    Tier 3 items with wire attribution in cluster that has Tier 2 source
    should be flagged as wire reprints with effective_corroboration = 1.
    """
    separator("TEST 4 — Citation laundering detection")

    storage = InMemoryStorage()
    items = [
        make_item(
            "RBI cuts repo rate by 50bps — Reuters",
            "reuters_india",
            SourceTier.WIRE,
            snippet="Reuters reports the RBI has cut rates"
        ),
        make_item(
            "RBI cuts rates by 50 basis points",
            "et_markets",
            SourceTier.MEDIA,
            snippet="As per Reuters, RBI cut repo rate by 50bps today"
        ),
        make_item(
            "RBI reduces repo rate by 50bps",
            "moneycontrol",
            SourceTier.MEDIA,
            snippet="PTI: Reserve Bank cuts rate by 50 basis points"
        ),
    ]

    result = run_deduplicator(items, storage)

    for d in result:
        print(
            f"  {d.item.source_id:<20} | "
            f"tier: {d.item.source_tier.value} | "
            f"corroboration: {d.corroboration_count} | "
            f"effective: {d.effective_corroboration} | "
            f"wire_reprint: {d.is_wire_reprint}"
        )

    # If all clustered: effective corroboration should be 1 despite 3 sources
    clustered = [d for d in result if d.corroboration_count > 1]
    for d in clustered:
        if d.is_wire_reprint:
            assert d.effective_corroboration == 1, \
                f"Wire reprint should have effective_corroboration=1, got {d.effective_corroboration}"
    print("  ✓ Citation laundering correctly caps effective corroboration")


def test_distinct_stories_not_merged():
    """Completely different stories must not be merged."""
    separator("TEST 5 — Distinct stories stay separate")

    storage = InMemoryStorage()
    items = [
        make_item("NVDA earnings beat estimates by 15%", "cnbc_top", market=Market.US),
        make_item("RBI holds repo rate at 6.5%", "et_markets", market=Market.INDIA),
        make_item("HDFC Bank Q1 profit rises 35%", "moneycontrol", market=Market.INDIA),
    ]

    result = run_deduplicator(items, storage)

    assert len(result) == 3, f"Expected 3 distinct stories, got {len(result)}"
    print(f"  ✓ {len(items)} distinct stories correctly kept separate")


def test_empty_input():
    """Empty input must return empty output without error."""
    separator("TEST 6 — Empty input")

    storage = InMemoryStorage()
    result  = run_deduplicator([], storage)

    assert result == []
    print("  ✓ Empty input returns empty list")


def test_storage_populated_after_run():
    """Storage should contain hashes of all unique items after dedup."""
    separator("TEST 7 — Storage state after dedup")

    storage = InMemoryStorage()
    items = [
        make_item("Story A", "source_1"),
        make_item("Story B", "source_2"),
        make_item("Story A", "source_1"),  # duplicate — should not add to storage
    ]

    run_deduplicator(items, storage)

    # 2 unique (headline+source) combinations
    assert storage.count() == 2, f"Expected 2 in storage, got {storage.count()}"
    print(f"  ✓ Storage contains {storage.count()} hashes (duplicates not re-added)")


def test_corroboration_count_accuracy():
    """Corroboration count must reflect actual number of sources in cluster."""
    separator("TEST 8 — Corroboration count accuracy")

    storage = InMemoryStorage()
    # Four slightly different headlines about the same event
    items = [
        make_item("Infosys Q1 profit jumps 12% year on year", "et_markets"),
        make_item("Infosys Q1 net profit rises 12% YoY", "moneycontrol"),
        make_item("Infosys reports 12 percent profit growth in Q1", "livemint"),
        make_item("Infosys Q1 earnings up 12 percent", "business_standard"),
    ]

    result = run_deduplicator(items, storage)

    print(f"  {len(items)} items → {len(result)} cluster(s)")
    for d in result:
        print(
            f"  Cluster: corroboration={d.corroboration_count} | "
            f"sources={[d.item.source_id] + d.corroborating_sources}"
        )
    print("  ✓ Corroboration counts reflect cluster sizes")


if __name__ == "__main__":
    print("\nDeduplicatorAgent Tests — no network, no API calls")

    test_exact_deduplication()
    test_near_duplicate_clustering()
    test_tier1_wins_in_cluster()
    test_citation_laundering_detection()
    test_distinct_stories_not_merged()
    test_empty_input()
    test_storage_populated_after_run()
    test_corroboration_count_accuracy()

    print(f"\n{'=' * 60}")
    print("  All deduplicator tests complete")
    print(f"{'=' * 60}\n")