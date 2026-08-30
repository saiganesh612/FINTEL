# tests/test_validator.py
# Run: python -m tests.test_validator

import logging
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

from pipeline.models import RawItem, SourceTier, Market
from pipeline.storage.base import compute_hash
from pipeline.agents.deduplicator import DeduplicatedItem
from pipeline.agents.relevance_filter import FilteredItem, RelevanceTag
from pipeline.agents.noise_scorer import ScoredItem
from pipeline.agents.validator import (
    run_validator, CONFIRMED, UNCONFIRMED, SINGLE_SOURCE,
)


def make_scored(
    headline: str,
    source_id: str = "et_markets",
    tier: SourceTier = SourceTier.MEDIA,
    effective_corroboration: int = 1,
    corroboration_count: Optional[int] = None,
    noise_score: float = 5.0,
    market: Market = Market.INDIA,
) -> ScoredItem:
    if corroboration_count is None:
        corroboration_count = effective_corroboration

    item = RawItem(
        source_id=source_id,
        source_name=source_id,
        source_tier=tier,
        headline=headline,
        snippet="",
        url="https://example.com",
        market=market,
        published_at=datetime.now(timezone.utc),
    )
    deduped = DeduplicatedItem(
        item=item,
        item_hash=compute_hash(headline, source_id),
        corroboration_count=corroboration_count,
        corroborating_sources=[],
        is_wire_reprint=False,
        effective_corroboration=effective_corroboration,
    )
    filtered = FilteredItem(
        deduped=deduped,
        tag=RelevanceTag.RELEVANT,
        matched_keywords=["test"],
        sector_impact=[],
    )
    return ScoredItem(filtered=filtered, noise_score=noise_score, score_reasons=["test reason"])


def separator(title):
    print(f"\n{'=' * 55}\n  {title}\n{'=' * 55}")


def test_official_tier_always_confirmed():
    separator("TEST 1 — OFFICIAL tier always CONFIRMED regardless of corroboration")

    items = [
        make_scored("NSE filing single obs", tier=SourceTier.OFFICIAL, effective_corroboration=1),
        make_scored("NSE filing multi obs", tier=SourceTier.OFFICIAL, effective_corroboration=3),
    ]
    output = run_validator(items)

    assert len(output.validated_items) == 2
    assert len(output.pending_deep_check) == 0
    for v in output.validated_items:
        assert v.trust_label == CONFIRMED, f"Expected CONFIRMED, got {v.trust_label}"
    print("  ✓ Both OFFICIAL items CONFIRMED, none routed to pending_deep_check")


def test_wire_single_observation_unconfirmed():
    separator("TEST 2 — WIRE tier, single observation → UNCONFIRMED")

    items = [make_scored("Reuters exclusive", tier=SourceTier.WIRE, effective_corroboration=1)]
    output = run_validator(items)

    assert len(output.validated_items) == 1
    assert output.validated_items[0].trust_label == UNCONFIRMED
    assert len(output.pending_deep_check) == 0
    print("  ✓ Single-observation WIRE item is UNCONFIRMED, not routed to deep-check")


def test_wire_corroborated_confirmed():
    separator("TEST 3 — WIRE tier, effective_corroboration >= 2 → CONFIRMED")

    items = [make_scored("Reuters + PTI both report", tier=SourceTier.WIRE, effective_corroboration=2)]
    output = run_validator(items)

    assert output.validated_items[0].trust_label == CONFIRMED
    print("  ✓ Corroborated WIRE item is CONFIRMED")


def test_media_single_source_routed_to_deep_check():
    separator("TEST 4 — MEDIA tier, single observation → SINGLE_SOURCE, routed to pending_deep_check")

    items = [make_scored("ET Markets exclusive report", tier=SourceTier.MEDIA, effective_corroboration=1)]
    output = run_validator(items)

    assert len(output.validated_items) == 0, "SINGLE_SOURCE items must NOT appear in validated_items"
    assert len(output.pending_deep_check) == 1
    assert output.pending_deep_check[0].trust_label == SINGLE_SOURCE
    print("  ✓ Single-source MEDIA item correctly held out of validated_items")


def test_media_corroborated_unconfirmed():
    separator("TEST 5 — MEDIA tier, effective_corroboration >= 2, no Tier1/2 → UNCONFIRMED")

    items = [make_scored("Two MEDIA outlets report", tier=SourceTier.MEDIA, effective_corroboration=2)]
    output = run_validator(items)

    assert len(output.validated_items) == 1
    assert output.validated_items[0].trust_label == UNCONFIRMED
    assert len(output.pending_deep_check) == 0
    print("  ✓ Multi-MEDIA-source item is UNCONFIRMED (not CONFIRMED — no Tier 1/2 anchor)")


def test_community_single_source_routed():
    separator("TEST 6 — COMMUNITY tier, single observation → SINGLE_SOURCE")

    items = [make_scored("Reddit post", tier=SourceTier.COMMUNITY, effective_corroboration=1)]
    output = run_validator(items)

    assert len(output.pending_deep_check) == 1
    assert output.pending_deep_check[0].trust_label == SINGLE_SOURCE
    print("  ✓ COMMUNITY tier follows same rule as MEDIA (dead path today — Reddit deferred — but correct)")


def test_effective_corroboration_used_not_raw_count():
    separator("TEST 7 — Uses effective_corroboration, NOT raw corroboration_count (citation-laundering aware)")

    # 8 raw sources reprinting one wire story — but effective_corroboration
    # correctly collapsed to 1 by DeduplicatorAgent. Validator must trust
    # effective_corroboration, not the inflated raw count, or the whole
    # citation-laundering fix is undone at this step.
    items = [make_scored(
        "Wire story reprinted 8x",
        tier=SourceTier.WIRE,
        corroboration_count=8,
        effective_corroboration=1,
    )]
    output = run_validator(items)

    assert output.validated_items[0].trust_label == UNCONFIRMED, (
        "BUG: used raw corroboration_count (8, >=2) instead of effective_corroboration (1) — "
        "would wrongly CONFIRM a manufactured-consensus wire reprint"
    )
    print("  ✓ Correctly used effective_corroboration=1, not raw corroboration_count=8")


def test_mixed_batch_split_correctly():
    separator("TEST 8 — Mixed batch: correct 3-way split, counts add up")

    items = [
        make_scored("Official filing", tier=SourceTier.OFFICIAL, effective_corroboration=1),
        make_scored("Wire single", tier=SourceTier.WIRE, effective_corroboration=1),
        make_scored("Wire corroborated", tier=SourceTier.WIRE, effective_corroboration=2),
        make_scored("Media single", tier=SourceTier.MEDIA, effective_corroboration=1),
        make_scored("Media single 2", tier=SourceTier.MEDIA, effective_corroboration=1),
    ]
    output = run_validator(items)

    assert len(output.validated_items) + len(output.pending_deep_check) == len(items), (
        "Every input item must produce exactly one output item somewhere"
    )
    assert len(output.pending_deep_check) == 2, f"Expected 2 SINGLE_SOURCE, got {len(output.pending_deep_check)}"
    assert len(output.validated_items) == 3
    print(f"  ✓ 5 in → {len(output.validated_items)} validated + {len(output.pending_deep_check)} pending deep-check")


def test_validated_item_properties():
    separator("TEST 9 — ValidatedItem properties delegate correctly")

    items = [make_scored("Test headline", tier=SourceTier.OFFICIAL, noise_score=7.5)]
    output = run_validator(items)

    v = output.validated_items[0]
    assert v.item.headline == "Test headline"
    assert v.noise_score == 7.5
    print("  ✓ .item and .noise_score properties delegate through to the underlying ScoredItem")


def test_empty_input():
    separator("TEST 10 — Empty input")
    output = run_validator([])
    assert output.validated_items == []
    assert output.pending_deep_check == []
    print("  ✓ Empty input returns empty ValidatorOutput")


if __name__ == "__main__":
    print("\nValidatorAgent Tests — no network, no API calls")

    test_official_tier_always_confirmed()
    test_wire_single_observation_unconfirmed()
    test_wire_corroborated_confirmed()
    test_media_single_source_routed_to_deep_check()
    test_media_corroborated_unconfirmed()
    test_community_single_source_routed()
    test_effective_corroboration_used_not_raw_count()
    test_mixed_batch_split_correctly()
    test_validated_item_properties()
    test_empty_input()

    print(f"\n{'=' * 55}")
    print("  All validator tests complete")
    print(f"{'=' * 55}\n")