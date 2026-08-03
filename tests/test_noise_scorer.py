# tests/test_noise_scorer.py
# Run: python -m tests.test_noise_scorer

import logging
from datetime import datetime, timezone
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

from pipeline.models import RawItem, SourceTier, Market
from pipeline.storage.base import compute_hash
from pipeline.agents.deduplicator import DeduplicatedItem
from pipeline.agents.relevance_filter import FilteredItem, RelevanceTag
from pipeline.agents.noise_scorer import (
    run_noise_scorer, _score_item, _load_keyword_to_ticker_map,
    ScoredItem, NOISE_FLOOR
)


def make_filtered(
    headline:            str,
    source_id:            str = "et_markets",
    tier:                  SourceTier = SourceTier.MEDIA,
    snippet:               str = "",
    market:                Market = Market.INDIA,
    category:              Optional[str] = None,
    corroboration_count:   int = 1,
    matched_keywords:      Optional[List[str]] = None,
    tag:                   RelevanceTag = RelevanceTag.RELEVANT,
) -> FilteredItem:
    item = RawItem(
        source_id    = source_id,
        source_name  = source_id,
        source_tier  = tier,
        headline     = headline,
        snippet      = snippet,
        url          = "https://example.com",
        market       = market,
        category     = category,
        published_at = datetime.now(timezone.utc),
    )
    deduped = DeduplicatedItem(
        item                  = item,
        item_hash             = compute_hash(headline, source_id),
        corroboration_count   = corroboration_count,
        corroborating_sources = [],
        is_wire_reprint       = False,
    )
    return FilteredItem(
        deduped          = deduped,
        tag              = tag,
        matched_keywords = matched_keywords or [],
        sector_impact    = [],
    )


def separator(title):
    print(f"\n{'=' * 55}\n  {title}\n{'=' * 55}")


# ── Base tier scoring — bypasses the noise floor by calling
# _score_item directly, since MEDIA and COMMUNITY tiers alone
# score below the 2.0 floor and would otherwise be silently
# dropped before we could inspect their exact score. ──────────

def test_tier_base_scores():
    separator("TEST 1 — Tier base scores")
    kw_map = _load_keyword_to_ticker_map()

    cases = [
        (SourceTier.OFFICIAL,  3),
        (SourceTier.WIRE,      2),
        (SourceTier.MEDIA,     1),
        (SourceTier.COMMUNITY, 0),
    ]
    for tier, expected in cases:
        f = make_filtered("Plain headline with no bonus triggers", tier=tier)
        scored = _score_item(f, kw_map)
        assert scored.noise_score == expected, (
            f"{tier.name}: expected {expected}, got {scored.noise_score} | {scored.score_reasons}"
        )
    print("  ✓ OFFICIAL=3, WIRE=2, MEDIA=1, COMMUNITY=0")


def test_corroboration_bonus():
    separator("TEST 2 — Corroboration bonus")
    kw_map = _load_keyword_to_ticker_map()

    f_none = make_filtered("Headline one", tier=SourceTier.WIRE, corroboration_count=1)
    f_med  = make_filtered("Headline two", tier=SourceTier.WIRE, corroboration_count=2)
    f_high = make_filtered("Headline three", tier=SourceTier.WIRE, corroboration_count=3)

    s_none = _score_item(f_none, kw_map)
    s_med  = _score_item(f_med, kw_map)
    s_high = _score_item(f_high, kw_map)

    assert s_none.noise_score == 2, f"Expected 2 (no bonus), got {s_none.noise_score}"
    assert s_med.noise_score == 3,  f"Expected 3 (+1), got {s_med.noise_score}"
    assert s_high.noise_score == 4, f"Expected 4 (+2), got {s_high.noise_score}"
    print("  ✓ corroboration 1→+0, 2→+1, 3→+2")


def test_numbers_bonus():
    separator("TEST 3 — Numbers/percentage detection")
    kw_map = _load_keyword_to_ticker_map()

    with_pct = make_filtered("Revenue up 24.5% this quarter", tier=SourceTier.WIRE)
    with_cr  = make_filtered("Company raises ₹500 crore via QIP", tier=SourceTier.WIRE)
    without  = make_filtered("Company reports quarterly performance", tier=SourceTier.WIRE)

    assert _score_item(with_pct, kw_map).noise_score == 3, "Expected 2 base +1 numbers = 3"
    assert _score_item(with_cr, kw_map).noise_score == 3, "Expected 2 base +1 numbers = 3"
    assert _score_item(without, kw_map).noise_score == 2, "Expected 2 base, no numbers bonus"
    print("  ✓ Percentage and crore-figure text both trigger +1; plain text does not")


def test_filing_reference_bonus():
    separator("TEST 4 — Filing/official release reference")
    kw_map = _load_keyword_to_ticker_map()

    with_ref = make_filtered(
        "Company disclosure",
        tier=SourceTier.WIRE,
        snippet="Filed pursuant to Regulation 30 of SEBI LODR"
    )
    without = make_filtered("Company announcement", tier=SourceTier.WIRE, snippet="General update")

    assert _score_item(with_ref, kw_map).noise_score == 3, "Expected 2 base +1 filing ref = 3"
    assert _score_item(without, kw_map).noise_score == 2, "Expected 2 base, no bonus"
    print("  ✓ 'pursuant to' triggers +1; plain text does not")


def test_sensational_penalty():
    separator("TEST 5 — Sensational language penalty")
    kw_map = _load_keyword_to_ticker_map()

    sensational = make_filtered("Stock set to explode after this news", tier=SourceTier.OFFICIAL)
    plain       = make_filtered("Stock rises after earnings report", tier=SourceTier.OFFICIAL)

    assert _score_item(sensational, kw_map).noise_score == 1, "Expected 3 base -2 = 1"
    assert _score_item(plain, kw_map).noise_score == 3, "Expected 3 base, no penalty"
    print("  ✓ Sensational keyword triggers -2; plain text does not")


def test_anonymous_source_penalty():
    separator("TEST 6 — Anonymous source penalty")
    kw_map = _load_keyword_to_ticker_map()

    anon = make_filtered(
        "Company may consider stake sale",
        tier=SourceTier.WIRE,
        snippet="according to people familiar with the matter"
    )
    named = make_filtered(
        "Company confirms stake sale",
        tier=SourceTier.WIRE,
        snippet="CEO confirmed the deal in an interview"
    )

    assert _score_item(anon, kw_map).noise_score == 1, "Expected 2 base -1 = 1"
    assert _score_item(named, kw_map).noise_score == 2, "Expected 2 base, no penalty"
    print("  ✓ 'people familiar with' triggers -1; named-source text does not")


def test_risk_flag_penalty():
    separator("TEST 7 — risk_flag ticker penalty (OQ2)")
    kw_map = _load_keyword_to_ticker_map()

    flagged = make_filtered(
        "Adani Green announces new project",
        tier=SourceTier.WIRE,
        matched_keywords=["adani green"],
    )
    unflagged = make_filtered(
        "Reliance announces new project",
        tier=SourceTier.WIRE,
        matched_keywords=["reliance"],
    )

    s_flagged = _score_item(flagged, kw_map)
    s_unflagged = _score_item(unflagged, kw_map)

    assert s_flagged.noise_score == 1, f"Expected 2 base -1 risk_flag = 1, got {s_flagged.noise_score}"
    assert s_unflagged.noise_score == 2, f"Expected 2 base, no risk_flag, got {s_unflagged.noise_score}"
    assert any("risk_flag" in r for r in s_flagged.score_reasons)
    print("  ✓ ADANIGREEN (has risk_flag) penalized; RELIANCE (no risk_flag) is not")


def test_category_direct_match():
    separator("TEST 8 — Category modifier: direct canonical matches")
    kw_map = _load_keyword_to_ticker_map()

    cases = [
        ("Result",       5),   # Financial Results: 3 base(OFFICIAL) + 3 modifier = 6... see note below
        ("Buyback",      5),
        ("Insider Buy",  5),
        ("Insider Sell", 4),
        ("Dividend",     4),
        ("AGM/EGM",      2),
    ]
    for category, _ in cases:
        pass  # placeholder to keep structure explicit before real assertions below

    # Explicit per-category checks (OFFICIAL tier base = 3):
    checks = {
        "Result":       3 + 3,   # Financial Results +3
        "Buyback":      3 + 2,   # Buyback +2
        "Insider Buy":  3 + 2,   # Insider Buy +2
        "Insider Sell": 3 + 1,   # Insider Sell +1
        "Dividend":     3 + 1,   # Dividend +1
        "AGM/EGM":      3 - 1,   # AGM Notice -1 (the bug fixed above)
    }
    for category, expected in checks.items():
        f = make_filtered(
            "Corporate filing headline",
            tier=SourceTier.OFFICIAL,
            category=category,
        )
        scored = _score_item(f, kw_map)
        assert scored.noise_score == expected, (
            f"{category}: expected {expected}, got {scored.noise_score} | {scored.score_reasons}"
        )
    print("  ✓ Result→+3, Buyback→+2, Insider Buy→+2, Insider Sell→+1, Dividend→+1, AGM/EGM→-1")


def test_category_ambiguous_deferred():
    separator("TEST 9 — Ambiguous categories score 0 modifier (M3 LLM-deferred)")
    kw_map = _load_keyword_to_ticker_map()

    for category in ["Acquisition", "Regulatory", "Credit Rating", "Change in Directors"]:
        f = make_filtered("Regulatory filing", tier=SourceTier.OFFICIAL, category=category)
        scored = _score_item(f, kw_map)
        assert scored.noise_score == 3, (
            f"{category}: expected 3 (base only, no modifier), got {scored.noise_score}"
        )
        assert any("ambiguous" in r for r in scored.score_reasons), scored.score_reasons
    print("  ✓ Acquisition/Regulatory/Credit Rating/Director Change all score base-only, flagged ambiguous")


def test_category_unmapped_gap():
    separator("TEST 10 — Unmapped category logs a gap, scores 0 modifier")
    kw_map = _load_keyword_to_ticker_map()

    f = make_filtered(
        "Company announces management restructuring",
        tier=SourceTier.OFFICIAL,
        category="Change in Management",   # deliberately not in NATIVE_TO_CANONICAL_MAP
    )
    scored = _score_item(f, kw_map)
    assert scored.noise_score == 3, f"Expected 3 (base only), got {scored.noise_score}"
    assert any("unmapped (gap)" in r for r in scored.score_reasons), scored.score_reasons
    print("  ✓ 'Change in Management' (genuine gap) scores base-only, logged for later weighting")


def test_category_modifier_tier1_only():
    separator("TEST 11 — Category modifier never applies outside Tier 1")
    kw_map = _load_keyword_to_ticker_map()

    f = make_filtered(
        "Financial results reported by wire service",
        tier=SourceTier.WIRE,          # not OFFICIAL
        category="Financial Results",   # would be +3 if this were Tier 1
    )
    scored = _score_item(f, kw_map)
    assert scored.noise_score == 2, f"Expected 2 (WIRE base only, no category modifier), got {scored.noise_score}"
    print("  ✓ Tier 2 item with a category set still gets no category modifier")


def test_score_clamping():
    separator("TEST 12 — Score clamps to [0, 10]")
    kw_map = _load_keyword_to_ticker_map()

    # Stack every available bonus — this is the realistic ceiling
    # (3 tier + 2 corroboration + 1 numbers + 1 filing ref + 3 category = 10).
    # The cap can't currently be exceeded by any legal combination of
    # rules, so this confirms the maximum lands exactly at 10, not over.
    maxed = make_filtered(
        "Merger approved, deal worth ₹5000 crore",
        tier=SourceTier.OFFICIAL,
        snippet="Filed pursuant to Regulation 30",
        corroboration_count=3,
        category="Merger/Amalgamation",
    )
    assert _score_item(maxed, kw_map).noise_score == 10.0, _score_item(maxed, kw_map).score_reasons

    # Stack every available penalty on the lowest tier — should clamp
    # at 0, never go negative.
    minned = make_filtered(
        "Stock set to explode — sources close to the company say secret merger guaranteed",
        tier=SourceTier.COMMUNITY,
        matched_keywords=["adani green"],
    )
    assert _score_item(minned, kw_map).noise_score == 0.0, _score_item(minned, kw_map).score_reasons
    print("  ✓ Max realistic combination = 10.0 exactly; heavy penalties clamp at 0.0, never negative")


def test_noise_floor_drop():
    separator("TEST 13 — Items below noise floor are dropped")

    items = [
        make_filtered("Well-corroborated official filing", tier=SourceTier.OFFICIAL, corroboration_count=3),
        make_filtered("Weak community post", tier=SourceTier.COMMUNITY, corroboration_count=1),
    ]
    result = run_noise_scorer(items)

    assert len(result) == 1, f"Expected 1 (COMMUNITY item below floor {NOISE_FLOOR}), got {len(result)}"
    assert result[0].item.headline == "Well-corroborated official filing"
    print(f"  ✓ 1/2 kept — item scoring below {NOISE_FLOOR} correctly dropped")


def test_sorted_descending():
    separator("TEST 14 — Results sorted descending by noise_score")

    items = [
        make_filtered("Low score wire item", tier=SourceTier.WIRE, corroboration_count=1),
        make_filtered("High score official filing", tier=SourceTier.OFFICIAL, corroboration_count=3, category="Financial Results"),
        make_filtered("Mid score wire item", tier=SourceTier.WIRE, corroboration_count=2),
    ]
    result = run_noise_scorer(items)

    scores = [r.noise_score for r in result]
    assert scores == sorted(scores, reverse=True), f"Not sorted descending: {scores}"
    assert result[0].item.headline == "High score official filing"
    print(f"  ✓ Sorted descending: {scores}")


def test_empty_input():
    separator("TEST 15 — Empty input")
    result = run_noise_scorer([])
    assert result == []
    print("  ✓ Empty input returns empty list")


if __name__ == "__main__":
    print("\nNoiseScorerAgent Tests — no network, no API calls")

    test_tier_base_scores()
    test_corroboration_bonus()
    test_numbers_bonus()
    test_filing_reference_bonus()
    test_sensational_penalty()
    test_anonymous_source_penalty()
    test_risk_flag_penalty()
    test_category_direct_match()
    test_category_ambiguous_deferred()
    test_category_unmapped_gap()
    test_category_modifier_tier1_only()
    test_score_clamping()
    test_noise_floor_drop()
    test_sorted_descending()
    test_empty_input()

    print(f"\n{'=' * 55}")
    print("  All noise scorer tests complete")
    print(f"{'=' * 55}\n")