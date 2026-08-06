# pipeline/agents/noise_scorer.py
#
# NoiseScorerAgent — scores each FilteredItem 0.0-10.0 for signal
# quality using static additive rules. Items scoring below the noise
# floor (2.0) are dropped before reaching ValidatorAgent.
#
# LLM escalation for the 4 ambiguous Tier 1 categories (Acquisition,
# Regulatory Order, Credit Rating Change, Director Change) is
# deferred to M3 — NIM isn't wired yet. Per decision: these score
# their base tier weight with NO category modifier (neither positive
# nor negative), so a Tier 1 item is never silently degraded below
# what a correctly-scored item would earn just because we can't yet
# judge direction. Every other category applies its static weight now.
#
# CATEGORY NORMALIZATION: each Tier 1 collector (NSE, BSE, SEC, Form 4,
# SEBI) speaks its own category vocabulary. NATIVE_TO_CANONICAL_MAP
# translates each source's native label (RawItem.category) into this
# table's canonical categories. Native labels with no entry are
# genuine gaps — the weight table doesn't cover them yet. Gaps score
# 0 (never negative) and get logged, so real weights can be set later
# from observed frequency.

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Pattern, List

from pipeline.agents.relevance_filter import FilteredItem
from pipeline.models import RawItem, SourceTier
from pipeline.storage.audit_log import write_audit_log

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent / "config"
WATCHLIST_PATH = CONFIG_DIR / "watchlist.json"

NOISE_FLOOR = 2.0

# Base additive rules
TIER_BASE_SCORE = {
    SourceTier.OFFICIAL:  3,
    SourceTier.WIRE:      2,
    SourceTier.MEDIA:     1,
    SourceTier.COMMUNITY: 0,
}

CORROBORATION_BONUS_HIGH = 2  # effective_corroboration >= 3
CORROBORATION_BONUS_MED = 1   # effective_corroboration == 2
NUMBERS_BONUS = 1
FILING_REFERENCE_BONUS = 1
SENSATIONAL_PENALTY = -2
ANONYMOUS_PENALTY = -1
RISK_FLAG_PENALTY = -1

# Detection patterns 
# - match the word boundry approach used in relevance layer
NUMBER_PATTERN = re.compile(
    r"\d+(\.\d+)?\s*%|₹\s*\d|\$\s*\d|\d+\s*(crore|cr|lakh|million|billion|bn)\b",
    re.IGNORECASE
)

FILING_REFERENCE_PHRASES = [
    "pursuant to", "in compliance with", "disclosure under",
    "filed with", "regulation 30", "regulation 29",
]
FILING_REFERENCE_PATTERNS = [
    re.compile(r"\b" + re.escape(p) + r"\b", re.IGNORECASE)
    for p in FILING_REFERENCE_PHRASES
]

ANONYMOUS_SOURCE_PHRASES = [
    "sources said", "people familiar with", "sources close to",
    "according to sources", "on condition of anonymity",
]
ANONYMOUS_SOURCE_PATTERNS = [
    re.compile(r"\b" + re.escape(p) + r"\b", re.IGNORECASE)
    for p in ANONYMOUS_SOURCE_PHRASES
]

SENSATIONAL_KEYWORDS = [
    "skyrocket", "crash", "explode", "moon", "must buy",
    "hot tip", "secret", "guaranteed",
]
SENSATIONAL_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in SENSATIONAL_KEYWORDS
]

CATEGORY_WEIGHTS: Dict[str, float] = {
    "Financial Results":          3,
    "Merger/Amalgamation":        3,
    "Buyback":                    2,
    "Insider Buy":                2,
    "Insider Sell":               1,   # used by SignalGeneratorAgent's
    "Rights Issue":               1,
    "Dividend":                   1,
    "Outcome of Board Meeting":   1,
    "Board Meeting Notice":       0,
    "AGM Notice":                -1,
    "Award/Grant":                0,   # compensation grant, not a market trade — intentionally neutral
    "Tax Withholding":            0,   # mechanical, non-signal
    "Gift":                       0,   # non-signal by nature
}

# These categories will be given to LLM to analyse the content and provide known category
# so relevant weight can be provided before degrading the news
AMBIGUOUS_CATEGORIES = {
    "Acquisition",            # M&A — acquirer vs target impact differs
    "Regulatory Order",       # direction varies by order type
    "Credit Rating Change",   # upgrade vs downgrade differ
    "Director Change",        # CFO exit ≠ routine board rotation
}

# Native label (RawItem.category, as populated by each collector) →
# canonical category above. Anything NOT in this map is a genuine
# gap — logged and scored 0, not guessed.
NATIVE_TO_CANONICAL_MAP: Dict[str, str] = {
    # NSE
    "Financial Results": "Financial Results",
    "Buyback":            "Buyback",
    # NSE "Board Meeting" and "Corporate Action" intentionally left
    # unmapped — feed content (notice vs outcome; which action
    # subtype) wasn't verified against real data yet. Check
    # test_nse_live.py output before mapping these.
    #
    # NSE SAST Reg 29 is deliberately NOT mapped into "Acquisition"
    # below — see collision note above. Requires the nse_collector.py
    # rename first, then its own mapping decision (currently unmapped
    # → gap, per decision 3).

    # BSE
    "Result":                    "Financial Results",
    "Outcome of Board Meeting":  "Outcome of Board Meeting",
    "Dividend":                  "Dividend",
    "Rights":                    "Rights Issue",
    "AGM/EGM":                   "AGM Notice",
    "Merger/Amalgamation":       "Merger/Amalgamation",
    "Acquisition":               "Acquisition",            # ambiguous — real M&A, LLM-deferred
    "Change in Directors":       "Director Change",        # ambiguous, LLM-deferred
    "Regulatory":                "Regulatory Order",       # ambiguous, LLM-deferred
    "Credit Rating":             "Credit Rating Change",   # ambiguous, LLM-deferred

    # SEC 8-K (native label = ITEM_DECRIPTIONS value from sec_collector.py)
    "Results of Operations / Earnings":                     "Financial Results",
    "Completion of Acquisition or Disposition":              "Acquisition",             # ambiguous, LLM-deferred
    "Departure/Election of Directors or Principal Officers": "Director Change",         # ambiguous, LLM-deferred

    # SEC Form 4 (native label = FORM4_TRANSACTION_LABELS value)
    "Insider Buy":     "Insider Buy",
    "Insider Sell":    "Insider Sell",
    "Award/Grant":     "Award/Grant",
    "Tax Withholding": "Tax Withholding",
    "Gift":            "Gift",

    # SEBI (native label from sebi_collector.py PATH_CATEGORY_MAP)
    "Enforcement Order": "Regulatory Order",   # ambiguous, LLM-deferred
    # "Circular" and "Press Release" intentionally unmapped — no
    # matching bucket in the weight table yet.
}


@dataclass
class ScoredItem:
    filtered: FilteredItem
    noise_score: float
    score_reasons: List[str] = field(default_factory=list)

    @property
    def item(self) -> RawItem:
        return self.filtered.item

    @property
    def tag(self):
        return self.filtered.tag

    @property
    def sector_impact(self) -> List[str]:
        return self.filtered.sector_impact


def _load_keyword_to_ticker_map() -> Dict[str, dict]:
    """
    keyword -> ticker-record lookup from watchlist.json, used only
    for the risk_flag rule (OQ2). Intentionally narrow — does NOT
    resolve sectors/peers/time horizon; that's TaggerAgent's job.
    """
    with open(WATCHLIST_PATH, 'r') as f:
        watchlist = json.load(f)

    mapping: Dict[str, dict] = {}
    for market in ("india", "us"):
        for ticker in watchlist.get(market, []):
            for kw in ticker.get("keywords", []):
                key = kw.lower()
                if key in mapping and mapping[key]["ticker_display"] != ticker["ticker_display"]:
                    logger.warning(
                        f"NoiseScorer: keyword '{key}' is shared between "
                        f"{mapping[key]['ticker_display']} and {ticker['ticker_display']} — "
                        f"risk_flag lookup will silently use whichever was inserted last. "
                        f"Verify this collision is intentional."
                    )
                mapping[key] = ticker

    return mapping


def _matches_any(text: str, patterns: List[Pattern]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _score_item(filtered: FilteredItem, keyword_to_ticker: Dict[str, dict]) -> ScoredItem:
    item = filtered.item
    search_text = f"{item.headline} {item.snippet}"

    score = 0.0
    reasons: List[str] = []

    tier_score = TIER_BASE_SCORE.get(item.source_tier, 0)
    score += tier_score
    reasons.append(f"tier {item.source_tier.name}: +{tier_score}")

    corroboration = filtered.effective_corroboration
    if corroboration >= 3:
        score += CORROBORATION_BONUS_HIGH
        reasons.append(f"corroboration >=3: +{CORROBORATION_BONUS_HIGH}")
    elif corroboration == 2:
        score += CORROBORATION_BONUS_MED
        reasons.append(f"corroboration ==2: +{CORROBORATION_BONUS_MED}")

    if NUMBER_PATTERN.search(search_text):
        score += NUMBERS_BONUS
        reasons.append(f"contains numbers/percentages: +{NUMBERS_BONUS}")

    if _matches_any(search_text, FILING_REFERENCE_PATTERNS):
        score += FILING_REFERENCE_BONUS
        reasons.append(f"references filing/official release: +{FILING_REFERENCE_BONUS}")

    if _matches_any(search_text, SENSATIONAL_PATTERNS):
        score += SENSATIONAL_PENALTY
        reasons.append(f"sensational language: {SENSATIONAL_PENALTY}")

    if _matches_any(search_text, ANONYMOUS_SOURCE_PATTERNS):
        score += ANONYMOUS_PENALTY
        reasons.append(f"anonymous source mentioned: {ANONYMOUS_PENALTY}")

    matched_ticker = None
    for kw in filtered.matched_keywords:
        record = keyword_to_ticker.get(kw.lower())
        if record and record.get("risk_flag"):
            matched_ticker = record
            break

    if matched_ticker:
        score += RISK_FLAG_PENALTY
        reasons.append(f"ticker {matched_ticker['ticker_display']} has risk_flag: {RISK_FLAG_PENALTY}")

    if item.source_tier == SourceTier.OFFICIAL and item.category:
        canonical = NATIVE_TO_CANONICAL_MAP.get(item.category)

        if canonical is None:
            logger.warning(
                f"NoiseScorer: unmapped Tier 1 category '{item.category}' "
                f"from source '{item.source_id}' — scoring 0 modifier. "
                f"Add to NATIVE_TO_CANONICAL_MAP once a real weight is decided."
            )
            reasons.append(f"category '{item.category}' unmapped (gap): +0")

        elif canonical in AMBIGUOUS_CATEGORIES:
            reasons.append(f"category '{canonical}' ambiguous — LLM deferred: +0")

        else:
            modifier = CATEGORY_WEIGHTS.get(canonical, 0)
            score += modifier
            reasons.append(f"category '{canonical}': {'+' if modifier >= 0 else ''}{modifier}")

    score = max(0.0, min(10.0, score))
    return ScoredItem(filtered=filtered, noise_score=score, score_reasons=reasons)


def run_noise_scorer(items: List[FilteredItem], session_type: str = "unknown") -> List[ScoredItem]:
    """
    Score every FilteredItem (RELEVANT and MACRO alike) and return
    them sorted descending by noise_score. Items below NOISE_FLOOR
    are dropped.
    """
    if not items:
        return []

    logger.info(f"NoiseScorerAgent: scoring {len(items)} filtered items")
    keyword_to_ticker = _load_keyword_to_ticker_map()

    scored = [_score_item(item, keyword_to_ticker) for item in items]
    kept = [s for s in scored if s.noise_score >= NOISE_FLOOR]
    dropped = len(scored) - len(kept)

    kept.sort(key=lambda s: s.noise_score, reverse=True)
    logger.info(
        f"NoiseScorerAgent: {len(items)} in → {len(kept)} kept, {dropped} dropped "
        f"(floor={NOISE_FLOOR})"
    )

    audit_records = [
        {
            "item_hash":     s.filtered.deduped.item_hash,
            "source_id":     s.item.source_id,
            "source_tier":   s.item.source_tier.name,
            "headline":      s.item.headline,
            "url":           s.item.url,
            "published_at":  s.item.published_at.isoformat() if s.item.published_at else None,
            "category":      s.item.category,
            "tag":           s.tag.value,
            "noise_score":   s.noise_score,
            "score_reasons": s.score_reasons,
            "decision":      "KEPT" if s.noise_score >= NOISE_FLOOR else "DROPPED_BELOW_FLOOR",
        }
        for s in scored
    ]
    write_audit_log(stage="noise_scorer", session_type=session_type, records=audit_records)
    return kept
