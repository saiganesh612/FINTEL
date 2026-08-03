# pipeline/agents/relevance_filter.py
#
# Filters deduplicated news items down to those relevant to our
# watchlist or to macro events that affect the broader market.
#
# Four passes run in order. An item is kept if it matches ANY pass.
# An item that matches no pass is permanently dropped — no LLM call,
# no second chance. This is intentional: false drops are recoverable
# (the event will appear in future sessions or from other sources),
# but false keeps pollute every downstream agent with noise.
#
# Pass 1 — Ticker keyword match:
#   Case-insensitive WHOLE-WORD match against every keyword in watchlist.json.
#   Catches company names, ticker symbols, product names, and brand names.
#   Tags: RELEVANT
#
# Pass 2 — Sector keyword match:
#   Matches against sector names from watchlist.json sector_weights.
#   Catches sector-wide news ("IT sector", "pharma stocks") that doesn't
#   name a specific ticker but affects all companies in that sector.
#   Tags: RELEVANT
#
# Pass 3 — Tier 1 macro source:
#   Any item from RBI, Fed, SEBI is macro-relevant by definition.
#   These sources don't publish stock-specific news — everything they
#   publish has broad market implications.
#   Tags: MACRO
#
# Pass 4 — Macro keyword match:
#   Catches macro events from Tier 2/3 sources that don't mention
#   specific tickers but move entire markets. Rate decisions, inflation
#   prints, GDP data, crude oil, currency movements.
#   Tags: MACRO
#
# MACRO-tagged items receive a sector_impact list in FilteredItem,
# mapping the macro event to the sectors and tickers it affects.
# This replaces the peer_tickers lookup for macro items — instead of
# "HDFCBANK's peers are ICICIBANK and SBIN", a rate cut maps to
# "Banking, NBFC, Real Estate, Infrastructure" sectors.

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, List, Pattern

from pipeline.agents.deduplicator import DeduplicatedItem
from pipeline.models.raw_item import RawItem

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent / "config"
WATCHLIST_PATH = CONFIG_DIR / "watchlist.json"
SOURCES_PATH = CONFIG_DIR / "sources.json"

class RelevanceTag(str, Enum):
    RELEVANT = "RELEVANT"  # matches a watchlist ticker or sector
    MACRO = "MACRO"        # broad market event, no specific ticker


# ── Macro source IDs ───────────────────────────────────────────
# Items from these sources are macro-relevant regardless of content.
# These are Tier 1 official government/regulatory sources that only
# publish policy decisions and regulatory actions.
MACRO_SOURCE_IDS = {
    "rbi_press",
    "fed_press",
    "sebi_disclosures"
}

# ── India macro keywords ───────────────────────────────────────
# Events that move the entire Indian market regardless of which
# company is mentioned.
INDIAN_MACRO_KEYWORDS = {
    "repo rate", "reverse repo", "crr", "slr",
    "monetary policy", "mpc", "rbi",
    "inflation", "cpi", "wpi",
    "gdp", "gross domestic product",
    "monsoon", "kharif", "rabi",
    "union budget", "fiscal deficit", "current account deficit",
    "rupee", "usdinr", "inr",
    "crude oil", "brent", "wti",
    "fii", "dii", "fpi",
    "sebi", "nifty", "sensex",
    "interest rate", "rate hike", "rate cut",
}

# ── US macro keywords ──────────────────────────────────────────
US_MACRO_KEYWORDS = {
    "fed funds rate", "federal reserve", "fomc",
    "interest rate", "rate hike", "rate cut",
    "inflation", "cpi", "pce",
    "payrolls", "unemployment", "nonfarm",
    "yield curve", "treasury", "10-year yield",
    "dxy", "dollar index",
    "crude oil", "brent", "wti",
    "gdp", "recession",
    "tariff", "trade war", "sanctions",
    "s&p 500", "nasdaq", "dow jones",
}

ALL_MACRO_KEYWORDS = INDIAN_MACRO_KEYWORDS | US_MACRO_KEYWORDS

# ── Sector-to-tickers impact map ───────────────────────────────
# Maps macro event keywords to the sectors they most directly affect.
# TaggerAgent uses this to surface affected tickers for MACRO items
# without needing a ticker match in the article text.
MACRO_SECTOR_IMPACT = {
    "repo rate":        ["Banking", "Fintech & NBFC", "Infrastructure", "Conglomerate"],
    "reverse repo":     ["Banking", "Fintech & NBFC"],
    "rate hike":        ["Banking", "Fintech & NBFC", "Infrastructure"],
    "rate cut":         ["Banking", "Fintech & NBFC", "Infrastructure", "Auto & EV"],
    "interest rate":    ["Banking", "Fintech & NBFC", "Infrastructure"],
    "monetary policy":  ["Banking", "Fintech & NBFC", "Infrastructure"],
    "inflation":        ["FMCG", "Auto & EV", "Infrastructure"],
    "cpi":              ["FMCG", "Banking"],
    "gdp":              ["Infrastructure", "Banking", "Conglomerate"],
    "crude oil":        ["Conglomerate", "Auto & EV", "Infrastructure"],
    "rupee":            ["Information Technology", "Pharma & Healthcare", "Conglomerate"],
    "usdinr":           ["Information Technology", "Pharma & Healthcare"],
    "fii":              ["Banking", "Information Technology", "AI & Semiconductors (US)"],
    "monsoon":          ["FMCG", "Infrastructure", "Pharma & Healthcare"],
    "union budget":     ["Infrastructure", "Defence", "Banking", "Conglomerate"],
    "tariff":           ["Electronics & EMS", "Auto & EV", "AI & Semiconductors (US)"],
    "fed funds rate":   ["Banking", "AI & Semiconductors (US)", "Healthcare (US)"],
    "fomc":             ["Banking", "AI & Semiconductors (US)"],
    "yield curve":      ["Banking", "Infrastructure"],
}


def _compile_keyword_patterns(keywords: set) -> Dict[str, Pattern]:
    """
    Compile each keyword into a word-boundary regex pattern once.

    \\b matches only at a transition between a word character and a
    non-word character, so \\bLLY\\b matches "LLY reported earnings"
    but refuses to match inside "Bollywood" — there's no boundary
    between the 'o' and 'l' in "Bo|lly|wood", both are word chars.

    re.escape handles keywords containing regex-special characters
    that appear naturally in this data — "&" (L&T), "-" (10-year
    yield), "(" / ")" (AI & Semiconductors (US)) — so they're matched
    as literal text, not interpreted as regex syntax.
    """
    return {
        kw: re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
        for kw in keywords
    }


# Macro keywords are a fixed module-level set, so compile once at
# import time — every pipeline run reuses the same compiled patterns.
_MACRO_KEYWORD_PATTERNS: Dict[str, Pattern] = _compile_keyword_patterns(ALL_MACRO_KEYWORDS)


@dataclass
class FilteredItem:
    """
    A news item that passed the relevance filter, tagged with
    why it was kept and what market context it carries.
    """
    deduped: DeduplicatedItem
    tag: RelevanceTag
    matched_keywords: List[str] = field(default_factory=list)
    sector_impact: List[str] = field(default_factory=list)

    @property
    def item(self) -> RawItem:
        return self.deduped.item

    @property
    def effective_corroboration(self) -> int:
        return self.deduped.effective_corroboration


def _load_watchlist_keywords() -> tuple[set, set]:
    """
    Load ticker keywords and sector names from watchlist.json.
    Returns (ticker_keywords, sector_names) as lowercase sets.
    """
    with open(WATCHLIST_PATH, "r") as f:
        watchlist = json.load(f)

    ticker_keywords: set = set()
    for market in ("india", "us"):
        for ticker in watchlist.get(market, []):
            for kw in ticker.get("keywords", []):
                ticker_keywords.add(kw.lower())

    sector_names: set = set()
    for sector_name in watchlist.get("sector_weights", {}).keys():
        sector_names.add(sector_name.lower())

    return ticker_keywords, sector_names


def _find_macro_sectors(text: str) -> tuple[List[str], List[str]]:
    """
    Find macro keywords present in text (whole-word match) and
    return matched keywords and their associated sector impact list.
    """
    matched_kws: List[str] = []
    sectors_hit: set = set()

    for kw, pattern in _MACRO_KEYWORD_PATTERNS.items():
        if pattern.search(text):
            matched_kws.append(kw)
            for sector in MACRO_SECTOR_IMPACT.get(kw, []):
                sectors_hit.add(sector)

    return matched_kws, sorted(sectors_hit)


def run_relevance_filter(items: List[DeduplicatedItem]) -> List[FilteredItem]:
    """
    Filter deduplicated items to those relevant to the watchlist
    or to macro events affecting the broader market.

    Items that pass no filter pass are permanently dropped.
    Items that pass are tagged RELEVANT or MACRO and carry
    matched keywords and sector impact for downstream agents.

    Args:
        items: Deduplicated items from DeduplicatorAgent

    Returns:
        List[FilteredItem] — subset of input that passed at least one pass
    """
    if not items:
        return []

    logger.info(f"RelevanceFilterAgent: processing {len(items)} deduplicated items")
    ticker_keywords, sector_names = _load_watchlist_keywords()

    # Compiled once per run, reused across every item in the loop below —
    # not recompiled per item.
    ticker_patterns = _compile_keyword_patterns(ticker_keywords)
    sector_patterns = _compile_keyword_patterns(sector_names)

    kept: List[FilteredItem] = []
    dropped = 0
    dropped_by_source: Dict[str, int] = {}
    kept_by_source: Dict[str, int] = {}
    pass_counts = {"sector": 0, "ticker": 0, "macro_source": 0, "macro_keyword": 0}

    for deduped in items:
        item = deduped.item
        source_id = item.source_id
        search_text = f"{item.headline} {item.snippet}".lower()

        matched_keywords: List[str] = []
        sector_impact: List[str] = []
        tag: Optional[RelevanceTag] = None

        # Pass 1: Ticker keyword match
        if tag is None:
            hits = [kw for kw, pattern in ticker_patterns.items() if pattern.search(search_text)]
            if hits:
                tag = RelevanceTag.RELEVANT
                matched_keywords = hits
                pass_counts["ticker"] += 1

        # Pass 2: Sector keyword match
        if tag is None:
            hits = [s for s, pattern in sector_patterns.items() if pattern.search(search_text)]
            if hits:
                tag = RelevanceTag.RELEVANT
                matched_keywords = hits
                pass_counts["sector"] += 1

        # Pass 3: Tier 1 macro source
        if tag is None:
            if source_id in MACRO_SOURCE_IDS:
                tag = RelevanceTag.MACRO
                matched_keywords = [f"source:{source_id}"]
                _, sector_impact = _find_macro_sectors(search_text)
                pass_counts["macro_source"] += 1

        # Pass 4: Macro keyword match
        if tag is None:
            macro_kws, sectors = _find_macro_sectors(search_text)
            if macro_kws:
                tag = RelevanceTag.MACRO
                matched_keywords = macro_kws
                sector_impact = sectors
                pass_counts["macro_keyword"] += 1

        # Drop if no pass matched
        if tag is None:
            dropped += 1
            dropped_by_source[source_id] = dropped_by_source.get(source_id, 0) + 1
            continue

        kept.append(FilteredItem(
            deduped=deduped,
            tag=tag,
            matched_keywords=matched_keywords,
            sector_impact=sector_impact
        ))
        kept_by_source[source_id] = kept_by_source.get(source_id, 0) + 1

    logger.info(
        f"RelevanceFilterAgent: {len(items)} in → {len(kept)} kept, {dropped} dropped | "
        f"ticker: {pass_counts['ticker']} | "
        f"sector: {pass_counts['sector']} | "
        f"macro_source: {pass_counts['macro_source']} | "
        f"macro_keyword: {pass_counts['macro_keyword']}"
    )

    if dropped_by_source:
        breakdown = ", ".join(
            f"{sid}: {count}"
            for sid, count in sorted(dropped_by_source.items(), key=lambda x: -x[1])
        )
        logger.info(f"RelevanceFilterAgent: dropped breakdown by source — {breakdown}")

    if kept_by_source:
        kept_breakdown = ", ".join(
            f"{sid}: {count}"
            for sid, count in sorted(kept_by_source.items(), key=lambda x: -x[1])
        )
        logger.info(f"RelevanceFilterAgent: kept breakdown by source — {kept_breakdown}")

    return kept
