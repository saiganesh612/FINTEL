# pipeline/collector_agent.py
#
# CollectorAgent — the orchestrator for all data collection.
#
# This is not a LangGraph node yet (that comes in M3).
# It's a pure Python implementation of the agent pattern:
#   - Knows the goal: collect all relevant data for this session
#   - Has tools: 7 adapters, each behind a standard interface
#   - Has decision logic: which tools to call depends on session_type
#   - Has resilience: one tool failing never stops the others
#
# Output: CollectorOutput dataclass consumed by DeduplicatorAgent.
# All downstream agents receive this single structured object — never
# raw adapter output directly.

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any
from datetime import timezone, datetime

from pipeline.collectors.alphavantage_collector import collect_alphavantage
from pipeline.collectors.bse_collector import collect_bse
from pipeline.collectors.nse_collector import collect_nse
from pipeline.collectors.price_collector import collect_prices
from pipeline.collectors.rss_collector import collect_rss
from pipeline.collectors.sec_collector import collect_sec
from pipeline.models import RawItem

logger = logging.getLogger(__name__)


@dataclass
class CollectorOutput:
    """
    Structured output from CollectorAgent.
    Everything the rest of the pipeline needs, in one object.

    @dataclass is a Python decorator that auto-generates __init__,
    __repr__, and __eq__ methods based on the field definitions.
    Saves boilerplate and makes the structure self-documenting.
    """
    session_type: str
    collected_at: datetime
    raw_items: List[RawItem]        # all news/announcements items
    price_data: Dict[str, Any]      # OHLCV + fundamentals per ticker
    source_counts: Dict[str, int]   # items per source_id
    errors: List[str] = field(default_factory=list)

    @property
    def total_items(self) -> int:
        return len(self.raw_items)
    
    @property
    def has_data(self) -> bool:
        return self.total_items > 0 or bool(self.price_data)
    

def run_collector(session_type: str) -> CollectorOutput:
    """
    Main entry point. Runs all applicable adapters for the given session.
    Each adapter is wrapped individually — one failure never propagates.

    Adapter selection logic:
      RSS sources      → every session (global news never sleeps)
      NSE collector    → India sessions only (NSE_OPEN, NSE_MID, NSE_CLOSE)
      BSE collector    → India sessions only
      SEC EDGAR        → US sessions only (US_OPEN, US_CLOSE)
      Alpha Vantage    → US sessions only (budget-aware)
      Price data       → every session (yfinance is free and fast)

    This logic lives here, not in each adapter, because the orchestrator
    owns the decision of what runs when. Adapters just do what they're told.
    """
    logger.info(f"CollectorAgent starting — session: {session_type}")
    started_at = datetime.now(timezone.utc)

    all_items:     List[RawItem]  = []
    source_counts: Dict[str, int] = {}
    errors:        List[str]      = []

    # ── Adapter registry ──────────────────────────────────────
    # List of (adapter_name, adapter_function) tuples.
    # All adapters receive session_type and return List[RawItem].
    # Price collector is handled separately (returns dict, not RawItem list).
    NEWS_ADAPTERS = [
        ("RSS",           collect_rss),
        ("NSE",           collect_nse),
        ("BSE",           collect_bse),
        ("SEC EDGAR",     collect_sec),
        ("Alpha Vantage", collect_alphavantage),
    ]

    for adapter_name, adapter_fn in NEWS_ADAPTERS:
        try:
            items = adapter_fn(session_type=session_type)

            # Count items per source_id for the run log
            for item in items:
                source_counts[item.source_id] = source_counts.get(item.source_id, 0) + 1

            all_items.extend(items)
            logger.info(f"[{adapter_name}] returned {len(items)} items")

        except Exception as e:
            # Catch-all safety net — each adapter has its own error handling,
            # but this ensures CollectorAgent itself never crashes.
            error_msg = f"{adapter_name} adapter raised unexpected error: {e}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)

    # ── Price data ────────────────────────────────────────────
    price_data: Dict[str, Any] = {}
    try:
        price_data = collect_prices(session_type)
        logger.info(f"Price data: {len(price_data)} tickers fetched")
    except Exception as e:
        error_msg = f"Price collector raised unexpected error: {e}"
        logger.error(error_msg, exc_info=True)
        errors.append(error_msg)

    # ── Summary log ───────────────────────────────────────────
    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info(
        f"CollectorAgent complete | "
        f"Session: {session_type} | "
        f"Total items: {len(all_items)} | "
        f"Tickers with price data: {len(price_data)} | "
        f"Errors: {len(errors)} | "
        f"Duration: {duration:.1f}s"
    )

    for source_id, count in sorted(source_counts.items()):
        logger.info(f"  {source_id}: {count} items")

    return CollectorOutput(
        session_type  = session_type,
        collected_at  = started_at,
        raw_items     = all_items,
        price_data    = price_data,
        source_counts = source_counts,
        errors        = errors,
    )
