# pipeline/collectors/alphavantage_collector.py
#
# Alpha Vantage News Sentiment Collector
#
# Endpoint: NEWS_SENTIMENT
# Free tier: 25 API calls/day total across all endpoints.
#
# STRATEGY (finalised after live testing):
#   - 1 call per session (US_OPEN + US_CLOSE) = 2 calls/day for news
#   - No ticker filter in API params — AV uses AND logic for comma-separated
#     tickers, not OR. A batch of 5 returns articles mentioning ALL 5 simultaneously.
#     Confirmed in live testing: batches return 0 articles.
#   - limit=1000 (verified AV max) — 3 days of market news rarely hits this limit
#   - time_from window: 12h per session, 72h on Monday US_OPEN (weekend gap)
#   - Filter in code via _articles_to_raw_items() using watchlist tickers
#   - Remaining ~23 calls/day available for TA indicators in M4
#
# BUDGET ALLOCATION:
#   News (this collector): 2 calls/day
#   Technical indicators (M4): ~23 calls/day
#   Reserve: never touch below 0
#
# Sentiment score: -1.0 (very bearish) to +1.0 (very bullish)
# Relevance score:  0.0 (not about ticker) to 1.0 (entirely about it)
#
# FILTERS applied in code (not API params):
#   relevance_score >= 0.3  — article meaningfully mentions the ticker
#   abs(sentiment_score) >= 0.15 — not neutral noise

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import requests

from pipeline.models import Market, RawItem, SourceTier

logger = logging.getLogger(__name__)

CONFIG_DIR  = Path(__file__).parent.parent / "config"
BUDGET_FILE = CONFIG_DIR / "av_budget.json"

AV_BASE_URL = "https://www.alphavantage.co/query"

# ── Budget constants ───────────────────────────────────────────
BUDGET_FOR_NEWS       = 2    # 1 call per US session (US_OPEN + US_CLOSE)
BUDGET_FOR_TECHNICALS = 23   # reserved for TA
BUDGET_RESERVE        = 0    # implicit — never go below 0 total

# ── Article quality filters ────────────────────────────────────
RELEVANCE_MIN   = 0.3   # below = article barely mentions the ticker
SENTIMENT_NOISE = 0.15  # within ±0.15 of zero = neutral, no signal value

# ── Rate limiting ──────────────────────────────────────────────
CALL_INTERVAL_SECONDS = 1.2  # AV free tier: 1 request/second hard limit

# ── Topic labels with high trading signal value ────────────────
HIGH_SIGNAL_TOPICS = {
    "earnings",
    "ipo",
    "mergers_and_acquisitions",
    "financial_markets",
    "economy_fiscal",
    "economy_monetary",
    "economy_macro",
    "technology",
}


# ─────────────────────────────────────────────
# BUDGET MANAGEMENT
# ─────────────────────────────────────────────

def _load_budget() -> dict:
    """
    Load today's API call count from tracking file.
    Resets automatically when calendar date changes.
    Returns: {"date": "YYYY-MM-DD", "calls_used": N}

    av_budget.json persists between local runs so you never accidentally
    exhaust your daily budget across multiple test runs.
    In GitHub Actions each run is ephemeral — budget resets naturally.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if BUDGET_FILE.exists():
        try:
            with open(BUDGET_FILE, "r") as f:
                budget = json.load(f)
            if budget.get("date") == today:
                return budget
        except Exception:
            pass

    # New day or corrupt/missing file — reset
    return {"date": today, "calls_used": 0}


def _save_budget(budget: dict) -> None:
    """Persist updated call count to file."""
    BUDGET_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(BUDGET_FILE, "w") as f:
        json.dump(budget, f)


def _budget_remaining(budget: dict) -> int:
    """How many news calls are still available today."""
    return max(0, BUDGET_FOR_NEWS - budget["calls_used"])


# ─────────────────────────────────────────────
# DATE / TIME UTILITIES
# ─────────────────────────────────────────────

def _av_time_from(hours_back: int) -> str:
    """
    Build time_from string in AV's required format: YYYYMMDDTHHMM.
    AV docs confirm this exact format — no separators, T as literal char.
    Example: 20260715T0930

    hours_back: how far back to look from now (UTC).
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    return since.strftime("%Y%m%dT%H%M")


def _resolve_time_window(session_type: str) -> str:
    """
    Determine the correct time_from value per session and day.

    US_OPEN on Monday: 72h back — covers Friday close through weekend.
    US_OPEN other days: 12h back — catches overnight and pre-market news.
    US_CLOSE any day: 12h back — captures the full trading session.

    DeduplicatorAgent handles any overlap between sessions via SHA-256 hashing.
    """
    now       = datetime.now(timezone.utc)
    is_monday = now.weekday() == 0  # Monday = 0 in Python's weekday()

    if session_type == "US_OPEN" and is_monday:
        hours = 72   # Friday news + full weekend
        logger.info("Alpha Vantage: Monday US_OPEN — using 72h window for weekend gap")
    else:
        hours = 12   # normal session window

    return _av_time_from(hours_back=hours)


# ─────────────────────────────────────────────
# SENTIMENT HELPERS
# ─────────────────────────────────────────────

def _classify_sentiment(score: float) -> str:
    """
    Map numeric sentiment score to human-readable label.
    Uses AV's own classification boundaries from official docs.
    Preserves decimal precision — score is stored as-is in raw_payload.
    """
    if score >= 0.35:
        return "Bullish"
    elif score >= 0.15:
        return "Somewhat Bullish"
    elif score <= -0.35:
        return "Bearish"
    elif score <= -0.15:
        return "Somewhat Bearish"
    else:
        return "Neutral"


def _parse_av_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse AV's datetime format: "20240115T093000" (no separators).
    Returns UTC-aware datetime.
    """
    if not dt_str:
        return None
    try:
        dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ─────────────────────────────────────────────
# API CALL
# ─────────────────────────────────────────────

def _fetch_news_sentiment(api_key: str, time_from: str) -> Optional[dict]:
    """
    Single API call to AV NEWS_SENTIMENT endpoint.
    No ticker filter — returns broad market news, filtered in code.
    limit=1000 (verified AV maximum) — rarely hit even across 3 days.

    Handles all three AV error response formats:
      "Note"          — rate limit hit (5 calls/min or 25/day exceeded)
      "Information"   — API key restriction or endpoint access issue
      "Error Message" — invalid parameter or key format

    Returns raw response dict or None on any error.
    """
    params = {
        "function":  "NEWS_SENTIMENT",
        "time_from": time_from,
        "sort":      "LATEST",
        "limit":     "1000",
        "apikey":    api_key,
    }

    try:
        response = requests.get(AV_BASE_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        # AV embeds errors inside JSON body — not in HTTP status codes
        if "Note" in data:
            logger.warning(f"Alpha Vantage: rate limit — {data['Note'][:120]}")
            return None
        if "Information" in data:
            logger.warning(f"Alpha Vantage: restriction — {data['Information'][:120]}")
            return None
        if "Error Message" in data:
            logger.warning(f"Alpha Vantage: error — {data['Error Message'][:120]}")
            return None

        feed_count = len(data.get("feed", []))
        av_total   = data.get("items", "unknown")
        logger.info(f"Alpha Vantage: {feed_count} articles received (AV reports {av_total} total in window)")
        return data

    except requests.exceptions.Timeout:
        logger.warning("Alpha Vantage: request timed out")
        return None
    except Exception as e:
        logger.warning(f"Alpha Vantage: unexpected error — {e}", exc_info=True)
        return None


# ─────────────────────────────────────────────
# ARTICLE → RAWITEM CONVERSION
# ─────────────────────────────────────────────

def _articles_to_raw_items(
    articles:          List[dict],
    watchlist_tickers: set,
) -> List[RawItem]:
    """
    Convert AV article dicts to RawItems, filtering to watchlist tickers.

    Each article contains ticker_sentiment[] — one entry per ticker mentioned.
    We iterate all articles and extract entries matching our watchlist.
    One article can produce multiple RawItems (one per matching ticker).

    Filters applied:
      relevance_score >= RELEVANCE_MIN (0.3)  — ticker meaningfully mentioned
      abs(sentiment_score) >= SENTIMENT_NOISE (0.15) — non-neutral signal

    Scores stored at full decimal precision in raw_payload.
    Not rounded — financial values carry meaning even at 2+ decimal places.
    (Design decision: see project notes on decimal precision)
    """
    items: List[RawItem] = []

    for article in articles:
        headline = (article.get("title") or "").strip()
        if not headline:
            continue

        url           = article.get("url", "")
        summary       = article.get("summary", "")
        published_at  = _parse_av_datetime(article.get("time_published"))
        overall_score = float(article.get("overall_sentiment_score", 0) or 0)

        # Extract high-signal topics (relevance >= 0.5 to avoid weak matches)
        topics = article.get("topics", [])
        high_signal_topic_labels = [
            t.get("topic", "") for t in topics
            if t.get("topic", "") in HIGH_SIGNAL_TOPICS
            and float(t.get("relevance_score", 0) or 0) >= 0.5
        ]

        # Process per-ticker sentiment entries
        ticker_sentiments = article.get("ticker_sentiment", [])

        for ts in ticker_sentiments:
            ticker    = (ts.get("ticker") or "").upper()
            relevance = float(ts.get("relevance_score", 0) or 0)
            sentiment = float(ts.get("ticker_sentiment_score", overall_score) or overall_score)

            # Only process our watchlist tickers
            if ticker not in watchlist_tickers:
                continue

            # Apply quality filters
            if relevance < RELEVANCE_MIN:
                continue

            if abs(sentiment) < SENTIMENT_NOISE:
                continue

            # Use AV's label if available, classify ourselves otherwise
            av_label = (ts.get("ticker_sentiment_label") or "").strip()
            sentiment_label = (
                av_label.replace("-", " ")
                if av_label
                else _classify_sentiment(sentiment)
            )

            enriched_headline = f"[{ticker}] [{sentiment_label}] {headline}"

            snippet = (
                f"AV Sentiment | {ticker} | "
                f"Sentiment: {sentiment_label} ({sentiment:+.4f}) | "
                f"Relevance: {relevance:.4f} | "
                f"Topics: {', '.join(high_signal_topic_labels) or 'general'} | "
                f"{summary[:200]}"
            )

            items.append(RawItem(
                source_id    = "alpha_vantage_news",
                source_name  = "Alpha Vantage News Sentiment",
                source_tier  = SourceTier.MEDIA,
                headline     = enriched_headline,
                snippet      = snippet,
                url          = url,
                published_at = published_at,
                market       = Market.US,
                raw_payload  = {
                    "ticker":            ticker,
                    "sentiment_score":   sentiment,    # full precision, not rounded
                    "relevance_score":   relevance,    # full precision, not rounded
                    "sentiment_label":   sentiment_label,
                    "overall_score":     overall_score,
                    "topics":            high_signal_topic_labels,
                },
            ))

    return items


# ─────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────

def collect_alphavantage(session_type: str) -> List[RawItem]:
    """
    Fetch AV news sentiment for all US watchlist tickers.
    Runs at both US_OPEN and US_CLOSE — 1 call per session, 2 calls/day total.

    Time windows:
      Monday US_OPEN : 72h back — catches Friday + weekend news
      Other US_OPEN  : 12h back — overnight and pre-market news
      US_CLOSE       : 12h back — full trading session coverage

    No ticker filter in API call — broad news returned, filtered in code.
    This avoids AV's AND logic for comma-separated tickers (confirmed bug
    in live testing: batches of multiple tickers return 0 articles).
    """
    if session_type not in {"US_OPEN", "US_CLOSE"}:
        return []

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
    if not api_key:
        logger.warning("Alpha Vantage: ALPHA_VANTAGE_API_KEY not set — skipping")
        return []

    # Load US watchlist tickers for in-code filtering
    watchlist_path = CONFIG_DIR / "watchlist.json"
    with open(watchlist_path, "r") as f:
        watchlist = json.load(f)
    us_tickers = {item["ticker_display"] for item in watchlist.get("us", [])}

    # Budget check
    budget    = _load_budget()
    remaining = _budget_remaining(budget)

    if remaining <= 0:
        logger.warning(
            f"Alpha Vantage: daily news budget exhausted "
            f"({budget['calls_used']}/{BUDGET_FOR_NEWS} used) — skipping"
        )
        return []

    # Resolve time window based on session and day of week
    time_from = _resolve_time_window(session_type)
    logger.info(
        f"Alpha Vantage: {session_type} | "
        f"time_from: {time_from} | "
        f"budget remaining: {remaining} | "
        f"filtering for {len(us_tickers)} US tickers in code"
    )

    all_items: List[RawItem] = []

    try:
        data = _fetch_news_sentiment(api_key=api_key, time_from=time_from)

        # Respect 1 req/second rate limit
        time.sleep(CALL_INTERVAL_SECONDS)

        if data is None:
            logger.warning("Alpha Vantage: no data returned — budget incremented, skipping")
            return []

        articles  = data.get("feed", [])
        all_items = _articles_to_raw_items(
            articles          = articles,
            watchlist_tickers = us_tickers,
        )

        # Log coverage — which tickers got articles vs which got nothing
        tickers_found = {
            item.raw_payload.get("ticker")
            for item in all_items
            if item.raw_payload.get("ticker")
        }
        tickers_missing = us_tickers - tickers_found

        logger.info(
            f"Alpha Vantage: {len(all_items)} relevant items from "
            f"{len(articles)} articles | "
            f"Coverage: {len(tickers_found)}/{len(us_tickers)} tickers"
        )
        if tickers_missing:
            logger.info(
                f"Alpha Vantage: no coverage this session for: "
                f"{sorted(tickers_missing)} "
                f"(quiet news day for these tickers — RSS Tier 3 covers them)"
            )

    except Exception as e:
        logger.warning(f"Alpha Vantage: unexpected error — {e}", exc_info=True)

    finally:
        # Always increment budget — call was made regardless of result
        budget["calls_used"] += 1
        _save_budget(budget)
        logger.info(
            f"Alpha Vantage: budget used today: "
            f"{budget['calls_used']}/{BUDGET_FOR_NEWS}"
        )

    return all_items