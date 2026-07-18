# tests/test_alphavantage.py
#
# Live test for Alpha Vantage NEWS_SENTIMENT collector.
# Costs real API calls — each run costs from your daily budget.
# Run sparingly. Use only to verify collector works after changes.
#
# Run: python -m tests.test_alphavantage

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)

import requests

API_KEY     = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
AV_URL      = "https://www.alphavantage.co/query"
BUDGET_FILE = Path(__file__).parent.parent / "pipeline" / "config" / "av_budget.json"


def separator(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def test_api_key_present():
    """Fail fast if API key is not set."""
    separator("TEST 0 — API Key Check")

    if not API_KEY:
        print("  ✗ ALPHA_VANTAGE_API_KEY not set in environment")
        print("  Set it in your .env file: ALPHA_VANTAGE_API_KEY=your_key")
        sys.exit(1)

    masked = API_KEY[:4] + "..." + API_KEY[-4:]
    print(f"  ✓ API key present: {masked}")


def test_raw_response_structure():
    """
    Fetch raw response with no ticker filter to verify actual field names.
    Uses limit=1 to cost minimum — just checking structure, not content.
    Costs 1 API call.
    """
    separator("TEST 1 — Raw Response Structure (no ticker filter, limit=1)")

    from pipeline.collectors.alphavantage_collector import _av_time_from

    params = {
        "function":  "NEWS_SENTIMENT",
        "time_from": _av_time_from(hours_back=24),
        "sort":      "LATEST",
        "limit":     "1",
        "apikey":    API_KEY,
    }

    response = requests.get(AV_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    # Check for error responses AV embeds in JSON body
    if "Note" in data:
        print(f"  ✗ Rate limit hit: {data['Note'][:120]}")
        sys.exit(1)
    if "Information" in data:
        print(f"  ✗ API restriction: {data['Information'][:120]}")
        sys.exit(1)
    if "Error Message" in data:
        print(f"  ✗ API error: {data['Error Message'][:120]}")
        sys.exit(1)

    print(f"\n  Top-level keys: {list(data.keys())}")
    print(f"  items field   : {data.get('items', 'MISSING')}")

    feed = data.get("feed", [])
    print(f"  Articles in feed: {len(feed)}")

    if not feed:
        print("  ⚠ Empty feed — no news in last 24h window, try again later")
        return

    article = feed[0]
    print(f"\n  Article field names (actual from API):")
    for key, val in article.items():
        if key == "ticker_sentiment":
            print(f"    {key}: list of {len(val)} entries")
            if val:
                print(f"      Sample entry keys  : {list(val[0].keys())}")
                print(f"      Sample entry values: {val[0]}")
        elif key == "topics":
            print(f"    {key}: list of {len(val)} entries")
            if val:
                print(f"      Sample entry: {val[0]}")
        else:
            print(f"    {key}: {str(val)[:80]}")


def test_budget_tracking():
    """
    Verify budget file exists, is valid JSON, and resets on new day.
    Costs 0 API calls.
    """
    separator("TEST 2 — Budget File Check")

    if not BUDGET_FILE.exists():
        print(f"  ✗ Budget file missing: {BUDGET_FILE}")
        print("  Create it: echo '{\"date\": \"2000-01-01\", \"calls_used\": 0}' > pipeline/config/av_budget.json")
        return

    with open(BUDGET_FILE) as f:
        budget = json.load(f)

    print(f"  Budget file path    : {BUDGET_FILE}")
    print(f"  Budget file contents: {budget}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if budget.get("date") == today:
        from pipeline.collectors.alphavantage_collector import BUDGET_FOR_NEWS
        remaining = max(0, BUDGET_FOR_NEWS - budget.get("calls_used", 0))
        print(f"  Today's calls used  : {budget['calls_used']}/{BUDGET_FOR_NEWS}")
        print(f"  Remaining today     : {remaining}")
        if remaining == 0:
            print("  ⚠ Budget exhausted — reset file to run further tests")
    else:
        print(f"  ✓ Date stale ({budget.get('date')}) — will reset on next collector run")


def test_session_routing():
    """
    Verify only US sessions trigger the collector.
    Costs 0 API calls — returns early before any network call.
    """
    separator("TEST 3 — Session Routing (no API calls)")

    from pipeline.collectors.alphavantage_collector import collect_alphavantage

    for session in ["NSE_OPEN", "NSE_MID", "NSE_CLOSE"]:
        items = collect_alphavantage(session)
        status = "✓" if items == [] else "✗ should have returned []"
        print(f"  {status} {session:<14} → {len(items)} items (expected 0)")

    print("\n  US sessions (US_OPEN, US_CLOSE) skipped — would consume API calls")
    print("  Run TEST 4 to test live US session collection")


def test_full_collector():
    """
    Run the actual collector for US_OPEN session.
    Costs 1 API call (one call, no ticker filter, limit=1000).
    Verifies full pipeline: fetch → parse → RawItem creation.
    """
    separator("TEST 4 — Full Collector (US_OPEN, 1 real call, limit=1000)")

    from pipeline.collectors.alphavantage_collector import collect_alphavantage
    from pipeline.models import SourceTier, Market

    print("\n  ⚠ This test uses 1 of your daily Alpha Vantage calls.")
    print("  Press Ctrl+C to cancel, or wait 3 seconds to proceed...")
    import time
    time.sleep(3)

    items = collect_alphavantage("US_OPEN")
    print(f"\n  Total RawItems produced: {len(items)}")

    if not items:
        print("  ⚠ No items returned. Possible reasons:")
        print("    - Budget exhausted for today (check av_budget.json)")
        print("    - All 1000 articles had neutral sentiment for our 14 US tickers")
        print("    - Relevance score < 0.3 for all watchlist ticker mentions")
        print("    - time_from window too narrow — no articles in last 12h")
        return

    # Structural validation
    errors          = []
    sentiments_seen = set()
    tickers_seen    = set()

    for i, item in enumerate(items):
        if not item.headline:
            errors.append(f"Item {i}: empty headline")
        if item.source_tier != SourceTier.MEDIA:
            errors.append(f"Item {i}: wrong tier {item.source_tier} (expected MEDIA)")
        if item.market != Market.US:
            errors.append(f"Item {i}: wrong market {item.market} (expected US)")
        if item.source_id != "alpha_vantage_news":
            errors.append(f"Item {i}: wrong source_id {item.source_id}")
        if item.fetched_at is None:
            errors.append(f"Item {i}: missing fetched_at")
        if "sentiment_score" not in item.raw_payload:
            errors.append(f"Item {i}: sentiment_score missing from raw_payload")
        if "relevance_score" not in item.raw_payload:
            errors.append(f"Item {i}: relevance_score missing from raw_payload")

        # Extract ticker and sentiment from headline: [TICKER] [Sentiment] text
        try:
            parts = item.headline.split("] [")
            if len(parts) >= 2:
                ticker    = parts[0].lstrip("[")
                sentiment = parts[1].split("]")[0]
                tickers_seen.add(ticker)
                sentiments_seen.add(sentiment)
        except Exception:
            pass

    if errors:
        print(f"\n  ✗ Structural errors ({len(errors)}):")
        for e in errors[:10]:
            print(f"    {e}")
    else:
        print(f"  ✓ All {len(items)} items have valid structure")

    print(f"\n  Watchlist tickers with coverage : {sorted(tickers_seen)}")
    print(f"  Sentiments seen                 : {sorted(sentiments_seen)}")

    # Check which of our 14 US tickers got no coverage
    from pipeline.collectors.alphavantage_collector import collect_alphavantage
    import json
    watchlist_path = Path(__file__).parent.parent / "pipeline" / "config" / "watchlist.json"
    with open(watchlist_path) as f:
        watchlist = json.load(f)
    us_tickers = {item["ticker_display"] for item in watchlist.get("us", [])}
    missing    = us_tickers - tickers_seen

    if missing:
        print(f"\n  ⚠ No coverage this session for: {sorted(missing)}")
        print("    Normal if market is quiet for these tickers today")
    else:
        print(f"\n  ✓ All {len(us_tickers)} US watchlist tickers have at least 1 article")

    print(f"\n  --- Sample items (first 5) ---")
    for item in items[:5]:
        print(f"\n  Headline  : {item.headline[:100]}")
        print(f"  Snippet   : {item.snippet[:120]}")
        print(f"  Score     : {item.raw_payload.get('sentiment_score')}")
        print(f"  Relevance : {item.raw_payload.get('relevance_score')}")
        print(f"  Published : {item.published_at}")


def test_sentiment_distribution():
    """
    Check distribution of sentiment labels in results.
    Helps verify our SENTIMENT_NOISE filter isn't too aggressive.
    Costs 0 API calls — reads from last test run's results.
    Must run after test_full_collector.
    """
    separator("TEST 5 — Sentiment Distribution Check")

    from pipeline.collectors.alphavantage_collector import collect_alphavantage

    # Lightweight call — check if budget available
    with open(BUDGET_FILE) as f:
        budget = json.load(f)

    from pipeline.collectors.alphavantage_collector import BUDGET_FOR_NEWS
    remaining = max(0, BUDGET_FOR_NEWS - budget.get("calls_used", 0))

    if remaining <= 0:
        print("  Budget exhausted — skipping to avoid extra API calls")
        print("  Reset av_budget.json and re-run if needed")
        return

    items = collect_alphavantage("US_CLOSE")

    if not items:
        print("  No items to analyse")
        return

    from collections import Counter
    sentiment_counts = Counter(item.raw_payload.get("sentiment_label") for item in items)
    ticker_counts    = Counter(item.raw_payload.get("ticker") for item in items)

    print(f"\n  Total items: {len(items)}")

    print(f"\n  Sentiment distribution:")
    for label, count in sorted(sentiment_counts.items(), key=lambda x: -x[1]):
        bar = "█" * count
        print(f"    {label:<22} {count:>3}  {bar}")

    print(f"\n  Items per ticker:")
    for ticker, count in sorted(ticker_counts.items(), key=lambda x: -x[1]):
        print(f"    {ticker:<8} {count:>3} items")

    # Score range
    scores = [item.raw_payload.get("sentiment_score", 0) for item in items]
    if scores:
        print(f"\n  Score range: {min(scores):.3f} to {max(scores):.3f}")
        print(f"  Avg score  : {sum(scores)/len(scores):.3f}")


if __name__ == "__main__":
    print("\nAlpha Vantage Collector Live Test")
    print("Strategy: 1 call/session, no ticker filter, limit=1000, filter in code")
    print("Budget:   2 calls/day for news, ~23 remaining for TA (M4)")

    test_api_key_present()       # 0 calls
    test_raw_response_structure()  # 1 call
    test_budget_tracking()         # 0 calls
    test_session_routing()         # 0 calls
    test_full_collector()          # 1 call
    test_sentiment_distribution()  # 1 call

    print(f"\n{'=' * 60}")
    print("  All tests complete")
    print(f"{'=' * 60}\n")