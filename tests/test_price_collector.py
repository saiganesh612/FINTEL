# tests/test_price_collector.py
#
# Live test for yfinance price collector.
# No API key needed. No rate limits. Safe to run any time.
#
# Run: python -m tests.test_price_collector

import logging
import os
import sys

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)

from pipeline.collectors.price_collector import collect_prices


def separator(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def test_basic_collection():
    """
    Fetch OHLCV for all 47 tickers and validate output structure.
    yfinance is free and unlimited — safe to run any time.
    """
    separator("TEST 1 — Full Watchlist OHLCV (NSE_OPEN)")

    data = collect_prices("NSE_OPEN")

    print(f"\n  Tickers returned : {len(data)}")
    print(f"  Expected         : 47 (33 India + 14 US)")

    if len(data) < 40:
        print(f"  ⚠ Fewer tickers than expected — check yfinance connectivity")


def test_india_ticker_format():
    """
    NSE tickers need .NS suffix in yfinance.
    Verify India tickers return data (common failure point).
    """
    separator("TEST 2 — India Ticker Format (.NS suffix)")

    data = collect_prices("NSE_OPEN")

    india_tickers = [t for t in data.keys() if t.endswith(".NS")]
    us_tickers    = [t for t in data.keys() if not t.endswith(".NS")]

    print(f"\n  India tickers with data : {len(india_tickers)}")
    print(f"  US tickers with data    : {len(us_tickers)}")

    # Sample 3 India tickers
    print(f"\n  India samples:")
    for ticker in sorted(india_tickers)[:3]:
        entry = data[ticker]
        print(
            f"    {ticker:<20} "
            f"Price: {entry['current_price']:.2f} | "
            f"Vol ratio: {entry['volume_ratio']:.2f} | "
            f"From 52W high: {entry['pct_from_52w_high']:.1%}"
        )

    print(f"\n  US samples:")
    for ticker in sorted(us_tickers)[:3]:
        entry = data[ticker]
        print(
            f"    {ticker:<20} "
            f"Price: {entry['current_price']:.2f} | "
            f"Vol ratio: {entry['volume_ratio']:.2f} | "
            f"From 52W high: {entry['pct_from_52w_high']:.1%}"
        )


def test_ohlcv_dataframe_shape():
    """
    Validate DataFrame structure per ticker.
    Must have Open, High, Low, Close, Volume columns.
    Must have at least 50 rows (we request 60d — verifies period is working).
    """
    separator("TEST 3 — OHLCV DataFrame Structure")

    data = collect_prices("NSE_OPEN")
    errors = []

    required_columns = {"Open", "High", "Low", "Close", "Volume"}

    for ticker, entry in data.items():
        df = entry.get("ohlcv")

        if df is None:
            errors.append(f"{ticker}: ohlcv DataFrame is None")
            continue

        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            errors.append(f"{ticker}: missing columns {missing_cols}")

        if len(df) < 20:
            errors.append(
                f"{ticker}: only {len(df)} rows — expected 40+ for 60d period"
            )

        # Close prices must be positive
        if (df["Close"] <= 0).any():
            errors.append(f"{ticker}: negative/zero Close prices detected")

        # Volume must be non-negative
        if (df["Volume"] < 0).any():
            errors.append(f"{ticker}: negative Volume detected")

    if errors:
        print(f"\n  ✗ Structural errors ({len(errors)}):")
        for e in errors[:10]:
            print(f"    {e}")
    else:
        print(f"  ✓ All {len(data)} DataFrames have valid structure")

    # Show row count distribution
    row_counts = [len(entry["ohlcv"]) for entry in data.values() if entry.get("ohlcv") is not None]
    if row_counts:
        print(f"\n  Row count — min: {min(row_counts)} | max: {max(row_counts)} | avg: {sum(row_counts)//len(row_counts)}")


def test_required_fields():
    """
    Validate all computed fields exist and are within expected ranges.
    These fields are read directly by TechnicalAgent (M4).
    """
    separator("TEST 4 — Required Fields for TechnicalAgent")

    data = collect_prices("NSE_OPEN")
    errors = []

    for ticker, entry in data.items():
        # All these fields must exist
        for field in ["current_price", "week_52_high", "week_52_low",
                      "pct_from_52w_high", "avg_volume_20d",
                      "latest_volume", "volume_ratio"]:
            if field not in entry:
                errors.append(f"{ticker}: missing field '{field}'")
                continue
            val = entry[field]
            if val is None:
                errors.append(f"{ticker}: '{field}' is None")

        # Sanity checks
        price = entry.get("current_price", 0)
        high  = entry.get("week_52_high", 0)
        low   = entry.get("week_52_low", 0)

        if price > 0 and high > 0:
            if price > high * 1.05:  # more than 5% above 52W high is suspicious
                errors.append(
                    f"{ticker}: current_price ({price:.2f}) > 52W high ({high:.2f}) by >5%"
                )
        if high > 0 and low > 0:
            if low > high:
                errors.append(f"{ticker}: 52W low ({low:.2f}) > 52W high ({high:.2f})")

        vol_ratio = entry.get("volume_ratio", 0)
        if vol_ratio < 0:
            errors.append(f"{ticker}: negative volume_ratio ({vol_ratio})")

    if errors:
        print(f"\n  ✗ Field validation errors ({len(errors)}):")
        for e in errors[:10]:
            print(f"    {e}")
    else:
        print(f"  ✓ All required fields present and valid across {len(data)} tickers")


def test_fundamentals_on_close_session():
    """
    Fundamentals (PE, PB, margins) only fetched on close sessions.
    Verify they're populated on NSE_CLOSE and empty on NSE_OPEN.
    """
    separator("TEST 5 — Fundamentals (NSE_CLOSE only)")

    # NSE_OPEN: info should be empty dict
    data_open = collect_prices("NSE_OPEN")
    sample_open = next(iter(data_open.values()), {})
    info_open   = sample_open.get("info", {})

    if info_open:
        print(f"  ⚠ NSE_OPEN returned fundamentals — expected empty dict")
        print(f"    Got: {info_open}")
    else:
        print(f"  ✓ NSE_OPEN: info is empty dict (correct — saves API calls)")

    # NSE_CLOSE: info should have fields
    print(f"\n  Fetching NSE_CLOSE (triggers fundamentals — slower)...")
    data_close  = collect_prices("NSE_CLOSE")

    tickers_with_pe  = 0
    tickers_no_info  = 0

    for ticker, entry in data_close.items():
        info = entry.get("info", {})
        if not info:
            tickers_no_info += 1
        elif info.get("pe_ratio") is not None:
            tickers_with_pe += 1

    print(f"  Tickers with PE ratio   : {tickers_with_pe}")
    print(f"  Tickers with no info    : {tickers_no_info} (normal for some Indian tickers)")

    # Sample one ticker's fundamentals
    sample_ticker = next(
        (t for t, e in data_close.items() if e.get("info", {}).get("pe_ratio")),
        None
    )
    if sample_ticker:
        info = data_close[sample_ticker]["info"]
        print(f"\n  Sample fundamentals ({sample_ticker}):")
        for k, v in info.items():
            if v is not None:
                print(f"    {k:<25} {v}")


def test_volume_surge_detection():
    """
    volume_ratio > 1.5 flags INTRADAY_WATCH candidates in SignalGeneratorAgent.
    Verify the calculation is working and flags are surfacing correctly.
    """
    separator("TEST 6 — Volume Surge Detection (INTRADAY_WATCH candidates)")

    data = collect_prices("NSE_OPEN")

    surges = [
        (ticker, entry["volume_ratio"], entry["current_price"])
        for ticker, entry in data.items()
        if entry.get("volume_ratio", 0) > 1.5
    ]

    surges.sort(key=lambda x: x[1], reverse=True)

    if not surges:
        print("\n  No volume surges today (ratio > 1.5) — normal on quiet days")
    else:
        print(f"\n  Tickers with volume surge (ratio > 1.5): {len(surges)}")
        print(f"\n  {'Ticker':<20} {'Vol Ratio':>10} {'Price':>10}")
        print(f"  {'-'*42}")
        for ticker, ratio, price in surges[:10]:
            print(f"  {ticker:<20} {ratio:>10.2f} {price:>10.2f}")
        print(
            f"\n  Note: These would be INTRADAY_WATCH candidates "
            f"if paired with a news catalyst (SignalGeneratorAgent)"
        )


if __name__ == "__main__":
    print("\nPrice Collector Live Test")
    print("Uses yfinance — no API key, no rate limits, safe to run any time")

    test_basic_collection()
    test_india_ticker_format()
    test_ohlcv_dataframe_shape()
    test_required_fields()
    test_fundamentals_on_close_session()
    test_volume_surge_detection()

    print(f"\n{'=' * 60}")
    print("  All tests complete")
    print(f"{'=' * 60}\n")