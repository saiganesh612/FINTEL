# pipeline/collectors/price_collector.py
#
# Price Collector — fetches OHLCV price data for all 47 watchlist tickers.
# Uses yfinance (Yahoo Finance wrapper). No API key. No rate limits.
#
# This collector is different from the others:
# It does NOT produce RawItems (this is not news).
# It produces a structured price dict consumed directly by:
#   - TechnicalAgent: computes RSI, MACD, EMAs from this OHLCV data
#   - FundamentalsAgent: pulls PE, PB, margins from ticker.info

import json
import logging
from pathlib import Path
from typing import Dict, List, Any

import yfinance as yf

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent / "config"
WATCHLIST_PATH = CONFIG_DIR / "watchlist.json"

# 60 trading days (~3 months) of history.
# Sufficient to compute:
#   EMA 9, 20, 50  (need at least 50 days)
#   RSI 14         (needs 14+ days)
#   MACD 12/26/9   (needs 26+ days)
# We don't go to 200 days here — EMA 200 requires 200 days.
# TechnicalAgent will fetch longer history when needed for EMA200.
HISTORY_PERIOD = "60d"

def _load_all_tickers() -> List[str]:
    """Load all ticker_yf values from watchlist (India + US combined)."""
    with open(WATCHLIST_PATH, "r") as f:
        watchlist = json.load(f)

    tickers: List[str] = []
    for item in watchlist.get("india", []):
        tickers.append(item["ticker_yf"])
    for item in watchlist.get("us", []):
        tickers.append(item["ticker_yf"])

    return tickers


def collect_prices(session_type: str) -> Dict[str, Any]:
    """
    Fetch OHLCV + basic info for all watchlist tickers.

    yfinance.download() is used for OHLCV — it batches all tickers
    in a single HTTP request, much faster than one call per ticker.

    ticker.info is fetched individually — it contains fundamentals
    (PE ratio, market cap, margins) that change weekly, not intraday.
    We only fetch info on NSE_CLOSE and US_CLOSE to avoid redundant calls.

    Args:
        session_type: current pipeline session

    Returns:
        Dict keyed by ticker_yf with price data and metadata.
        Empty dict if yfinance is unavailable.
    """
    tickers = _load_all_tickers()

    # Fetch OHLCV for all tickers in one batched call.
    # group_by="ticker" structures output as:
    #   data["RELIANCE.NS"]["Close"] instead of data["Close"]["RELIANCE.NS"]
    # auto_adjust=True returns adjusted close (corrects for splits, dividends).
    # progress=False suppresses yfinance's download progress bar in logs.
    logger.info(f"Fetching OHLCV for {len(tickers)} tickers — period: {HISTORY_PERIOD}")

    try:
        raw_data = yf.download(
            tickers=tickers,
            period=HISTORY_PERIOD,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True
        )
    except Exception as e:
        logger.warning(f"yfinance bulk download failed: {e}", exc_info=True)
        return {}
    
    price_data: Dict[str, Any] = {}
    fetch_info = session_type in {"NSE_CLOSE", "US_CLOSE"}

    for ticker in tickers:
        try:
            if len(tickers) == 1:
                df = raw_data
            else:
                df = raw_data[ticker] if ticker in raw_data else None

            if df is None or df.empty:
                logger.warning(f"[{ticker}] No OHLCV data returned")
                continue

            # Drop rows where Close is NaN (missing trading days, holidays)
            df = df.dropna(subset=["Close"])
            if df.empty:
                continue

            latest = df.iloc[-1] # Most recent row
            current_price = float(latest["Close"])

            entry: Dict[str, Any] = {
                "ohlcv": df,
                "current_price": current_price,
                "week_52_high": float(df["High"].max()),
                "week_52_low": float(df["Low"].max()),
                # pct_from_52w_high: negative means below 52W high
                # e.g. -0.12 means 12% below its yearly peak
                "pct_from_52w_high": ((current_price - float(df["High"].max())) / float(df["High"].max())),
                "avg_volume_20d": float(df["Volume"].tail(20).mean()),
                "latest_volume": float(latest["Volume"]),
                # volume_ratio > 1 means today's volume is above the 20-day average
                # > 1.5 = notable volume surge, flags INTRADAY_WATCH potential
                "volume_ratio": (
                    float(latest["Volume"]) / float(df["Volume"].tail(20).mean())
                    if df["Volume"].tail(20).mean() > 0 else 1.0
                ),
                "info": {}
            }

            # Fundamentals — only on close sessions, data is stable
            if fetch_info:
                try:
                    ticker_obj = yf.Ticker(ticker=ticker)
                    info = ticker_obj.info or {}
                    entry["info"] = {
                        "market_cap":         info.get("marketCap"),
                        "pe_ratio":           info.get("trailingPE"),
                        "pb_ratio":           info.get("priceToBook"),
                        "revenue_growth":     info.get("revenueGrowth"),
                        "profit_margins":     info.get("profitMargins"),
                        "debt_to_equity":     info.get("debtToEquity"),
                        "52w_high":           info.get("fiftyTwoWeekHigh"),
                        "52w_low":            info.get("fiftyTwoWeekLow"),
                        "sector":             info.get("sector"),
                        "industry":           info.get("industry"),
                        "short_name":         info.get("shortName"),
                    }
                except Exception as e:
                    logger.warning(f"[{ticker}] info fetch failed: {e}")

            price_data[ticker] = entry
            logger.info(
                f"[{ticker}] ₹/$ {current_price:.2f} | "
                f"Vol ratio: {entry['volume_ratio']:.2f} | "
                f"From 52W high: {entry['pct_from_52w_high']:.1%}"
            )

        except Exception as e:
            logger.warning(f"[{ticker}] processing error: {e}", exc_info=True)
            continue

    logger.info(f"Price collection complete: {len(price_data)}/{len(tickers)} tickers")
    return price_data

