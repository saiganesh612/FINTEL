# tests/test_sec.py
# Run: python -m tests.test_sec_live

import logging, sys, os
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

from pipeline.collectors.sec_collector import collect_sec, _fetch_8k_filings

def separator(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")

def test_cik_verification():
    """Verify CIK filtering eliminates false positives."""
    separator("TEST 1 — CIK Verification (NVDA)")
    items = _fetch_8k_filings("NVDA", "0001045810", days_back=14)
    print(f"\n  Items returned: {len(items)}")
    for item in items:
        print(f"\n  {item}")
        print(f"  URL: {item.url}")
        # Verify URL contains NVIDIA's CIK (1045810)
        assert "1045810" in item.url, f"Wrong CIK in URL: {item.url}"
        print(f"  ✓ CIK verified in URL")

def test_full_us_session():
    """Full US_OPEN session — all 14 tickers."""
    separator("TEST 2 — Full US_OPEN Session")
    items = collect_sec("US_OPEN")
    print(f"\n  Total items: {len(items)}")
    from collections import Counter
    counts = Counter(item.source_id for item in items)
    print(f"  8-K filings : {counts.get('sec_edgar_8k', 0)}")
    print(f"  Form 4      : {counts.get('sec_edgar_form4', 0)}")
    for item in items[:3]:
        print(f"\n  {item}")
        print(f"  Snippet: {item.snippet[:120]}")

def test_session_routing():
    """Only US sessions should run."""
    separator("TEST 3 — Session Routing")
    for session in ["NSE_OPEN", "US_OPEN", "US_CLOSE", "NSE_CLOSE"]:
        items = collect_sec(session)
        expected = session in {"US_OPEN", "US_CLOSE"}
        status = "✓" if expected else ("✓ skipped" if not items else "✗ should have skipped")
        print(f"  {status} | {session:<14} | {len(items)} items")

if __name__ == "__main__":
    test_cik_verification()
    test_full_us_session()
    test_session_routing()
    print(f"\n{'=' * 60}\n  All tests complete\n{'=' * 60}\n")