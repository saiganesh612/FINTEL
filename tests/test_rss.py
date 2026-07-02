# test_rss.py
# Run with: python test_rss.py
# Delete this file after testing — it's not part of the pipeline

import logging

# Configure logging so we see output in terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

from pipeline.collectors.rss_collector import collect_rss

# Test with NSE_OPEN session — should hit Reuters India, ET Markets, Moneycontrol etc.
items = collect_rss("NSE_OPEN")

print(f"\n{'='*60}")
print(f"Total items collected: {len(items)}")
print(f"{'='*60}\n")

for item in items[:5]:   # Print first 5 only
    print(item)
    print(f"  Market : {item.market.value}")
    print(f"  Tier   : {item.source_tier.value}")
    print(f"  URL    : {item.url[:80]}")
    print()