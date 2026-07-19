import os
import sys
import logging
from datetime import datetime, timezone

# Configure logging once at the entry point.
# All other modules use logging.getLogger(__name__)
# and inherit this configuration automatically.
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    stream  = sys.stdout,   # GitHub Actions captures stdout for the run log
)

logger = logging.getLogger(__name__)

def main():
    session = os.environ.get("SESSION", "UNKNOWN")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    logger.info("=" * 60)
    logger.info(f"FINTEL Pipeline starting")
    logger.info(f"Session  : {session}")
    logger.info(f"Triggered: {now}")
    logger.info("=" * 60)

    if session == "UNKNOWN":
        logger.error("SESSION env variable not set — aborting")
        sys.exit(1)

    # Data Collection
    from pipeline.collector_agent import run_collector
    output = run_collector(session_type=session)

    logger.info(f"Collection complete: {output.total_items} raw items")
    logger.info(f"Price data: {len(output.price_data)} tickers")

    if output.errors:
        logger.warning(f"{len(output.errors)} non-fatal errors occurred:")
        for err in output.errors:
            logger.warning(f"  {err}")

    # Placeholder for M2.5 — DeduplicatorAgent will process output next
    logger.info("(Deduplicator) not yet implemented — pipeline stopping here")
    logger.info("=" * 60)
    logger.info("FINTEL Pipeline run complete")


if __name__ == "__main__":
    main()
