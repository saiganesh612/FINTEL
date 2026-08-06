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
    
    if output.errors:
        logger.warning(f"{len(output.errors)} non-fatal errors occurred:")
        for err in output.errors:
            logger.warning(f"  {err}")

    # errors=0 means no adapter crashed entirely.
    # Per-item failures (e.g. BSE timeouts on individual tickers)
    # are handled within each adapter and visible in WARNING logs above.
    logger.info(
        f"Collection complete: {output.total_items} raw items | "
        f"Price data: {len(output.price_data)} tickers | "
        f"Adapter crashes: {len(output.errors)}"
    )

    # Deduplication
    from pipeline.agents.deduplicator import run_deduplicator
    from pipeline.storage.base import InMemoryStorage

    storage = InMemoryStorage()
    deduped = run_deduplicator(items=output.raw_items, storage=storage, session_type=session)

    multi_source  = sum(1 for d in deduped if d.corroboration_count > 1)
    wire_reprints = sum(1 for d in deduped if d.is_wire_reprint)

    logger.info(
        f"Deduplication complete: {output.total_items} raw → {len(deduped)} unique | "
        f"Removed: {output.total_items - len(deduped)} | "
        f"Multi-source: {multi_source} | "
        f"Wire reprints: {wire_reprints}"
    )

    # Relevance filtering
    from pipeline.agents.relevance_filter import run_relevance_filter, RelevanceTag

    filtered = run_relevance_filter(items=deduped, session_type=session)
    relevant_count = sum(1 for f in filtered if f.tag == RelevanceTag.RELEVANT)
    macro_count = sum(1 for f in filtered if f.tag == RelevanceTag.MACRO)

    logger.info(
        f"Relevance filter complete: {len(deduped)} deduped → {len(filtered)} kept | "
        f"RELEVANT: {relevant_count} | MACRO: {macro_count} | "
        f"Dropped: {len(deduped) - len(filtered)}"
    )

    # Noise scoring
    from pipeline.agents.noise_scorer import run_noise_scorer

    scored = run_noise_scorer(items=filtered, session_type=session)
    logger.info(
        f"Noise scoring complete: {len(filtered)} filtered → {len(scored)} kept | "
        f"Dropped below floor: {len(filtered) - len(scored)}"
    )
    if scored:
        logger.info(f"Top scored item: [{scored[0].noise_score}] {scored[0].item.headline[:80]}")
    
    # Placeholder — ValidatorAgent next
    logger.info("(Validator) not yet implemented — pipeline stopping here")
    logger.info("=" * 60)
    logger.info("FINTEL Pipeline run complete")


if __name__ == "__main__":
    main()
