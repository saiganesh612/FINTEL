# pipeline/storage/audit_log.py
#
# Writes a full, human-inspectable record of every KEEP/DROP decision
# made by any filtering/scoring agent, to a JSON file per run.
#
# WHY THIS EXISTS: aggregate counts ("268 dropped") tell you HOW MUCH
# was filtered, never WHETHER the filtering was correct. Real confidence
# requires reading the actual headlines that got dropped/merged/scored
# down and judging by eye. This file is that record — every item, its
# decision, and why — not a sample, the full set.

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

AUDIT_DIR = Path(__file__).parent.parent.parent / "artifacts" / "audit"

def write_audit_log(stage: str, session_type: str, records: List[Dict[str, Any]]) -> Path:
    """
    Write one stage's full decision record to a JSON file.

    Args:
        stage: "deduplicator" | "relevance_filter" | "noise_scorer"
        session_type: current pipeline session (e.g. "NSE_OPEN")
        records: one dict per item — decision + reasoning + headline

    Returns:
        Path to the written file (for the GitHub Actions upload step)
    """
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    run_number = os.environ.get("GITHUB_RUN_NUMBER", "local")
    run_dir = AUDIT_DIR / f"run{run_number}"
    run_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = run_dir / f"{stage}_{session_type}_run{run_number}_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({
            "stage": stage,
            "session_type": session_type,
            "generated_at": timestamp,
            "record_count": len(records),
            "records": records
        }, f, indent=2, ensure_ascii=False)

    logger.info(f"Audit log written: {filepath} ({len(records)} records)")
    return filepath
