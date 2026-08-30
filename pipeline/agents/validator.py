# pipeline/agents/validator.py
#
# ValidatorAgent — assigns a trust label to every scored item using
# only two signals: source tier and effective_corroboration (the
# citation-laundering-corrected count, not raw corroboration_count —
# raw count would let a wire story reprinted 8x earn CONFIRMED via
# corroboration even when only 1 independent observation exists).
#
# Decision table (derived from agent_graph.md's Node 5, confirmed
# against how DeduplicatorAgent picks a canonical item — canonical is
# always the lowest-tier-value item present in a cluster, so checking
# canonical.source_tier alone is equivalent to checking "does this
# cluster contain a Tier X source"):
#
#   Tier                | corrob == 1    | corrob >= 2
#   --------------------|-----------------|------------------
#   OFFICIAL            | CONFIRMED       | CONFIRMED
#   WIRE                | UNCONFIRMED     | CONFIRMED
#   MEDIA / COMMUNITY   | SINGLE_SOURCE   | UNCONFIRMED
#
# SINGLE_SOURCE items are routed to pending_deep_check, not
# validated_items. DeepCheckAgent is meant to attempt verification before these are trusted
# further. Until it exists, they are held out of validated_items
# entirely rather than silently downgraded into it — run.py counts
# and logs them explicitly so nothing is silently lost track of.

import logging
from dataclasses import dataclass, field
from typing import List, Tuple

from pipeline.agents.noise_scorer import ScoredItem
from pipeline.models import RawItem, SourceTier
from pipeline.storage.audit_log import write_audit_log

logger = logging.getLogger(__name__)
CONFIRMED = "CONFIRMED"
UNCONFIRMED = "UNCONFIRMED"
SINGLE_SOURCE = "SINGLE_SOURCE"

@dataclass
class ValidatedItem:
    scored: ScoredItem
    trust_label: str
    validation_reason: str

    @property
    def item(self) -> RawItem:
        return self.scored.item

    @property
    def tag(self):
        return self.scored.tag

    @property
    def noise_score(self) -> float:
        return self.scored.noise_score


@dataclass
class ValidatedOutput:
    validated_items: List[ValidatedItem]
    pending_deep_check: List[ValidatedItem]


def _assign_trust_label(scored: ScoredItem) -> Tuple[str, str]:
    """Returns (trust_label, reason) using (source_tier, effective_corroboration) only."""
    tier = scored.item.source_tier
    corroboration = scored.filtered.effective_corroboration

    if tier == SourceTier.OFFICIAL:
        return CONFIRMED, f"OFFICIAL tier — auto-CONFIRMED regardless of corroboration (effective={corroboration})"

    if tier == SourceTier.WIRE:
        if corroboration >= 2:
            return CONFIRMED, f"WIRE tier, effective_corroboration={corroboration} (>=2) — CONFIRMED"
        return UNCONFIRMED, f"WIRE tier, single observation (effective_corroboration=1) — needs a second independent source"

    # Media or Community
    if corroboration >= 2:
        return UNCONFIRMED, f"{tier.name} tier, effective_corroboration={corroboration} (>=2), no Tier 1/2 source in cluster — UNCONFIRMED"
    return SINGLE_SOURCE, f"{tier.name} tier, single observation, no corroboration — routed to pending_deep_check"


def run_validator(items: List[ScoredItem], session_type: str = "unknown") -> ValidatedOutput:
    """
    Assign trust labels to every scored item. CONFIRMED and
    UNCONFIRMED go to validated_items; SINGLE_SOURCE goes to
    pending_deep_check, held separately, never silently merged in.
    """
    if not items:
        return ValidatedOutput(validated_items=[], pending_deep_check=[])

    logger.info(f"ValidatorAgent: processing {len(items)} scored items")

    validated_items: List[ValidatedItem] = []
    pending_deep_check: List[ValidatedItem] = []
    audit_records = []
    label_counts = {CONFIRMED: 0, UNCONFIRMED: 0, SINGLE_SOURCE: 0}

    for scored in items:
        label, reason = _assign_trust_label(scored=scored)
        validated = ValidatedItem(scored=scored, trust_label=label, validation_reason=reason)
        label_counts[label] += 1

        (pending_deep_check if label == SINGLE_SOURCE else validated_items).append(validated)
        audit_records.append({
            "item_hash":                scored.filtered.deduped.item_hash,
            "source_id":                scored.item.source_id,
            "source_tier":              scored.item.source_tier.name,
            "headline":                 scored.item.headline,
            "url":                      scored.item.url,
            "published_at":             scored.item.published_at.isoformat() if scored.item.published_at else None,
            "category":                 scored.item.category,
            "noise_score":              scored.noise_score,
            "effective_corroboration":  scored.filtered.effective_corroboration,
            "trust_label":              label,
            "validation_reason":        reason,
        })

    logger.info(
        f"ValidatorAgent: {len(items)} in → "
        f"CONFIRMED: {label_counts[CONFIRMED]} | "
        f"UNCONFIRMED: {label_counts[UNCONFIRMED]} | "
        f"SINGLE_SOURCE (pending deep check): {label_counts[SINGLE_SOURCE]}"
    )

    write_audit_log(stage="validator", session_type=session_type, records=audit_records)
    return ValidatedOutput(validated_items=validated_items, pending_deep_check=pending_deep_check)

