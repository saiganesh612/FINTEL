# pipeline/agents/deduplicator.py
#
# Deduplicates raw news items collected across all sources in a session.
# Two items are considered duplicates at two levels:
#
# Level 1 — Exact: same headline from same source.
#            Caught by SHA-256 hash comparison. Fast, zero false positives.
#
# Level 2 — Near-duplicate: same story reported by different sources.
#            Caught by headline similarity using RapidFuzz token_set_ratio,
#            with snippet similarity as a secondary signal for borderline cases.
#
# Why token_set_ratio over other algorithms:
# Financial news frequently describes the same event with different word counts.
# "RBI holds rates" and "RBI holds repo rate at 6.5% for fifth consecutive meeting"
# are the same story — one outlet is brief, another verbose. token_set_ratio
# returns high similarity when one string is a subset of another, which is
# exactly the pattern we see across Indian financial outlets covering the same
# wire story at different lengths.
#
# Snippet as secondary signal:
# When headline similarity is borderline (SNIPPET_ASSIST_RANGE), snippet
# similarity is consulted to confirm or reject clustering. This handles cases
# where two different companies have similarly worded headlines ("Q1 profit
# rises 15%") but clearly different snippets. Snippet is never used as a
# primary signal because different outlets covering the same event often
# quote different people, producing low snippet similarity despite being
# the same story.
#
# Citation laundering detection:
# A single Reuters/PTI report republished by 8 Indian outlets should not
# appear as 8 independent confirmations. When a cluster contains a Tier 2
# wire source and Tier 3 items attribute the story to wire services,
# effective_corroboration is capped at 1 to reflect the true observation count.

import logging
from dataclasses import dataclass, field
from typing import List

from rapidfuzz import fuzz, utils

from pipeline.models import RawItem, SourceTier
from pipeline.storage.base import InMemoryStorage, compute_hash

logger = logging.getLogger(__file__)

# ── Similarity thresholds ──────────────────────────────────────
# Headlines with token_set_ratio above HEADLINE_CLUSTER_THRESHOLD
# are treated as the same story and merged into one cluster.
# RapidFuzz returns scores 0-100.
HEADLINE_CLUSTER_THRESHOLD = 85

# When headline similarity falls in this range, snippet similarity
# is consulted as a secondary signal to confirm clustering.
# Below the lower bound: reject without checking snippet.
# Above the upper bound: accept without checking snippet.
SNIPPET_ASSIST_RANGE = (65, 85)

# Combined weighted score must exceed this for borderline cluster acceptance.
# 0.7 * headline + 0.3 * snippet >= COMBINED_THRESHOLD
COMBINED_THRESHOLD = 80

# ── Wire attribution keywords ──────────────────────────────────
# Presence of any of these in a Tier 3 item's text indicates it is
# republishing a wire service story rather than original reporting.
WIRE_ATTRIBUTION_KEYWORDS = {
    "reuters", "pti", "ani", "bloomberg", "afp",
    "press trust", "associated press", "ap news"
}

@dataclass
class DeduplicatedItem:
    """
    A news item after deduplication, carrying corroboration metadata
    accumulated during same-story clustering.

    effective_corroboration is the signal used by NoiseScorerAgent —
    not corroboration_count, which may be inflated by wire reprints.
    """
    item: RawItem
    item_hash: str
    corroboration_count: int = 1
    corroborating_sources: List[str] = field(default_factory=list)
    is_wire_reprint: bool = False

    @property
    def effective_corroboration(self) -> int:
        """
        Corroboration count adjusted for citation laundering.
        Wire reprints from multiple Tier 3 outlets count as one
        independent observation, not one per outlet.
        """
        if self.is_wire_reprint and self.item.source_tier == SourceTier.MEDIA:
            return 1
        return self.corroboration_count


def _headline_similarity(a: str, b: str) -> float:
    """
    Compute headline similarity using RapidFuzz token_set_ratio.

    Preprocessing via utils.default_process:
      - Lowercases both strings
      - Removes non-alphanumeric characters

    This ensures "RBI holds rates!" and "rbi holds rates" score identically.

    Returns score 0-100.
    """
    return fuzz.token_set_ratio(a, b, processor=utils.default_process)


def _snippet_similarity(a: str, b: str) -> float:
    """
    Compute snippet similarity using RapidFuzz token_set_ratio.
    Used only as a secondary signal when headline similarity is borderline.

    Returns score 0-100.
    """
    if not a or not b:
        return 0.0

    return fuzz.token_set_ratio(a, b, processor=utils.default_process)


def _combined_similarity(
    headline_a: str, snippet_a: str,
    headline_b: str, snipper_b: str
) -> float:
    """
    Weighted combination of headline and snippet similarity.
    Used only when headline similarity falls in SNIPPET_ASSIST_RANGE.

    Weights: headline 70%, snippet 30%.
    Headline carries more weight because snippets from different outlets
    on the same story often differ significantly (different quotes,
    different analyst reactions) even when the story is identical.
    """
    h_score = _headline_similarity(headline_a, headline_b)
    s_score = _snippet_similarity(snippet_a, snipper_b)
    return 0.7 * h_score + 0.3 * s_score


def _should_cluster(item_a: RawItem, item_b: RawItem) -> bool:
    """
    Determine whether two items represent the same story.

    Decision logic:
      headline_score >= HEADLINE_CLUSTER_THRESHOLD  → cluster (headline alone sufficient)
      headline_score < SNIPPET_ASSIST_RANGE lower   → reject (clearly different)
      headline_score in SNIPPET_ASSIST_RANGE        → check combined score
    """
    h_score = _headline_similarity(item_a.headline, item_b.headline)
    if h_score >= HEADLINE_CLUSTER_THRESHOLD:
        return True

    low, high = SNIPPET_ASSIST_RANGE
    if h_score < low:
        return False

    # Borderline - check snippet for similarity
    combined = _combined_similarity(
        item_a.headline, item_a.snippet,
        item_b.headline, item_b.snippet
    )
    return combined >= COMBINED_THRESHOLD


def _is_wire_reprint(item: RawItem) -> bool:
    """
    Detect whether a Tier 3 item is republishing a wire service story.
    Only applied to MEDIA tier — Tier 1 and 2 are authoritative by definition.
    """
    if item.source_tier != SourceTier.MEDIA:
        return False

    text = f"{item.headline} {item.snippet}".lower()
    return any(keyword in text for keyword in WIRE_ATTRIBUTION_KEYWORDS)


def _has_tier2_source(items: List[RawItem]) -> bool:
    """Check if any item in a cluster originates from a Tier 2 wire source."""
    return any(item.source_tier == SourceTier.WIRE for item in items)


def _build_clusters(items: List[RawItem]) -> List[List[RawItem]]:
    """
    Group items into clusters where items represent the same news story.

    Greedy approach: each item is tested against existing cluster
    representatives. First match wins — item joins that cluster.
    No match → item starts a new cluster.

    Complexity: O(n x k) where k = number of distinct clusters.
    In practice k << n because many items cluster together,
    making this significantly faster than O(n²) worst case.
    """
    clusters: List[List[RawItem]] = []
    representatives: List[RawItem] = []

    for item in items:
        isMatched = False
        for ind, rep in enumerate(representatives):
            if _should_cluster(item, rep):
                clusters[ind].append(item)
                isMatched = True
                break
        if not isMatched:
            # representatives acts as an anchor for cluster, locating at the same indexes
            # avoiding unnecessary extra loops or complex nested dicts
            clusters.append([item])
            representatives.append(item)

    return clusters


def _merge_cluster(cluster: List[RawItem]) -> DeduplicatedItem:
    """
    Merge a cluster of same-story items into one canonical DeduplicatedItem.

    Canonical item selection priority:
      1. Tier 1 (OFFICIAL) — NSE/BSE filing is the authoritative source
      2. Tier 2 (WIRE)     — original wire reporting
      3. First item        — earliest collected from remaining tiers

    Citation laundering: if cluster contains a Tier 2 source and
    the canonical item is a wire reprint, effective_corroboration = 1.
    """
    sorted_cluster = sorted(cluster, key=lambda item: item.source_tier.value)
    canonical = sorted_cluster[0]
    canonical_hash = compute_hash(canonical.headline, canonical.source_id)

    corroborating_sources = [
        item.source_id for item in cluster
        if item.source_id != canonical.source_id
    ]

    cluster_has_wire = _has_tier2_source(cluster)
    item_is_reprint = _is_wire_reprint(canonical)
    is_wire_reprint = cluster_has_wire and item_is_reprint

    return DeduplicatedItem(
        item=canonical,
        item_hash=canonical_hash,
        corroboration_count=len(cluster),
        corroborating_sources=corroborating_sources,
        is_wire_reprint=is_wire_reprint
    )


def run_deduplicator(items: List[RawItem], storage: InMemoryStorage) -> List[DeduplicatedItem]:
    """
    Deduplicate raw items from the current collection session.

    Pass 1 — Exact deduplication:
      SHA-256 hash of (headline + source_id). Same article fetched
      from multiple adapters is dropped immediately. Storage backend
      tracks seen hashes — swap InMemoryStorage for SupabaseStorage
      to gain cross-session deduplication without changing this function.

    Pass 2 — Near-duplicate clustering:
      Groups items with similar headlines into clusters using RapidFuzz
      token_set_ratio, with snippet as secondary signal for borderline
      cases. Merges each cluster into one DeduplicatedItem with
      corroboration metadata attached.

    Args:
        items:   Raw items from CollectorOutput.raw_items
        storage: Hash tracking backend for exact deduplication

    Returns:
        List[DeduplicatedItem] — deduplicated, with corroboration context
    """
    if not items:
        return []

    logger.info(f"DeduplicatorAgent: processing {len(items)} raw items")

    # ------ Exact Deduplication ------
    unique_items: List[RawItem] = []
    exact_dupes = 0

    for item in items:
        _hash = compute_hash(item.headline, item.source_id)
        if storage.exists(_hash):
            exact_dupes += 1
            continue
        storage.save(_hash, {
            "source_id": item.source_id,
            "headline": item.headline[:80],
            "fetched_at": item.fetched_at.isoformat()
        })
        unique_items.append(item)

    logger.info(f"DeduplicatorAgent: exact dedup — {len(unique_items)} unique, {exact_dupes} duplicates removed")

    # ------ Near-duplicate clustering ------
    clusters = _build_clusters(unique_items)
    merged_clusters: List[DeduplicatedItem] = [_merge_cluster(cluster) for cluster in clusters]

    multi_source = sum(1 for cluster in clusters if len(cluster) > 1)
    wire_reprints = sum(1 for d in merged_clusters if d.is_wire_reprint)

    logger.info(
        f"DeduplicatorAgent: clustering — "
        f"{len(unique_items)} items → {len(merged_clusters)} clusters | "
        f"{multi_source} multi-source stories | "
        f"{wire_reprints} wire reprints detected"
    )
    logger.info(
        f"DeduplicatorAgent: complete — "
        f"{len(items)} raw → {len(merged_clusters)} deduplicated "
        f"({len(items) - len(merged_clusters)} removed)"
    )

    return merged_clusters
