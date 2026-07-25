# pipeline/storage/base.py
#
# Storage abstraction for the deduplication layer.
# Provides a consistent interface for checking and recording
# whether a news item has already been processed, regardless
# of where that state is actually persisted.
#
# All storage backends implement StorageBackend.
# Swap implementations by changing which backend is injected into agents

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

class StorageBackend(ABC):
    """
    Abstract base class for deduplication storage.

    Defines the minimum interface any storage implementation
    must provide. Agents depend on this interface, not on
    any specific storage technology.
    """

    @abstractmethod
    def exists(self, item_hash: str) -> bool:
        """
        Check whether an item with this hash has already been processed.

        Args:
            item_hash: SHA-256 hex digest identifying the item.

        Returns:
            True if already seen, False if new.
        """
    
    @abstractmethod
    def count(self) -> int:
        """Return the number of items currently tracked."""


class InMemoryStorage(StorageBackend):
    """
    In-memory deduplication store for single-session use.

    Items tracked here are lost when the process exits.
    Suitable for deduplicating within a single pipeline run —
    catches same-story items from multiple sources in the same session.
    """

    def __init__(self):
        self._store: dict[str, dict] = {}

    def exists(self, item_hash) -> bool:
        return item_hash in self._store
    
    def save(self, item_hash: str, metadata: dict) -> None:
        self._store[item_hash] = {
            **metadata,
            "recorded_at": datetime.now(tz=timezone.utc).isoformat()
        }

    def count(self):
        return len(self._store)
    
    def clear(self) -> None:
        """
        Reset the store.
        Useful between test runs to ensure a clean state.
        """
        self._store.clear()


def compute_hash(headline: str, source_id: str) -> str:
    """
    Compute a stable SHA-256 hash identifying a specific news item.

    The hash is based on the normalised headline and source identifier.
    Two articles with the same headline from different sources
    produce different hashes — they are separate observations
    even if the content is identical.

    Normalisation (lowercase + strip) ensures that minor formatting
    differences in how a source encodes the same headline don't
    produce spurious cache misses.

    Args:
        headline:  Raw headline text from the source.
        source_id: Source identifier from sources.json.

    Returns:
        64-character hex string (SHA-256 digest).
    """
    normalised = headline.lower().strip()
    raw = f"{normalised}::{source_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
