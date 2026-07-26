# pipeline/models/raw_item.py
#
# RawItem — the standard shape every news collector must output.
# No matter which source (RSS, NSE, Reddit, SEC), every piece of
# news gets converted into this exact structure before anything
# else in the pipeline touches it.

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any, Dict

from pydantic import BaseModel, Field, field_validator

# ─────────────────────────────────────────────
# ENUMS
# Fixed sets of allowed values for key fields.
# Pydantic will reject anything not in these lists.
# ─────────────────────────────────────────────

class SourceTier(int, Enum):
    """
    How much we trust this source.
    int + Enum means the value is stored as an integer (1, 2, 3, 4)
    but referenced by name (SourceTier.OFFICIAL) in code.
    """
    OFFICIAL  = 1   # NSE, BSE, SEC EDGAR, SEBI, RBI, Fed Reserve
    WIRE      = 2   # Reuters, PTI — high editorial accountability
    MEDIA     = 3   # ET Markets, Moneycontrol, CNBC, MarketWatch
    COMMUNITY = 4   # Reddit — signal hunting only, never CONFIRMED alone


class Market(str, Enum):
    """
    Which market this item is relevant to.
    str + Enum means it serialises as a plain string in JSON ("india", "us", "global").
    This matters when inserting into Supabase — PostgreSQL stores it as text.
    """
    INDIA  = "india"
    US     = "us"
    GLOBAL = "global"


class RawItem(BaseModel):
    """
    The standard shipping container for all news data in FINTEL.

    Every collector (RSS, NSE scraper, Reddit, SEC EDGAR) must
    return List[RawItem]. This contract means the rest of the
    pipeline — Deduplicator, Validator, Signal Generator — never
    needs to know which source a piece of news came from at the
    structural level. It always gets the same shape.
    """
    source_id: str
    source_name: str
    source_tier: SourceTier
    headline: str
    snippet: str = ""
    url: str = ""
    published_at: Optional[datetime] = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    market: Market
    raw_payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("snippet")
    @classmethod
    def trim_snippet(cls, v: str) -> str:
        """
        Trim snippet to 500 characters maximum.
        Some RSS feeds return entire article bodies — we don't need that.
        500 chars is enough context for the noise scorer and validator.
        """
        return v[:500] if v else ""
    
    @field_validator("headline")
    @classmethod
    def clean_headline(cls, v: str) -> str:
        """
        Strip leading/trailing whitespace from headlines.
        RSS feeds often include extra spaces and newlines in titles.
        """
        return v.strip()
    
    @field_validator("fetched_at", "published_at", mode="before")
    @classmethod
    def ensure_utc(cls, v):
        """
        Ensure all datetimes are timezone-aware and in UTC.

        Why this matters:
        A naive datetime (no timezone info) like datetime(2024, 1, 15, 9, 30)
        is ambiguous — is that IST? EST? UTC? We can't compare it safely.
        An aware datetime like datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        is unambiguous. We always want the latter.

        mode="before" means this runs BEFORE Pydantic's own type validation,
        so we can handle both string inputs and datetime inputs.
        """
        if v is None:
            return v
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    
    def to_dict(self) -> dict:
        """
        Convert to a plain Python dictionary for Supabase insertion.

        Supabase (PostgreSQL) can't store Python objects directly.
        It needs plain types: strings, numbers, None.
        Datetimes must be ISO 8601 strings: "2024-01-15T09:30:00+00:00"
        ISO 8601 = the international standard for date/time strings.
        """
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_tier": self.source_tier.value,
            "headline": self.headline,
            "snippet": self.snippet,
            "url": self.url,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "fetched_at": self.fetched_at.isoformat(),
            "market": self.market.value
        }
    
    def __repr__(self):
        """
        Controls what you see when you print() a RawItem or view it in the debugger.
        Without this, you'd see a wall of text. With it, you see the key info at a glance.

        Example output:
        RawItem(source='reuters_india', tier=2, headline='RBI holds rates at 6.5%...')
        """
        return (
            f"RawItem("
            f"source='{self.source_id}', "
            f"tier={self.source_tier.value}, "
            f"headline='{self.headline[:60]}...')"
        )
