from __future__ import annotations

from zoneinfo import ZoneInfo

HONG_KONG = ZoneInfo("Asia/Hong_Kong")
TOKYO = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")
US_CENTRAL = ZoneInfo("US/Central")
US_EASTERN = ZoneInfo("US/Eastern")


def ensure_time_zone(time_zone: ZoneInfo | str, /) -> ZoneInfo:
    """Ensure the object is a time zone."""
    return time_zone if isinstance(time_zone, ZoneInfo) else ZoneInfo(time_zone)


def get_time_zone_name(time_zone: ZoneInfo | str, /) -> str:
    """Get the name of a time zone."""
    return ensure_time_zone(time_zone).key


__all__ = [
    "HONG_KONG",
    "TOKYO",
    "US_CENTRAL",
    "US_EASTERN",
    "UTC",
    "ensure_time_zone",
    "get_time_zone_name",
]
