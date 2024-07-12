from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from typing_extensions import override

HONG_KONG = ZoneInfo("Asia/Hong_Kong")
TOKYO = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")
US_CENTRAL = ZoneInfo("US/Central")
US_EASTERN = ZoneInfo("US/Eastern")


def ensure_time_zone(time_zone: ZoneInfo | dt.tzinfo | str, /) -> ZoneInfo:
    """Ensure the object is a time zone."""
    if isinstance(time_zone, ZoneInfo):
        return time_zone
    if isinstance(time_zone, str):
        return ZoneInfo(time_zone)
    if time_zone is dt.UTC:
        return UTC
    raise EnsureTimeZoneError(time_zone=time_zone)


@dataclass(kw_only=True)
class EnsureTimeZoneError(Exception):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Unsupported time zone: {self.time_zone}"


def get_time_zone_name(time_zone: ZoneInfo | dt.timezone | str, /) -> str:
    """Get the name of a time zone."""
    return ensure_time_zone(time_zone).key


__all__ = [
    "HONG_KONG",
    "TOKYO",
    "US_CENTRAL",
    "US_EASTERN",
    "UTC",
    "EnsureTimeZoneError",
    "ensure_time_zone",
    "get_time_zone_name",
]
