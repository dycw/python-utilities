from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from typing_extensions import override

if TYPE_CHECKING:
    from utilities.types import ZoneInfoLike

HongKong = ZoneInfo("Asia/Hong_Kong")
Tokyo = ZoneInfo("Asia/Tokyo")
USCentral = ZoneInfo("US/Central")
USEastern = ZoneInfo("US/Eastern")
UTC = ZoneInfo("UTC")


def ensure_time_zone(time_zone: ZoneInfoLike | dt.tzinfo, /) -> ZoneInfo:
    """Ensure the object is a time zone."""
    if isinstance(time_zone, ZoneInfo):
        return time_zone
    if isinstance(time_zone, str):
        return ZoneInfo(time_zone)
    if time_zone is dt.UTC:
        return UTC
    raise EnsureTimeZoneError(time_zone=time_zone)


@dataclass(kw_only=True, slots=True)
class EnsureTimeZoneError(Exception):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Unsupported time zone: {self.time_zone}"


def get_time_zone_name(time_zone: ZoneInfoLike | dt.timezone, /) -> str:
    """Get the name of a time zone."""
    return ensure_time_zone(time_zone).key


__all__ = [
    "UTC",
    "EnsureTimeZoneError",
    "HongKong",
    "Tokyo",
    "USCentral",
    "USEastern",
    "ensure_time_zone",
    "get_time_zone_name",
]
