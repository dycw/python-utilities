from __future__ import annotations

from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from utilities.datetime import get_now, get_today

if TYPE_CHECKING:
    import datetime as dt


HongKong = ZoneInfo("Asia/Hong_Kong")
Tokyo = ZoneInfo("Asia/Tokyo")
USCentral = ZoneInfo("US/Central")
USEastern = ZoneInfo("US/Eastern")


def get_now_hong_kong() -> dt.datetime:
    """Get the current time in Hong Kong."""
    return get_now(time_zone=HongKong)


NOW_HONG_KONG = get_now_hong_kong()


def get_now_tokyo() -> dt.datetime:
    """Get the current time in Tokyo."""
    return get_now(time_zone=Tokyo)


NOW_TOKYO = get_now_tokyo()


def get_today_hong_kong() -> dt.date:
    """Get the current date in Hong Kong."""
    return get_today(time_zone=HongKong)


TODAY_HONG_KONG = get_today_hong_kong()


def get_today_tokyo() -> dt.date:
    """Get the current date in Tokyo."""
    return get_today(time_zone=Tokyo)


TODAY_TOKYO = get_today_tokyo()


__all__ = [
    "NOW_HONG_KONG",
    "NOW_TOKYO",
    "TODAY_HONG_KONG",
    "TODAY_TOKYO",
    "HongKong",
    "Tokyo",
    "USCentral",
    "USEastern",
    "get_now_hong_kong",
    "get_now_tokyo",
    "get_today_hong_kong",
    "get_today_tokyo",
]
