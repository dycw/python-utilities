from __future__ import annotations

from zoneinfo import ZoneInfo

from utilities.datetime import (
    get_now_hong_kong,
    get_now_tokyo,
    get_today_hong_kong,
    get_today_tokyo,
)

HongKong = ZoneInfo("Asia/Hong_Kong")
Tokyo = ZoneInfo("Asia/Tokyo")
USCentral = ZoneInfo("US/Central")
USEastern = ZoneInfo("US/Eastern")


NOW_HONG_KONG = get_now_hong_kong()
NOW_TOKYO = get_now_tokyo()
TODAY_HONG_KONG = get_today_hong_kong()
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
]
