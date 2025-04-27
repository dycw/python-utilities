from __future__ import annotations

import datetime as dt
from logging import getLogger
from typing import TYPE_CHECKING

from tzlocal import get_localzone

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo


def get_local_time_zone() -> ZoneInfo:
    """Get the local time zone, with the logging disabled."""
    logger = getLogger("tzlocal")  # avoid import cycle
    init_disabled = logger.disabled
    logger.disabled = True
    time_zone = get_localzone()
    logger.disabled = init_disabled
    return time_zone


def get_now_local() -> dt.datetime:
    """Get the current local time."""
    return dt.datetime.now(tz=get_local_time_zone())


NOW_LOCAL = get_now_local()


def get_today_local() -> dt.date:
    """Get the current, timezone-aware local date."""
    return get_now_local().date()


TODAY_LOCAL = get_today_local()


__all__ = [
    "NOW_LOCAL",
    "TODAY_LOCAL",
    "get_local_time_zone",
    "get_now_local",
    "get_today_local",
]
