from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from tzlocal import get_localzone

from utilities.datetime import get_now, get_today

if TYPE_CHECKING:
    import datetime as dt
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
    return get_now(time_zone="local")


NOW_LOCAL = get_now_local()


def get_today_local() -> dt.date:
    """Get the current, timezone-aware local date."""
    return get_today(time_zone="local")


TODAY_LOCAL = get_today_local()


__all__ = [
    "NOW_LOCAL",
    "TODAY_LOCAL",
    "get_local_time_zone",
    "get_now_local",
    "get_today_local",
]
