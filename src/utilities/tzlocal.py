from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from tzlocal import get_localzone

from utilities.datetime import get_now_local, get_today_local

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


NOW_LOCAL = get_now_local()
TODAY_LOCAL = get_today_local()


__all__ = ["NOW_LOCAL", "TODAY_LOCAL", "get_local_time_zone"]
