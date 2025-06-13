from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, cast

from tzlocal import get_localzone

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import TimeZone


def get_local_time_zone() -> ZoneInfo:
    """Get the local time zone, with the logging disabled."""
    logger = getLogger("tzlocal")  # avoid import cycle
    init_disabled = logger.disabled
    logger.disabled = True
    time_zone = get_localzone()
    logger.disabled = init_disabled
    return time_zone


LOCAL_TIME_ZONE = get_local_time_zone()
LOCAL_TIME_ZONE_NAME = cast("TimeZone", LOCAL_TIME_ZONE.key)


__all__ = ["LOCAL_TIME_ZONE", "LOCAL_TIME_ZONE_NAME", "get_local_time_zone"]
