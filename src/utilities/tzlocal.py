from __future__ import annotations

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


__all__ = ["get_local_time_zone"]
