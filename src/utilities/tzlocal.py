from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from tzlocal import get_localzone

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo


def get_local_time_zone() -> ZoneInfo:
    logger = getLogger("tzlocal")
    return get_localzone()


__all__ = ["get_local_time_zone"]
