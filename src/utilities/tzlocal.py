from __future__ import annotations

from typing import TYPE_CHECKING

from tzlocal import get_localzone

from utilities.logging import temp_logger

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo


def get_local_time_zone() -> ZoneInfo:
    with temp_logger("tzlocal", disabled=True):
        return get_localzone()


__all__ = ["get_local_time_zone"]
