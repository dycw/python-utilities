from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import (
    NOW_LOCAL,
    TODAY_LOCAL,
    get_local_time_zone,
    get_now_local,
    get_today_local,
)
from utilities.zoneinfo import UTC


class TestGetLocalTimeZone:
    def test_main(self) -> None:
        time_zone = get_local_time_zone()
        assert isinstance(time_zone, ZoneInfo)


class TestGetNowLocal:
    def test_function(self) -> None:
        self._assert(get_now_local())

    def test_constant(self) -> None:
        self._assert(NOW_LOCAL)

    def _assert(self, datetime: dt.datetime, /) -> None:
        assert isinstance(datetime, dt.datetime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        assert datetime.tzinfo in {ETC, HongKong, Tokyo, UTC}


class TestGetTodayLocal:
    def test_function(self) -> None:
        assert isinstance(get_today_local(), dt.date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_LOCAL, dt.date)
