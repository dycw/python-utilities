from __future__ import annotations

import datetime as dt
import zoneinfo
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.tzdata import (
    NOW_HONG_KONG,
    NOW_TOKYO,
    HongKong,
    Tokyo,
    USCentral,
    USEastern,
    get_now_hong_kong,
    get_now_tokyo,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetNow:
    @given(case=sampled_from([(get_now_hong_kong, HongKong), (get_now_tokyo, Tokyo)]))
    def test_function(
        self, *, case: tuple[Callable[[], dt.datetime], ZoneInfo]
    ) -> None:
        func, time_zone = case
        self._assert(func(), time_zone)

    @given(case=sampled_from([(NOW_HONG_KONG, HongKong), (NOW_TOKYO, Tokyo)]))
    def test_constant(self, *, case: tuple[dt.datetime, ZoneInfo]) -> None:
        datetime, time_zone = case
        self._assert(datetime, time_zone)

    def _assert(self, datetime: dt.datetime, time_zone: ZoneInfo, /) -> None:
        assert isinstance(datetime, dt.datetime)
        assert now.tzinfo is time_zone


class TestTimeZones:
    @given(
        time_zone=sampled_from([
            HongKong,
            Tokyo,
            USCentral,
            USEastern,
        ])
    )
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
