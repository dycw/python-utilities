from __future__ import annotations

import datetime as dt
from re import escape
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import mark, param, raises

from utilities.zoneinfo import (
    UTC,
    EnsureTimeZoneError,
    HongKong,
    Tokyo,
    USCentral,
    USEastern,
    ensure_time_zone,
    get_time_zone_name,
)


class TestGetTimeZoneName:
    @given(data=data())
    @mark.parametrize(
        "time_zone",
        [
            param("Asia/Hong_Kong"),
            param("Asia/Tokyo"),
            param("US/Central"),
            param("US/Eastern"),
            param("UTC"),
        ],
    )
    def test_main(self, *, data: DataObject, time_zone: str) -> None:
        zone_info_or_str = data.draw(sampled_from([ZoneInfo(time_zone), time_zone]))
        result = get_time_zone_name(zone_info_or_str)
        assert result == time_zone


class TestEnsureZoneInfo:
    @given(data=data())
    @mark.parametrize(
        ("time_zone", "expected"),
        [
            param(HongKong, HongKong),
            param(Tokyo, Tokyo),
            param(USCentral, USCentral),
            param(USEastern, USEastern),
            param(UTC, UTC),
            param(dt.UTC, UTC),
        ],
    )
    def test_main(
        self, *, data: DataObject, time_zone: ZoneInfo | dt.timezone, expected: ZoneInfo
    ) -> None:
        zone_info_or_str = data.draw(
            sampled_from([time_zone, get_time_zone_name(time_zone)])
        )
        result = ensure_time_zone(zone_info_or_str)
        assert result is expected

    def test_error(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(
            EnsureTimeZoneError, match=escape("Unsupported time zone: UTC+12:00")
        ):
            _ = ensure_time_zone(time_zone)


class TestTimeZones:
    @mark.parametrize(
        "time_zone", [param(HongKong), param(Tokyo), param(USCentral), param(USEastern)]
    )
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
