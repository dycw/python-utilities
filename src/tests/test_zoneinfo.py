from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import DataObject, data, datetimes, sampled_from, timezones
from pytest import raises

from utilities.hypothesis import zoned_datetimes
from utilities.tzlocal import get_local_time_zone
from utilities.zoneinfo import (
    UTC,
    HongKong,
    Tokyo,
    USCentral,
    USEastern,
    _EnsureTimeZoneInvalidTZInfoError,
    _EnsureTimeZoneLocalDateTimeError,
    ensure_time_zone,
    get_time_zone_name,
)

if TYPE_CHECKING:
    from utilities.types import TimeZone


class TestEnsureTimeZone:
    @given(
        data=data(),
        case=sampled_from([
            (HongKong, HongKong),
            (Tokyo, Tokyo),
            (USCentral, USCentral),
            (USEastern, USEastern),
            (UTC, UTC),
            (dt.UTC, UTC),
        ]),
    )
    def test_time_zone(
        self, *, data: DataObject, case: tuple[ZoneInfo | dt.timezone, ZoneInfo]
    ) -> None:
        time_zone, expected = case
        zone_info_or_str: ZoneInfo | dt.timezone | TimeZone = data.draw(
            sampled_from([time_zone, get_time_zone_name(time_zone)])
        )
        result = ensure_time_zone(zone_info_or_str)
        assert result is expected

    def test_local(self) -> None:
        result = ensure_time_zone("local")
        expected = get_local_time_zone()
        assert result is expected

    @given(data=data(), time_zone=timezones())
    def test_zoned_datetime(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        datetime = data.draw(zoned_datetimes(time_zone=time_zone))
        result = ensure_time_zone(datetime)
        assert result is time_zone

    def test_error_invalid_tzinfo(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(
            _EnsureTimeZoneInvalidTZInfoError, match="Unsupported time zone: .*"
        ):
            _ = ensure_time_zone(time_zone)

    @given(datetime=datetimes())
    def test_error_local_datetime(self, *, datetime: dt.datetime) -> None:
        with raises(_EnsureTimeZoneLocalDateTimeError, match="Local datetime: .*"):
            _ = ensure_time_zone(datetime)


class TestGetTimeZoneName:
    @given(
        data=data(),
        time_zone=sampled_from([
            "Asia/Hong_Kong",
            "Asia/Tokyo",
            "US/Central",
            "US/Eastern",
            "UTC",
        ]),
    )
    def test_main(self, *, data: DataObject, time_zone: TimeZone) -> None:
        zone_info_or_str: ZoneInfo | TimeZone = data.draw(
            sampled_from([ZoneInfo(time_zone), time_zone])
        )
        result = get_time_zone_name(zone_info_or_str)
        assert result == time_zone

    def test_local(self) -> None:
        result = get_time_zone_name("local")
        expected = get_local_time_zone().key
        assert result is expected


class TestTimeZones:
    @given(time_zone=sampled_from([HongKong, Tokyo, USCentral, USEastern]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
