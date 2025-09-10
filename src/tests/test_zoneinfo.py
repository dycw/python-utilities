from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Literal, cast
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import DataObject, data, datetimes, just, sampled_from
from pytest import mark, param, raises

from utilities.hypothesis import zone_infos, zoned_date_times
from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import (
    UTC,
    _ToTimeZoneNameInvalidKeyError,
    _ToTimeZoneNameInvalidTZInfoError,
    _ToTimeZoneNamePlainDateTimeError,
    _ToZoneInfoInvalidTZInfoError,
    _ToZoneInfoPlainDateTimeError,
    to_time_zone_name,
    to_zone_info,
)

if TYPE_CHECKING:
    from utilities.types import TimeZoneLike


class TestToZoneInfo:
    @given(time_zone=zone_infos())
    def test_zone_info(self, *, time_zone: ZoneInfo) -> None:
        result = to_zone_info(time_zone)
        assert result is time_zone

    @given(data=data(), time_zone=zone_infos())
    def test_zoned_date_time(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        date_time = data.draw(zoned_date_times(time_zone=time_zone))
        result = to_zone_info(date_time)
        assert result is time_zone

    @mark.parametrize("time_zone", [param("local"), param("localtime")])
    def test_local(self, *, time_zone: Literal["local", "localtime"]) -> None:
        result = to_zone_info(time_zone)
        assert result is LOCAL_TIME_ZONE

    @given(time_zone=zone_infos())
    def test_str(self, *, time_zone: ZoneInfo) -> None:
        result = to_zone_info(cast("TimeZoneLike", time_zone.key))
        assert result is time_zone

    def test_tz_info(self) -> None:
        result = to_zone_info(dt.UTC)
        assert result is UTC

    @given(data=data(), time_zone=zone_infos())
    def test_py_zoned_date_time(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        date_time = data.draw(datetimes(timezones=just(time_zone)))
        result = to_zone_info(date_time)
        assert result is time_zone

    def test_error_invalid_tz_info(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(
            _ToZoneInfoInvalidTZInfoError, match=r"Invalid time-zone: UTC\+12:00"
        ):
            _ = to_zone_info(time_zone)

    @given(date_time=datetimes())
    def test_error_plain_date_time(self, *, date_time: dt.datetime) -> None:
        with raises(_ToZoneInfoPlainDateTimeError, match=r"Plain date-time: .*"):
            _ = to_zone_info(date_time)


class TestToTimeZoneName:
    @given(time_zone=zone_infos())
    def test_zone_info(self, *, time_zone: ZoneInfo) -> None:
        result = to_time_zone_name(time_zone)
        expected = time_zone.key
        assert result == expected

    @given(data=data(), time_zone=zone_infos())
    def test_zoned_date_time(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        date_time = data.draw(zoned_date_times(time_zone=time_zone))
        result = to_time_zone_name(date_time)
        expected = time_zone.key
        assert result == expected

    @mark.parametrize("time_zone", [param("local"), param("localtime")])
    def test_local(self, *, time_zone: Literal["local", "localtime"]) -> None:
        result = to_time_zone_name(time_zone)
        assert result == LOCAL_TIME_ZONE_NAME

    @given(time_zone=zone_infos())
    def test_str(self, *, time_zone: ZoneInfo) -> None:
        result = to_time_zone_name(cast("TimeZoneLike", time_zone.key))
        expected = time_zone.key
        assert result == expected

    def test_tz_info(self) -> None:
        result = to_time_zone_name(dt.UTC)
        expected = UTC.key
        assert result == expected

    @given(data=data(), time_zone=zone_infos())
    def test_py_zoned_date_time(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        date_time = data.draw(datetimes(timezones=just(time_zone)))
        result = to_time_zone_name(date_time)
        expected = time_zone.key
        assert result == expected

    def test_error_invalid_key(self) -> None:
        with raises(
            _ToTimeZoneNameInvalidKeyError, match=r"Invalid time-zone: 'invalid'"
        ):
            _ = to_time_zone_name(cast("TimeZoneLike", "invalid"))

    def test_error_invalid_tz_info(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(_ToTimeZoneNameInvalidTZInfoError, match=r"Invalid time-zone: .*"):
            _ = to_time_zone_name(time_zone)

    @given(date_time=datetimes())
    def test_error_plain_date_time(self, *, date_time: dt.datetime) -> None:
        with raises(_ToTimeZoneNamePlainDateTimeError, match=r"Plain date-time: .*"):
            _ = to_time_zone_name(date_time)


class TestTimeZones:
    @given(time_zone=sampled_from([HongKong, Tokyo, UTC]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
