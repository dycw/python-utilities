from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Literal, cast

from hypothesis import given
from hypothesis.strategies import DataObject, data, datetimes, just
from pytest import mark, param, raises

from utilities.constants import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME, UTC
from utilities.core import (
    ToTimeZoneNameInvalidKeyError,
    ToTimeZoneNameInvalidTZInfoError,
    ToTimeZoneNamePlainDateTimeError,
    ToZoneInfoInvalidTZInfoError,
    ToZoneInfoPlainDateTimeError,
    to_time_zone_name,
    to_zone_info,
)
from utilities.hypothesis import zone_infos, zoned_date_times

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import TimeZoneLike


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
            ToTimeZoneNameInvalidKeyError, match=r"Invalid time-zone: 'invalid'"
        ):
            _ = to_time_zone_name(cast("TimeZoneLike", "invalid"))

    def test_error_invalid_tz_info(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(ToTimeZoneNameInvalidTZInfoError, match=r"Invalid time-zone: .*"):
            _ = to_time_zone_name(time_zone)

    @given(date_time=datetimes())
    def test_error_plain_date_time(self, *, date_time: dt.datetime) -> None:
        with raises(ToTimeZoneNamePlainDateTimeError, match=r"Plain date-time: .*"):
            _ = to_time_zone_name(date_time)


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
            ToZoneInfoInvalidTZInfoError, match=r"Invalid time-zone: UTC\+12:00"
        ):
            _ = to_zone_info(time_zone)

    @given(date_time=datetimes())
    def test_error_plain_date_time(self, *, date_time: dt.datetime) -> None:
        with raises(ToZoneInfoPlainDateTimeError, match=r"Plain date-time: .*"):
            _ = to_zone_info(date_time)
