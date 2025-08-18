from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Literal, assert_never
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    datetimes,
    just,
    sampled_from,
    timezones,
)
from pytest import mark, param, raises

from utilities.hypothesis import zoned_date_times
from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import (
    UTC,
    _ToZoneInfoInvalidTZInfoError,
    _ToZoneInfoPlainDateTimeError,
    to_time_zone_name,
    to_zone_info,
)

if TYPE_CHECKING:
    from utilities.types import TimeZone


class TestToZoneInfo:
    @mark.parametrize(
        ("zone_info", "expected"),
        [
            param(HongKong, HongKong),
            param(Tokyo, Tokyo),
            param(UTC, UTC),
            param(dt.UTC, UTC),
        ],
    )
    @mark.parametrize("case", [param("zone_info"), param("key")])
    def test_time_zone(
        self,
        *,
        zone_info: ZoneInfo,
        case: Literal["zone_info", "key"],
        expected: ZoneInfo,
    ) -> None:
        match case:
            case "zone_info":
                obj = zone_info
            case "key":
                obj = zone_info.key
            case never:
                assert_never(never)
        result = to_zone_info(obj)
        assert result is expected

    def test_local(self) -> None:
        result = to_zone_info("local")
        assert result is LOCAL_TIME_ZONE

    @given(data=data(), time_zone=timezones())
    def test_standard_zoned_date_time(
        self, *, data: DataObject, time_zone: ZoneInfo
    ) -> None:
        datetime = data.draw(datetimes(timezones=just(time_zone)))
        result = to_zone_info(datetime)
        assert result is time_zone

    @given(data=data(), time_zone=timezones())
    def test_whenever_zoned_date_time(
        self, *, data: DataObject, time_zone: ZoneInfo
    ) -> None:
        datetime = data.draw(zoned_date_times(time_zone=time_zone))
        result = to_zone_info(datetime)
        assert result is time_zone

    def test_error_invalid_tzinfo(self) -> None:
        time_zone = dt.timezone(dt.timedelta(hours=12))
        with raises(_ToZoneInfoInvalidTZInfoError, match="Invalid time-zone: .*"):
            _ = to_zone_info(time_zone)

    @given(datetime=datetimes())
    def test_error_local_datetime(self, *, datetime: dt.datetime) -> None:
        with raises(_ToZoneInfoPlainDateTimeError, match="Plain date-time: .*"):
            _ = to_zone_info(datetime)


class TestGetTimeZoneName:
    @given(data=data(), time_zone=sampled_from(["Asia/Hong_Kong", "Asia/Tokyo", "UTC"]))
    def test_main(self, *, data: DataObject, time_zone: TimeZone) -> None:
        zone_info_or_str: ZoneInfo | TimeZone = data.draw(
            sampled_from([ZoneInfo(time_zone), time_zone])
        )
        result = to_time_zone_name(zone_info_or_str)
        assert result == time_zone

    def test_local(self) -> None:
        result = to_time_zone_name("local")
        assert result == LOCAL_TIME_ZONE_NAME


class TestTimeZones:
    @given(time_zone=sampled_from([HongKong, Tokyo, UTC]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
