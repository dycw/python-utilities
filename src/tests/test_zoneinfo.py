from __future__ import annotations

import datetime as dt
from re import escape
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import mark, param, raises

from utilities.reprlib import custom_repr
from utilities.zoneinfo import (
    HONG_KONG,
    TOKYO,
    US_CENTRAL,
    US_EASTERN,
    UTC,
    EnsureTimeZoneError,
    ensure_time_zone,
    get_time_zone_name,
)


class TestGetTimeZoneName:
    @given(data=data())
    @mark.parametrize(
        ("time_zone"),
        [param("Asia/Hong_Kong"), param("Asia/Tokyo"), param("UTC")],
        ids=custom_repr,
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
            param(HONG_KONG, HONG_KONG),
            param(TOKYO, TOKYO),
            param(UTC, UTC),
            param(dt.UTC, UTC),
        ],
        ids=custom_repr,
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
        "time_zone",
        [param(HONG_KONG), param(TOKYO), param(US_CENTRAL), param(US_EASTERN)],
        ids=custom_repr,
    )
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
