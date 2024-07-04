from __future__ import annotations

from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import mark, param

from utilities.datetime import UTC, get_time_zone_name
from utilities.zoneinfo import (
    HONG_KONG,
    TOKYO,
    US_CENTRAL,
    US_EASTERN,
    ensure_time_zone,
)


class TestEnsureZoneInfo:
    @given(data=data())
    @mark.parametrize("time_zone", [param(HONG_KONG), param(TOKYO), param(UTC)])
    def test_main(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        zone_info_or_str = data.draw(
            sampled_from([time_zone, get_time_zone_name(time_zone)])
        )
        result = ensure_time_zone(zone_info_or_str)
        assert result is time_zone


class TestTimeZones:
    @mark.parametrize(
        "time_zone",
        [param(HONG_KONG), param(TOKYO), param(US_CENTRAL), param(US_EASTERN)],
    )
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
