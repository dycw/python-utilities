from __future__ import annotations

from zoneinfo import ZoneInfo

from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME, get_local_time_zone


class TestGetLocalTimeZone:
    def test_function(self) -> None:
        time_zone = get_local_time_zone()
        assert isinstance(time_zone, ZoneInfo)

    def test_constants(self) -> None:
        assert isinstance(LOCAL_TIME_ZONE, ZoneInfo)
        assert isinstance(LOCAL_TIME_ZONE_NAME, str)
