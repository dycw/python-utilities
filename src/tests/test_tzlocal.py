from __future__ import annotations

from zoneinfo import ZoneInfo

from utilities.tzlocal import get_local_time_zone


class TestGetLocalTimeZone:
    def test_main(self) -> None:
        time_zone = get_local_time_zone()
        assert isinstance(time_zone, ZoneInfo)
