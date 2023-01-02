from zoneinfo import ZoneInfo

from utilities.zoneinfo import UTC


class TestZoneInfo:
    def test_utc(self) -> None:
        assert isinstance(UTC, ZoneInfo)
