from __future__ import annotations

from zoneinfo import ZoneInfo

from pytest import mark, param

from utilities.zoneinfo import HONG_KONG, TOKYO, US_CENTRAL, US_EASTERN


class TestTimeZones:
    @mark.parametrize(
        "time_zone",
        [param(HONG_KONG), param(TOKYO), param(US_CENTRAL), param(US_EASTERN)],
    )
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
