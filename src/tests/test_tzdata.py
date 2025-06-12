from __future__ import annotations

from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.tzdata import HongKong, Tokyo, USCentral, USEastern


class TestTimeZones:
    @given(time_zone=sampled_from([HongKong, Tokyo, USCentral, USEastern]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        assert isinstance(time_zone, ZoneInfo)
