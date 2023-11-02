from __future__ import annotations

from datetime import tzinfo

from pytest import mark, param

from utilities.zoneinfo import HONG_KONG, TOKYO


class TestTimeZones:
    @mark.parametrize("timezone", [param(HONG_KONG), param(TOKYO)])
    def test_main(self, *, timezone: tzinfo) -> None:
        assert isinstance(timezone, tzinfo)
