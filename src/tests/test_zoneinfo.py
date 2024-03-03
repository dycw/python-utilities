from __future__ import annotations

from zoneinfo import ZoneInfo

import pytest

from utilities.zoneinfo import HONG_KONG, TOKYO


class TestTimeZones:
    @pytest.mark.parametrize("timezone", [pytest.param(HONG_KONG), pytest.param(TOKYO)])
    def test_main(self, *, timezone: ZoneInfo) -> None:
        assert isinstance(timezone, ZoneInfo)
