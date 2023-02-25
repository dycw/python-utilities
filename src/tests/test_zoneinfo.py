from datetime import tzinfo

from utilities.zoneinfo import HONG_KONG


class TestTimeZones:
    def test_main(self) -> None:
        assert isinstance(HONG_KONG, tzinfo)
