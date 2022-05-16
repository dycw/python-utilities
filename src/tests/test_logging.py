from dycw_utilities.enum import StrEnum
from dycw_utilities.logging import LogLevel


class TestLogLevel:
    def test_main(self) -> None:
        assert issubclass(LogLevel, StrEnum)
        assert len(LogLevel) == 5
