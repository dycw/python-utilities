from utilities.enum import StrEnum
from utilities.logging import LogLevel


class TestLogLevel:
    def test_main(self) -> None:
        assert issubclass(LogLevel, StrEnum)
        assert len(LogLevel) == 5
