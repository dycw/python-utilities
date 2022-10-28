from logging import getLogger

from utilities.enum import StrEnum
from utilities.logging import LogLevel
from utilities.logging import basic_config


class TestBasicConfig:
    def test_main(self) -> None:
        basic_config()
        logger = getLogger(__name__)
        logger.info("message")


class TestLogLevel:
    def test_main(self) -> None:
        assert issubclass(LogLevel, StrEnum)
        assert len(LogLevel) == 5
