from __future__ import annotations

from logging import Formatter
from typing import TYPE_CHECKING

from coloredlogs import ColoredFormatter
from pytest import mark, param, raises

from utilities._core_errors import MaybeColoredFormatterError
from utilities.core import MaybeColoredFormatter, one

if TYPE_CHECKING:
    from logging import Logger

    from pytest import LogCaptureFixture


class TestMaybeColoredFormatter:
    @mark.parametrize(
        ("color", "expected"),
        [param(False, "test_main"), param(True, "\x1b[34mtest_main\x1b[0m")],
    )
    def test_main(
        self, *, logger: Logger, color: bool, caplog: LogCaptureFixture, expected: str
    ) -> None:
        formatter = MaybeColoredFormatter(fmt="{funcName}", color=color)
        logger.info("message")
        record = one(r for r in caplog.records if r.name == logger.name)
        assert formatter.format(record) == expected

    @mark.parametrize(
        ("color", "expected"), [param(False, Formatter), param(True, ColoredFormatter)]
    )
    def test_color(self, *, color: bool, expected: type[Formatter]) -> None:
        formatter = MaybeColoredFormatter(color=color)
        assert type(formatter._formatter) is expected

    def test_error(self) -> None:
        with raises(
            MaybeColoredFormatterError,
            match=r"Cannot supply ignored arguments: 'datefmt', '%', None",
        ):
            _ = MaybeColoredFormatter(datefmt="datefmt")
