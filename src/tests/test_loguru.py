from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, cast

from hypothesis import given
from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from loguru._recattrs import RecordFile, RecordLevel, RecordProcess, RecordThread
from pytest import CaptureFixture, mark, param, raises

from tests.test_loguru_functions import (
    func_test_log_contextualize,
    func_test_log_entry_disabled,
    func_test_log_entry_inc_and_dec,
    func_test_log_entry_non_default_level,
    func_test_log_error,
    func_test_log_error_expected,
    func_test_log_exit_duration,
    func_test_log_exit_explicit,
)
from utilities.hypothesis import text_ascii
from utilities.loguru import (
    LEVEL_CONFIGS,
    GetLoggingLevelNumberError,
    HandlerConfiguration,
    InterceptHandler,
    LogLevel,
    _GetLoggingLevelNameEmptyError,
    _GetLoggingLevelNameNonUniqueError,
    get_logging_level_name,
    get_logging_level_number,
    logged_sleep_async,
    logged_sleep_sync,
    make_except_hook,
    make_filter,
    make_slack_sink,
    make_slack_sink_async,
)
from utilities.text import ensure_str, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable

    from loguru import Record
    from pytest import CaptureFixture

    from utilities.iterables import MaybeIterable
    from utilities.types import Duration


class TestGetLoggingLevelNameAndNumber:
    @mark.parametrize(
        ("name", "number"),
        [
            param(LogLevel.TRACE, 5),
            param(LogLevel.DEBUG, 10),
            param(LogLevel.INFO, 20),
            param(LogLevel.SUCCESS, 25),
            param(LogLevel.WARNING, 30),
            param(LogLevel.ERROR, 40),
            param(LogLevel.CRITICAL, 50),
        ],
        ids=str,
    )
    def test_main(self, *, name: str, number: int) -> None:
        assert get_logging_level_number(name) == number
        assert get_logging_level_name(number) == name

    def test_error_name_empty(self) -> None:
        with raises(
            _GetLoggingLevelNameEmptyError, match="There is no level with severity 0"
        ):
            _ = get_logging_level_name(0)

    def test_error_name_non_unique(self) -> None:
        _ = logger.level("TEST-1", no=99)
        _ = logger.level("TEST-2", no=99)
        with raises(
            _GetLoggingLevelNameNonUniqueError,
            match="There must be exactly one level with severity 99; got 'TEST-1', 'TEST-2' and perhaps more",
        ):
            _ = get_logging_level_name(99)

    def test_error_number(self) -> None:
        with raises(
            GetLoggingLevelNumberError, match="Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number("invalid")


class TestHandlerConfiguration:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        logger.trace("message 1")
        out1 = capsys.readouterr().out
        assert out1 == ""

        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        logger.trace("message 2")
        out2 = capsys.readouterr().out
        expected = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| tests\.test_loguru:test_main:\d+ - message 2"
        assert search(expected, out2), out2


class TestInterceptHandler:
    def test_main(self) -> None:
        _ = InterceptHandler()


class TestLevelConfiguration:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "format": "<level>{message}</level>",
            "colorize": True,
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        logger.info("message 1")
        out1 = capsys.readouterr().out
        expected1 = "\x1b[1mmessage 1\x1b[0m\n"
        assert out1 == expected1

        _ = logger.configure(levels=LEVEL_CONFIGS)

        logger.info("message 2")
        out2 = capsys.readouterr().out
        expected2 = "\x1b[32m\x1b[1mmessage 2\x1b[0m\n"
        assert out2 == expected2


class TestLog:
    datetime: ClassVar[str] = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "
    loguru: ClassVar[str] = r"tests\.test_loguru_functions:"
    trace: ClassVar[str] = datetime + r"TRACE    \| " + loguru
    debug: ClassVar[str] = datetime + r"DEBUG    \| " + loguru
    error: ClassVar[str] = datetime + r"ERROR    \| " + loguru

    def test_entry(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_entry_inc_and_dec(1) == (2, 0)
        out = capsys.readouterr().out
        line1, line2, line3 = out.splitlines()
        expected1 = self.trace + r"func_test_log_entry_inc_and_dec:\d+ - ➢$"
        assert search(expected1, line1), line1
        expected2 = self.trace + r"_func_test_log_entry_inc:\d+ - ➢$"
        assert search(expected2, line2), line2
        expected3 = self.trace + r"_func_test_log_entry_dec:\d+ - ➢$"
        assert search(expected3, line3), line3

    def test_entry_disabled(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_entry_disabled(1) == 2
        out = capsys.readouterr().out
        assert out == ""

    def test_entry_non_default_level(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_entry_non_default_level(1) == 2
        out = capsys.readouterr().out
        expected = self.debug + r"func_test_log_entry_non_default_level:\d+ - ➢$"
        assert search(expected, out), out

    def test_error(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        with raises(ValueError, match="Got an odd number: 1"):
            _ = func_test_log_error(1)
        out = capsys.readouterr().out
        line1, line2, line3, *_ = out.splitlines()
        expected1 = self.trace + r"func_test_log_error:\d+ - ➢$"
        assert search(expected1, line1), line1
        expected2 = (
            self.error
            + r"func_test_log_error:\d+ - ValueError\('Got an odd number: 1'\)$"
        )
        assert search(expected2, line2), line2
        assert line3 == "Traceback (most recent call last):"
        exp_last = strip_and_dedent(
            """
                raise ValueError(msg)
                                 └ 'Got an odd number: 1'

            ValueError: Got an odd number: 1
            """
        )
        lines_last = "\n".join(out.splitlines()[-4:])
        assert lines_last == exp_last

    def test_error_expected(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        with raises(ValueError, match="Got an odd number: 1"):
            _ = func_test_log_error_expected(1)
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        expected = self.trace + r"func_test_log_error_expected:\d+ - ➢$"
        assert search(expected, line), line

    def test_exit_explicit(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_exit_explicit(1) == 2
        out = capsys.readouterr().out
        line1, line2 = out.splitlines()
        expected1 = self.trace + r"func_test_log_exit_explicit:\d+ - ➢$"
        assert search(expected1, line1), line1
        expected2 = self.debug + r"func_test_log_exit_explicit:\d+ - ✔$"
        assert search(expected2, line2), line2

    def test_exit_duration(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_exit_duration(1) == 2
        out = capsys.readouterr().out
        line1, line2 = out.splitlines()
        expected1 = self.trace + r"func_test_log_exit_duration:\d+ - ➢ | {}$"
        assert search(expected1, line1), line1
        expected2 = (
            self.trace
            + r"func_test_log_exit_duration:\d+ - ✔ | {'⏲': \d:\d{2}:\d{2}\.\d{6}}$"
        )
        assert search(expected2, line2), line2

    def test_contextualize(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_log_contextualize(1) == 2
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        expected = (
            self.trace + r"func_test_log_contextualize:\d+ - ➢ \| {'key': 'value'}$"
        )
        assert search(expected, line), line


class TestLoggedSleep:
    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    def test_sync(self, *, duration: Duration) -> None:
        logged_sleep_sync(duration)

    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    async def test_async(self, *, duration: Duration) -> None:
        await logged_sleep_async(duration)


class TestMakeExceptHook:
    def test_main(self) -> None:
        _ = make_except_hook(dummy_key="dummy_value")


class TestMakeFilter:
    def test_main(self) -> None:
        filter_func = make_filter(final_filter=True)
        assert filter_func(self._record)

    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.TRACE, False),
            param(LogLevel.DEBUG, False),
            param(LogLevel.INFO, True),
            param(LogLevel.SUCCESS, False),
            param(LogLevel.WARNING, False),
            param(LogLevel.ERROR, False),
            param(LogLevel.CRITICAL, False),
        ],
    )
    def test_level(self, *, level: LogLevel, expected: bool) -> None:
        filter_func = make_filter(level=level, final_filter=True)
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.TRACE, True),
            param(LogLevel.DEBUG, True),
            param(LogLevel.INFO, True),
            param(LogLevel.SUCCESS, False),
            param(LogLevel.WARNING, False),
            param(LogLevel.ERROR, False),
            param(LogLevel.CRITICAL, False),
        ],
    )
    def test_min_level(self, *, level: LogLevel, expected: bool) -> None:
        filter_func = make_filter(min_level=level, final_filter=True)
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.TRACE, False),
            param(LogLevel.DEBUG, False),
            param(LogLevel.INFO, True),
            param(LogLevel.SUCCESS, True),
            param(LogLevel.WARNING, True),
            param(LogLevel.ERROR, True),
            param(LogLevel.CRITICAL, True),
        ],
    )
    def test_max_level(self, *, level: LogLevel, expected: bool) -> None:
        filter_func = make_filter(max_level=level, final_filter=True)
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("name_include", "name_exclude", "expected"),
        [
            param(None, None, True),
            param("__main__", None, True),
            param("other", None, False),
            param(None, "__main__", False),
            param(None, "other", True),
        ],
    )
    def test_name_exists(
        self,
        *,
        name_include: MaybeIterable[str] | None,
        name_exclude: MaybeIterable[str] | None,
        expected: bool,
    ) -> None:
        filter_func = make_filter(
            name_include=name_include, name_exclude=name_exclude, final_filter=True
        )
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("name_include", "name_exclude"),
        [
            param(None, None),
            param("__main__", None),
            param("other", None),
            param(None, "__main__"),
            param(None, "other"),
        ],
    )
    def test_name_does_not_exist(
        self,
        *,
        name_include: MaybeIterable[str] | None,
        name_exclude: MaybeIterable[str] | None,
    ) -> None:
        filter_func = make_filter(
            name_include=name_include, name_exclude=name_exclude, final_filter=True
        )
        record: Record = cast(Any, self._record | {"name": None})
        assert filter_func(record)

    @mark.parametrize(
        ("extra_include_all", "extra_exclude_any", "expected"),
        [
            param(None, None, True),
            param("x", None, True),
            param("y", None, True),
            param("z", None, False),
            param(["x", "y"], None, True),
            param(["y", "z"], None, False),
            param(["x", "z"], None, False),
            param("invalid", None, False),
            param(None, "x", False),
            param(None, "y", False),
            param(None, "z", True),
            param(None, ["x", "y"], False),
            param(None, ["y", "z"], False),
            param(None, ["x", "z"], False),
            param(None, "invalid", True),
        ],
    )
    def test_extra_inc_all_exc_any(
        self,
        *,
        extra_include_all: MaybeIterable[str] | None,
        extra_exclude_any: MaybeIterable[str] | None,
        expected: bool,
    ) -> None:
        filter_func = make_filter(
            extra_include_all=extra_include_all,
            extra_exclude_any=extra_exclude_any,
            final_filter=True,
        )
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("extra_include_any", "extra_exclude_all", "expected"),
        [
            param(None, None, True),
            param("x", None, True),
            param("y", None, True),
            param("z", None, False),
            param(["x", "y"], None, True),
            param(["y", "z"], None, True),
            param(["x", "z"], None, True),
            param("invalid", None, False),
            param(None, "x", False),
            param(None, "y", False),
            param(None, "z", True),
            param(None, ["x", "y"], False),
            param(None, ["y", "z"], True),
            param(None, ["x", "z"], True),
            param(None, "invalid", True),
        ],
    )
    def test_extra_inc_any_exc_all(
        self,
        *,
        extra_include_any: MaybeIterable[str] | None,
        extra_exclude_all: MaybeIterable[str] | None,
        expected: bool,
    ) -> None:
        filter_func = make_filter(
            extra_include_any=extra_include_any,
            extra_exclude_all=extra_exclude_all,
            final_filter=True,
        )
        result = filter_func(self._record)
        assert result is expected

    @mark.parametrize(
        ("name", "final_filter", "expected"),
        [
            param("__main__", None, True),
            param("__main__", True, True),
            param("__main__", False, False),
            param("__main__", lambda: True, True),
            param("__main__", lambda: False, False),
            param("other", None, False),
            param("other", True, False),
            param("other", False, False),
            param("other", lambda: True, False),
            param("other", lambda: False, False),
        ],
    )
    def test_final_filter(
        self,
        *,
        name: str,
        final_filter: bool | Callable[[], bool] | None,
        expected: bool,
    ) -> None:
        filter_func = make_filter(name_include=name, final_filter=final_filter)
        result = filter_func(self._record)
        assert result is expected

    @property
    def _record(self) -> Record:
        record = {
            "elapsed": dt.timedelta(seconds=11, microseconds=635587),
            "exception": None,
            "extra": {"x": 1, "y": 2},
            "file": RecordFile(
                name="1723464958.py",
                path="/var/folders/z2/t3tvc2yn33j0zdd910j7805r0000gn/T/ipykernel_98745/1723464958.py",
            ),
            "function": "<module>",
            "level": RecordLevel(name="INFO", no=20, icon="ℹ️ "),  # noqa: RUF001
            "line": 1,
            "message": "l2",
            "module": "1723464958",
            "name": "__main__",
            "process": RecordProcess(id_=98745, name="MainProcess"),
            "thread": RecordThread(id_=8420429632, name="MainThread"),
            "time": dt.datetime(
                2024,
                8,
                31,
                14,
                3,
                52,
                388537,
                tzinfo=dt.timezone(dt.timedelta(seconds=32400), "JST"),
            ),
        }
        return cast(Any, record)


class TestMakeSlackSink:
    @given(url=text_ascii())
    def test_sync(self, *, url: str) -> None:
        sink = make_slack_sink(url)
        handler: HandlerConfiguration = {"sink": sink, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])
        logger.trace("message")

    @given(url=text_ascii())
    async def test_async(self, *, url: str) -> None:
        sink = make_slack_sink_async(url)
        handler: HandlerConfiguration = {"sink": sink, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])
        logger.trace("message")
        await logger.complete()
