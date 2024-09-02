from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from loguru._recattrs import RecordFile, RecordLevel, RecordProcess, RecordThread
from pytest import CaptureFixture, mark, param, raises

from tests.functions import (
    add_sync_info,
    diff_pairwise_then_add_async,
    diff_pairwise_then_add_sync,
)
from utilities.loguru import (
    LEVEL_CONFIGS,
    GetLoggingLevelError,
    HandlerConfiguration,
    InterceptHandler,
    LogLevel,
    _log_from_depth_up,
    _LogFromDepthUpError,
    get_logging_level,
    logged_sleep_async,
    logged_sleep_sync,
    make_catch_hook,
    make_except_hook,
    make_filter,
)
from utilities.text import ensure_str, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable

    from _pytest.capture import CaptureFixture
    from loguru import Record

    from utilities.iterables import MaybeIterable
    from utilities.types import Duration


class TestGetLoggingLevel:
    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.TRACE, 5),
            param(LogLevel.DEBUG, 10),
            param(LogLevel.INFO, 20),
            param(LogLevel.SUCCESS, 25),
            param(LogLevel.WARNING, 30),
            param(LogLevel.ERROR, 40),
            param(LogLevel.CRITICAL, 50),
        ],
    )
    def test_main(self, *, level: str, expected: int) -> None:
        assert get_logging_level(level) == expected

    def test_error(self) -> None:
        with raises(GetLoggingLevelError, match="Invalid logging level: 'invalid'"):
            _ = get_logging_level("invalid")


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
        assert search(expected, out2)


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


class TestLogCall:
    @mark.only
    def test_sync(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert diff_pairwise_then_add_sync(1000, 100, 10, 1) == 909
        out = capsys.readouterr().out
        line1, line2, line3, line4 = out.splitlines()
        head = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| "
        expected1 = (
            head
            + r"tests\.test_loguru:test_sync:\d+ -  \| {'𝑓': 'diff_pairwise_then_add_sync'}"  # noqa: RUF001
        )
        assert search(expected1, line1)
        head_mid = head + r"tests\.functions:diff_pairwise_then_add_sync:\d+ -  \| "
        expected2 = head_mid + "{'𝑓': 'diff_sync'}"  # noqa: RUF001
        assert search(expected2, line2)
        expected3 = head_mid + "{'𝑓': 'diff_sync'}"  # noqa: RUF001
        assert search(expected3, line3)
        expected4 = head_mid + "{'𝑓': 'add_sync'}"  # noqa: RUF001
        assert search(expected4, line4)

    async def test_async(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert await diff_pairwise_then_add_async(1000, 100, 10, 1) == 909
        out = capsys.readouterr().out
        line1, line2, line3, line4 = out.splitlines()
        head = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| "
        expected1 = (
            head
            + r"tests\.test_loguru:test_async:\d+ -  \| {'<diff_pairwise_then_add_async>': 'x=1000, y=100, z=10, w=1'}"
        )
        assert search(expected1, line1)
        head_mid = head + r"tests\.functions:diff_pairwise_then_add_async:\d+ -  \| "
        expected2 = head_mid + "{'<diff_async>': 'x=1000, y=100'}"
        assert search(expected2, line2)
        expected3 = head_mid + "{'<diff_async>': 'x=10, y=1'}"
        assert search(expected3, line3)
        expected4 = head_mid + "{'<add_async>': 'x=900, y=9'}"
        assert search(expected4, line4)

    def test_custom_level(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert add_sync_info(1, 2) == 3
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        expected = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| INFO     \| tests\.test_loguru:test_custom_level:\d+ -  \| {'<add_sync_info>': 'x=1, y=2'}"
        assert search(expected, line)


class TestLogFromDepthUp:
    @mark.parametrize(
        ("depth", "expected"),
        [
            param(
                0,
                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| utilities\.loguru:_log_from_depth_up:\d+ - Hello world",
            ),
            param(
                1,
                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| tests\.test_loguru:test_main:\d+ - Hello world",
            ),
            param(
                2,
                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| _pytest\.python:pytest_pyfunc_call:\d+ - Hello world",
            ),
        ],
    )
    def test_main(self, *, capsys: CaptureFixture, depth: int, expected: str) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])
        _log_from_depth_up(logger, depth, LogLevel.TRACE, "Hello world")
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        assert search(expected, line)

    def test_error_call_stack_not_deep_enough(self) -> None:
        with raises(_LogFromDepthUpError, match="Depth must be non-negative; got -1"):
            _log_from_depth_up(logger, -1, LogLevel.TRACE, "")

    def test_error_negative_depth(self) -> None:
        with raises(_LogFromDepthUpError, match="Depth must be non-negative; got -1"):
            _log_from_depth_up(logger, -1, LogLevel.TRACE, "")


class TestLoggedSleep:
    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    def test_sync(self, *, duration: Duration) -> None:
        logged_sleep_sync(duration)

    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    async def test_async(self, *, duration: Duration) -> None:
        await logged_sleep_async(duration)


class TestMakeCatchHook:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.ERROR,
            "format": f"{default_format} | {{extra[dummy_key]}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        catch_on_error = make_catch_hook(dummy_key="dummy_value")

        @logger.catch(onerror=catch_on_error)
        def divide_by_zero(x: float, /) -> float:
            return x / 0

        _ = divide_by_zero(1.0)
        exp_first = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| ERROR    \| tests\.test_loguru:test_main:\d+ - Uncaught ZeroDivisionError\('float division by zero'\) \| dummy_value"
        self._run_tests(capsys, exp_first)

    def test_default(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        @logger.catch
        def divide_by_zero(x: float, /) -> float:
            return x / 0

        _ = divide_by_zero(1.0)
        exp_first = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| ERROR    \| tests\.test_loguru:test_default:\d+ - An error has been caught in function 'test_default', process 'MainProcess' \(\d+\), thread 'MainThread' \(\d+\)"
        self._run_tests(capsys, exp_first)

    def _run_tests(self, capsys: CaptureFixture, exp_first: str, /) -> None:
        out = capsys.readouterr().out
        lines = out.splitlines()
        assert search(exp_first, lines[0])
        exp_last = strip_and_dedent("""
                return x / 0
                       └ 1.0

            ZeroDivisionError: float division by zero
        """)
        assert search(exp_last, "\n".join(lines[-4:]))


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
