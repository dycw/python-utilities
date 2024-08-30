from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from pytest import CaptureFixture, mark, param

from utilities.loguru import (
    HandlerConfiguration,
    InterceptHandler,
    LogLevel,
    logged_sleep_async,
    logged_sleep_sync,
    make_catch_hook,
)
from utilities.text import ensure_str, strip_and_dedent

if TYPE_CHECKING:
    from utilities.types import Duration


class TestInterceptHandler:
    def test_main(self) -> None:
        _ = InterceptHandler()


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
