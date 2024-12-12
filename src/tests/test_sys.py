from __future__ import annotations

from asyncio import sleep
from logging import basicConfig
from pathlib import Path
from re import search
from sys import exc_info

from pytest import LogCaptureFixture, raises

from tests.conftest import SKIPIF_CI
from tests.test_traceback_funcs.one import func_one
from utilities.iterables import one
from utilities.sys import (
    VERSION_MAJOR_MINOR,
    MakeExceptHookError,
    _get_default_logging_path,
    make_except_hook,
)
from utilities.text import strip_and_dedent


class TestGetDefaultLoggingPath:
    def test_main(self) -> None:
        assert isinstance(_get_default_logging_path(), Path)


class TestMakeExceptHook:
    def test_none(self, *, caplog: LogCaptureFixture) -> None:
        basicConfig(format="{message}", style="{")
        hook = make_except_hook()
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        assert len(caplog.records) == 0

    def test_log_raw(self, *, caplog: LogCaptureFixture) -> None:
        hook = make_except_hook(log_raw=True)
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        record = one(caplog.records)
        expected = "Result (56) must be divisible by 10"
        assert record.message == expected

    def test_log_assembled_path(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        hook = make_except_hook(log_assembled=True, log_assembled_dir=tmp_path)
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        self._assert_assemble(tmp_path, caplog)

    def test_non_error(self) -> None:
        hook = make_except_hook()
        exc_type, exc_val, traceback = exc_info()
        with raises(MakeExceptHookError, match="No exception to log"):
            hook(exc_type, exc_val, traceback)

    def test_callback_sync(self) -> None:
        flag = False

        def set_true() -> None:
            nonlocal flag
            flag = True

        hook = make_except_hook(callbacks=[set_true])
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        assert flag

    @SKIPIF_CI
    def test_callback_async(self) -> None:
        flag = False

        async def set_true() -> None:
            nonlocal flag
            flag = True
            await sleep(0.01)

        hook = make_except_hook(callbacks=[set_true])
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        assert flag

    def _assert_assemble(self, tmp_path: Path, caplog: LogCaptureFixture, /) -> None:
        expected = strip_and_dedent("""
            ExcPath(
                frames=[
                    _Frame(
                        module='tests.test_traceback_funcs.one',
                        name='func_one',
                        code_line='assert result % 10 == 0, f"Result ({result}) must be divisible by 10"',
                        line_num=16,
                        args=(1, 2, 3, 4),
                        kwargs={'c': 5, 'd': 6, 'e': 7},
                        locals={
                            'a': 2,
                            'b': 4,
                            'c': 10,
                            'args': (6, 8),
                            'kwargs': {'d': 12, 'e': 14},
                            'result': 56
                        }
                    )
                ],
                error=AssertionError('Result (56) must be divisible by 10')
            )""")
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        file = one(files)
        assert search(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.txt$", file.name)
        with file.open("r") as fh:
            assert fh.read() == expected

        record = one(caplog.records)
        assert record.message == expected


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
