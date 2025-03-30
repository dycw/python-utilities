from __future__ import annotations

from asyncio import sleep
from logging import basicConfig
from sys import exc_info
from typing import TYPE_CHECKING, Literal

from pytest import LogCaptureFixture, raises

from tests.conftest import SKIPIF_CI
from tests.test_logging import TestSetupLogging
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.iterables import one
from utilities.logging import setup_logging
from utilities.sys import VERSION_MAJOR_MINOR, MakeExceptHookError, make_except_hook

if TYPE_CHECKING:
    from pathlib import Path
    from re import Pattern


class TestMakeExceptHook:
    def test_logging(self, *, caplog: LogCaptureFixture) -> None:
        basicConfig(format="{message}", style="{")
        hook = make_except_hook()
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        assert len(caplog.records) == 1
        record = one(caplog.records)
        expected = ""
        assert record.message == expected

    def test_with_setup_logging_decorated(
        self,
        *,
        tmp_path: Path,
        caplog: LogCaptureFixture,
        traceback_func_one: Pattern[str],
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
        hook = make_except_hook(logger=name)
        self._assert_files_and_caplog(tmp_path, caplog, "init")
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        self._assert_files_and_caplog(tmp_path, caplog, ("post", traceback_func_one))

    def test_with_setup_logging_undecorated(
        self,
        *,
        tmp_path: Path,
        caplog: LogCaptureFixture,
        traceback_func_untraced: Pattern[str],
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
        hook = make_except_hook(logger=name)
        self._assert_files_and_caplog(tmp_path, caplog, "init")
        try:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        self._assert_files_and_caplog(
            tmp_path, caplog, ("post", traceback_func_untraced)
        )

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

    def _assert_files_and_caplog(
        self,
        path: Path,
        caplog: LogCaptureFixture,
        check: Literal["init"] | tuple[Literal["post"], Pattern[str]],
        /,
    ) -> None:
        TestSetupLogging.assert_files(path, check)
        match check:
            case "init":
                assert len(caplog.records) == 0
            case ("post", _):
                assert len(caplog.records) == 1
                record = one(caplog.records)
                assert record.message == ""


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
