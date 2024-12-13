from __future__ import annotations

from logging import ERROR, getLogger
from typing import TYPE_CHECKING, Literal

from beartype.roar import BeartypeCallHintReturnViolation
from pytest import raises

from tests.conftest import FLAKY, SKIPIF_CI
from tests.test_traceback_funcs.beartype import func_beartype
from tests.test_traceback_funcs.beartype_error import func_beartype_error_first
from tests.test_traceback_funcs.chain import func_chain_first
from tests.test_traceback_funcs.decorated_async import func_decorated_async_first
from tests.test_traceback_funcs.decorated_sync import func_decorated_sync_first
from tests.test_traceback_funcs.error_bind import (
    func_error_bind_async,
    func_error_bind_sync,
)
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.recursive import func_recursive
from tests.test_traceback_funcs.runtime_async import (
    disable_trace_for_func_runtime_async,
    func_runtime_async,
)
from tests.test_traceback_funcs.runtime_sync import (
    disable_trace_for_func_runtime_sync,
    func_runtime_sync,
)
from tests.test_traceback_funcs.setup import func_setup
from tests.test_traceback_funcs.task_group_one import func_task_group_one_first
from tests.test_traceback_funcs.task_group_two import func_task_group_two_first
from tests.test_traceback_funcs.two import func_two_first
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.iterables import OneNonUniqueError, one
from utilities.text import ensure_str, strip_and_dedent
from utilities.traceback import (
    ExcChain,
    ExcGroup,
    ExcPath,
    TracebackHandler,
    _CallArgsError,
    assemble_exception_paths,
    trace,
    yield_exceptions,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from pathlib import Path
    from traceback import FrameSummary
    from types import FrameType


class TestAssembleExceptionsPaths:
    def test_func_one(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 1
        frame = one(exc_path)
        assert frame.module == "tests.test_traceback_funcs.one"
        assert frame.name == "func_one"
        assert (
            frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame.locals["a"] == 2
        assert frame.locals["b"] == 4
        assert frame.locals["args"] == (6, 8)
        assert frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_two(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 2
        frame1, frame2 = exc_path
        assert frame1.module == "tests.test_traceback_funcs.two"
        assert frame1.name == "func_two_first"
        assert frame1.code_line == "return func_two_second(a, b, *args, c=c, **kwargs)"
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == "tests.test_traceback_funcs.two"
        assert frame2.name == "func_two_second"
        assert (
            frame2.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["a"] == 4
        assert frame2.locals["b"] == 8
        assert frame2.locals["args"] == (12, 16)
        assert frame2.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_beartype(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_beartype(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 1
        frame = one(exc_path)
        assert frame.module == "tests.test_traceback_funcs.beartype"
        assert frame.name == "func_beartype"
        assert (
            frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame.locals["a"] == 2
        assert frame.locals["b"] == 4
        assert frame.locals["args"] == (6, 8)
        assert frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_beartype_error(self) -> None:
        with raises(BeartypeCallHintReturnViolation) as exc_info:
            _ = func_beartype_error_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 2
        frame1, frame2 = exc_path
        assert frame1.module == "tests.test_traceback_funcs.beartype_error"
        assert frame1.name == "func_beartype_error_first"
        assert (
            frame1.code_line
            == "return func_beartype_error_second(a, b, *args, c=c, **kwargs)"
        )
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module is None
        assert frame2.name == "func_beartype_error_second"
        assert frame2.code_line == ""
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["args"] == (2, 4, 6, 8)
        assert frame2.locals["kwargs"] == {"c": 10, "d": 12, "e": 14}
        assert isinstance(exc_path.error, BeartypeCallHintReturnViolation)

    def test_func_chain(self) -> None:
        with raises(ValueError, match=".*") as exc_info:
            _ = func_chain_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_chain = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_chain, ExcChain)
        assert len(exc_chain) == 2
        path1, path2 = exc_chain
        assert isinstance(path1, ExcPath)
        assert len(path1) == 1
        frame1 = one(path1)
        assert frame1.module == "tests.test_traceback_funcs.chain"
        assert frame1.name == "func_chain_second"
        assert (
            frame1.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame1.args == (2, 4, 6, 8)
        assert frame1.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame1.locals["a"] == 4
        assert frame1.locals["b"] == 8
        assert frame1.locals["args"] == (12, 16)
        assert frame1.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(path2, ExcPath)
        frame2 = one(path2)
        assert frame2.module == "tests.test_traceback_funcs.chain"
        assert frame2.name == "func_chain_first"
        assert frame2.code_line == "raise ValueError(msg) from error"
        assert frame2.args == (1, 2, 3, 4)
        assert frame2.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame2.locals["a"] == 2
        assert frame2.locals["b"] == 4
        assert frame2.locals["args"] == (6, 8)
        assert frame2.locals["kwargs"] == {"d": 12, "e": 14}

    def test_func_decorated_sync(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_decorated_sync_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        self._assert_decorated(exc_path, "sync")
        assert len(exc_path) == 5

    async def test_func_decorated_async(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = await func_decorated_async_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = assemble_exception_paths(exc_info.value)
        assert isinstance(error, ExcPath)
        self._assert_decorated(error, "async")

    def test_func_recursive(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_recursive(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 2
        frame1, frame2 = exc_path
        assert frame1.module == "tests.test_traceback_funcs.recursive"
        assert frame1.name == "func_recursive"
        assert frame1.code_line == "return func_recursive(a, b, *args, c=c, **kwargs)"
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == "tests.test_traceback_funcs.recursive"
        assert frame2.name == "func_recursive"
        assert (
            frame2.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["a"] == 4
        assert frame2.locals["b"] == 8
        assert frame2.locals["args"] == (12, 16)
        assert frame2.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_runtime_sync(self) -> None:
        with raises(AssertionError) as exc_info1:
            _ = func_runtime_sync(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path1 = assemble_exception_paths(exc_info1.value)
        assert isinstance(exc_path1, ExcPath)
        with disable_trace_for_func_runtime_sync():
            with raises(AssertionError) as exc_info2:
                _ = func_runtime_sync(1, 2, 3, 4, c=5, d=6, e=7)
            exc_path2 = assemble_exception_paths(exc_info2.value)
            assert isinstance(exc_path2, AssertionError)
        with raises(AssertionError) as exc_info3:
            _ = func_runtime_sync(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path3 = assemble_exception_paths(exc_info3.value)
        assert isinstance(exc_path3, ExcPath)

    async def test_func_runtime_async(self) -> None:
        with raises(AssertionError) as exc_info1:
            _ = await func_runtime_async(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path1 = assemble_exception_paths(exc_info1.value)
        assert isinstance(exc_path1, ExcPath)
        with disable_trace_for_func_runtime_async():
            with raises(AssertionError) as exc_info2:
                _ = await func_runtime_async(1, 2, 3, 4, c=5, d=6, e=7)
            exc_path2 = assemble_exception_paths(exc_info2.value)
            assert isinstance(exc_path2, AssertionError)
        with raises(AssertionError) as exc_info3:
            _ = await func_runtime_async(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path3 = assemble_exception_paths(exc_info3.value)
        assert isinstance(exc_path3, ExcPath)

    def test_func_setup(self) -> None:
        with raises(AssertionError) as exc_info1:
            _ = func_setup(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path1 = assemble_exception_paths(exc_info1.value)
        assert isinstance(exc_path1, AssertionError)

    async def test_func_task_group_one(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            await func_task_group_one_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_group = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_group, ExcGroup)
        assert exc_group.path is not None
        assert len(exc_group.path) == 1
        path_frame = one(exc_group.path)
        assert path_frame.module == "tests.test_traceback_funcs.task_group_one"
        assert path_frame.name == "func_task_group_one_first"
        assert path_frame.code_line == "async with TaskGroup() as tg:"
        assert path_frame.args == (1, 2, 3, 4)
        assert path_frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert path_frame.locals["a"] == 2
        assert path_frame.locals["b"] == 4
        assert path_frame.locals["args"] == (6, 8)
        assert path_frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_group.path.error, ExceptionGroup)
        assert len(exc_group.errors) == 1
        exc_path = one(exc_group.errors)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 1
        frame = one(exc_path)
        assert frame.module == "tests.test_traceback_funcs.task_group_one"
        assert frame.name == "func_task_group_one_second"
        assert (
            frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.args == (2, 4, 6, 8)
        assert frame.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame.locals["a"] == 4
        assert frame.locals["b"] == 8
        assert frame.locals["args"] == (12, 16)
        assert frame.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    @FLAKY
    @SKIPIF_CI
    async def test_func_task_group_two(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            await func_task_group_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_group = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_group, ExcGroup)
        assert exc_group.path is not None
        assert len(exc_group.path) == 1
        frame0 = one(exc_group.path)
        assert frame0.module == "tests.test_traceback_funcs.task_group_two"
        assert frame0.name == "func_task_group_two_first"
        assert frame0.code_line == "async with TaskGroup() as tg:"
        assert frame0.args == (1, 2, 3, 4)
        assert frame0.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame0.locals["a"] == 2
        assert frame0.locals["b"] == 4
        assert frame0.locals["args"] == (6, 8)
        assert frame0.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_group.path.error, ExceptionGroup)
        assert len(exc_group.errors) == 2
        exc_path1, exc_path2 = exc_group.errors
        assert isinstance(exc_path1, ExcPath)
        assert len(exc_path1) == 1
        frame1 = one(exc_path1)
        assert frame1.module == "tests.test_traceback_funcs.task_group_two"
        assert frame1.name == "func_task_group_two_second"
        assert (
            frame1.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame1.args == (2, 4, 6, 8)
        assert frame1.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame1.locals["a"] == 4
        assert frame1.locals["b"] == 8
        assert frame1.locals["args"] == (12, 16)
        assert frame1.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path1.error, AssertionError)
        assert isinstance(exc_path2, ExcPath)
        assert len(exc_path2) == 1
        frame2 = one(exc_path2)
        assert frame2.module == "tests.test_traceback_funcs.task_group_two"
        assert frame2.name == "func_task_group_two_second"
        assert (
            frame2.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame2.args == (3, 5, 7, 9)
        assert frame2.kwargs == {"c": 11, "d": 13, "e": 15}
        assert frame2.locals["a"] == 6
        assert frame2.locals["b"] == 10
        assert frame2.locals["args"] == (14, 18)
        assert frame2.locals["kwargs"] == {"d": 26, "e": 30}
        assert isinstance(exc_path2.error, AssertionError)

    def test_func_untraced(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        error = assemble_exception_paths(exc_info.value)
        assert isinstance(error, AssertionError)

    def test_custom_error(self) -> None:
        @trace
        def raises_custom_error() -> bool:
            return one([True, False])

        with raises(OneNonUniqueError) as exc_info:
            _ = raises_custom_error()
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert exc_path.error.first is True
        assert exc_path.error.second is False

    def test_error_bind_sync(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = func_error_bind_sync(1)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_bind_sync'; missing a required argument: 'b'
            args[0] = 1
            """
        )
        assert msg == expected

    async def test_error_bind_async(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = await func_error_bind_async(1, 2, 3)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_bind_async'; too many positional arguments
            args[0] = 1
            args[1] = 2
            args[2] = 3
            """
        )
        assert msg == expected

    def _assert_decorated(
        self, exc_path: ExcPath, sync_or_async: Literal["sync", "async"], /
    ) -> None:
        assert len(exc_path) == 5
        frame1, frame2, _, frame4, frame5 = exc_path
        match sync_or_async:
            case "sync":
                maybe_await = ""
            case "async":
                maybe_await = "await "
        assert frame1.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame1.name == f"func_decorated_{sync_or_async}_first"
        assert (
            frame1.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_second(a, b, *args, c=c, **kwargs)"
        )
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame2.name == f"func_decorated_{sync_or_async}_second"
        assert (
            frame2.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_third(a, b, *args, c=c, **kwargs)"
        )
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["a"] == 4
        assert frame2.locals["b"] == 8
        assert frame2.locals["args"] == (12, 16)
        assert frame2.locals["kwargs"] == {"d": 24, "e": 28}
        assert frame4.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame4.name == f"func_decorated_{sync_or_async}_fourth"
        assert (
            frame4.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_fifth(a, b, *args, c=c, **kwargs)"
        )
        assert frame4.args == (8, 16, 24, 32)
        assert frame4.kwargs == {"c": 40, "d": 48, "e": 56}
        assert frame4.locals["a"] == 16
        assert frame4.locals["b"] == 32
        assert frame4.locals["args"] == (48, 64)
        assert frame4.locals["kwargs"] == {"d": 96, "e": 112}
        assert frame5.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame5.name == f"func_decorated_{sync_or_async}_fifth"
        assert (
            frame5.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame5.args == (16, 32, 48, 64)
        assert frame5.kwargs == {"c": 80, "d": 96, "e": 112}
        assert frame5.locals["a"] == 32
        assert frame5.locals["b"] == 64
        assert frame5.locals["args"] == (96, 128)
        assert frame5.locals["kwargs"] == {"d": 192, "e": 224}
        assert isinstance(exc_path.error, AssertionError)


class TestTracebackHandler:
    def test_decorated(self, *, tmp_path: Path) -> None:
        name = TestTracebackHandler.test_decorated.__qualname__
        logger = getLogger(name)
        handler = TracebackHandler(path=tmp_path)
        logger.addHandler(handler)
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        self.assert_file(tmp_path, "decorated")

    def test_undecorated(self, *, tmp_path: Path) -> None:
        name = TestTracebackHandler.test_undecorated.__qualname__
        logger = getLogger(name)
        handler = TracebackHandler(path=tmp_path)
        logger.addHandler(handler)
        try:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        self.assert_file(tmp_path, "undecorated")

    def test_no_logging(self, *, tmp_path: Path) -> None:
        name = TestTracebackHandler.test_no_logging.__qualname__
        logger = getLogger(name)
        logger.setLevel(ERROR)
        handler = TracebackHandler(path=tmp_path)
        handler.setLevel(ERROR)
        logger.addHandler(handler)
        logger.error("message")
        assert len(list(tmp_path.iterdir())) == 0

    @classmethod
    def assert_file(
        cls, path: Path, check: Literal["decorated", "undecorated"], /
    ) -> None:
        files = list(path.iterdir())
        assert len(files) == 1
        with one(files).open() as fh:
            contents = fh.read()
        match check:
            case "decorated":
                cls._check_decorated(contents)
            case "undecorated":
                cls._check_undecorated(contents)

    @classmethod
    def _check_decorated(cls, text: str, /) -> None:
        expected = strip_and_dedent(
            """
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
                )
                """
        )
        assert text == expected

    @classmethod
    def _check_undecorated(cls, text: str, /) -> None:
        lines = text.splitlines()
        assert len(lines) == 8
        assert lines[0] == "Traceback (most recent call last):"
        tail = "\n".join(lines[5:])
        expected = strip_and_dedent("""
                    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
                           ^^^^^^^^^^^^^^^^
                AssertionError: Result (56) must be divisible by 10
                """)
        assert tail == expected


class TestYieldExceptions:
    def test_main(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def f() -> None:
            try:
                return g()
            except FirstError:
                raise SecondError from FirstError

        def g() -> None:
            raise FirstError

        with raises(SecondError) as exc_info:
            f()
        errors = list(yield_exceptions(exc_info.value))
        assert len(errors) == 2
        first, second = errors
        assert isinstance(first, SecondError)
        assert isinstance(second, FirstError)


class TestYieldExtendedFrameSummaries:
    def test_main(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        try:
            f()
        except NotImplementedError as error:
            frames = list(yield_extended_frame_summaries(error))
            assert len(frames) == 3
            expected = [
                TestYieldExtendedFrameSummaries.test_main.__qualname__,
                f.__qualname__,
                g.__qualname__,
            ]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.qualname == exp
        else:
            msg = "Expected an error"
            raise RuntimeError(msg)

    def test_extra(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        def extra(summary: FrameSummary, frame: FrameType, /) -> tuple[int | None, int]:
            left = None if summary.locals is None else len(summary.locals)
            return left, len(frame.f_locals)

        try:
            f()
        except NotImplementedError as error:
            frames = list(yield_extended_frame_summaries(error, extra=extra))
            assert len(frames) == 3
            expected = [(5, 5), (1, 1), (None, 0)]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.extra == exp


class TestYieldFrames:
    def test_main(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        with raises(NotImplementedError) as exc_info:
            f()
        frames = list(yield_frames(traceback=exc_info.tb))
        assert len(frames) == 3
        expected = ["test_main", "f", "g"]
        for frame, exp in zip(frames, expected, strict=True):
            assert frame.f_code.co_name == exp
