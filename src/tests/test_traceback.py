from __future__ import annotations

from asyncio import TaskGroup
from typing import TYPE_CHECKING, Any, Literal

from pytest import mark, raises

from tests.test_traceback_funcs.async_ import func_async
from tests.test_traceback_funcs.decorated_async import func_decorated_async_first
from tests.test_traceback_funcs.decorated_sync import func_decorated_sync_first
from tests.test_traceback_funcs.error import func_error_async, func_error_sync
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.recursive import func_recursive
from tests.test_traceback_funcs.two import func_two_first
from utilities.functions import get_func_name, get_func_qualname
from utilities.iterables import OneNonUniqueError, one
from utilities.text import ensure_str, strip_and_dedent
from utilities.traceback import (
    ExcGroup,
    ExcPath,
    TraceMixin,
    _CallArgs,
    _CallArgsError,
    _TraceMixinFrame,
    assemble_exception_paths,
    trace,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from traceback import FrameSummary
    from types import FrameType


@mark.only
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
        assert frame.line_num == 16
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert set(frame.locals) == {"a", "b", "c", "args", "kwargs", "result"}
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
        first, second = exc_path
        assert first.module == "tests.test_traceback_funcs.two"
        assert first.name == "func_two_first"
        assert first.code_line == "return func_two_second(a, b, *args, c=c, **kwargs)"
        assert first.args == (1, 2, 3, 4)
        assert first.kwargs == {"c": 5, "d": 6, "e": 7}
        assert first.locals["a"] == 2
        assert first.locals["b"] == 4
        assert first.locals["args"] == (6, 8)
        assert first.locals["kwargs"] == {"d": 12, "e": 14}
        assert second.module == "tests.test_traceback_funcs.two"
        assert second.name == "func_two_second"
        assert (
            second.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert second.args == (2, 4, 6, 8)
        assert second.kwargs == {"c": 10, "d": 12, "e": 14}
        assert second.locals["a"] == 4
        assert second.locals["b"] == 8
        assert second.locals["args"] == (12, 16)
        assert second.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

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
        first, second = exc_path
        assert first.module == "tests.test_traceback_funcs.recursive"
        assert first.name == "func_recursive"
        assert first.code_line == "return func_recursive(a, b, *args, c=c, **kwargs)"
        assert first.args == (1, 2, 3, 4)
        assert first.kwargs == {"c": 5, "d": 6, "e": 7}
        assert first.locals["a"] == 2
        assert first.locals["b"] == 4
        assert first.locals["args"] == (6, 8)
        assert first.locals["kwargs"] == {"d": 12, "e": 14}
        assert second.module == "tests.test_traceback_funcs.recursive"
        assert second.name == "func_recursive"
        assert (
            second.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert second.args == (2, 4, 6, 8)
        assert second.kwargs == {"c": 10, "d": 12, "e": 14}
        assert second.locals["a"] == 4
        assert second.locals["b"] == 8
        assert second.locals["args"] == (12, 16)
        assert second.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    async def test_task_group(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            async with TaskGroup() as tg:
                _ = tg.create_task(func_async(1, 2, 3, 4, c=5, d=6, e=7))
        exc_group = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_group, ExcGroup)
        assert len(exc_group) == 1
        exc_path = one(exc_group)
        assert isinstance(exc_path, ExcPath)
        frame = one(exc_path)
        assert frame.module == "tests.test_traceback_funcs.async_"
        assert frame.name == "func_async"
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

    def _assert_decorated(
        self, exc_path: ExcPath, sync_or_async: Literal["sync", "async"], /
    ) -> None:
        assert len(exc_path) == 5
        first, second, _, fourth, fifth = exc_path
        match sync_or_async:
            case "sync":
                maybe_await = ""
            case "async":
                maybe_await = "await "
        assert first.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert first.name == f"func_decorated_{sync_or_async}_first"
        assert (
            first.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_second(a, b, *args, c=c, **kwargs)"
        )
        assert first.args == (1, 2, 3, 4)
        assert first.kwargs == {"c": 5, "d": 6, "e": 7}
        assert first.locals["a"] == 2
        assert first.locals["b"] == 4
        assert first.locals["args"] == (6, 8)
        assert first.locals["kwargs"] == {"d": 12, "e": 14}
        assert second.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert second.name == f"func_decorated_{sync_or_async}_second"
        assert (
            second.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_third(a, b, *args, c=c, **kwargs)"
        )
        assert second.args == (2, 4, 6, 8)
        assert second.kwargs == {"c": 10, "d": 12, "e": 14}
        assert second.locals["a"] == 4
        assert second.locals["b"] == 8
        assert second.locals["args"] == (12, 16)
        assert second.locals["kwargs"] == {"d": 24, "e": 28}
        assert fourth.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert fourth.name == f"func_decorated_{sync_or_async}_fourth"
        assert (
            fourth.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_fifth(a, b, *args, c=c, **kwargs)"
        )
        assert fourth.args == (8, 16, 24, 32)
        assert fourth.kwargs == {"c": 40, "d": 48, "e": 56}
        assert fourth.locals["a"] == 16
        assert fourth.locals["b"] == 32
        assert fourth.locals["args"] == (48, 64)
        assert fourth.locals["kwargs"] == {"d": 96, "e": 112}
        assert fifth.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert fifth.name == f"func_decorated_{sync_or_async}_fifth"
        assert (
            fifth.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert fifth.args == (16, 32, 48, 64)
        assert fifth.kwargs == {"c": 80, "d": 96, "e": 112}
        assert fifth.locals["a"] == 32
        assert fifth.locals["b"] == 64
        assert fifth.locals["args"] == (96, 128)
        assert fifth.locals["kwargs"] == {"d": 192, "e": 224}
        assert isinstance(exc_path.error, AssertionError)


class TestTrace:
    def test_pretty(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        result = error.pretty(location=False)
        expected = strip_and_dedent("""
            Error running:

              1. func_two_first
              2. func_two_second
              >> AssertionError: Result (112) must be divisible by 10

            Frames:

              1/2. func_two_first

                Inputs:

                  args[0] = 1
                  args[1] = 2
                  args[2] = 3
                  args[3] = 4
                  kwargs[c] = 5
                  kwargs[d] = 6
                  kwargs[e] = 7

                Locals:

                  a = 2
                  b = 4
                  c = 10
                  args = (6, 8)
                  kwargs = {'d': 12, 'e': 14}

                >> return func_two_second(a, b, *args, c=c, **kwargs)

              2/2. func_two_second

                Inputs:

                  args[0] = 2
                  args[1] = 4
                  args[2] = 6
                  args[3] = 8
                  kwargs[c] = 10
                  kwargs[d] = 12
                  kwargs[e] = 14

                Locals:

                  a = 4
                  b = 8
                  c = 20
                  args = (12, 16)
                  kwargs = {'d': 24, 'e': 28}
                  result = 112

                >> assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
                >> AssertionError: Result (112) must be divisible by 10
        """)
        assert result == expected

    def test_error_bind_sync(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = func_error_sync(1)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_sync'; missing a required argument: 'b'
            args[0] = 1
            """
        )
        assert msg == expected

    async def test_error_bind_async(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = await func_error_async(1, 2, 3)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_async'; too many positional arguments
            args[0] = 1
            args[1] = 2
            args[2] = 3
            """
        )
        assert msg == expected

    def _assert(
        self,
        frame: _TraceMixinFrame[_CallArgs | None],
        depth: int,
        max_depth: int,
        func: Callable[..., Any],
        filename: str,
        first_line_num: int,
        line_num: int,
        col_num: int,
        end_col_num: int,
        code_line: str,
        /,
        *,
        extra_locals: dict[str, Any] | None = None,
    ) -> None:
        assert frame.depth == depth
        assert frame.max_depth == max_depth
        assert get_func_qualname(frame.func) == get_func_qualname(func)
        scale = 2 ** (depth - 1)
        assert frame.args == (scale, 2 * scale, 3 * scale, 4 * scale)
        assert frame.kwargs == {"c": 5 * scale, "d": 6 * scale, "e": 7 * scale}
        assert frame.filename.parts[-2:] == ("test_traceback_funcs", filename)
        assert frame.module == func.__module__
        assert frame.name == get_func_name(func)
        assert frame.qualname == get_func_name(func)
        assert frame.code_line == code_line
        assert frame.first_line_num == first_line_num
        assert frame.line_num == line_num
        assert frame.end_line_num == line_num
        assert frame.col_num == col_num
        assert frame.end_col_num == end_col_num
        assert (frame.extra is None) or isinstance(frame.extra, _CallArgs)
        scale_plus = 2 * scale
        locals_ = (
            {
                "a": scale_plus,
                "b": 2 * scale_plus,
                "c": 5 * scale_plus,
                "args": (3 * scale_plus, 4 * scale_plus),
                "kwargs": {"d": 6 * scale_plus, "e": 7 * scale_plus},
            }
            | ({"result": frame.locals["result"]} if depth == max_depth else {})
            | ({} if extra_locals is None else extra_locals)
        )
        assert frame.locals == locals_

    @property
    def _code_line_assert(self) -> str:
        return 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'

    def _code_line_call(self, func: str, /) -> str:
        return f"return {func}(a, b, *args, c=c, **kwargs)"


class TestYieldExtendedFrameSummaries:
    def test_explicit_traceback(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        with raises(NotImplementedError) as exc_info:
            f()
        frames = list(
            yield_extended_frame_summaries(exc_info.value, traceback=exc_info.tb)
        )
        assert len(frames) == 3
        expected = [
            TestYieldExtendedFrameSummaries.test_explicit_traceback.__qualname__,
            f.__qualname__,
            g.__qualname__,
        ]
        for frame, exp in zip(frames, expected, strict=True):
            assert frame.qualname == exp

    def test_implicit_traceback(self) -> None:
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
                TestYieldExtendedFrameSummaries.test_implicit_traceback.__qualname__,
                f.__qualname__,
                g.__qualname__,
            ]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.qualname == exp

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
