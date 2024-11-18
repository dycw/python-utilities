from __future__ import annotations

from asyncio import TaskGroup
from inspect import signature
from re import escape
from typing import TYPE_CHECKING, Any

from pytest import raises

from tests.test_traceback_funcs.async_ import func_async
from tests.test_traceback_funcs.decorated import (
    func_decorated_fifth,
    func_decorated_first,
    func_decorated_fourth,
    func_decorated_second,
    func_decorated_third,
)
from tests.test_traceback_funcs.error import func_error_async, func_error_sync
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.recursive import func_recursive
from tests.test_traceback_funcs.two import func_two_first, func_two_second
from utilities.functions import get_func_name
from utilities.iterables import OneNonUniqueError, one
from utilities.text import strip_and_dedent
from utilities.traceback import (
    TraceMixin,
    _TraceMixinFrame,
    trace,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestTrace:
    def test_func_one(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        self._assert(
            frame, 1, 1, func_one, "one.py", 8, 16, 11, 27, self._code_line_assert
        )

    def test_func_two(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        expected = [
            (func_two_first, 8, 15, 11, 54, self._code_line_call("func_two_second")),
            (func_two_second, 18, 26, 11, 27, self._code_line_assert),
        ]
        for depth, (frame, (func, ln1st, ln, col, col1st, code_ln)) in enumerate(
            zip(error.frames, expected, strict=True), start=1
        ):
            self._assert(
                frame, depth, 2, func, "two.py", ln1st, ln, col, col1st, code_ln
            )

    def test_func_decorated(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_decorated_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        expected = [
            (
                func_decorated_first,
                21,
                30,
                11,
                60,
                self._code_line_call("func_decorated_second"),
            ),
            (
                func_decorated_second,
                33,
                43,
                11,
                59,
                self._code_line_call("func_decorated_third"),
            ),
            (
                func_decorated_third,
                46,
                56,
                11,
                60,
                self._code_line_call("func_decorated_fourth"),
            ),
            (
                func_decorated_fourth,
                59,
                70,
                11,
                59,
                self._code_line_call("func_decorated_fifth"),
            ),
            (func_decorated_fifth, 73, 88, 11, 27, self._code_line_assert),
        ]
        for depth, (frame, (func, ln1st, ln, col, col1st, code_ln)) in enumerate(
            zip(error.frames, expected, strict=True), start=1
        ):
            self._assert(
                frame, depth, 5, func, "decorated.py", ln1st, ln, col, col1st, code_ln
            )

    def test_func_recursive(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_recursive(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        assert len(error.frames) == 2
        expected = [
            (
                13,
                23,
                15,
                72,
                "return func_recursive(a, b, *args, c=c, _is_last=True, **kwargs)",
                {"result": 56},
            ),
            (10, 21, 11, 27, self._code_line_assert, {}),
        ]
        for depth, (frame, (ln1st, ln, col, col1st, code_ln, extra)) in enumerate(
            zip(error.frames, expected, strict=True), start=1
        ):
            if depth != 2:
                continue
            self._assert(
                frame,
                depth,
                2,
                func_recursive,
                "recursive.py",
                ln1st,
                ln,
                col,
                col1st,
                code_ln,
                extra_locals=extra,
            )

    async def test_func_async(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = await func_async(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        self._assert(
            frame, 1, 1, func_async, "async_.py", 9, 18, 11, 27, self._code_line_assert
        )

    async def test_task_group(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            async with TaskGroup() as tg:
                _ = tg.create_task(func_async(1, 2, 3, 4, c=5, d=6, e=7))
        error = one(exc_info.value.exceptions)
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        self._assert(
            frame, 1, 1, func_async, "async_.py", 9, 18, 11, 27, self._code_line_assert
        )

    def test_custom_error(self) -> None:
        @trace
        def raises_custom_error() -> bool:
            return one([True, False])

        with raises(OneNonUniqueError) as exc_info:
            _ = raises_custom_error()
        one_error = exc_info.value
        assert isinstance(one_error, TraceMixin)
        assert one_error.first is True
        assert one_error.second is False

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
        with raises(
            TypeError,
            match=escape(
                "func_error_sync() missing 1 required positional argument: 'b'"
            ),
        ):
            _ = func_error_sync(1)  # pyright: ignore[reportCallIssue]

    async def test_error_bind_async(self) -> None:
        with raises(
            TypeError,
            match=escape(
                "func_error_async() takes 2 positional arguments but 3 were given"
            ),
        ):
            _ = await func_error_async(1, 2, 3)  # pyright: ignore[reportCallIssue]

    def _assert(
        self,
        frame: _TraceMixinFrame,
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
        assert get_func_name(frame.func) == get_func_name(func)
        assert signature(frame.func) == signature(func)
        scale = 2 ** (depth - 1)
        assert frame.args == (scale, 2 * scale, 3 * scale, 4 * scale)
        assert frame.kwargs == {"c": 5 * scale, "d": 6 * scale, "e": 7 * scale}
        assert frame.filename.parts[-2:] == ("test_traceback_funcs", filename)
        assert frame.name == get_func_name(func)
        assert frame.qualname == get_func_name(func)
        assert frame.code_line == code_line
        assert frame.first_line_num == first_line_num
        assert frame.line_num == line_num
        assert frame.end_line_num == line_num
        assert frame.col_num == col_num
        assert frame.end_col_num == end_col_num
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
