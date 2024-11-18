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
from tests.test_traceback_funcs.two import func_two_first, func_two_second
from utilities.functions import get_func_name
from utilities.iterables import one
from utilities.text import strip_and_dedent
from utilities.traceback import (
    TraceMixin,
    _TraceMixinFrame,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from collections.abc import Callable
from pytest import mark, param


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

    @mark.only
    def test_func_decorated(self) -> None:
        result = func_decorated_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 148
        with raises(AssertionError) as exc_info:
            _ = func_decorated_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        expected = [
            (
                21,
                25,
                func_decorated_first,
                "return func_decorated_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)",
            ),
            (
                28,
                33,
                func_decorated_second,
                "return func_decorated_third(2 * a, 2 * b, *args, c=2 * c, **kwargs)",
            ),
            (
                36,
                41,
                func_decorated_third,
                "return func_decorated_fourth(2 * a, 2 * b, *args, c=2 * c, **kwargs)",
            ),
            (
                44,
                50,
                func_decorated_fourth,
                "return func_decorated_fifth(2 * a, 2 * b, *args, c=2 * c, **kwargs)",
            ),
            (53, 63, func_decorated_fifth, self._code_line_assert),
        ]
        for depth, (frame, (first_ln, ln, func, code_ln)) in enumerate(
            zip(error.frames, expected, strict=True), start=1
        ):
            self._assert(
                frame, depth, 5, func, "decorated.py", first_ln, ln, code_ln, result
            )

    async def test_func_async(self) -> None:
        result = await func_async(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 28
        with raises(AssertionError) as exc_info:
            _ = await func_async(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        self._assert(
            frame, 1, 1, func_async, "async_.py", 9, 13, self._code_line_assert, result
        )

    async def test_task_group(self) -> None:
        result = await func_async(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 28
        with raises(ExceptionGroup) as exc_info:
            async with TaskGroup() as tg:
                _ = tg.create_task(func_async(1, 2, 3, 4, c=5, d=6, e=7, f=-result))
        error_group = exc_info.value
        error = one(error_group.exceptions)
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        self._assert(
            frame, 1, 1, func_async, "async_.py", 9, 13, self._code_line_assert, result
        )

    def test_pretty(self) -> None:
        result = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 36
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        result = error.pretty(location=False)
        expected = strip_and_dedent("""
            Error running:

              1. func_two_first
              2. func_two_second
              >> AssertionError: Result (0) must be positive

            Traced frames:

              1/2. func_two_first
                args[0] = 1
                args[1] = 2
                args[2] = 3
                args[3] = 4
                kwargs['c'] = 5
                kwargs['d'] = 6
                kwargs['e'] = 7
                kwargs['f'] = -36
                >> return func_two_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)

              2/2. func_two_second
                args[0] = 2
                args[1] = 4
                args[2] = 3
                args[3] = 4
                kwargs['c'] = 10
                kwargs['d'] = 6
                kwargs['e'] = 7
                kwargs['f'] = -36
                >> assert result > 0, f"Result ({result}) must be positive"
                >> AssertionError: Result (0) must be positive
        """)
        assert result == expected

    def test_error_sync(self) -> None:
        with raises(
            TypeError,
            match=escape(
                "func_error_sync() missing 1 required positional argument: 'b'"
            ),
        ):
            _ = func_error_sync(1)  # pyright: ignore[reportCallIssue]

    async def test_error_async(self) -> None:
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
        locals_ = {
            "a": scale_plus,
            "b": 2 * scale_plus,
            "c": 5 * scale_plus,
            "args": (3 * scale_plus, 4 * scale_plus),
            "kwargs": {"d": 6 * scale_plus, "e": 7 * scale_plus},
        } | ({"result": frame.locals["result"]} if depth == max_depth else {})
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
