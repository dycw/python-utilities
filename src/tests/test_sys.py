from __future__ import annotations

from asyncio import TaskGroup
from inspect import signature
from re import escape
from typing import TYPE_CHECKING, Any

from pytest import raises

from tests.test_sys_funcs.async_ import func_async
from tests.test_sys_funcs.decorated import (
    func_decorated_fifth,
    func_decorated_first,
    func_decorated_fourth,
    func_decorated_second,
    func_decorated_third,
)
from tests.test_sys_funcs.error import func_error_async, func_error_sync
from tests.test_sys_funcs.one import func_one
from tests.test_sys_funcs.two import func_two_first, func_two_second
from utilities.functions import get_func_name
from utilities.iterables import one
from utilities.sys import VERSION_MAJOR_MINOR, TraceMixin, _TraceMixinFrame
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetExcTraceInfo:
    def test_func_one(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        frame = one(error.frames)
        assert frame.depth == 1
        assert frame.max_depth == 1
        assert get_func_name(frame.func) == get_func_name(func_one)
        assert signature(frame.func) == signature(func_one)
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame.filename.parts[-2:] == ("test_sys_funcs", "one.py")
        assert frame.name == "func_one"
        assert frame.qualname == "func_one"
        assert (
            frame.line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.first_line_num == 8
        assert frame.line_num == 16
        assert frame.end_line_num == 16
        assert frame.col_num == 11
        assert frame.end_col_num == 27
        assert frame.locals == {
            "a": 2,
            "b": 4,
            "c": 10,
            "args": (6, 8),
            "kwargs": {"d": 12, "e": 14},
            "result": 56,
        }

    def test_func_two(self) -> None:
        result = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 36
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        error = exc_info.value
        assert isinstance(error, TraceMixin)
        expected = [
            (
                8,
                10,
                func_two_first,
                "return func_two_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)",
            ),
            (13, 16, func_two_second, self._assert_code_line),
        ]
        for depth, (frame, (first_ln, ln, func, code_ln)) in enumerate(
            zip(error.frames, expected, strict=True), start=1
        ):
            self._assert(frame, depth, 2, func, "two.py", first_ln, ln, code_ln, result)

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
            (53, 63, func_decorated_fifth, self._assert_code_line),
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
            frame, 1, 1, func_async, "async_.py", 9, 13, self._assert_code_line, result
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
            frame, 1, 1, func_async, "async_.py", 9, 13, self._assert_code_line, result
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
        code_line: str,
        result: int,
        /,
    ) -> None:
        assert frame.depth == depth
        assert frame.max_depth == max_depth
        assert get_func_name(frame.func) == get_func_name(func)
        assert signature(frame.func) == signature(func)
        assert frame.filename.parts[-2:] == ("test_sys_funcs", filename)
        assert frame.first_line_num == first_line_num
        assert frame.line_num == line_num
        assert frame.line == code_line
        assert frame.args == (2 ** (depth - 1), 2**depth, 3, 4)
        assert frame.kwargs == {"c": 5 * 2 ** (depth - 1), "d": 6, "e": 7, "f": -result}

    @property
    def _assert_code_line(self) -> str:
        return 'assert result > 0, f"Result ({result}) must be positive"'


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
