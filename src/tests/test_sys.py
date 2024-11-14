from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pytest import mark, param

from tests.test_sys_funcs.decorated import (
    func_decorated_fifth,
    func_decorated_first,
    func_decorated_fourth,
    func_decorated_second,
    func_decorated_third,
)
from tests.test_sys_funcs.one import func_one
from tests.test_sys_funcs.two import func_two_first, func_two_second
from utilities.iterables import one
from utilities.sentinel import sentinel
from utilities.sys import (
    VERSION_MAJOR_MINOR,
    _FrameInfo,
    _GetCallerOutput,
    get_caller,
    get_exc_trace_info,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetCaller:
    @mark.parametrize(
        ("depth", "expected"),
        [param(1, "inner"), param(2, "outer"), param(3, "test_main")],
        ids=str,
    )
    def test_main(self, *, depth: int, expected: str) -> None:
        def outer() -> _GetCallerOutput:
            return inner()

        def inner() -> _GetCallerOutput:
            return get_caller(depth=depth)

        result = outer()
        assert result["module"] == "tests.test_sys"
        assert result["name"] == expected

    @mark.parametrize(
        ("depth", "expected"),
        [param(1, "inner"), param(2, "mid"), param(3, "outer"), param(4, "test_depth")],
        ids=str,
    )
    def test_depth(self, *, depth: int, expected: str) -> None:
        def outer() -> _GetCallerOutput:
            return mid()

        def mid() -> _GetCallerOutput:
            return inner()

        def inner() -> _GetCallerOutput:
            return get_caller(depth=depth)

        result = outer()
        assert result["module"] == "tests.test_sys"
        assert result["name"] == expected


class TestGetExcTraceInfo:
    def test_func_one(self) -> None:
        result = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 28
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frame = one(exc_info.frames)
            assert frame.depth == 1
            assert frame.max_depth == 1
            assert frame.filename.parts[-2:] == ("test_sys_funcs", "one.py")
            assert frame.first_line_num == 8
            assert frame.line_num == 11
            assert frame.func.__name__ == func_one.__name__
            assert frame.args == (1, 2, 3, 4)
            assert frame.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert frame.result is sentinel
            assert isinstance(frame.error, AssertionError)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    def test_func_two(self) -> None:
        result = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 36
        try:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            expected = [(8, 10, func_two_first), (13, 16, func_two_second)]
            for depth, (frame, (first_ln, ln, func)) in enumerate(
                zip(exc_info.frames, expected, strict=True), start=1
            ):
                self._assert(frame, depth, 2, "two.py", first_ln, ln, func, result)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    def test_func_decorated(self) -> None:
        result = func_decorated_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 148
        try:
            _ = func_decorated_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            expected = [
                (21, 25, func_decorated_first),
                (28, 33, func_decorated_second),
                (36, 41, func_decorated_third),
                (44, 50, func_decorated_fourth),
                (53, 63, func_decorated_fifth),
            ]
            for depth, (frame, (first_ln, ln, func)) in enumerate(
                zip(exc_info.frames, expected, strict=True), start=1
            ):
                self._assert(
                    frame, depth, 5, "decorated.py", first_ln, ln, func, result
                )
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    def _assert(
        self,
        frame: _FrameInfo,
        depth: int,
        max_depth: int,
        filename: str,
        first_line_num: int,
        line_num: int,
        func: Callable[..., Any],
        result: int,
        /,
    ) -> None:
        assert frame.depth == depth
        assert frame.max_depth == max_depth
        assert frame.filename.parts[-2:] == ("test_sys_funcs", filename)
        assert frame.first_line_num == first_line_num
        assert frame.line_num == line_num
        assert frame.func.__name__ == func.__name__
        assert frame.args == (2 ** (depth - 1), 2**depth, 3, 4)
        assert frame.kwargs == {"c": 5 * 2 ** (depth - 1), "d": 6, "e": 7, "f": -result}
        assert frame.result is sentinel
        assert isinstance(frame.error, AssertionError)


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
