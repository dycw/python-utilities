from __future__ import annotations

from pytest import mark, param

from tests.test_sys_funcs.one import func_one
from tests.test_sys_funcs.one_deco import func_one_deco_after, func_one_deco_before
from tests.test_sys_funcs.two import func_two_first, func_two_second
from tests.test_sys_funcs.two_deco import (
    func_two_deco_after_first,
    func_two_deco_before_first,
)
from utilities.iterables import one
from utilities.sentinel import sentinel
from utilities.sys import (
    VERSION_MAJOR_MINOR,
    _GetCallerOutput,
    get_caller,
    get_exc_trace_info,
)


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
            frames = exc_info.frames
            assert len(frames) == 2
            for frame in frames:
                assert frame.max_depth == 2
                assert frame.filename.parts[-2:] == ("test_sys_funcs", "two.py")
                assert frame.result is sentinel
                assert isinstance(frame.error, AssertionError)
            first, second = frames
            assert first.depth == 1
            assert first.first_line_num == 8
            assert first.line_num == 10
            assert first.func.__name__ == func_two_first.__name__
            assert first.args == (1, 2, 3, 4)
            assert first.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert second.depth == 2
            assert second.first_line_num == 13
            assert second.line_num == 16
            assert second.func.__name__ == func_two_second.__name__
            assert second.args == (2, 4, 3, 4)
            assert second.kwargs == {"c": 10, "d": 6, "e": 7, "f": -result}
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    # @mark.only
    def test_func_one_deco_before(self) -> None:
        result = func_one_deco_before(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 28
        try:
            _ = func_one_deco_before(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frame = one(exc_info.frames)
            assert frame.depth == 1
            assert frame.max_depth == 1
            assert frame.filename.parts[-2:] == ("test_sys_funcs", "one_deco.py")
            assert frame.first_line_num == 21
            assert frame.line_num == 27
            assert frame.func.__name__ == func_one_deco_before.__name__
            assert frame.args == (1, 2, 3, 4)
            assert frame.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert frame.result is sentinel
            assert isinstance(frame.error, AssertionError)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    # @mark.only
    def test_func_one_deco_after(self) -> None:
        result = func_one_deco_after(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 28
        try:
            _ = func_one_deco_after(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frame = one(exc_info.frames)
            assert frame.depth == 1
            assert frame.max_depth == 1
            assert frame.filename.parts[-2:] == ("test_sys_funcs", "one_deco.py")
            assert frame.first_line_num == 31
            assert frame.line_num == 37
            assert frame.func.__name__ == func_one_deco_after.__name__
            assert frame.args == (1, 2, 3, 4)
            assert frame.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert frame.result is sentinel
            assert isinstance(frame.error, AssertionError)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    # @mark.only
    def test_func_two_deco_before(self) -> None:
        result = func_two_deco_before_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 36
        try:
            _ = func_two_deco_before_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frame = one(exc_info.frames)
            assert frame.depth == 1
            assert frame.max_depth == 1
            assert frame.filename.parts[-2:] == ("test_sys_funcs", "one_deco.py")
            assert frame.first_line_num == 21
            assert frame.line_num == 27
            assert frame.func.__name__ == func_one_deco_before.__name__
            assert frame.args == (1, 2, 3, 4)
            assert frame.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert frame.result is sentinel
            assert isinstance(frame.error, AssertionError)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)

    @mark.only
    def test_func_two_deco_after(self) -> None:
        result = func_two_deco_after_first(1, 2, 3, 4, c=5, d=6, e=7)
        assert result == 36
        try:
            _ = func_two_deco_after_first(1, 2, 3, 4, c=5, d=6, e=7, f=-result)
        except AssertionError:
            exc_info = get_exc_trace_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frame = one(exc_info.frames)
            assert frame.depth == 1
            assert frame.max_depth == 1
            assert frame.filename.parts[-2:] == ("test_sys_funcs", "one_deco.py")
            assert frame.first_line_num == 21
            assert frame.line_num == 27
            assert frame.func.__name__ == func_one_deco_before.__name__
            assert frame.args == (1, 2, 3, 4)
            assert frame.kwargs == {"c": 5, "d": 6, "e": 7, "f": -result}
            assert frame.result is sentinel
            assert isinstance(frame.error, AssertionError)
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
