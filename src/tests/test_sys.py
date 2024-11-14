from __future__ import annotations

from pathlib import Path
from string import ascii_lowercase

from pytest import mark, param

from tests.test_sys_funcs.one import func_one
from utilities.sys import (
    VERSION_MAJOR_MINOR,
    _GetCallerOutput,
    get_caller,
    get_exception_info,
    trace,
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


@trace
def _get_exception_info_first(
    a: float, b: float, /, *args: float, c: float = 0, **kwargs: float
) -> float:
    sum_ = a + b + c + sum(args) + sum(kwargs.values())
    diff = a - b
    return _get_exception_info_second(
        sum_,
        diff,
        *args[::2],
        z=c,
        **{k: v for k, v in kwargs.items() if k[0] in ascii_lowercase[::2]},
    )


@trace
def _get_exception_info_second(
    x: float, y: float, /, *args: float, z: float = 0, **kwargs: float
) -> float:
    sum_ = x + y + z + sum(args) + sum(kwargs.values())
    diff = x - y
    return sum_ / diff


class TestGetExceptionInfo:
    @mark.skip
    def test_main(self) -> None:
        a = 134217729.0
        b = 1e-8
        try:
            _ = _get_exception_info_first(a, b, c=0)
        except ZeroDivisionError:
            exc_info = get_exception_info()
            assert exc_info.exc_type is ZeroDivisionError
            assert isinstance(exc_info.exc_value, ZeroDivisionError)
            frames = exc_info.frames
            assert len(frames) == 3
            for frame in frames:
                assert frame.filename == Path(__file__)
            first, second, third = frames
            assert first.func_name == TestGetCaller.test_main.__name__
            assert second.func_name == _get_exception_info_first.__name__
            assert third.func_name == _get_exception_info_second.__name__
            assert 0, exc_info

    def test_func_one(self) -> None:
        assert func_one(1, 2, 3, 4, c=5, d=6, e=7) == 28
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7, f=-29)
        except AssertionError:
            exc_info = get_exception_info()
            assert exc_info.exc_type is AssertionError
            assert isinstance(exc_info.exc_value, AssertionError)
            frames = exc_info.frames
            assert len(frames) == 1
            frame = frames[0]
            assert frame.filename == Path(__file__)
            assert frame.func_name == func_one.__name__
            assert 0, exc_info
        else:  # pragma: no cover
            msg = "Expected an assertion"
            raise AssertionError(msg)


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
