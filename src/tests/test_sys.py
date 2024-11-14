from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import floats
from pytest import mark, param

from utilities.sys import (
    VERSION_MAJOR_MINOR,
    _GetCallerOutput,
    get_caller,
    get_exception_info,
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


def _get_exception_info_first(a: float, b: float, /) -> float:
    c = a + b
    d = a - b
    return _get_exception_info_second(c, d)


def _get_exception_info_second(c: float, d: float, /) -> float:
    e = c + d
    f = c - d
    return e / f


class TestGetExceptionInfo:
    def test_main(self) -> None:
        a = 134217729.0
        b = 1e-8
        try:
            _ = _get_exception_info_first(a, b)
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


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
