from __future__ import annotations

import datetime as dt
from types import NoneType
from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import mark, param, raises

from utilities.pathvalidate import valid_path_home
from utilities.types import (
    Duration,
    EnsureHashableError,
    IterableStrs,
    Number,
    PathLike,
    SequenceStrs,
    ensure_class,
    ensure_hashable,
    is_hashable,
    is_sized_not_str,
    issubclass_except_bool_int,
)


class TestDuration:
    @mark.parametrize("x", [param(0), param(0.0), param(dt.timedelta(0))])
    def test_success(self, *, x: Duration) -> None:
        die_if_unbearable(x, Duration)

    def test_error(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("0", Duration)


class TestEnsureClass:
    @mark.parametrize(
        ("obj", "expected"), [param(None, NoneType), param(NoneType, NoneType)]
    )
    def test_main(self, *, obj: Any, expected: type[Any]) -> None:
        assert ensure_class(obj) is expected


class TestEnsureHashable:
    @mark.parametrize("obj", [param(0), param((1, 2, 3))])
    def test_main(self, *, obj: Any) -> None:
        assert ensure_hashable(obj) == obj

    def test_error(self) -> None:
        with raises(EnsureHashableError):
            _ = ensure_hashable([1, 2, 3])


class TestIsHashable:
    @mark.parametrize(
        ("obj", "expected"),
        [param(0, True), param((1, 2, 3), True), param([1, 2, 3], False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_hashable(obj) is expected


class TestIsSizedNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


class TestIsSubclassExceptBoolInt:
    @mark.parametrize(
        ("x", "y", "expected"),
        [param(bool, bool, True), param(bool, int, False), param(int, int, True)],
    )
    def test_main(self, *, x: type[Any], y: type[Any], expected: bool) -> None:
        assert issubclass_except_bool_int(x, y) is expected

    def test_subclass_of_int(self) -> None:
        class MyInt(int):
            ...

        assert not issubclass_except_bool_int(bool, MyInt)


class TestIterableStrs:
    @mark.parametrize(
        "x",
        [
            param(["a", "b", "c"]),
            param(("a", "b", "c")),
            param({"a", "b", "c"}),
            param({"a": 1, "b": 2, "c": 3}),
        ],
    )
    def test_pass(self, *, x: IterableStrs) -> None:
        die_if_unbearable(x, IterableStrs)

    def test_fail(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("abc", IterableStrs)


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_ok(self, *, x: Number) -> None:
        die_if_unbearable(x, Number)

    def test_error(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("0", Number)


class TestPathLike:
    @mark.parametrize("path", [param(valid_path_home()), param("~")])
    def test_main(self, *, path: PathLike) -> None:
        die_if_unbearable(path, PathLike)

    def test_error(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable(None, PathLike)


class TestSequenceStrs:
    @mark.parametrize("x", [param(["a", "b", "c"]), param(("a", "b", "c"))])
    def test_pass(self, *, x: SequenceStrs) -> None:
        die_if_unbearable(x, SequenceStrs)

    @mark.parametrize(
        "x", [param({"a", "b", "c"}), param({"a": 1, "b": 2, "c": 3}), param("abc")]
    )
    def test_fail(self, *, x: IterableStrs | str) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable(x, SequenceStrs)
