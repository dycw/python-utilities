from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from pytest import mark, param, raises

from utilities.datetime import get_now, get_today
from utilities.sentinel import sentinel
from utilities.types import (
    Duration,
    EnsureBoolError,
    EnsureClassError,
    EnsureDateError,
    EnsureDatetimeError,
    EnsureFloatError,
    EnsureHashableError,
    EnsureIntError,
    EnsureMemberError,
    EnsureNumberError,
    EnsureSizedError,
    EnsureSizedNotStrError,
    EnsureTimeError,
    Number,
    PathLike,
    ensure_bool,
    ensure_class,
    ensure_date,
    ensure_datetime,
    ensure_float,
    ensure_hashable,
    ensure_int,
    ensure_member,
    ensure_number,
    ensure_sized,
    ensure_sized_not_str,
    ensure_time,
    is_hashable,
    is_sized,
    is_sized_not_str,
    is_string_mapping,
    is_tuple_or_string_mapping,
    issubclass_except_bool_int,
    make_isinstance,
)


class TestDuration:
    @mark.parametrize("x", [param(0), param(0.0), param(dt.timedelta(0))])
    def test_success(self, *, x: Duration) -> None:
        assert isinstance(x, Duration)

    def test_error(self) -> None:
        assert not isinstance("0", Duration)


class TestEnsureBool:
    @mark.parametrize(
        ("obj", "nullable"), [param(True, False), param(True, True), param(None, True)]
    )
    def test_main(self, *, obj: bool | None, nullable: bool) -> None:
        _ = ensure_bool(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a boolean"),
            param(True, "Object .* must be a boolean or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureBoolError, match=f"{match}; got .* instead"):
            _ = ensure_bool(sentinel, nullable=nullable)


class TestEnsureClass:
    @mark.parametrize(
        ("obj", "cls", "nullable"),
        [
            param(True, bool, False),
            param(True, bool, True),
            param(True, (bool,), False),
            param(True, (bool,), True),
            param(None, bool, True),
        ],
    )
    def test_main(self, *, obj: Any, cls: Any, nullable: bool) -> None:
        _ = ensure_class(obj, cls, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be an instance of .*"),
            param(True, "Object .* must be an instance of .* or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureClassError, match=f"{match}; got .* instead"):
            _ = ensure_class(sentinel, bool, nullable=nullable)


class TestEnsureDate:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(get_today(), False), param(get_today(), True), param(None, True)],
    )
    def test_main(self, *, obj: dt.date | None, nullable: bool) -> None:
        _ = ensure_date(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a date"),
            param(True, "Object .* must be a date or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureDateError, match=f"{match}; got .* instead"):
            _ = ensure_date(sentinel, nullable=nullable)


class TestEnsureDatetime:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(get_now(), False), param(get_now(), True), param(None, True)],
    )
    def test_main(self, *, obj: dt.datetime | None, nullable: bool) -> None:
        _ = ensure_datetime(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a datetime"),
            param(True, "Object .* must be a datetime or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureDatetimeError, match=f"{match}; got .* instead"):
            _ = ensure_datetime(sentinel, nullable=nullable)


class TestEnsureFloat:
    @mark.parametrize(
        ("obj", "nullable"), [param(0.0, False), param(0.0, True), param(None, True)]
    )
    def test_main(self, *, obj: float | None, nullable: bool) -> None:
        _ = ensure_float(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a float"),
            param(True, "Object .* must be a float or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureFloatError, match=f"{match}; got .* instead"):
            _ = ensure_float(sentinel, nullable=nullable)


class TestEnsureHashable:
    @mark.parametrize("obj", [param(0), param((1, 2, 3))])
    def test_main(self, *, obj: Any) -> None:
        assert ensure_hashable(obj) == obj

    def test_error(self) -> None:
        with raises(EnsureHashableError, match=r"Object .* must be hashable\."):
            _ = ensure_hashable([1, 2, 3])


class TestEnsureInt:
    @mark.parametrize(
        ("obj", "nullable"), [param(0, False), param(0, True), param(None, True)]
    )
    def test_main(self, *, obj: int | None, nullable: bool) -> None:
        _ = ensure_int(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be an integer"),
            param(True, "Object .* must be an integer or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureIntError, match=f"{match}; got .* instead"):
            _ = ensure_int(sentinel, nullable=nullable)


class TestEnsureMember:
    @mark.parametrize(
        ("obj", "nullable"),
        [
            param(True, True),
            param(True, False),
            param(False, True),
            param(False, False),
            param(None, True),
        ],
    )
    def test_main(self, *, obj: Any, nullable: bool) -> None:
        _ = ensure_member(obj, {True, False}, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a member of .*"),
            param(True, "Object .* must be a member of .* or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureMemberError, match=match):
            _ = ensure_member(sentinel, {True, False}, nullable=nullable)


class TestEnsureNumber:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(0, False), param(0.0, False), param(0.0, True), param(None, True)],
    )
    def test_main(self, *, obj: Number, nullable: bool) -> None:
        _ = ensure_number(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a number"),
            param(True, "Object .* must be a number or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureNumberError, match=f"{match}; got .* instead"):
            _ = ensure_number(sentinel, nullable=nullable)


class TestEnsureSized:
    @mark.parametrize("obj", [param([]), param(()), param("")])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized(obj)

    def test_error(self) -> None:
        with raises(EnsureSizedError, match=r"Object .* must be sized"):
            _ = ensure_sized(None)


class TestEnsureSizedNotStr:
    @mark.parametrize("obj", [param([]), param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized_not_str(obj)

    @mark.parametrize("obj", [param(None), param("")])
    def test_error(self, *, obj: Any) -> None:
        with raises(
            EnsureSizedNotStrError, match="Object .* must be sized, but not a string"
        ):
            _ = ensure_sized_not_str(obj)


class TestEnsureTime:
    @mark.parametrize(
        ("obj", "nullable"),
        [
            param(get_now().time(), False),
            param(get_now().time(), True),
            param(None, True),
        ],
    )
    def test_main(self, *, obj: dt.time | None, nullable: bool) -> None:
        _ = ensure_time(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a time"),
            param(True, "Object .* must be a time or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureTimeError, match=f"{match}; got .* instead"):
            _ = ensure_time(sentinel, nullable=nullable)


class TestIsHashable:
    @mark.parametrize(
        ("obj", "expected"),
        [param(0, True), param((1, 2, 3), True), param([1, 2, 3], False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_hashable(obj) is expected


class TestIsSized:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", True)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized(obj) is expected


class TestIsSizedNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


class TestIsStringMapping:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_string_mapping(obj)
        assert result is expected


class TestIsSubclassExceptBoolInt:
    @mark.parametrize(
        ("x", "y", "expected"),
        [param(bool, bool, True), param(bool, int, False), param(int, int, True)],
    )
    def test_main(self, *, x: type[Any], y: type[Any], expected: bool) -> None:
        assert issubclass_except_bool_int(x, y) is expected

    def test_subclass_of_int(self) -> None:
        class MyInt(int): ...

        assert not issubclass_except_bool_int(bool, MyInt)


class TestIsTupleOrStringMapping:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((1, 2, 3), True),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_tuple_or_string_mapping(obj)
        assert result is expected


class TestMakeIsInstance:
    @mark.parametrize(
        ("obj", "expected"), [param(True, True), param(False, True), param(None, False)]
    )
    def test_main(self, *, obj: bool | None, expected: bool) -> None:
        func = make_isinstance(bool)
        result = func(obj)
        assert result is expected


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_ok(self, *, x: Number) -> None:
        assert isinstance(x, Number)

    def test_error(self) -> None:
        assert not isinstance(None, Number)


class TestPathLike:
    @mark.parametrize("path", [param(Path.home()), param("~")])
    def test_main(self, *, path: PathLike) -> None:
        assert isinstance(path, PathLike)

    def test_error(self) -> None:
        assert not isinstance(None, PathLike)
