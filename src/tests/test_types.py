from __future__ import annotations

import datetime as dt
from types import NoneType
from typing import Any, cast

import pytest
from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation

from utilities.pathvalidate import valid_path_home
from utilities.types import (
    Duration,
    EnsureClassError,
    EnsureHashableError,
    EnsureNotNoneError,
    EnsureSizedError,
    EnsureSizedNotStrError,
    IterableStrs,
    Number,
    PathLike,
    SequenceStrs,
    ensure_class,
    ensure_hashable,
    ensure_not_none,
    ensure_sized,
    ensure_sized_not_str,
    get_class,
    get_class_name,
    if_not_none,
    is_hashable,
    is_sized,
    is_sized_not_str,
    issubclass_except_bool_int,
)


class TestDuration:
    @pytest.mark.parametrize(
        "x", [pytest.param(0), pytest.param(0.0), pytest.param(dt.timedelta(0))]
    )
    def test_success(self, *, x: Duration) -> None:
        die_if_unbearable(x, Duration)

    def test_error(self) -> None:
        with pytest.raises(BeartypeDoorHintViolation):
            die_if_unbearable("0", Duration)


class TestEnsureClass:
    def test_single_pass(self) -> None:
        result = ensure_class(None, NoneType)
        assert isinstance(result, NoneType)

    def test_multiple_pass(self) -> None:
        result = ensure_class(None, (NoneType, int))
        assert isinstance(result, NoneType)

    def test_single_error(self) -> None:
        with pytest.raises(
            EnsureClassError, match=r"Object .* must be an instance of .*; got .*\."
        ):
            _ = ensure_class(None, int)

    def test_multiple_error(self) -> None:
        with pytest.raises(
            EnsureClassError, match=r"Object .* must be an instance of .*, .*; got .*\."
        ):
            _ = ensure_class(None, (int, float))


class TestEnsureHashable:
    @pytest.mark.parametrize("obj", [pytest.param(0), pytest.param((1, 2, 3))])
    def test_main(self, *, obj: Any) -> None:
        assert ensure_hashable(obj) == obj

    def test_error(self) -> None:
        with pytest.raises(EnsureHashableError, match=r"Object .* must be hashable\."):
            _ = ensure_hashable([1, 2, 3])


class TestEnsureNotNone:
    def test_main(self) -> None:
        maybe_int = cast(int | None, 0)
        result = ensure_not_none(maybe_int)
        assert result == 0

    def test_error(self) -> None:
        with pytest.raises(EnsureNotNoneError, match=r"Object must not be None\."):
            _ = ensure_not_none(None)


class TestEnsureSized:
    @pytest.mark.parametrize(
        "obj", [pytest.param([]), pytest.param(()), pytest.param("")]
    )
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized(obj)

    def test_error(self) -> None:
        with pytest.raises(EnsureSizedError, match=r"Object .* must be sized\."):
            _ = ensure_sized(None)


class TestEnsureSizedNotStr:
    @pytest.mark.parametrize("obj", [pytest.param([]), pytest.param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized_not_str(obj)

    @pytest.mark.parametrize("obj", [pytest.param(None), pytest.param("")])
    def test_error(self, *, obj: Any) -> None:
        with pytest.raises(
            EnsureSizedNotStrError, match=r"Object .* must be sized, but not a string\."
        ):
            _ = ensure_sized_not_str(obj)


class TestGetClass:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [pytest.param(None, NoneType), pytest.param(NoneType, NoneType)],
    )
    def test_main(self, *, obj: Any, expected: type[Any]) -> None:
        assert get_class(obj) is expected


class TestGetClassName:
    def test_class(self) -> None:
        class Example: ...

        assert get_class_name(Example) == "Example"

    def test_instance(self) -> None:
        class Example: ...

        assert get_class_name(Example()) == "Example"


class TestIfNotNone:
    def test_uses_first(self) -> None:
        result = if_not_none(0, "0")
        assert result == 0

    def test_uses_second(self) -> None:
        result = if_not_none(None, 0)
        assert result == 0


class TestIsHashable:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [
            pytest.param(0, True),
            pytest.param((1, 2, 3), True),
            pytest.param([1, 2, 3], False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_hashable(obj) is expected


class TestIsSized:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [
            pytest.param(None, False),
            pytest.param([], True),
            pytest.param((), True),
            pytest.param("", True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized(obj) is expected


class TestIsSizedNotStr:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [
            pytest.param(None, False),
            pytest.param([], True),
            pytest.param((), True),
            pytest.param("", False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


class TestIsSubclassExceptBoolInt:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(bool, bool, True),
            pytest.param(bool, int, False),
            pytest.param(int, int, True),
        ],
    )
    def test_main(self, *, x: type[Any], y: type[Any], expected: bool) -> None:
        assert issubclass_except_bool_int(x, y) is expected

    def test_subclass_of_int(self) -> None:
        class MyInt(int): ...

        assert not issubclass_except_bool_int(bool, MyInt)


class TestIterableStrs:
    @pytest.mark.parametrize(
        "x",
        [
            pytest.param(["a", "b", "c"]),
            pytest.param(("a", "b", "c")),
            pytest.param({"a", "b", "c"}),
            pytest.param({"a": 1, "b": 2, "c": 3}),
        ],
    )
    def test_pass(self, *, x: IterableStrs) -> None:
        die_if_unbearable(x, IterableStrs)

    def test_fail(self) -> None:
        with pytest.raises(BeartypeDoorHintViolation):
            die_if_unbearable("abc", IterableStrs)


class TestNumber:
    @pytest.mark.parametrize("x", [pytest.param(0), pytest.param(0.0)])
    def test_ok(self, *, x: Number) -> None:
        die_if_unbearable(x, Number)

    def test_error(self) -> None:
        with pytest.raises(BeartypeDoorHintViolation):
            die_if_unbearable("0", Number)


class TestPathLike:
    @pytest.mark.parametrize(
        "path", [pytest.param(valid_path_home()), pytest.param("~")]
    )
    def test_main(self, *, path: PathLike) -> None:
        die_if_unbearable(path, PathLike)

    def test_error(self) -> None:
        with pytest.raises(BeartypeDoorHintViolation):
            die_if_unbearable(None, PathLike)


class TestSequenceStrs:
    @pytest.mark.parametrize(
        "x", [pytest.param(["a", "b", "c"]), pytest.param(("a", "b", "c"))]
    )
    def test_pass(self, *, x: SequenceStrs) -> None:
        die_if_unbearable(x, SequenceStrs)

    @pytest.mark.parametrize(
        "x",
        [
            pytest.param({"a", "b", "c"}),
            pytest.param({"a": 1, "b": 2, "c": 3}),
            pytest.param("abc"),
        ],
    )
    def test_fail(self, *, x: IterableStrs | str) -> None:
        with pytest.raises(BeartypeDoorHintViolation):
            die_if_unbearable(x, SequenceStrs)
