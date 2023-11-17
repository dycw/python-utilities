from __future__ import annotations

from typing import Any

from pytest import mark, param

from utilities.types import NoneType, ensure_class, issubclass_except_bool_int


class TestEnsureClass:
    @mark.parametrize(
        ("x", "expected"), [param(None, NoneType), param(NoneType, NoneType)]
    )
    def test_main(self, *, x: Any, expected: type[Any]) -> None:
        assert ensure_class(x) is expected


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


class TestNoneType:
    def test_main(self) -> None:
        assert isinstance(None, NoneType)
