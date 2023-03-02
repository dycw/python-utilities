from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import mark, param, raises

from utilities.types import NoneType, Number, ensure_class, issubclass_except_bool_int


class TestEnsureClass:
    @mark.parametrize(
        ("x", "expected"), [param(None, NoneType), param(NoneType, NoneType)]
    )
    def test_main(self, x: Any, expected: type[Any]) -> None:
        assert ensure_class(x) is expected


class TestIsSubclassExceptBoolInt:
    @mark.parametrize(
        ("x", "y", "expected"),
        [param(bool, bool, True), param(bool, int, False), param(int, int, True)],
    )
    def test_main(self, x: type[Any], y: type[Any], expected: bool) -> None:
        assert issubclass_except_bool_int(x, y) is expected

    def test_subclass_of_int(self) -> None:
        class MyInt(int):
            ...

        assert not issubclass_except_bool_int(bool, MyInt)


class TestNoneType:
    def test_main(self) -> None:
        assert isinstance(None, NoneType)


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_success(self, x: Number) -> None:
        die_if_unbearable(x, Number)

    def test_error(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("0", Number)
