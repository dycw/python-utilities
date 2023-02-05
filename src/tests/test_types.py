from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import mark, param, raises

from utilities.types import NoneType, Number


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
