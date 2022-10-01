from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import mark
from pytest import param
from pytest import raises

from utilities.types import Number


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_success(self, x: Number) -> None:
        die_if_unbearable(x, Number)

    def test_error(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("0", Number)
