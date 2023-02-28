from beartype.door import die_if_unbearable
from pytest import mark
from pytest import param
from pytest import raises

from utilities.types import Number
from utilities.typing import NeverError
from utilities.typing import never


class TestNever:
    def test_main(self) -> None:
        with raises(NeverError):
            never(None)  # type: ignore[]


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_main(self, x: Number) -> None:
        die_if_unbearable(x, Number)
