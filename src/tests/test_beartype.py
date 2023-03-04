from collections.abc import Iterable
from typing import Annotated, Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from numpy import empty, zeros
from numpy.typing import NDArray
from pytest import mark, param, raises

from utilities.beartype import IterableStrs, NDim0, NDim1, NDim2, NDim3


class TestIterableStrs:
    def test_main(self) -> None:
        die_if_unbearable(["a", "b", "c"], IterableStrs)
        die_if_unbearable("abc", Iterable[str])
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("abc", IterableStrs)


class TestNDims:
    @mark.parametrize(
        ("ndim", "hint"),
        [param(0, NDim0), param(1, NDim1), param(2, NDim2), param(3, NDim3)],
    )
    def test_main(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])
