from typing import Annotated, Any

from beartype.door import die_if_unbearable
from numpy import empty, zeros
from numpy.typing import NDArray
from pytest import mark, param

from utilities.beartype.numpy import (
    DTypeB,
    DTypeDD,
    DTypeDns,
    DTypeDY,
    DTypeF,
    DTypeI,
    DTypeO,
    NDim0,
    NDim1,
    NDim2,
    NDim3,
)
from utilities.numpy import datetime64D, datetime64ns, datetime64Y


class TestAnnotations:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, DTypeB),
            param(datetime64D, DTypeDD),
            param(datetime64Y, DTypeDY),
            param(datetime64ns, DTypeDns),
            param(float, DTypeF),
            param(int, DTypeI),
            param(object, DTypeO),
        ],
    )
    def test_dtypes(self, dtype: Any, hint: Any) -> None:
        arr = empty(0, dtype=dtype)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])

    @mark.parametrize(
        ("ndim", "hint"),
        [param(0, NDim0), param(1, NDim1), param(2, NDim2), param(3, NDim3)],
    )
    def test_ndims(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])
