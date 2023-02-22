from typing import Any

from beartype.door import die_if_unbearable
from numpy import empty, zeros
from pytest import mark, param

from utilities.numpy import datetime64D, datetime64ns, datetime64Y
from utilities.numpy.typing import (
    NDArray0,
    NDArray1,
    NDArray2,
    NDArray3,
    NDArrayB,
    NDArrayB0,
    NDArrayB1,
    NDArrayB2,
    NDArrayB3,
    NDArrayDD,
    NDArrayDD0,
    NDArrayDD1,
    NDArrayDD2,
    NDArrayDD3,
    NDArrayDns,
    NDArrayDns0,
    NDArrayDns1,
    NDArrayDns2,
    NDArrayDns3,
    NDArrayDY,
    NDArrayDY0,
    NDArrayDY1,
    NDArrayDY2,
    NDArrayDY3,
    NDArrayF,
    NDArrayF0,
    NDArrayF1,
    NDArrayF2,
    NDArrayF3,
    NDArrayI,
    NDArrayI0,
    NDArrayI1,
    NDArrayI2,
    NDArrayI3,
    NDArrayO,
    NDArrayO0,
    NDArrayO1,
    NDArrayO2,
    NDArrayO3,
)


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, NDArrayB),
            param(datetime64D, NDArrayDD),
            param(datetime64Y, NDArrayDY),
            param(datetime64ns, NDArrayDns),
            param(float, NDArrayF),
            param(int, NDArrayI),
            param(object, NDArrayO),
        ],
    )
    def test_dtype(self, dtype: Any, hint: Any) -> None:
        arr = empty(0, dtype=dtype)
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("ndim", "hint"),
        [
            param(0, NDArray0),
            param(1, NDArray1),
            param(2, NDArray2),
            param(3, NDArray3),
        ],
    )
    def test_ndim(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("dtype", "ndim", "hint"),
        [
            # ndim 0
            param(bool, 0, NDArrayB0),
            param(datetime64D, 0, NDArrayDD0),
            param(datetime64Y, 0, NDArrayDY0),
            param(datetime64ns, 0, NDArrayDns0),
            param(float, 0, NDArrayF0),
            param(int, 0, NDArrayI0),
            param(object, 0, NDArrayO0),
            # ndim 1
            param(bool, 1, NDArrayB1),
            param(datetime64D, 1, NDArrayDD1),
            param(datetime64Y, 1, NDArrayDY1),
            param(datetime64ns, 1, NDArrayDns1),
            param(float, 1, NDArrayF1),
            param(int, 1, NDArrayI1),
            param(object, 1, NDArrayO1),
            # ndim 2
            param(bool, 2, NDArrayB2),
            param(datetime64D, 2, NDArrayDD2),
            param(datetime64Y, 2, NDArrayDY2),
            param(datetime64ns, 2, NDArrayDns2),
            param(float, 2, NDArrayF2),
            param(int, 2, NDArrayI2),
            param(object, 2, NDArrayO2),
            # ndim 3
            param(bool, 3, NDArrayB3),
            param(datetime64D, 3, NDArrayDD3),
            param(datetime64Y, 3, NDArrayDY3),
            param(datetime64ns, 3, NDArrayDns3),
            param(float, 3, NDArrayF3),
            param(int, 3, NDArrayI3),
            param(object, 3, NDArrayO3),
        ],
    )
    def test_compound(self, dtype: Any, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=dtype)
        die_if_unbearable(arr, hint)
