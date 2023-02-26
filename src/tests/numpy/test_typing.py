from typing import Any

from beartype.door import die_if_unbearable
from numpy import empty
from numpy import zeros
from pytest import mark
from pytest import param

from utilities.numpy import datetime64D
from utilities.numpy import datetime64ns
from utilities.numpy import datetime64Y
from utilities.numpy.typing import NDArray0
from utilities.numpy.typing import NDArray1
from utilities.numpy.typing import NDArray2
from utilities.numpy.typing import NDArray3
from utilities.numpy.typing import NDArrayB
from utilities.numpy.typing import NDArrayB0
from utilities.numpy.typing import NDArrayB1
from utilities.numpy.typing import NDArrayB2
from utilities.numpy.typing import NDArrayB3
from utilities.numpy.typing import NDArrayDD
from utilities.numpy.typing import NDArrayDD0
from utilities.numpy.typing import NDArrayDD1
from utilities.numpy.typing import NDArrayDD2
from utilities.numpy.typing import NDArrayDD3
from utilities.numpy.typing import NDArrayDns
from utilities.numpy.typing import NDArrayDns0
from utilities.numpy.typing import NDArrayDns1
from utilities.numpy.typing import NDArrayDns2
from utilities.numpy.typing import NDArrayDns3
from utilities.numpy.typing import NDArrayDY
from utilities.numpy.typing import NDArrayDY0
from utilities.numpy.typing import NDArrayDY1
from utilities.numpy.typing import NDArrayDY2
from utilities.numpy.typing import NDArrayDY3
from utilities.numpy.typing import NDArrayF
from utilities.numpy.typing import NDArrayF0
from utilities.numpy.typing import NDArrayF1
from utilities.numpy.typing import NDArrayF2
from utilities.numpy.typing import NDArrayF3
from utilities.numpy.typing import NDArrayI
from utilities.numpy.typing import NDArrayI0
from utilities.numpy.typing import NDArrayI1
from utilities.numpy.typing import NDArrayI2
from utilities.numpy.typing import NDArrayI3
from utilities.numpy.typing import NDArrayO
from utilities.numpy.typing import NDArrayO0
from utilities.numpy.typing import NDArrayO1
from utilities.numpy.typing import NDArrayO2
from utilities.numpy.typing import NDArrayO3


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
