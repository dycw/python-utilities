from typing import Any

from beartype.door import die_if_unbearable
from numpy import empty
from numpy import zeros
from pytest import mark
from pytest import param
from xarray import DataArray

from utilities.numpy import datetime64ns
from utilities.xarray.typing import DataArray0
from utilities.xarray.typing import DataArray1
from utilities.xarray.typing import DataArray2
from utilities.xarray.typing import DataArray3
from utilities.xarray.typing import DataArrayB
from utilities.xarray.typing import DataArrayB0
from utilities.xarray.typing import DataArrayB1
from utilities.xarray.typing import DataArrayB2
from utilities.xarray.typing import DataArrayB3
from utilities.xarray.typing import DataArrayDns
from utilities.xarray.typing import DataArrayDns0
from utilities.xarray.typing import DataArrayDns1
from utilities.xarray.typing import DataArrayDns2
from utilities.xarray.typing import DataArrayDns3
from utilities.xarray.typing import DataArrayF
from utilities.xarray.typing import DataArrayF0
from utilities.xarray.typing import DataArrayF1
from utilities.xarray.typing import DataArrayF2
from utilities.xarray.typing import DataArrayF3
from utilities.xarray.typing import DataArrayI
from utilities.xarray.typing import DataArrayI0
from utilities.xarray.typing import DataArrayI1
from utilities.xarray.typing import DataArrayI2
from utilities.xarray.typing import DataArrayI3
from utilities.xarray.typing import DataArrayO
from utilities.xarray.typing import DataArrayO0
from utilities.xarray.typing import DataArrayO1
from utilities.xarray.typing import DataArrayO2
from utilities.xarray.typing import DataArrayO3


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, DataArrayB),
            param(datetime64ns, DataArrayDns),
            param(float, DataArrayF),
            param(int, DataArrayI),
            param(object, DataArrayO),
        ],
    )
    def test_dtype(self, dtype: Any, hint: Any) -> None:
        arr = DataArray(empty(0, dtype=dtype))
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("ndim", "hint"),
        [
            param(0, DataArray0),
            param(1, DataArray1),
            param(2, DataArray2),
            param(3, DataArray3),
        ],
    )
    def test_ndim(self, ndim: int, hint: Any) -> None:
        arr = DataArray(empty(zeros(ndim, dtype=int), dtype=float))
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("dtype", "ndim", "hint"),
        [
            # ndim 0
            param(bool, 0, DataArrayB0),
            param(datetime64ns, 0, DataArrayDns0),
            param(float, 0, DataArrayF0),
            param(int, 0, DataArrayI0),
            param(object, 0, DataArrayO0),
            # ndim 1
            param(bool, 1, DataArrayB1),
            param(datetime64ns, 1, DataArrayDns1),
            param(float, 1, DataArrayF1),
            param(int, 1, DataArrayI1),
            param(object, 1, DataArrayO1),
            # ndim 2
            param(bool, 2, DataArrayB2),
            param(datetime64ns, 2, DataArrayDns2),
            param(float, 2, DataArrayF2),
            param(int, 2, DataArrayI2),
            param(object, 2, DataArrayO2),
            # ndim 3
            param(bool, 3, DataArrayB3),
            param(datetime64ns, 3, DataArrayDns3),
            param(float, 3, DataArrayF3),
            param(int, 3, DataArrayI3),
            param(object, 3, DataArrayO3),
        ],
    )
    def test_compound(self, dtype: Any, ndim: int, hint: Any) -> None:
        arr = DataArray(empty(zeros(ndim, dtype=int), dtype=dtype))
        die_if_unbearable(arr, hint)
