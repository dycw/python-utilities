from typing import Annotated
from typing import Any

from beartype.door import die_if_unbearable
from numpy import empty
from numpy import zeros
from numpy.typing import NDArray
from pytest import mark
from pytest import param

from utilities.beartype.numpy import DTypeB
from utilities.beartype.numpy import DTypeDD
from utilities.beartype.numpy import DTypeDns
from utilities.beartype.numpy import DTypeDY
from utilities.beartype.numpy import DTypeF
from utilities.beartype.numpy import DTypeI
from utilities.beartype.numpy import DTypeO
from utilities.beartype.numpy import NDim0
from utilities.beartype.numpy import NDim1
from utilities.beartype.numpy import NDim2
from utilities.beartype.numpy import NDim3
from utilities.numpy import datetime64D
from utilities.numpy import datetime64ns
from utilities.numpy import datetime64Y


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
        [
            param(0, NDim0),
            param(1, NDim1),
            param(2, NDim2),
            param(3, NDim3),
        ],
    )
    def test_ndims(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])
