from __future__ import annotations

from typing import Annotated
from typing import Any

from beartype.door import die_if_unbearable
from numpy import empty
from numpy import zeros
from numpy.typing import NDArray
from pytest import mark
from pytest import param

from utilities.beartype import NDim0
from utilities.beartype import NDim1
from utilities.beartype import NDim2
from utilities.beartype import NDim3


class TestNDims:
    @mark.parametrize(
        ("ndim", "hint"),
        [param(0, NDim0), param(1, NDim1), param(2, NDim2), param(3, NDim3)],
    )
    def test_main(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])
