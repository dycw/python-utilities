from typing import Any

from pandas import Series
from pytest import mark
from pytest import param

from dycw_utilities.pandas import Int64
from dycw_utilities.pandas import boolean
from dycw_utilities.pandas import string


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
    def test_main(self, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)
