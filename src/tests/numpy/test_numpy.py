from typing import Any, Optional

from numpy import array, nan
from numpy.testing import assert_allclose
from pandas import DatetimeTZDtype, Series
from pytest import mark, param

from utilities.numpy import ffill, has_dtype
from utilities.numpy.typing import NDArrayF1


class TestHasDtype:
    @mark.parametrize(
        ("x", "dtype", "expected"),
        [
            param(array([]), float, True),
            param(array([]), (float,), True),
            param(array([]), int, False),
            param(array([]), (int,), False),
            param(array([]), "Int64", False),
            param(array([]), ("Int64",), False),
            param(Series([], dtype="Int64"), "Int64", True),
            param(Series([], dtype="Int64"), int, False),
            param(
                Series([], dtype=DatetimeTZDtype(tz="UTC")),
                DatetimeTZDtype(tz="UTC"),
                True,
            ),
            param(
                Series([], dtype=DatetimeTZDtype(tz="UTC")),
                DatetimeTZDtype(tz="Asia/Hong_Kong"),
                False,
            ),
        ],
    )
    def test_main(self, x: Any, dtype: Any, expected: bool) -> None:
        assert has_dtype(x, dtype) is expected


class TestFFill:
    @mark.parametrize(
        ("limit", "expected"),
        [
            param(None, array([0.1, 0.1, 0.2, 0.2, 0.2, 0.3])),
            param(1, array([0.1, 0.1, 0.2, 0.2, nan, 0.3])),
        ],
    )
    def test_main(self, limit: Optional[int], expected: NDArrayF1) -> None:
        arr = array([0.1, nan, 0.2, nan, nan, 0.3])
        result = ffill(arr, limit=limit)
        assert_allclose(result, expected, equal_nan=True)
