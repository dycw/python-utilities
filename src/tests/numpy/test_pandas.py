from __future__ import annotations

from typing import Any

from pandas import DatetimeTZDtype, Series
from pytest import mark, param

from utilities.numpy import has_dtype


class TestHasDtype:
    @mark.parametrize(
        ("dtype", "against", "expected"),
        [
            param("Int64", "Int64", True),
            param("Int64", ("Int64",), True),
            param("Int64", int, False),
            param(DatetimeTZDtype(tz="UTC"), DatetimeTZDtype(tz="UTC"), True),
            param(
                DatetimeTZDtype(tz="UTC"), DatetimeTZDtype(tz="Asia/Hong_Kong"), False
            ),
        ],
    )
    def test_main(self, *, dtype: Any, against: Any, expected: bool) -> None:
        result = has_dtype(Series([], dtype=dtype), against)
        assert result is expected
