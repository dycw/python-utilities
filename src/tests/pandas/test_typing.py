from typing import Any

from beartype.door import die_if_unbearable
from pandas import Index, Series
from pytest import mark, param

from utilities.numpy import datetime64ns
from utilities.pandas.typing import (
    IndexB,
    IndexDns,
    IndexF,
    IndexI,
    IndexO,
    SeriesB,
    SeriesDns,
    SeriesF,
    SeriesI,
    SeriesO,
)


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, IndexB),
            param(datetime64ns, IndexDns),
            param(float, IndexF),
            param(int, IndexI),
            param(object, IndexO),
        ],
    )
    def test_index(self, dtype: Any, hint: Any) -> None:
        index = Index([], dtype=dtype)
        die_if_unbearable(index, hint)

    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, SeriesB),
            param(datetime64ns, SeriesDns),
            param(float, SeriesF),
            param(int, SeriesI),
            param(object, SeriesO),
        ],
    )
    def test_series(self, dtype: Any, hint: Any) -> None:
        series = Series([], dtype=dtype)
        die_if_unbearable(series, hint)
