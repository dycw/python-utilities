from typing import Any

from beartype.door import die_if_unbearable
from pandas import Index, Series
from pytest import mark, param

from utilities.numpy import datetime64ns
from utilities.pandas import Int64, boolean, string
from utilities.pandas.typing import (
    IndexB,
    IndexBn,
    IndexDns,
    IndexF,
    IndexI,
    IndexI64,
    IndexO,
    IndexS,
    SeriesB,
    SeriesBn,
    SeriesDns,
    SeriesF,
    SeriesI,
    SeriesI64,
    SeriesO,
    SeriesS,
)


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(Int64, IndexI64),
            param(bool, IndexB),
            param(boolean, IndexBn),
            param(datetime64ns, IndexDns),
            param(float, IndexF),
            param(int, IndexI),
            param(object, IndexO),
            param(string, IndexS),
        ],
    )
    def test_index(self, dtype: Any, hint: Any) -> None:
        index = Index([], dtype=dtype)
        die_if_unbearable(index, hint)

    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(Int64, SeriesI64),
            param(bool, SeriesB),
            param(boolean, SeriesBn),
            param(datetime64ns, SeriesDns),
            param(float, SeriesF),
            param(int, SeriesI),
            param(object, SeriesO),
            param(string, SeriesS),
        ],
    )
    def test_series(self, dtype: Any, hint: Any) -> None:
        series = Series([], dtype=dtype)
        die_if_unbearable(series, hint)
