from typing import Any

from beartype.door import die_if_unbearable
from pandas import Index, Series
from pytest import mark, param

from utilities.numpy import datetime64ns
from utilities.pandas import (
    Int64,
    boolean,
    category,
    datetime64nshk,
    datetime64nsutc,
    string,
)
from utilities.pandas.typing import (
    IndexB,
    IndexBn,
    IndexC,
    IndexD,
    IndexDhk,
    IndexDutc,
    IndexF,
    IndexI,
    IndexI64,
    IndexO,
    IndexS,
    SeriesB,
    SeriesBn,
    SeriesC,
    SeriesD,
    SeriesDhk,
    SeriesDutc,
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
            param(category, IndexC),
            param(datetime64ns, IndexD),
            param(datetime64nshk, IndexDhk),
            param(datetime64nsutc, IndexDutc),
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
            param(category, SeriesC),
            param(datetime64ns, SeriesD),
            param(datetime64nshk, SeriesDhk),
            param(datetime64nsutc, SeriesDutc),
            param(float, SeriesF),
            param(int, SeriesI),
            param(object, SeriesO),
            param(string, SeriesS),
        ],
    )
    def test_series(self, dtype: Any, hint: Any) -> None:
        series = Series([], dtype=dtype)
        die_if_unbearable(series, hint)
