from typing import Any

from beartype.door import die_if_unbearable
from pandas import Index
from pandas import Series
from pytest import mark
from pytest import param

from utilities.numpy import datetime64ns
from utilities.pandas.typing import IndexB
from utilities.pandas.typing import IndexDns
from utilities.pandas.typing import IndexF
from utilities.pandas.typing import IndexI
from utilities.pandas.typing import IndexO
from utilities.pandas.typing import SeriesB
from utilities.pandas.typing import SeriesDns
from utilities.pandas.typing import SeriesF
from utilities.pandas.typing import SeriesI
from utilities.pandas.typing import SeriesO


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
