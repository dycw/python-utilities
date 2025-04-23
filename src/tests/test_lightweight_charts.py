from __future__ import annotations

import datetime as dt
from typing import cast

from lightweight_charts import Chart
from polars import DataFrame, Date, Float64, Int64, col
from pytest import fixture, raises

from utilities.lightweight_charts import (
    _SetDataFrameEmptyError,
    _SetDataFrameNonUniqueError,
    set_dataframe,
)


@fixture
def df() -> DataFrame:
    data = [
        (dt.date(2000, 1, 1), 5824.0, 5824.75, 5823.75, 5824.25, 124),
        (dt.date(2000, 1, 2), 5821.0, 5821.5, 5820.75, 5820.75, 146),
        (dt.date(2000, 1, 3), 5820.0, 5821.0, 5819.75, 5820.5, 128),
        (dt.date(2000, 1, 4), 5822.5, 5822.5, 5822.25, 5822.25, 78),
        (dt.date(2000, 1, 5), 5822.5, 5822.5, 5821.5, 5821.75, 73),
        (dt.date(2000, 1, 6), 5817.0, 5817.0, 5816.0, 5816.5, 301),
        (dt.date(2000, 1, 7), 5817.75, 5818.75, 5817.5, 5818.75, 150),
        (dt.date(2000, 1, 8), 5821.0, 5821.25, 5821.0, 5821.25, 75),
        (dt.date(2000, 1, 9), 5818.0, 5819.0, 5818.0, 5818.75, 69),
        (dt.date(2000, 1, 10), 5818.75, 5819.25, 5818.5, 5819.0, 67),
    ]
    return DataFrame(
        data=data,
        schema={
            "date": Date,
            "open": Float64,
            "high": Float64,
            "low": Float64,
            "close": Float64,
            "volume": Int64,
        },
        orient="row",
    )


class TestSetDataFrame:
    def test_main(self, *, df: DataFrame) -> None:
        chart = Chart()
        set_dataframe(df, chart)

    def test_error_empty(self, *, df: DataFrame) -> None:
        df = df.drop("date")
        with raises(
            _SetDataFrameEmptyError,
            match="At least 1 column must be of date/datetime type; got 0",
        ):
            set_dataframe(df, cast("Chart", None))

    def test_error_non_unique(self, *, df: DataFrame) -> None:
        df = df.with_columns(col("date").alias("date2"))
        with raises(
            _SetDataFrameNonUniqueError,
            match=r"Schema\(.*\) must contain exactly 1 date/datetime column; got 'date', 'date2' and perhaps more",
        ):
            set_dataframe(df, cast("Chart", None))
