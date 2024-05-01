from __future__ import annotations

import datetime as dt

import polars as pl
from polars import datetime_range, int_range

from utilities.altair import plot_intraday_dataframe2
from utilities.datetime import UTC


class TestPlotIntradayDataFrame:
    def test_main(self) -> None:
        data = (
            datetime_range(
                dt.datetime(2024, 1, 1, tzinfo=UTC),
                dt.datetime(2024, 1, 8, 23, tzinfo=UTC),
                interval="1h",
                eager=True,
            )
            .rename("datetime")
            .to_frame()
            .with_columns(
                x=int_range(end=pl.len()), y=int_range(end=2 * pl.len(), step=2)
            )
        )
        _ = plot_intraday_dataframe2(data)
