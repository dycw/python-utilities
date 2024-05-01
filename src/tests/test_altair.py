from __future__ import annotations

import datetime as dt
from math import inf

import polars as pl
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import booleans, integers, none
from polars import DataFrame, Datetime, Float64, datetime_range, int_range
from pytest import fixture

from utilities.altair import plot_intraday_dataframe, vconcat_charts
from utilities.datetime import UTC, get_now


@fixture()
def time_series() -> DataFrame:
    return (
        datetime_range(
            dt.datetime(2024, 1, 1, tzinfo=UTC),
            dt.datetime(2024, 1, 7, 23, tzinfo=UTC),
            interval="1h",
            eager=True,
        )
        .rename("datetime")
        .to_frame()
        .with_columns(x=int_range(end=pl.len()), y=int_range(end=2 * pl.len(), step=2))
    )


class TestPlotIntradayDataFrame:
    @given(interactive=booleans(), width=integers(1, 100) | none())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(
        self, *, time_series: DataFrame, interactive: bool, width: int | None
    ) -> None:
        _ = plot_intraday_dataframe(time_series, interactive=interactive, width=width)

    def test_non_finite(self) -> None:
        data = DataFrame(
            [(get_now(), inf)],
            schema={"datetime": Datetime(time_zone=UTC), "value": Float64},
        )
        _ = plot_intraday_dataframe(data)


class TestVConcatCharts:
    @given(width=integers(1, 100) | none())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(self, *, time_series: DataFrame, width: int | None) -> None:
        chart1 = plot_intraday_dataframe(time_series, interactive=False)
        chart2 = plot_intraday_dataframe(time_series, interactive=False)
        _ = vconcat_charts(chart1, chart2, width=width)
