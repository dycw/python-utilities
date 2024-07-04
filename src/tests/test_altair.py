from __future__ import annotations

import datetime as dt
from math import inf

import polars as pl
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import booleans, integers, just, none, sampled_from
from polars import DataFrame, Datetime, Float64, datetime_range, int_range
from pytest import fixture

from utilities.altair import plot_dataframes, plot_intraday_dataframe, vconcat_charts
from utilities.datetime import get_now
from utilities.zoneinfo import UTC


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


class TestPlotDataFrame:
    @given(
        x=just("datetime") | none(),
        y=sampled_from([
            "x",
            "y",
            ("x", 50),
            ("y", 50),
            (["x", "y"], 50),
            ["x", "y"],
            [["x"], ["y"]],
            [["x"], ("y", 50)],
        ])
        | none(),
        height=integers(1, 100),
        width=integers(1, 100),
    )
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(
        self,
        *,
        time_series: DataFrame,
        x: str | None,
        y: str
        | tuple[str, int]
        | tuple[list[str], int]
        | list[str | list[str] | tuple[str, int] | tuple[list[str], int]]
        | None,
        height: int,
        width: int,
    ) -> None:
        _ = plot_dataframes(time_series, x=x, y=y, height=height, width=width)


class TestPlotIntradayDataFrame:
    @given(interactive=booleans(), width=integers(1, 100))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(
        self, *, time_series: DataFrame, interactive: bool, width: int
    ) -> None:
        _ = plot_intraday_dataframe(time_series, interactive=interactive, width=width)

    def test_non_finite(self) -> None:
        data = DataFrame(
            data=[(get_now(), inf)],
            schema={"datetime": Datetime(time_zone="UTC"), "value": Float64},
            orient="row",
        )
        _ = plot_intraday_dataframe(data)


class TestVConcatCharts:
    @given(width=integers(1, 100))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(self, *, time_series: DataFrame, width: int) -> None:
        chart1 = plot_intraday_dataframe(time_series, interactive=False)
        chart2 = plot_intraday_dataframe(time_series, interactive=False)
        _ = vconcat_charts(chart1, chart2, width=width)
