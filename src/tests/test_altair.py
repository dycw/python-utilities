from __future__ import annotations

import datetime as dt
from math import inf
from typing import TYPE_CHECKING, Literal

import polars as pl
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dates,
    floats,
    integers,
    just,
    lists,
    none,
    sampled_from,
    tuples,
)
from polars import DataFrame, Date, Float64, datetime_range, int_range
from pytest import fixture

from utilities.altair import (
    plot_dataframes,
    plot_intraday_dataframe,
    save_chart,
    save_charts_as_pdf,
    vconcat_charts,
)
from utilities.datetime import get_now
from utilities.hypothesis import zoned_datetimes
from utilities.polars import DatetimeUTC, zoned_datetime
from utilities.types import ensure_class
from utilities.zoneinfo import UTC, HongKong, Tokyo

if TYPE_CHECKING:
    from pathlib import Path
    from zoneinfo import ZoneInfo


@fixture
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


class TestPlotDataFrames:
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

    @given(time_zone=sampled_from([UTC, HongKong, Tokyo]))
    def test_auto_localization(self, *, time_zone: ZoneInfo) -> None:
        df = DataFrame(
            data=[(dt.datetime(2000, 1, 1, 12, tzinfo=time_zone), 0.0)],
            schema={"datetime": zoned_datetime(time_zone=time_zone), "value": Float64},
            orient="row",
        )
        chart = plot_dataframes(df, x="datetime", y="value")
        datetime = ensure_class(chart.data, DataFrame)["datetime"].item()
        expected = dt.datetime(2000, 1, 1, 12, tzinfo=time_zone).replace(tzinfo=None)
        assert datetime == expected

    @given(data=data(), case=sampled_from(["date", "datetime"]))
    def test_tooltip_format(
        self, *, data: DataObject, case: Literal["date", "datetime"]
    ) -> None:
        match case:
            case "date":
                values = data.draw(lists(tuples(dates(), floats(-10, 10))))
                dtype = Date()
            case "datetime":
                values = data.draw(lists(tuples(zoned_datetimes(), floats(-10, 10))))
                dtype = DatetimeUTC
        df = DataFrame(
            data=values, schema={"index": dtype, "value": Float64}, orient="row"
        )
        _ = plot_dataframes(df, x="index", y="value")


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
            schema={"datetime": DatetimeUTC, "value": Float64},
            orient="row",
        )
        _ = plot_intraday_dataframe(data)


class TestSaveChart:
    def test_main(self, *, time_series: DataFrame, tmp_path: Path) -> None:
        chart = plot_dataframes(time_series)
        save_chart(chart, tmp_path.joinpath("chart.png"))


class TestSaveChartsAsPdf:
    def test_main(self, *, time_series: DataFrame, tmp_path: Path) -> None:
        chart = plot_dataframes(time_series)
        save_charts_as_pdf(chart, path=tmp_path.joinpath("chart.pdf"))


class TestVConcatCharts:
    @given(width=integers(1, 100))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(self, *, time_series: DataFrame, width: int) -> None:
        chart1 = plot_intraday_dataframe(time_series, interactive=False)
        chart2 = plot_intraday_dataframe(time_series, interactive=False)
        _ = vconcat_charts(chart1, chart2, width=width)
