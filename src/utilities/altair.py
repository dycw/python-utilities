from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING, Any, cast

from altair import (
    X2,
    Chart,
    Color,
    HConcatChart,
    LayerChart,
    Tooltip,
    VConcatChart,
    X,
    Y,
    condition,
    layer,
    selection_interval,
    selection_point,
    value,
    vconcat,
)
from altair.utils.schemapi import Undefined

from utilities.iterables import always_iterable
from utilities.tempfile import TemporaryDirectory
from utilities.text import ensure_bytes
from utilities.types import PathLike, ensure_number

if TYPE_CHECKING:
    from collections.abc import Sequence

    from polars import DataFrame

_ChartLike = Chart | HConcatChart | LayerChart | VConcatChart
_HEIGHT = 400
_WIDTH = 800


@dataclass(frozen=True, kw_only=True)
class _PlotDataFramesSpec:
    y: Sequence[str]
    height: int = _HEIGHT


def plot_dataframes(
    data: DataFrame,
    /,
    *,
    x: str | None = None,
    y: str
    | tuple[str, int]
    | tuple[list[str], int]
    | list[str | list[str] | tuple[str, int] | tuple[list[str], int]]
    | None = None,
    var_name: str = "variable",
    value_name: str = "value",
    height: int = _HEIGHT,
    width: int = _WIDTH,
    interpolate: Any = Undefined,
) -> VConcatChart:
    """Plot a DataFrame as a set of time series, with a multi-line tooltip."""
    import polars as pl
    from polars import int_range

    if x is None:
        data = data.with_columns(_index=int_range(end=pl.len()))
        x_use = "_index"
    else:
        x_use = x
    if y is None:
        columns = [col for col in data.columns if col != x_use]
        specs = [_plot_dataframes_get_spec(columns, height=height)]
    elif isinstance(y, str | tuple):
        specs = [_plot_dataframes_get_spec(y, height=height)]
    else:
        specs = [_plot_dataframes_get_spec(y_i, height=height) for y_i in y]

    # charts
    dfs_long = [
        data.select(x_use, *spec.y).unpivot(
            on=spec.y, index=x_use, variable_name=var_name, value_name=value_name
        )
        for spec in specs
    ]
    charts = [Chart(df_long) for df_long in dfs_long]

    # lines
    selection = selection_point(bind="legend", fields=[var_name], nearest=False)
    lines = [
        chart.mark_line(interpolate=interpolate)
        .encode(
            x=x_use,
            y=Y(value_name).scale(zero=False),
            color=f"{var_name}:N",
            opacity=cast(Any, condition(selection, value(1.0), value(0.01))),
        )
        .add_params(selection)
        for chart in charts
    ]

    # points
    nearest = selection_point(
        nearest=True, on="pointerover", fields=[x_use], empty=False
    )
    points = [
        line.mark_point().encode(
            opacity=cast(Any, condition(nearest, value(1), value(0)))
        )
        for line in lines
    ]

    # rules
    rules = [
        chart.transform_pivot(var_name, value=value_name, groupby=[x_use])
        .mark_rule(color="gray")
        .encode(
            x=x_use,
            opacity=cast(Any, condition(nearest, value(0.3), value(0))),
            tooltip=[Tooltip(x_use, title=x_use)]
            + [Tooltip(c, type="quantitative") for c in spec.y],
        )
        .add_params(nearest)
        for chart, spec in zip(charts, specs, strict=True)
    ]

    # together
    layers = [
        layer(line, pts, rls).properties(width=width, height=spec.height)
        for line, pts, rls, spec in zip(lines, points, rules, specs, strict=True)
    ]
    zoom = selection_interval(bind="scales", encodings=["x"])
    return (
        vconcat(*layers).add_params(zoom).resolve_scale(color="independent", x="shared")
    )


def _plot_dataframes_get_spec(
    y: str | list[str] | tuple[str, int] | tuple[list[str], int],
    /,
    *,
    height: int = _HEIGHT,
) -> _PlotDataFramesSpec:
    if isinstance(y, str | list):
        return _PlotDataFramesSpec(y=list(always_iterable(y)), height=height)
    y0, height_use = y
    return _PlotDataFramesSpec(y=list(always_iterable(y0)), height=height_use)


def plot_intraday_dataframe(
    data: DataFrame,
    /,
    *,
    datetime: str = "datetime",
    value_name: str = "value",
    interactive: bool = True,
    bind_y: bool = False,
    width: int = _WIDTH,
) -> LayerChart:
    """Plot an intraday DataFrame."""
    import polars as pl
    from polars import col, int_range

    other_cols = [c for c in data.columns if c != datetime]
    data2 = data.sort(datetime).with_columns(
        int_range(end=pl.len()).alias(f"_{datetime}_index"),
        _date=col(datetime).dt.date(),
    )
    dates = (
        data2.select("_date").unique().with_columns(_date_index=int_range(end=pl.len()))
    )
    data3 = data2.join(dates, on=["_date"])
    unpivoted = data3.select(
        col(f"_{datetime}_index").alias(f"{datetime} index"), *other_cols
    ).unpivot(index=f"{datetime} index", value_name=value_name)

    y = Y(value_name).scale(zero=False)
    unpivoted_value = unpivoted[value_name]
    value_min, value_max = map(
        ensure_number, [unpivoted_value.min(), unpivoted_value.max()]
    )
    if isfinite(value_min) and isfinite(value_max):
        y = y.scale(domain=(value_min, value_max))
    lines = (
        Chart(unpivoted)
        .mark_line()
        .encode(
            x=X(f"{datetime} index").scale(domain=(0, data3.height), nice=False),
            y=y,
            color=Color("variable").legend(
                direction="horizontal", offset=10, orient="top-right", title=None
            ),
        )
    )
    nearest = selection_point(
        nearest=True, on="pointerover", fields=[f"{datetime} index"], empty=False
    )
    hover_line = (
        Chart(unpivoted)
        .transform_pivot("variable", value=value_name, groupby=[f"{datetime} index"])
        .mark_rule(color="black")
        .encode(
            x=f"{datetime} index",
            opacity=cast(Any, condition(nearest, value(1.0), value(0.0))),
            tooltip=[f"{datetime} index:Q"] + [f"{c}:Q" for c in other_cols],
        )
        .add_params(nearest)
    )

    data4 = (
        data3.group_by("_date_index")
        .agg(
            col(f"_{datetime}_index").min().alias(f"{datetime}_index_min"),
            (col(f"_{datetime}_index").max() + 1).alias(f"{datetime}_index_max"),
            col("_date").first().dt.strftime("%Y-%m-%d").alias("date_label"),
        )
        .sort("_date_index")
    )
    text = (
        Chart(data4)
        .mark_text(align="left", fontSize=9)
        .encode(x=X("datetime_index_min", title=None), y=value(5), text="date_label")
    )
    span = (
        Chart(data4)
        .mark_rect(opacity=0.1)
        .encode(
            x=X(f"{datetime}_index_min:Q", title=None),
            x2=X2(f"{datetime}_index_max:Q", title=None),
            y=value(0),
            color=Color("date_index:Q", legend=None).scale(scheme="category10"),
        )
    )

    chart = layer(lines, hover_line, text, span).properties(width=width)
    if interactive:
        chart = chart.interactive(bind_y=bind_y)
    return chart


def save_chart(
    chart: _ChartLike, path: PathLike, /, *, overwrite: bool = False
) -> None:
    """Atomically save a chart to disk."""
    from utilities.atomicwrites import writer

    with writer(path, overwrite=overwrite) as temp:
        chart.save(str(temp), format="png")


def save_charts_as_pdf(
    *charts: _ChartLike, path: PathLike, overwrite: bool = False
) -> None:
    """Atomically save a chart/set of charts to disk."""
    from img2pdf import convert

    from utilities.atomicwrites import writer

    with TemporaryDirectory() as temp_dir:
        temp_paths = [temp_dir.joinpath(f"{i}.png") for i in range(len(charts))]
        for chart, temp_path in zip(charts, temp_paths, strict=True):
            save_chart(chart, temp_path)
        data = ensure_bytes(convert(*temp_paths))
    with writer(path, overwrite=overwrite) as temp, temp.open(mode="wb") as fh:
        _ = fh.write(data)


def vconcat_charts(*charts: _ChartLike, width: int = _WIDTH) -> VConcatChart:
    """Vertically concatenate a set of charts."""
    charts_use = (c.properties(width=width) for c in charts)
    resize = selection_interval(bind="scales", encodings=["x"])
    charts_use = (c.add_params(resize) for c in charts_use)
    return vconcat(*charts_use).resolve_scale(color="independent", x="shared")


__all__ = [
    "plot_dataframes",
    "plot_intraday_dataframe",
    "save_chart",
    "save_charts_as_pdf",
    "vconcat_charts",
]
