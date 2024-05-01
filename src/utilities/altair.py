from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import polars as pl
from altair import X2, Chart, Color, X, Y, condition, layer, selection_point, value
from polars import col, int_range

if TYPE_CHECKING:
    from altair import LayerChart
    from polars import DataFrame


def plot_intraday_dataframe(
    data: DataFrame,
    /,
    *,
    datetime: str = "datetime",
    value_name: str = "value",
    bind_y: bool = False,
) -> LayerChart:
    """Plot an intraday DataFrame."""
    other_cols = [c for c in data.columns if c != datetime]
    data2 = data.with_columns(
        int_range(end=pl.len()).alias(f"_{datetime}_index"),
        _date=col(datetime).dt.date(),
    )
    dates = (
        data2.select("_date").unique().with_columns(_date_index=int_range(end=pl.len()))
    )
    data3 = data2.join(dates, on=["_date"])
    melted = data3.select(
        col(f"_{datetime}_index").alias(f"{datetime} index"), *other_cols
    ).melt(id_vars=f"{datetime} index", value_name=value_name)
    lines = (
        Chart(melted)
        .mark_line()
        .encode(
            x=X(f"{datetime} index").scale(domain=(0, data3.height), nice=False),
            y=Y(value_name).scale(zero=False),
            color=Color("variable").legend(
                direction="horizontal", offset=10, orient="top-right", title=None
            ),
        )
    )
    nearest = selection_point(
        nearest=True, on="pointerover", fields=[f"{datetime} index"], empty=False
    )
    hover_line = (
        Chart(melted)
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
    return layer(lines, hover_line, text, span).interactive(bind_y=bind_y)


__all__ = ["plot_intraday_dataframe"]
