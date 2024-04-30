from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from altair import Chart, Color, X, Y
from polars import col, int_range

if TYPE_CHECKING:
    from altair import LayerChart
    from polars import DataFrame


def plot_intraday_dataframe(
    data: DataFrame,
    /,
    *,
    datetime: str = "datetime",
    variable_name: str = "variable",  # unneeded
    value_name: str = "value",
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
    ).melt(
        id_vars=f"{datetime} index", variable_name=variable_name, value_name=value_name
    )
    lines = (
        Chart(melted)
        .mark_line()
        .encode(
            x=X(f"{datetime} index").scale(domain=(0, data3.height), nice=False),
            y=Y(value_name).scale(zero=False),
            color=Color(variable_name).legend(
                direction="horizontal", offset=10, orient="top-right", title=None
            ),
        )
    )

    _ = lines
    return lines
