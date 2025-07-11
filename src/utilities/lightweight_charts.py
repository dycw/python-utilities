from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from utilities.atomicwrites import writer  # pragma: no cover
from utilities.contextlib import enhanced_async_context_manager
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.reprlib import get_repr

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lightweight_charts import AbstractChart, Chart
    from lightweight_charts.abstract import SeriesCommon
    from polars import DataFrame
    from polars._typing import SchemaDict

    from utilities.types import PathLike


##


def save_chart(chart: Chart, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Atomically save a chart to disk."""
    chart.show(block=False)  # pragma: no cover
    with writer(path, overwrite=overwrite) as temp:  # pragma: no cover
        _ = temp.write_bytes(chart.screenshot())
    chart.exit()  # pragma: no cover


##


def set_dataframe(df: DataFrame, obj: AbstractChart | SeriesCommon, /) -> None:
    """Set a `polars` DataFrame onto a Chart."""
    from polars import Date, Datetime, col  # pragma: no cover

    try:
        name = one(k for k, v in df.schema.items() if isinstance(v, Date | Datetime))
    except OneEmptyError:
        raise _SetDataFrameEmptyError(schema=df.schema) from None
    except OneNonUniqueError as error:
        raise _SetDataFrameNonUniqueError(
            schema=df.schema, first=error.first, second=error.second
        ) from None
    return obj.set(
        df.select(
            col(name).alias("date").dt.strftime("iso"),
            *[c for c in df.columns if c != name],
        ).to_pandas()
    )


@dataclass(kw_only=True, slots=True)
class SetDataFrameError(Exception):
    schema: SchemaDict


@dataclass(kw_only=True, slots=True)
class _SetDataFrameEmptyError(SetDataFrameError):
    @override
    def __str__(self) -> str:
        return "At least 1 column must be of date/datetime type; got 0"


@dataclass(kw_only=True, slots=True)
class _SetDataFrameNonUniqueError(SetDataFrameError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"{get_repr(self.schema)} must contain exactly 1 date/datetime column; got {self.first!r}, {self.second!r} and perhaps more"


##


@enhanced_async_context_manager
async def yield_chart(chart: Chart, /) -> AsyncIterator[None]:
    """Yield a chart for visualization in a notebook."""
    try:  # pragma: no cover
        yield await chart.show_async()
    except BaseException:  # pragma: no cover  # noqa: BLE001, S110
        pass
    finally:  # pragma: no cover
        chart.exit()


__all__ = ["save_chart", "set_dataframe", "yield_chart"]
