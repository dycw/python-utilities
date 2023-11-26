from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import cast

from bs4 import Tag
from polars import DataFrame

from utilities.text import ensure_str


def table_tag_to_dataframe(table: Tag, /) -> DataFrame:
    """Convert a `table` tag into a DataFrame."""

    def get_text(tag: Tag, child: str, /) -> list[str]:
        children = cast(Iterable[Tag], tag.find_all(child))
        return [ensure_str(x.string) for x in children]

    th_rows: list[str] | None = None
    td_rows: list[list[str]] = []
    for tr in cast(Iterable[Tag], table.find_all("tr")):
        if len(th := get_text(tr, "th")) >= 1:
            if th_rows is None:
                th_rows = th
            else:
                msg = f"{table=}"
                raise TableTagToDataFrameError(msg)
        if len(td := get_text(tr, "td")) >= 1:
            td_rows.append(td)
    cols = list(zip(*td_rows, strict=True))
    df = DataFrame(cols)
    if th_rows is None:
        return df
    return df.rename({f"column_{i}": th for i, th in enumerate(th_rows)})


class TableTagToDataFrameError(Exception):
    ...


def yield_tables(tag: Tag, /) -> Iterator[DataFrame]:
    return map(table_tag_to_dataframe, tag.find_all("table"))


__all__ = ["TableTagToDataFrameError", "table_tag_to_dataframe", "yield_tables"]
