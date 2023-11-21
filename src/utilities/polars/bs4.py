from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import cast

from bs4 import Tag
from polars import DataFrame

from utilities.text import ensure_str


def yield_tables(tag: Tag, /) -> Iterator[DataFrame]:
    return map(_table_tag_to_dataframe, tag.find_all("table"))


def _table_tag_to_dataframe(table: Tag, /) -> DataFrame:
    th_rows: list[str] | None = None
    td_rows: list[list[str]] = []
    for tr in cast(Iterable[Tag], table.find_all("tr")):
        if len(th := _get_text(tr, "th")) >= 1:
            if th_rows is None:
                th_rows = th
            else:
                msg = f"{table=}"
                raise MultipleTHRowsError(msg)
        if len(td := _get_text(tr, "td")) >= 1:
            td_rows.append(td)
    cols = list(zip(*td_rows, strict=True))
    df = DataFrame(cols)
    if th_rows is None:
        return df
    return df.rename({f"column_{i}": th for i, th in enumerate(th_rows)})


class MultipleTHRowsError(Exception):
    """Raised when multiple TH rows are found."""


def _get_text(tag: Tag, child: str, /) -> list[str]:
    children = cast(Iterable[Tag], tag.find_all(child))
    return [ensure_str(x.string) for x in children]


__all__ = ["MultipleTHRowsError", "yield_tables"]
