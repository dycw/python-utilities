from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import cast

from bs4 import Tag
from polars import DataFrame
from typing_extensions import override

from utilities.iterables import OneEmptyError, OneNonUniqueError, one, transpose
from utilities.text import ensure_str


def table_tag_to_dataframe(table: Tag, /) -> DataFrame:
    """Convert a `table` tag into a DataFrame."""

    def get_text(tag: Tag, child: str, /) -> list[str] | None:
        children = cast(Iterable[Tag], tag.find_all(child))
        results = [ensure_str(x.string) for x in children]
        return results if len(results) >= 1 else None

    def yield_th_and_td_rows() -> Iterator[tuple[list[str] | None, list[str] | None]]:
        for tr in cast(Iterable[Tag], table.find_all("tr")):
            yield get_text(tr, "th"), get_text(tr, "td")

    ths, tds = transpose(yield_th_and_td_rows())
    try:
        th = one(th for th in ths if th is not None)
    except OneEmptyError:
        th = None
    except OneNonUniqueError as error:
        error = cast(OneNonUniqueError[list[str]], error)
        raise TableTagToDataFrameError(
            table=table, first=error.first, second=error.second
        ) from None
    tds_use = (td for td in tds if td is not None)
    cols = list(zip(*tds_use, strict=True))
    df = DataFrame(cols)
    if th is None:
        return df
    return df.rename({f"column_{i}": th for i, th in enumerate(th)})


@dataclass(kw_only=True)
class TableTagToDataFrameError(Exception):
    table: Tag
    first: list[str]
    second: list[str]

    @override
    def __str__(self) -> str:
        return f"Table {self.table} must contain exactly one `th` tag; got {self.first}, {self.second} and perhaps more."


def yield_tables(tag: Tag, /) -> Iterator[DataFrame]:
    return map(table_tag_to_dataframe, tag.find_all("table"))


__all__ = ["TableTagToDataFrameError", "table_tag_to_dataframe", "yield_tables"]
