from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

from pandas import (
    BooleanDtype,
    CategoricalDtype,
    DatetimeTZDtype,
    Index,
    Int64Dtype,
    Series,
    StringDtype,
)

if TYPE_CHECKING:  # pragma: no cover
    import datetime as dt

    IndexA: TypeAlias = Index[Any]  # pyright: ignore[reportInvalidTypeArguments]
    IndexB: TypeAlias = Index[bool]  # pyright: ignore[reportInvalidTypeArguments]
    IndexBn: TypeAlias = Index[BooleanDtype]  # pyright: ignore[reportInvalidTypeArguments]
    IndexC: TypeAlias = Index[CategoricalDtype]  # pyright: ignore[reportInvalidTypeArguments]
    IndexD: TypeAlias = Index[dt.datetime]  # pyright: ignore[reportInvalidTypeArguments]
    IndexDhk: TypeAlias = Index[DatetimeTZDtype]  # pyright: ignore[reportInvalidTypeArguments]
    IndexDutc: TypeAlias = Index[DatetimeTZDtype]  # pyright: ignore[reportInvalidTypeArguments]
    IndexF: TypeAlias = Index[float]  # pyright: ignore[reportInvalidTypeArguments]
    IndexI: TypeAlias = Index[int]  # pyright: ignore[reportInvalidTypeArguments]
    IndexI64: TypeAlias = Index[Int64Dtype]  # pyright: ignore[reportInvalidTypeArguments]
    IndexS: TypeAlias = Index[str]  # pyright: ignore[reportInvalidTypeArguments]
    IndexSt: TypeAlias = Index[StringDtype]  # pyright: ignore[reportInvalidTypeArguments]

    SeriesA: TypeAlias = Series[Any]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesB: TypeAlias = Series[bool]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesBn: TypeAlias = Series[BooleanDtype]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesC: TypeAlias = Series[CategoricalDtype]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesD: TypeAlias = Series[dt.datetime]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesDhk: TypeAlias = Series[DatetimeTZDtype]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesDutc: TypeAlias = Series[DatetimeTZDtype]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesF: TypeAlias = Series[float]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesI: TypeAlias = Series[int]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesI64: TypeAlias = Series[Int64Dtype]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesS: TypeAlias = Series[str]  # pyright: ignore[reportInvalidTypeArguments]
    SeriesSt: TypeAlias = Series[StringDtype]  # pyright: ignore[reportInvalidTypeArguments]
else:
    IndexA = IndexB = IndexBn = IndexC = IndexD = IndexDhk = IndexDutc = IndexF = (
        IndexI
    ) = IndexI64 = IndexS = IndexSt = Index
    SeriesA = SeriesB = SeriesBn = SeriesC = SeriesD = SeriesDhk = SeriesDutc = (
        SeriesF
    ) = SeriesI = SeriesI64 = SeriesS = SeriesSt = Series


__all__ = [
    "IndexA",
    "IndexB",
    "IndexBn",
    "IndexC",
    "IndexD",
    "IndexDhk",
    "IndexDutc",
    "IndexF",
    "IndexI",
    "IndexI64",
    "IndexS",
    "IndexSt",
    "SeriesA",
    "SeriesB",
    "SeriesBn",
    "SeriesC",
    "SeriesD",
    "SeriesDhk",
    "SeriesDutc",
    "SeriesF",
    "SeriesI",
    "SeriesI64",
    "SeriesS",
    "SeriesSt",
]
